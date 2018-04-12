//
//  Asynchronous client-to-server (DEALER to ROUTER).
//
//  While this example runs in a single process, that is just to make
//  it easier to start and stop the example. Each task has its own
//  context and conceptually acts as a separate process.

package main

import (
	zmq "github.com/pebbe/zmq3"
        "bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"time"
        "flag"
)

//  ---------------------------------------------------------------------
//  This is our client task
//  It connects to the server, and then sends a request once per second
//  It collects responses as they arrive, and it prints them out. We will
//  run several client tasks in parallel, each with a different random ID.

const NON_BLOCKING = 1
type Message struct {
   OpType string
   Msg_number int
   Data []byte 
}

func client_task(receiverIP string, numMessages int, msgSize int, opType string ) {

	client, _ := zmq.NewSocket(zmq.DEALER)
	defer client.Close()

	//  Set random identity to make tracing easier
	set_id(client)
	client.Connect("tcp://" + receiverIP + ":5555")

        poller := zmq.NewPoller()
        poller.Add(client, zmq.POLLIN)

        request_nbr := 0
        var message Message

        message.OpType=opType
        if message.OpType=="write" {
           message.Data = make([]byte, msgSize)
        }
        if message.OpType=="read" {
           message.Data = make([]byte, 0)
        }

	rand.Read(message.Data)

        var totTime int64 = 0
	for i:=0; i < numMessages; i++ {
		//time.Sleep(time.Second)
		//client.SendMessage(fmt.Sprintf("request #%d", request_nbr))
                start := time.Now()
                message.Msg_number = request_nbr
                message_to_send := CreateGobFromMessage(message)
                _, err := client.SendBytes(message_to_send.Bytes(), NON_BLOCKING)
		if err != nil {
			fmt.Println("Sending message failed ", request_nbr)
		}
		//fmt.Println("Sending ", message.Msg_number, len(message.Data))
		time.Sleep(1 * time.Millisecond)

                sockets, err := poller.Poll(-1) 
                for _, socket:= range sockets {
                   msgBytes, err := socket.Socket.RecvBytes(0)
                   message1 := CreateMessageFromGob(msgBytes)
		   //fmt.Println("Received ", message1.Msg_number, len(message1.Data))
                   if err!=nil {
		      fmt.Println("Error in receiving", len(message1.Data))

                   }
                }
	        elapsed := time.Since(start)
	        //fmt.Println("READ TIME:",  elapsed)
                totTime += int64(elapsed)

		request_nbr += 2
	}
        fmt.Println("Avg Latency " + opType, totTime/int64(1000*numMessages))



}

//  This is our server task.
//  It uses the multithreaded server model to deal requests out to a pool
//  of workers and route replies back to clients. One worker can handle
//  one request at a time but one client can talk to multiple workers at
//  once.

func server_task(msgSize int) {

	//  Frontend socket talks to clients over TCP
	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	frontend.Bind("tcp://*:5555")

	//  Backend socket talks to workers over inproc
	backend, _ := zmq.NewSocket(zmq.DEALER)
	defer backend.Close()
	backend.Bind("inproc://backend")

	//  Launch pool of worker threads, precise number is not critical
	for i := 0; i < 5; i++ {
		go server_worker(msgSize)
	}

	//  Connect backend to frontend via a proxy
	err := zmq.Proxy(frontend, backend, nil)
	log.Fatalln("Proxy interrupted:", err)
}

//  Each worker task works on one request at a time and sends a random number
//  of replies back, with random delays between replies:

func server_worker(msgSize int) {

	worker, _ := zmq.NewSocket(zmq.DEALER)
	defer worker.Close()
	worker.Connect("inproc://backend")
        msg_reply := make([][]byte, 2)
        for i := 0; i < len(msg_reply); i++ {
		msg_reply[i] = make([]byte, 0) // the frist frame  specifies the identity of the sender, the second specifies the content
	}

        read_data := make([]byte, msgSize)
	rand.Read(read_data)

	for {
                msg, err := worker.RecvMessageBytes(0)
		if err != nil {
	            fmt.Println("Somthing Wrong", err)
		}
		message := CreateMessageFromGob(msg[1])

                //sender
                msg_reply[0] = msg[0]

                //prepare response
                message.Msg_number++
                if message.OpType=="read" {
                   message.Data = read_data
                }
                if message.OpType=="write" {
                   message.Data = make([]byte, 0)
                }

		bytes_buffer_temp := CreateGobFromMessage(message)
		msg_reply[1] = bytes_buffer_temp.Bytes()

		worker.SendMessage(msg_reply)
	}
}

//  The main thread simply starts several clients, and a server, and then
//  waits for the server to finish.

func main() {
	rand.Seed(time.Now().UnixNano())

        procType := flag.String("process_type", "both", "process type sender/receiver")
        receiverIP:= flag.String("receiver_ip", "127.0.0.1", "recv process IP/port:12000")
        numMessages:= flag.Int("num_messages", 1000, "number of messages")
        msgSize:= flag.Int("size_message", 1000, "size of messages")
        flag.Parse()

        if *procType=="both" {
	   go server_task(*msgSize)
	   client_task(*receiverIP, *numMessages, *msgSize,"read")
	   client_task(*receiverIP, *numMessages, *msgSize,"write")
        }
        if *procType=="server" {
	   server_task(*msgSize)
        }
        if *procType=="client" {
	   client_task(*receiverIP, *numMessages, *msgSize, "read")
	   client_task(*receiverIP, *numMessages, *msgSize, "write")
        }

	//  Run for 5 seconds then quit
	time.Sleep(1000000 * time.Second)
}

func set_id(soc *zmq.Socket) {
	identity := fmt.Sprintf("%04X-%04X", rand.Intn(0x10000), rand.Intn(0x10000))
	soc.SetIdentity(identity)
}


func CreateGobFromMessage(message Message) bytes.Buffer {

	var message_to_send bytes.Buffer // to send out with zmq
	// Create an encoder and send a Value.
	enc := gob.NewEncoder(&message_to_send)
	err := enc.Encode(message)

	//	err := message_to_respond_enc.Encode(message)
	if err != nil {
		fmt.Println("Error gobfying message")
	}
	return message_to_send
}

func CreateMessageFromGob(messageBytes []byte) Message {

	var buffer bytes.Buffer
	var m Message

	buffer.Write(messageBytes)
	dec := gob.NewDecoder(&buffer)
	dec.Decode(&m)

	return m
}
