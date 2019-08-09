package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"sync"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := "localhost"
	//	port := 31161
	port := 50000
	//uri := "http://localhost:30830"
	//host := "104.47.142.90"
	//port := 50000
	////uri := "http://104.47.142.90:9090"
	doneCh := "done"
	deadCh := "dead"
	sendCount := 1000
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-stream-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer sender.Close()

	receiver1, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver1.Close()

	receiver2, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver2.Close()

	receiver3, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_c"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver3.Close()

	receiver4, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver4.Close()

	receiver5, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver5.Close()

	receiver6, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_c"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver6.Close()

	wg := sync.WaitGroup{}
	wg.Add(7)

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			stream := receiver1.NewStreamQueueMessage().SetChannel("receiverA")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverA")
				return
			}
			//log.Printf("Queue: ReceiverA,MessageID: %s, Body: %s,Seq: %d - send to queue receiverB", msg.Id, string(msg.Body), msg.Attributes.Sequence)
			//time.Sleep(10 * time.Millisecond)
			newMsg := receiver1.NewQueueMessage()
			newMsg.Id = msg.Id
			newMsg.Body = msg.Body
			newMsg.Metadata = msg.Metadata
			newMsg.ClientId = msg.ClientId
			newMsg.Policy.DelaySeconds = msg.Policy.DelaySeconds
			newMsg.Policy.ExpirationSeconds = msg.Policy.ExpirationSeconds
			newMsg.Policy.MaxReceiveCount = msg.Policy.MaxReceiveCount
			newMsg.Policy.MaxReceiveQueue = msg.Policy.MaxReceiveQueue
			newMsg.SetChannel("receiverB")
			res, err := newMsg.Send(ctx)
			if err != nil {
				log.Fatal(err)
			}
			if res.IsError {
				log.Fatal(res.Error)
			}
			err = msg.Ack()
			if res.IsError {
				log.Fatal(err)
			}
		}

	}()

	go func() {
		defer wg.Done()
		for {
			stream := receiver2.NewStreamQueueMessage().SetChannel("receiverB")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverB")
				return
			}
			//log.Printf("Queue: ReceiverB,MessageID: %s, Body: %s - send to new receiverC", msg.Id, string(msg.Body))
			//time.Sleep(10 * time.Millisecond)

			newMsg := receiver2.NewQueueMessage()
			newMsg.Id = msg.Id
			newMsg.Body = msg.Body
			newMsg.Metadata = msg.Metadata
			newMsg.ClientId = msg.ClientId
			newMsg.Policy.DelaySeconds = msg.Policy.DelaySeconds
			newMsg.Policy.ExpirationSeconds = msg.Policy.ExpirationSeconds
			newMsg.Policy.MaxReceiveCount = msg.Policy.MaxReceiveCount
			newMsg.Policy.MaxReceiveQueue = msg.Policy.MaxReceiveQueue
			newMsg.SetChannel("receiverC")
			res, err := newMsg.Send(ctx)
			if err != nil {
				log.Fatal(err)
			}
			if res.IsError {
				log.Fatal(res.Error)
			}
			err = msg.Ack()
			if res.IsError {
				log.Fatal(err)
			}

		}

	}()

	go func() {
		defer wg.Done()
		batch := sender.NewQueueMessages()

		for i := 1; i <= sendCount; i++ {
			messageID := uuid.New().String()
			batch.Add(sender.NewQueueMessage().
				SetId(messageID).
				SetChannel("receiverA").
				SetBody([]byte(fmt.Sprintf("%d", i))).
				SetMetadata(fmt.Sprintf("%d", i)))
		}
		log.Println("sending completed")
		batchRes, err := batch.Send(ctx)
		if err != nil {
			log.Fatal(err)
		}
		for _, res := range batchRes {
			if res.IsError {
				log.Fatal(res.Error)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			stream := receiver3.NewStreamQueueMessage().SetChannel("receiverC")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverC")
				return
			}
			//log.Printf("Queue: ReceiverC,MessageID: %s, Body: %s - send to new receiverD", msg.Id, string(msg.Body))
			//time.Sleep(10 * time.Millisecond)

			newMsg := receiver3.NewQueueMessage()
			newMsg.Id = msg.Id
			newMsg.Body = msg.Body
			newMsg.Metadata = msg.Metadata
			newMsg.ClientId = msg.ClientId
			newMsg.Policy.DelaySeconds = msg.Policy.DelaySeconds
			newMsg.Policy.ExpirationSeconds = msg.Policy.ExpirationSeconds
			newMsg.Policy.MaxReceiveCount = msg.Policy.MaxReceiveCount
			newMsg.Policy.MaxReceiveQueue = msg.Policy.MaxReceiveQueue
			newMsg.SetChannel("receiverD")
			res, err := newMsg.Send(ctx)

			if err != nil {
				log.Fatal(err)
			}
			if res.IsError {
				log.Fatal(res.Error)
			}
			err = msg.Ack()
			if res.IsError {
				log.Fatal(err)
			}
		}

	}()
	go func() {
		defer wg.Done()
		for {
			stream := receiver4.NewStreamQueueMessage().SetChannel("receiverD")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverA")
				return
			}
			//log.Printf("Queue: ReceiverD MessageID: %s, Body: %s - send to queue receiverE", msg.Id, string(msg.Body))
			//time.Sleep(10 * time.Millisecond)
			newMsg := receiver4.NewQueueMessage()
			newMsg.Id = msg.Id
			newMsg.Body = msg.Body
			newMsg.Metadata = msg.Metadata
			newMsg.ClientId = msg.ClientId
			newMsg.Policy.DelaySeconds = msg.Policy.DelaySeconds
			newMsg.Policy.ExpirationSeconds = msg.Policy.ExpirationSeconds
			newMsg.Policy.MaxReceiveCount = msg.Policy.MaxReceiveCount
			newMsg.Policy.MaxReceiveQueue = msg.Policy.MaxReceiveQueue
			newMsg.SetChannel("receiverE")
			res, err := newMsg.Send(ctx)
			if err != nil {
				log.Fatal(err)
			}
			if res.IsError {
				log.Fatal(res.Error)
			}
			err = msg.Ack()
			if res.IsError {
				log.Fatal(err)
			}

		}

	}()

	go func() {
		defer wg.Done()
		for {
			stream := receiver5.NewStreamQueueMessage().SetChannel("receiverE")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverE")
				return
			}
			//log.Printf("Queue: ReceiverE,MessageID: %s, Body: %s - send to new receiverF", msg.Id, string(msg.Body))
			//time.Sleep(10 * time.Millisecond)
			newMsg := receiver5.NewQueueMessage()
			newMsg.Id = msg.Id
			newMsg.Body = msg.Body
			newMsg.Metadata = msg.Metadata
			newMsg.ClientId = msg.ClientId
			newMsg.Policy.DelaySeconds = msg.Policy.DelaySeconds
			newMsg.Policy.ExpirationSeconds = msg.Policy.ExpirationSeconds
			newMsg.Policy.MaxReceiveCount = msg.Policy.MaxReceiveCount
			newMsg.Policy.MaxReceiveQueue = msg.Policy.MaxReceiveQueue
			newMsg.SetChannel("receiverF")
			res, err := newMsg.Send(ctx)
			if err != nil {
				log.Fatal(err)
			}
			if res.IsError {
				log.Fatal(res.Error)
			}
			err = msg.Ack()
			if res.IsError {
				log.Fatal(err)
			}

		}

	}()

	go func() {
		defer wg.Done()

		for {
			stream := receiver6.NewStreamQueueMessage().SetChannel("receiverF")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverF")
				return
			}
			//log.Printf("Queue: ReceiverF,MessageID: %s, Body: %s - send to sender done", msg.Id, string(msg.Body))
			//time.Sleep(10 * time.Millisecond)
			newMsg := receiver6.NewQueueMessage()
			newMsg.Id = msg.Id
			newMsg.Body = msg.Body
			newMsg.Metadata = msg.Metadata
			newMsg.ClientId = msg.ClientId
			newMsg.Policy.DelaySeconds = msg.Policy.DelaySeconds
			newMsg.Policy.ExpirationSeconds = msg.Policy.ExpirationSeconds
			newMsg.Policy.MaxReceiveCount = msg.Policy.MaxReceiveCount
			newMsg.Policy.MaxReceiveQueue = msg.Policy.MaxReceiveQueue
			newMsg.SetChannel(doneCh)
			res, err := newMsg.Send(ctx)

			if err != nil {
				log.Fatal(err)
			}
			if res.IsError {
				log.Fatal(res.Error)
			}
			err = msg.Ack()
			if res.IsError {
				log.Fatal(err)
			}
		}

	}()

	wg.Wait()
	res, err := sender.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		RequestID:           "some_request",
		ClientID:            "sender-client_id",
		Channel:             doneCh,
		MaxNumberOfMessages: int32(sendCount),
		WaitTimeSeconds:     5,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.IsError {
		log.Fatal(res.Error)
	}
	log.Printf("Done Messages - %d: Expried - %d", res.MessagesReceived, res.MessagesExpired)
	for i := 0; i < len(res.Messages); i++ {
		log.Printf("MessageID: %s, Body: %s, Seq: %d", res.Messages[i].Id, res.Messages[i].Body, res.Messages[i].Attributes.Sequence)
	}

	res, err = sender.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		RequestID:           "some_request",
		ClientID:            "sender-client_id",
		Channel:             deadCh,
		MaxNumberOfMessages: int32(sendCount),
		WaitTimeSeconds:     5,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.IsError {
		log.Fatal(res.Error)
	}
	log.Printf("Dead Letter Messages - %d: Expried - %d", res.MessagesReceived, res.MessagesExpired)
	for i := 0; i < len(res.Messages); i++ {
		log.Printf("MessageID: %s, Body: %s", res.Messages[i].Id, res.Messages[i].Body)
	}
}
