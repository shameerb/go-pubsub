package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/shameerb/go-pubsub/pkg/pubsub"
)

// this code is for testing purpose of the pub sub api.

func main() {
	wg := &sync.WaitGroup{}
	go func() {
		broker := pubsub.NewBroker(":8001")
		if err := broker.Start(); err != nil {
			log.Fatalf("Failed to create a broker %+v", err)
		}
	}()
	consumerFunc := func(wg *sync.WaitGroup) {
		defer wg.Done()
		consumer, err := pubsub.NewConsumer("localhost:8001")
		if err != nil {
			log.Fatalf("Some problem in creating the consumer, %v: %v", consumer.ID, err)
		}
		defer consumer.Close()

		if err := consumer.Subscribe("mytopic"); err != nil {
			log.Fatalf("Issue while subscribing for %v: %v", consumer.ID, err)
		}
		for msg := range consumer.Message {
			fmt.Printf("Received message: %v, %v, %s \n", consumer.ID, msg.Topic, string(msg.Data))
		}
	}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go consumerFunc(wg)
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Publishing")
	wg.Add(1)
	go func() {
		defer wg.Wait()
		publisher, err := pubsub.NewPublisher("localhost:8001")
		if err != nil {
			log.Fatalf("Failed to create publisher: %v", err)
		}
		defer publisher.Close()
		publisher.Publish("mytopic", []byte("Hello"))
	}()

	wg.Wait()
}
