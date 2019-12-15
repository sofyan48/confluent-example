package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	fmt.Println("Waiting For Consumer Broker")
	consumer("consumer")
}

func producer(prod *kafka.Producer, topics, word string) {
	go func() {
		for e := range prod.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	// Produce messages to topic (asynchronously)
	topic := topics
	prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(word),
	}, nil)
}

func consumer(topics string) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "sellerG",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topics, "^aRegex.*[Tt]opic"}, nil)
	for {

		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
		fmt.Println(string(msg.Value))
		fmt.Println("Hit Consumer Broker : ", string(msg.Value))
		if string(msg.Value) == "{\"name\": \"zakar\"}" {
			fmt.Println("Sending To Seller Broker")
			prod1, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
			if err != nil {
				fmt.Println("Errors :", err)
			}
			dataConsumen := "{\"seller\": \"PT. INDONESIA MAJU TERUS\", \"data\":" + string(msg.Value) + "}"
			producer(prod1, "seller", dataConsumen)
		}
	}
	c.Close()

}
