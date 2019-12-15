package main

import (
	"fmt"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	prod, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}
	fmt.Println("Sending To Consumer Broker")
	dataConsumen := "{\"name\": \"zakar\"}"
	producer(prod, "consumer", dataConsumen)
	defer prod.Close()
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
	prod.Flush(15 * 1000)
}
