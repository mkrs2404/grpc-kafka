package client

import "github.com/mkrs2404/grpc-kafka.git/pkg/kafka"

type Client struct {
	kafkaClient *kafka.KafkaConsumer
}

func Init(kafka *kafka.KafkaConsumer) *Client {
	return &Client{kafkaClient: kafka}
}

func (c Client) Consume() {
	messageChan, errorChan := c.kafkaClient.Consume()
	for {
		select {
		case message := <-messageChan:
			println(string(message.Value))
		case err := <-errorChan:
			println(err.Error())
			c.kafkaClient.Close()
		}
	}
}
