package kafka

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	messageBufferSize = 10
)

type KafkaConsumer struct {
	kafka *kafka.Consumer
}

func InitConsumer(topicName string) (*KafkaConsumer, error) {
	kafka, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group",
	})
	if err != nil {
		return nil, err
	}
	consumer := &KafkaConsumer{kafka: kafka}
	consumer.Subscribe(topicName)
	return consumer, nil
}

func (kc *KafkaConsumer) Subscribe(topic string) error {
	return kc.kafka.Subscribe(topic, nil)
}

func (kc *KafkaConsumer) Consume() (<-chan kafka.Message, <-chan error) {
	messageChan := make(chan kafka.Message, messageBufferSize)
	errorChan := make(chan error)

	go func() {
		run := true
		for run == true {
			ev := kc.kafka.Poll(1000)
			switch e := ev.(type) {
			case *kafka.Message:
				messageChan <- *e
			case kafka.Error:
				errorChan <- e
				run = false
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}()

	return messageChan, errorChan
}

func (kc *KafkaConsumer) Close() {
	kc.kafka.Close()
}
