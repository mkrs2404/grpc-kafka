package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	// TopicName is the name of the topic
	TopicName = "test-topic"
)

type KafkaProducer struct {
	kafka       *kafka.Producer
	adminClient *kafka.AdminClient
}

func InitProducer() (*KafkaProducer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
	}
	kp, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		return nil, err
	}

	producer := &KafkaProducer{
		kafka:       kp,
		adminClient: adminClient,
	}
	err = producer.CreateTopic()
	return producer, err
}

func (kp *KafkaProducer) Close() {
	kp.kafka.Close()
}

func (kp *KafkaProducer) CreateTopic() error {
	metadata, err := kp.adminClient.GetMetadata(&TopicName, false, 1000)
	fmt.Printf("Metadata for topic %s: %v\n", TopicName, metadata)
	return err
}

func (kp *KafkaProducer) Produce(message string) error {
	byteMessage := []byte(message)
	return kp.kafka.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &TopicName, Partition: kafka.PartitionAny},
			Value:          byteMessage},
		nil,
	)
}
