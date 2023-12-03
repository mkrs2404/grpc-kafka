package main

import (
	"context"
	"log"

	pb "github.com/mkrs2404/grpc-kafka.git/api/proto"
	"github.com/mkrs2404/grpc-kafka.git/pkg/client"
	"github.com/mkrs2404/grpc-kafka.git/pkg/kafka"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	grpcClient := pb.NewKafkaServiceClient(conn)
	resp, err := grpcClient.TopicName(context.Background(), &pb.KafkaTopicRequest{})
	if err != nil {
		log.Printf("failed to call TopicName: %v", err)
	}

	topicName := resp.GetTopicName()
	log.Printf("TopicName: %s\n", topicName)
	log.Printf("Response : %s\n", resp.String())

	kafkaConsumer, err := kafka.InitConsumer(topicName)
	if err != nil {
		log.Fatalf("failed to init kafka consumer: %v", err)
	}

	client := client.Init(kafkaConsumer)
	client.Consume()
}
