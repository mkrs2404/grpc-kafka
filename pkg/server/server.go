package server

import (
	"context"
	"log"
	"time"

	pb "github.com/mkrs2404/grpc-kafka.git/api/proto"
	"github.com/mkrs2404/grpc-kafka.git/pkg/kafka"
)

type Server struct {
	pb.UnimplementedKafkaServiceServer
	kafkaClient *kafka.KafkaProducer
}

func Init(kafka *kafka.KafkaProducer) *Server {
	return &Server{kafkaClient: kafka}
}

func (s Server) TopicName(context.Context, *pb.KafkaTopicRequest) (*pb.KafkaTopicResponse, error) {
	return &pb.KafkaTopicResponse{TopicName: kafka.TopicName}, nil
}

func (s Server) Produce() {
	for {
		s.kafkaClient.Produce("test")
		log.Printf("Produced message to topic %s", kafka.TopicName)
		time.Sleep(1 * time.Second)
	}
}
