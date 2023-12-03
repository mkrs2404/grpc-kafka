package main

import (
	"log"
	"net"

	pb "github.com/mkrs2404/grpc-kafka.git/api/proto"
	"github.com/mkrs2404/grpc-kafka.git/pkg/kafka"
	"github.com/mkrs2404/grpc-kafka.git/pkg/server"
	"google.golang.org/grpc"
)

func main() {
	address := "localhost:9090"
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	kafkaProducer, err := kafka.InitProducer()
	if err != nil {
		log.Fatalf("failed to init kafka producer: %v", err)
	}

	server := server.Init(kafkaProducer)
	grpcServer := grpc.NewServer()

	pb.RegisterKafkaServiceServer(grpcServer, server)
	log.Printf("Starting gRPC listener on %s", address)

	go server.Produce()

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
