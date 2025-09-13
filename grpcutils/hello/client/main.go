package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/justscrollorg/utility-service/grpcutils/hello/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultAddress = "localhost:50051"
	defaultName    = "world"
)

func main() {
	// Parse command line flags
	serverAddr := flag.String("server", defaultAddress, "Server address (host:port)")
	flag.Parse()

	// Get name from remaining args or use default
	name := defaultName
	if flag.NArg() > 0 {
		name = flag.Arg(0)
	}

	// Set up a connection to the server
	log.Printf("Connecting to server at %s", *serverAddr)
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	// Contact the server and print out its response
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.GetMessage())
}
