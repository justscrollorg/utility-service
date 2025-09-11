package main

import (
	"context"
	"testing"

	pb "github.com/justscrollorg/utility-service/proto"
)

func TestSayHello(t *testing.T) {
	// Create a server instance
	s := &server{}

	// Test cases
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Basic greeting", "World", "Hello World"},
		{"Empty name", "", "Hello "},
		{"Custom name", "GitHub Actions", "Hello GitHub Actions"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &pb.HelloRequest{Name: tc.input}
			resp, err := s.SayHello(context.Background(), req)

			if err != nil {
				t.Fatalf("SayHello failed: %v", err)
			}

			if resp.Message != tc.expected {
				t.Errorf("Expected %q, got %q", tc.expected, resp.Message)
			}
		})
	}
}

func TestSayHelloContext(t *testing.T) {
	s := &server{}
	
	// Test with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	
	req := &pb.HelloRequest{Name: "Test"}
	_, err := s.SayHello(ctx, req)
	
	// The function should still work even with cancelled context
	// since it doesn't check context cancellation
	if err != nil {
		t.Errorf("SayHello should handle cancelled context gracefully, got error: %v", err)
	}
}
