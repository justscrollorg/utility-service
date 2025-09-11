package main

import (
	"flag"
	"os"
	"testing"
)

func TestFlagParsing(t *testing.T) {
	// Save original command line args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Test default server address
	os.Args = []string{"client"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	
	serverAddr := flag.String("server", defaultAddress, "Server address (host:port)")
	flag.Parse()

	if *serverAddr != defaultAddress {
		t.Errorf("Expected default server address %q, got %q", defaultAddress, *serverAddr)
	}
}

func TestDefaultConstants(t *testing.T) {
	if defaultAddress == "" {
		t.Error("defaultAddress should not be empty")
	}

	if defaultName == "" {
		t.Error("defaultName should not be empty")
	}

	expectedAddress := "localhost:50051"
	if defaultAddress != expectedAddress {
		t.Errorf("Expected defaultAddress to be %q, got %q", expectedAddress, defaultAddress)
	}

	expectedName := "world"
	if defaultName != expectedName {
		t.Errorf("Expected defaultName to be %q, got %q", expectedName, defaultName)
	}
}
