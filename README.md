# Utility Service

A simple Go gRPC service with a "Hello World" endpoint.

## Features

- gRPC server with a simple Greeter service
- Protocol buffers for service definition
- Example client implementation
- Easy-to-use Makefile for building and running

## Prerequisites

- Go 1.19 or later
- Protocol Buffers compiler (`protoc`)
- gRPC Go plugins

## Installation

1. Clone the repository:
```bash
git clone https://github.com/justscrollorg/utility-service.git
cd utility-service
```

2. Install dependencies:
```bash
make deps
```

## Usage

### Running the Server

Start the gRPC server:
```bash
make server
```

The server will listen on `localhost:50051`.

### Running the Client

In another terminal, run the client:
```bash
make client
```

Or with a custom name:
```bash
make client-custom NAME="Your Name"
```

### Building Binaries

To build standalone binaries:
```bash
make build
```

This will create `bin/server` and `bin/client` executables.

## Project Structure

```
.
├── proto/
│   ├── hello.proto         # Protocol buffer definition
│   ├── hello.pb.go         # Generated protobuf code
│   └── hello_grpc.pb.go    # Generated gRPC code
├── server/
│   └── main.go             # gRPC server implementation
├── client/
│   └── main.go             # Example client
├── Makefile                # Build automation
├── go.mod                  # Go module definition
└── README.md               # This file
```

## API

### SayHello

**Request:**
```proto
message HelloRequest {
  string name = 1;
}
```

**Response:**
```proto
message HelloReply {
  string message = 1;
}
```

**Example:**
- Input: `name = "World"`
- Output: `message = "Hello World"`

## Development

### Regenerating Protocol Buffers

If you modify `proto/hello.proto`, regenerate the Go code:
```bash
make proto
```

### Cleaning Generated Files

```bash
make clean
```