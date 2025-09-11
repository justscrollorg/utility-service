.PHONY: proto server client clean

# Generate protobuf and gRPC code
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/hello.proto

# Build and run server
server:
	go run server/main.go

# Build and run client
client:
	go run client/main.go

# Build client with custom name
client-custom:
	go run client/main.go $(NAME)

# Build binaries
build:
	go build -o bin/server server/main.go
	go build -o bin/client client/main.go

# Clean generated files
clean:
	rm -rf bin/
	rm -f proto/*.pb.go

# Install dependencies
deps:
	go mod tidy
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
