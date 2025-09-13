# Build stage
FROM golang:1.24.1-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the servers
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o hello-server ./hello/server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mongo-server ./mongo/server

# Final stage
FROM alpine:latest

# Install ca-certificates for SSL/TLS and netcat for health checks
RUN apk --no-cache add ca-certificates netcat-openbsd

WORKDIR /root/

# Copy the binaries from builder stage
COPY --from=builder /app/hello-server .
COPY --from=builder /app/mongo-server .

# Expose gRPC ports
EXPOSE 50051 50052

# Run both services using shell
CMD sh -c './hello-server & ./mongo-server & wait'
