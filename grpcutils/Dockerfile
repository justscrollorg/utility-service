# Build stage
FROM golang:1.24.1-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server ./hello/server

# Final stage
FROM alpine:latest

# Install ca-certificates for SSL/TLS
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/server .

# Expose gRPC port
EXPOSE 50051

# Run the server
CMD ["./server"]
