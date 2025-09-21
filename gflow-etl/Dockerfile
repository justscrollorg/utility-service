FROM golang:1.24.1-alpine AS builder

# Set working directory
WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the applications
RUN CGO_ENABLED=0 GOOS=linux go build -o gflow-etl cmd/gflow-etl/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o messaging-demo cmd/messaging-demo/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o producer cmd/producer/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o producer-prod cmd/producer-prod/main.go

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy binaries and config
COPY --from=builder /app/gflow-etl .
COPY --from=builder /app/messaging-demo .
COPY --from=builder /app/producer .
COPY --from=builder /app/producer-prod .
COPY --from=builder /app/gflow-etl.yaml .

# Create directories for web assets (if needed)
RUN mkdir -p web/templates web/static

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
CMD ["./gflow-etl"]