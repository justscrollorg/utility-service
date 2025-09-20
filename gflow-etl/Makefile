# Build and deployment script for GFlow ETL

# Build application
build:
	CGO_ENABLED=0 GOOS=linux go build -o gflow-etl cmd/gflow-etl/main.go

# Build Docker image
docker-build:
	docker build -t justscroll/gflow-etl:latest .

# Push to registry
docker-push:
	docker push justscroll/gflow-etl:latest

# Deploy to Kubernetes
deploy:
	kubectl apply -f k8s/deployment.yaml

# Deploy integrated with grpcutils
deploy-integrated:
	kubectl apply -f ../grpcutils/k8s/

# Run locally for development
run-local:
	go run cmd/gflow-etl/main.go

# Run tests
test:
	go test ./...

# Generate demo data
demo:
	curl -X POST http://localhost:8080/api/v1/demo/start?events_per_sec=100

# Check pipeline status
status:
	curl http://localhost:8080/api/v1/pipelines

# View metrics
metrics:
	curl http://localhost:8080/api/v1/metrics

# Clean up deployment
clean:
	kubectl delete -f k8s/deployment.yaml

# Install dependencies
deps:
	go mod tidy
	go mod download

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run

.PHONY: build docker-build docker-push deploy deploy-integrated run-local test demo status metrics clean deps fmt lint