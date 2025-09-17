# Copilot Instructions for Utility Service

## Project Overview
- This is a Go-based microservice project providing gRPC APIs for Hello and MongoDB operations, with supporting infrastructure for Kafka and Redis.
- Major components:
  - `hello/`: gRPC Hello service (client/server, proto definitions)
  - `mongo/`: gRPC MongoDB service (client/server, proto definitions)
  - `_infra/`: Kubernetes manifests for Kafka, Redis, and service deployment
  - `grpcutils/`: Shared utilities, Dockerfile, Go modules

## Developer Workflows
- **Build/Run**: Use `Makefile` in `grpcutils/` for common build commands. For manual builds, use `go build` in service directories.
- **Kubernetes**: Deploy using manifests in `_infra/`. Port-forward services for local testing:
  - `kubectl port-forward service/utility-service 8080:50051` (Hello)
  - `kubectl port-forward service/utility-service 8081:50052` (MongoDB)
- **gRPC Testing**: Use `grpcurl` for direct API calls. See `README.md` for example commands for both Hello and MongoDB services.
- **Kafka/Redis**: Use port-forwarding and CLI commands as shown in `README.md` for topic management and Redis CLI access.

## Patterns & Conventions
- **Proto Files**: Located in `proto/` subfolders. Regenerate Go code with `protoc` as needed.
- **Service Structure**: Each service has `client/`, `server/`, and `proto/` directories. Follow this pattern for new services.
- **MongoDB Data**: Documents and filters are passed as JSON strings in gRPC requests (see examples in `README.md`).
- **Kubernetes**: Namespace, deployment, and service YAMLs are organized by technology in `_infra/`.
- **Go Modules**: All Go code uses modules; dependencies managed via `go.mod`/`go.sum` in `grpcutils/`.

## Integration Points
- **Kafka**: Managed via Kubernetes, with UI and CLI access. See `_infra/kafka/kafka.yaml` for config.
- **Redis**: Access via Kubernetes service (`redis.redis:6379`). See `_infra/redis/` for manifests.
- **MongoDB**: gRPC API for DB operations; see `mongo/proto/` for schema and `README.md` for usage.

## Key Files & Directories
- `README.md`: Essential usage examples and developer commands
- `_infra/`: All Kubernetes manifests
- `hello/`, `mongo/`: Service implementations and protos
- `grpcutils/`: Shared Go code, Dockerfile, build scripts

## Example: MongoDB Insert via gRPC
```
grpcurl -plaintext -d '{"database":"testdb","collection":"users","document":{"jsonData":"{\"name\":\"Alice\",\"email\":\"alice@example.com\",\"age\":25,\"department\":\"Engineering\"}"}}' localhost:8081 mongo.MongoService/InsertDocument
```

---
For unclear or missing conventions, review `README.md` and `_infra/` manifests. Ask maintainers for undocumented workflows.
