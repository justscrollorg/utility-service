# Utility Service

# grpc 

# server port forward
#  kubectl port-forward service/utility-service 8080:50051
#  kubectl port-forward service/utility-service 8081:50052


# Hello Service Commands (Port 8080):
# List all services
grpcurl -plaintext localhost:8080 list

# Basic greeting
grpcurl -plaintext -d '{"name":"Anurag"}' localhost:8080 hello.Greeter/SayHello

# Custom message
grpcurl -plaintext -d '{"name":"World"}' localhost:8080 hello.Greeter/SayHello

# MongoDB Service Commands (Port 8081):

# List all services
grpcurl -plaintext localhost:8081 list

# List MongoDB service methods
grpcurl -plaintext localhost:8081 list mongo.MongoService

# List all databases
grpcurl -plaintext -d '{}' localhost:8081 mongo.MongoService/ListDatabases

# List collections in a database
grpcurl -plaintext -d '{"database":"testdb"}' localhost:8081 mongo.MongoService/ListCollections

# Insert a single document
grpcurl -plaintext -d '{"database":"testdb","collection":"users","document":{"jsonData":"{\"name\":\"John Doe\",\"email\":\"john@example.com\",\"age\":30,\"city\":\"New York\"}"}}' localhost:8081 mongo.MongoService/InsertDocument

# Insert multiple documents
grpcurl -plaintext -d '{"database":"testdb","collection":"products","documents":[{"jsonData":"{\"name\":\"Laptop\",\"price\":999.99,\"category\":\"Electronics\"}"},{"jsonData":"{\"name\":\"Mouse\",\"price\":29.99,\"category\":\"Electronics\"}"}]}' localhost:8081 mongo.MongoService/InsertMany


# Find all documents in a collection
grpcurl -plaintext -d '{"database":"testdb","collection":"users"}' localhost:8081 mongo.MongoService/FindDocuments

# Find with filter (users older than 25)
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"age\":{\"$gt\":25}}"}}' localhost:8081 mongo.MongoService/FindDocuments

# Find with limit and pagination
grpcurl -plaintext -d '{"database":"testdb","collection":"users","limit":5,"skip":0}' localhost:8081 mongo.MongoService/FindDocuments

# Find with sorting (by age descending)
grpcurl -plaintext -d '{"database":"testdb","collection":"users","sort":"{\"age\":-1}"}' localhost:8081 mongo.MongoService/FindDocuments

# Update a single document
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"name\":\"John Doe\"}"},"update":{"jsonUpdate":"{\"$set\":{\"age\":31,\"status\":\"updated\"}}"}}' localhost:8081 mongo.MongoService/UpdateDocument

# Update with upsert (create if not exists)
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"name\":\"Jane Doe\"}"},"update":{"jsonUpdate":"{\"$set\":{\"age\":28,\"email\":\"jane@example.com\"}}"},"upsert":true}' localhost:8081 mongo.MongoService/UpdateDocument

# Update multiple documents
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"age\":{\"$gte\":30}}"},"update":{"jsonUpdate":"{\"$set\":{\"category\":\"senior\"}}"},"updateMany":true}' localhost:8081 mongo.MongoService/UpdateDocument


# Count all documents
grpcurl -plaintext -d '{"database":"testdb","collection":"users"}' localhost:8081 mongo.MongoService/CountDocuments

# Count with filter
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"age\":{\"$gte\":30}}"}}' localhost:8081 mongo.MongoService/CountDocuments


# Delete a single document
grpcurl -plaintext -d '{"database":"testdb","collection":"products","filter":{"jsonFilter":"{\"name\":\"Mouse\"}"}}' localhost:8081 mongo.MongoService/DeleteDocument

# Delete multiple documents
grpcurl -plaintext -d '{"database":"testdb","collection":"products","filter":{"jsonFilter":"{\"price\":{\"$lt\":50}}"}}' localhost:8081 mongo.MongoService/DeleteMany

# Create test database and users
grpcurl -plaintext -d '{"database":"testdb","collection":"users","document":{"jsonData":"{\"name\":\"Alice\",\"email\":\"alice@example.com\",\"age\":25,\"department\":\"Engineering\"}"}}' localhost:8081 mongo.MongoService/InsertDocument

grpcurl -plaintext -d '{"database":"testdb","collection":"users","document":{"jsonData":"{\"name\":\"Bob\",\"email\":\"bob@example.com\",\"age\":35,\"department\":\"Marketing\"}"}}' localhost:8081 mongo.MongoService/InsertDocument

# Create products
grpcurl -plaintext -d '{"database":"testdb","collection":"products","documents":[{"jsonData":"{\"name\":\"Laptop\",\"price\":999.99,\"category\":\"Electronics\",\"stock\":10}"},{"jsonData":"{\"name\":\"Book\",\"price\":19.99,\"category\":\"Education\",\"stock\":50}"}]}' localhost:8081 mongo.MongoService/InsertMany

# List all users
grpcurl -plaintext -d '{"database":"testdb","collection":"users"}' localhost:8081 mongo.MongoService/FindDocuments

# List all products
grpcurl -plaintext -d '{"database":"testdb","collection":"products"}' localhost:8081 mongo.MongoService/FindDocuments

# Update user department
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"name\":\"Alice\"}"},"update":{"jsonUpdate":"{\"$set\":{\"department\":\"Senior Engineering\",\"promoted\":true}}"}}' localhost:8081 mongo.MongoService/UpdateDocument

# Count users by department
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{\"department\":{\"$regex\":\"Engineering\"}}"}}' localhost:8081 mongo.MongoService/CountDocuments

# Delete all test data
grpcurl -plaintext -d '{"database":"testdb","collection":"users","filter":{"jsonFilter":"{}"}}' localhost:8081 mongo.MongoService/DeleteMany

grpcurl -plaintext -d '{"database":"testdb","collection":"products","filter":{"jsonFilter":"{}"}}' localhost:8081 mongo.MongoService/DeleteMany


# Kafka
# Kafka UI (Web Interface)
kubectl port-forward -n kafka svc/kafka-ui 8080:8080
# Open: http://localhost:8080

# Command Line Operations
kubectl exec -it kafka-0 -n kafka -- kafka-topics --bootstrap-server localhost:9092 --list


# # Redis
kubectl run redis-test --rm -i --tty --image redis:7.2-alpine -- redis-cli -h redis.redis -p 6379

# From application code, use:
# Host: redis.redis
# Port: 6379

