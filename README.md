# Utility Service

# grpc 

# server port forward
#  kubectl port-forward service/utility-service 8080:50051
# client
# List all services
grpcurl -plaintext localhost:8080 list

# List service methods
grpcurl -plaintext localhost:8080 list hello.Greeter

# Test with your name
grpcurl -plaintext -d '{"name":"Your Name"}' localhost:8080 hello.Greeter/SayHello

# Test with different messages
grpcurl -plaintext -d '{"name":"Testing multi-service architecture"}' localhost:8080 hello.Greeter/SayHello