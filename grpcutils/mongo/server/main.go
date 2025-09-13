package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/justscrollorg/utility-service/grpcutils/mongo/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50052" // Different port from hello service
)

type mongoServer struct {
	pb.UnimplementedMongoServiceServer
	client *mongo.Client
}

// Connect to MongoDB using connection string from environment
func connectToMongo() (*mongo.Client, error) {
	// Get MongoDB connection string from environment
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://mongo-0.mongo.mongo.svc.cluster.local:27017" // Headless service with StatefulSet pod
		log.Printf("[INFO] No MONGO_URI env var found, using default: %s", mongoURI)
	} else {
		log.Printf("[INFO] Using MONGO_URI from environment: %s", mongoURI)
	}

	// Set client options
	clientOptions := options.Client().ApplyURI(mongoURI)

	// Connect to MongoDB
	log.Printf("[INFO] Attempting to connect to MongoDB...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Printf("[ERROR] Failed to connect to MongoDB: %v", err)
		return nil, err
	}

	// Ping the database
	log.Printf("[INFO] Connected to MongoDB, attempting to ping...")
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Printf("[ERROR] Failed to ping MongoDB: %v", err)
		return nil, err
	}

	log.Printf("[SUCCESS] Successfully connected and pinged MongoDB!")
	return client, nil
}

// ListDatabases implementation
func (s *mongoServer) ListDatabases(ctx context.Context, req *pb.ListDatabasesRequest) (*pb.ListDatabasesResponse, error) {
	log.Printf("[REQUEST] ListDatabases called")

	result, err := s.client.ListDatabases(ctx, bson.M{})
	if err != nil {
		log.Printf("[ERROR] ListDatabases failed: %v", err)
		return &pb.ListDatabasesResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	var databases []*pb.DatabaseInfo
	for _, db := range result.Databases {
		log.Printf("[DEBUG] Found database: %s (size: %d bytes, empty: %t)", db.Name, db.SizeOnDisk, db.Empty)
		databases = append(databases, &pb.DatabaseInfo{
			Name:       db.Name,
			SizeOnDisk: db.SizeOnDisk,
			Empty:      db.Empty,
		})
	}

	log.Printf("[SUCCESS] ListDatabases completed successfully, returned %d databases", len(databases))
	return &pb.ListDatabasesResponse{
		Databases: databases,
		Success:   true,
	}, nil
}

// ListCollections implementation
func (s *mongoServer) ListCollections(ctx context.Context, req *pb.ListCollectionsRequest) (*pb.ListCollectionsResponse, error) {
	log.Printf("[REQUEST] ListCollections called for database: %s", req.Database)

	if req.Database == "" {
		log.Printf("[ERROR] ListCollections failed: database name is required")
		return &pb.ListCollectionsResponse{
			Success:      false,
			ErrorMessage: "database name is required",
		}, nil
	}

	log.Printf("[INFO] Accessing database: %s", req.Database)
	db := s.client.Database(req.Database)
	cursor, err := db.ListCollections(ctx, bson.M{})
	if err != nil {
		log.Printf("[ERROR] ListCollections failed for database %s: %v", req.Database, err)
		return &pb.ListCollectionsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}
	defer cursor.Close(ctx)

	log.Printf("[INFO] Successfully got collections cursor for database: %s", req.Database)
	var collections []*pb.CollectionInfo
	collectionCount := 0

	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Printf("[WARN] Failed to decode collection info: %v", err)
			continue
		}

		name, _ := result["name"].(string)
		colType, _ := result["type"].(string)
		log.Printf("[DEBUG] Found collection: %s (type: %s)", name, colType)

		collections = append(collections, &pb.CollectionInfo{
			Name: name,
			Type: colType,
		})
		collectionCount++
	}

	log.Printf("[SUCCESS] ListCollections completed for database %s, returned %d collections", req.Database, collectionCount)
	return &pb.ListCollectionsResponse{
		Collections: collections,
		Success:     true,
	}, nil
}

// InsertDocument implementation
func (s *mongoServer) InsertDocument(ctx context.Context, req *pb.InsertDocumentRequest) (*pb.InsertDocumentResponse, error) {
	log.Printf("[REQUEST] InsertDocument called for database: %s, collection: %s", req.Database, req.Collection)

	if req.Database == "" || req.Collection == "" {
		log.Printf("[ERROR] InsertDocument failed: database and collection names are required")
		return &pb.InsertDocumentResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	log.Printf("[DEBUG] Document to insert: %s", req.Document.JsonData)

	// Parse JSON document
	var doc bson.M
	if err := json.Unmarshal([]byte(req.Document.JsonData), &doc); err != nil {
		log.Printf("[ERROR] InsertDocument failed to parse JSON: %v", err)
		return &pb.InsertDocumentResponse{
			Success:      false,
			ErrorMessage: "invalid JSON document: " + err.Error(),
		}, nil
	}

	log.Printf("[INFO] Parsed document successfully, inserting into %s.%s", req.Database, req.Collection)
	collection := s.client.Database(req.Database).Collection(req.Collection)
	result, err := collection.InsertOne(ctx, doc)
	if err != nil {
		log.Printf("[ERROR] InsertDocument failed to insert: %v", err)
		return &pb.InsertDocumentResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	insertedID := result.InsertedID.(primitive.ObjectID).Hex()
	log.Printf("[SUCCESS] InsertDocument completed, inserted ID: %s", insertedID)

	return &pb.InsertDocumentResponse{
		InsertedId: insertedID,
		Success:    true,
	}, nil
}

// FindDocuments implementation
func (s *mongoServer) FindDocuments(ctx context.Context, req *pb.FindDocumentsRequest) (*pb.FindDocumentsResponse, error) {
	log.Printf("[REQUEST] FindDocuments called for database: %s, collection: %s", req.Database, req.Collection)

	if req.Database == "" || req.Collection == "" {
		log.Printf("[ERROR] FindDocuments failed: database and collection names are required")
		return &pb.FindDocumentsResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	// Parse filter
	var filter bson.M
	if req.Filter != nil && req.Filter.JsonFilter != "" {
		log.Printf("[DEBUG] Parsing filter: %s", req.Filter.JsonFilter)
		if err := json.Unmarshal([]byte(req.Filter.JsonFilter), &filter); err != nil {
			log.Printf("[ERROR] FindDocuments failed to parse filter: %v", err)
			return &pb.FindDocumentsResponse{
				Success:      false,
				ErrorMessage: "invalid filter JSON: " + err.Error(),
			}, nil
		}
		log.Printf("[DEBUG] Parsed filter successfully: %+v", filter)
	} else {
		filter = bson.M{} // Empty filter to get all documents
		log.Printf("[DEBUG] Using empty filter (find all documents)")
	}

	log.Printf("[INFO] Accessing collection %s.%s", req.Database, req.Collection)
	collection := s.client.Database(req.Database).Collection(req.Collection)

	// Set up find options
	findOptions := options.Find()
	if req.Limit > 0 {
		log.Printf("[DEBUG] Setting limit: %d", req.Limit)
		findOptions.SetLimit(int64(req.Limit))
	}
	if req.Skip > 0 {
		log.Printf("[DEBUG] Setting skip: %d", req.Skip)
		findOptions.SetSkip(int64(req.Skip))
	}
	if req.Sort != "" {
		log.Printf("[DEBUG] Parsing sort: %s", req.Sort)
		var sortDoc bson.M
		if err := json.Unmarshal([]byte(req.Sort), &sortDoc); err == nil {
			log.Printf("[DEBUG] Parsed sort successfully: %+v", sortDoc)
			findOptions.SetSort(sortDoc)
		} else {
			log.Printf("[WARN] Failed to parse sort specification: %v", err)
		}
	}

	log.Printf("[INFO] Executing find query...")
	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		log.Printf("[ERROR] FindDocuments query failed: %v", err)
		return &pb.FindDocumentsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}
	defer cursor.Close(ctx)

	log.Printf("[INFO] Query successful, processing results...")
	var documents []*pb.Document
	docCount := 0

	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Printf("[WARN] Failed to decode document: %v", err)
			continue
		}

		jsonData, err := json.Marshal(result)
		if err != nil {
			log.Printf("[WARN] Failed to marshal document to JSON: %v", err)
			continue
		}

		documents = append(documents, &pb.Document{
			JsonData: string(jsonData),
		})
		docCount++

		if docCount <= 3 { // Log first few documents for debugging
			log.Printf("[DEBUG] Document %d: %s", docCount, string(jsonData))
		}
	}

	log.Printf("[SUCCESS] FindDocuments completed, returned %d documents", docCount)
	return &pb.FindDocumentsResponse{
		Documents: documents,
		Success:   true,
		Count:     int32(docCount),
	}, nil
}

// UpdateDocument implementation
func (s *mongoServer) UpdateDocument(ctx context.Context, req *pb.UpdateDocumentRequest) (*pb.UpdateDocumentResponse, error) {
	if req.Database == "" || req.Collection == "" {
		return &pb.UpdateDocumentResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	// Parse filter
	var filter bson.M
	if err := json.Unmarshal([]byte(req.Filter.JsonFilter), &filter); err != nil {
		return &pb.UpdateDocumentResponse{
			Success:      false,
			ErrorMessage: "invalid filter JSON: " + err.Error(),
		}, nil
	}

	// Parse update
	var update bson.M
	if err := json.Unmarshal([]byte(req.Update.JsonUpdate), &update); err != nil {
		return &pb.UpdateDocumentResponse{
			Success:      false,
			ErrorMessage: "invalid update JSON: " + err.Error(),
		}, nil
	}

	collection := s.client.Database(req.Database).Collection(req.Collection)

	updateOptions := options.Update().SetUpsert(req.Upsert)

	var result *mongo.UpdateResult
	var err error

	if req.UpdateMany {
		result, err = collection.UpdateMany(ctx, filter, update, updateOptions)
	} else {
		result, err = collection.UpdateOne(ctx, filter, update, updateOptions)
	}

	if err != nil {
		return &pb.UpdateDocumentResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	response := &pb.UpdateDocumentResponse{
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		UpsertedCount: result.UpsertedCount,
		Success:       true,
	}

	if result.UpsertedID != nil {
		response.UpsertedId = result.UpsertedID.(primitive.ObjectID).Hex()
	}

	return response, nil
}

// DeleteDocument implementation
func (s *mongoServer) DeleteDocument(ctx context.Context, req *pb.DeleteDocumentRequest) (*pb.DeleteDocumentResponse, error) {
	if req.Database == "" || req.Collection == "" {
		return &pb.DeleteDocumentResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	// Parse filter
	var filter bson.M
	if err := json.Unmarshal([]byte(req.Filter.JsonFilter), &filter); err != nil {
		return &pb.DeleteDocumentResponse{
			Success:      false,
			ErrorMessage: "invalid filter JSON: " + err.Error(),
		}, nil
	}

	collection := s.client.Database(req.Database).Collection(req.Collection)

	var result *mongo.DeleteResult
	var err error

	if req.DeleteMany {
		result, err = collection.DeleteMany(ctx, filter)
	} else {
		result, err = collection.DeleteOne(ctx, filter)
	}

	if err != nil {
		return &pb.DeleteDocumentResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.DeleteDocumentResponse{
		DeletedCount: result.DeletedCount,
		Success:      true,
	}, nil
}

// InsertMany implementation
func (s *mongoServer) InsertMany(ctx context.Context, req *pb.InsertManyRequest) (*pb.InsertManyResponse, error) {
	if req.Database == "" || req.Collection == "" {
		return &pb.InsertManyResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	// Parse documents
	var docs []interface{}
	for _, doc := range req.Documents {
		var parsed bson.M
		if err := json.Unmarshal([]byte(doc.JsonData), &parsed); err != nil {
			return &pb.InsertManyResponse{
				Success:      false,
				ErrorMessage: "invalid JSON document: " + err.Error(),
			}, nil
		}
		docs = append(docs, parsed)
	}

	collection := s.client.Database(req.Database).Collection(req.Collection)
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return &pb.InsertManyResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	var insertedIds []string
	for _, id := range result.InsertedIDs {
		insertedIds = append(insertedIds, id.(primitive.ObjectID).Hex())
	}

	return &pb.InsertManyResponse{
		InsertedIds: insertedIds,
		Success:     true,
	}, nil
}

// DeleteMany implementation
func (s *mongoServer) DeleteMany(ctx context.Context, req *pb.DeleteManyRequest) (*pb.DeleteManyResponse, error) {
	if req.Database == "" || req.Collection == "" {
		return &pb.DeleteManyResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	// Parse filter
	var filter bson.M
	if err := json.Unmarshal([]byte(req.Filter.JsonFilter), &filter); err != nil {
		return &pb.DeleteManyResponse{
			Success:      false,
			ErrorMessage: "invalid filter JSON: " + err.Error(),
		}, nil
	}

	collection := s.client.Database(req.Database).Collection(req.Collection)
	result, err := collection.DeleteMany(ctx, filter)
	if err != nil {
		return &pb.DeleteManyResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.DeleteManyResponse{
		DeletedCount: result.DeletedCount,
		Success:      true,
	}, nil
}

// CountDocuments implementation
func (s *mongoServer) CountDocuments(ctx context.Context, req *pb.CountDocumentsRequest) (*pb.CountDocumentsResponse, error) {
	if req.Database == "" || req.Collection == "" {
		return &pb.CountDocumentsResponse{
			Success:      false,
			ErrorMessage: "database and collection names are required",
		}, nil
	}

	// Parse filter
	var filter bson.M
	if req.Filter != nil && req.Filter.JsonFilter != "" {
		if err := json.Unmarshal([]byte(req.Filter.JsonFilter), &filter); err != nil {
			return &pb.CountDocumentsResponse{
				Success:      false,
				ErrorMessage: "invalid filter JSON: " + err.Error(),
			}, nil
		}
	} else {
		filter = bson.M{}
	}

	collection := s.client.Database(req.Database).Collection(req.Collection)
	count, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return &pb.CountDocumentsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.CountDocumentsResponse{
		Count:   count,
		Success: true,
	}, nil
}

func main() {
	log.Printf("[STARTUP] MongoDB gRPC Server starting...")
	log.Printf("[STARTUP] Server will listen on port %s", port)

	// Connect to MongoDB
	log.Printf("[STARTUP] Initializing MongoDB connection...")
	client, err := connectToMongo()
	if err != nil {
		log.Fatalf("[FATAL] Failed to connect to MongoDB: %v", err)
	}
	defer func() {
		log.Printf("[SHUTDOWN] Disconnecting from MongoDB...")
		client.Disconnect(context.Background())
	}()

	log.Printf("[STARTUP] Creating TCP listener on port %s", port)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[FATAL] Failed to listen on port %s: %v", port, err)
	}

	log.Printf("[STARTUP] Creating gRPC server...")
	s := grpc.NewServer()
	pb.RegisterMongoServiceServer(s, &mongoServer{client: client})

	// Enable gRPC reflection
	log.Printf("[STARTUP] Enabling gRPC reflection...")
	reflection.Register(s)

	log.Printf("[SUCCESS] MongoDB gRPC server listening at %v", lis.Addr())
	log.Printf("[INFO] Available services: MongoService with gRPC reflection enabled")
	log.Printf("[INFO] Ready to accept connections...")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("[FATAL] Failed to serve: %v", err)
	}
}
