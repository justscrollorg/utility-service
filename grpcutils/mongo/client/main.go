package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "github.com/justscrollorg/utility-service/grpcutils/mongo/proto"
)

const (
	defaultAddress = "localhost:50052"
)

func main() {
	// Parse command line flags
	serverAddr := flag.String("server", defaultAddress, "Server address (host:port)")
	operation := flag.String("op", "list-dbs", "Operation: list-dbs, list-collections, insert, find, update, delete, count")
	database := flag.String("db", "", "Database name")
	collection := flag.String("coll", "", "Collection name")
	document := flag.String("doc", "", "Document JSON for insert operations")
	filter := flag.String("filter", "", "Filter JSON for find/update/delete operations")
	update := flag.String("update", "", "Update JSON for update operations")
	flag.Parse()

	log.Printf("[CLIENT] Starting MongoDB gRPC client")
	log.Printf("[CLIENT] Operation: %s", *operation)
	log.Printf("[CLIENT] Server: %s", *serverAddr)
	if *database != "" {
		log.Printf("[CLIENT] Database: %s", *database)
	}
	if *collection != "" {
		log.Printf("[CLIENT] Collection: %s", *collection)
	}

	// Set up a connection to the server
	log.Printf("[CLIENT] Connecting to MongoDB gRPC server at %s", *serverAddr)
	conn, err := grpc.Dial(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[CLIENT ERROR] Failed to connect: %v", err)
	}
	defer conn.Close()
	
	log.Printf("[CLIENT] Connected successfully, creating client...")
	client := pb.NewMongoServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch *operation {
	case "list-dbs":
		listDatabases(ctx, client)
	case "list-collections":
		if *database == "" {
			log.Fatal("Database name is required for list-collections operation")
		}
		listCollections(ctx, client, *database)
	case "insert":
		if *database == "" || *collection == "" || *document == "" {
			log.Fatal("Database, collection, and document are required for insert operation")
		}
		insertDocument(ctx, client, *database, *collection, *document)
	case "find":
		if *database == "" || *collection == "" {
			log.Fatal("Database and collection are required for find operation")
		}
		findDocuments(ctx, client, *database, *collection, *filter)
	case "update":
		if *database == "" || *collection == "" || *filter == "" || *update == "" {
			log.Fatal("Database, collection, filter, and update are required for update operation")
		}
		updateDocument(ctx, client, *database, *collection, *filter, *update)
	case "delete":
		if *database == "" || *collection == "" || *filter == "" {
			log.Fatal("Database, collection, and filter are required for delete operation")
		}
		deleteDocument(ctx, client, *database, *collection, *filter)
	case "count":
		if *database == "" || *collection == "" {
			log.Fatal("Database and collection are required for count operation")
		}
		countDocuments(ctx, client, *database, *collection, *filter)
	default:
		log.Fatalf("Unknown operation: %s", *operation)
	}
}

func listDatabases(ctx context.Context, client pb.MongoServiceClient) {
	log.Printf("[CLIENT] Calling ListDatabases...")
	resp, err := client.ListDatabases(ctx, &pb.ListDatabasesRequest{})
	if err != nil {
		log.Fatalf("[CLIENT ERROR] ListDatabases failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("[CLIENT ERROR] ListDatabases error: %s", resp.ErrorMessage)
	}

	log.Printf("[CLIENT SUCCESS] ListDatabases completed, found %d databases", len(resp.Databases))
	fmt.Println("Databases:")
	for _, db := range resp.Databases {
		fmt.Printf("  - Name: %s, Size: %d bytes, Empty: %t\n", db.Name, db.SizeOnDisk, db.Empty)
	}
}

func listCollections(ctx context.Context, client pb.MongoServiceClient, database string) {
	resp, err := client.ListCollections(ctx, &pb.ListCollectionsRequest{
		Database: database,
	})
	if err != nil {
		log.Fatalf("ListCollections failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("ListCollections error: %s", resp.ErrorMessage)
	}

	fmt.Printf("Collections in database '%s':\n", database)
	for _, coll := range resp.Collections {
		fmt.Printf("  - Name: %s, Type: %s\n", coll.Name, coll.Type)
	}
}

func insertDocument(ctx context.Context, client pb.MongoServiceClient, database, collection, document string) {
	log.Printf("[CLIENT] Calling InsertDocument for %s.%s", database, collection)
	log.Printf("[CLIENT] Document: %s", document)
	
	resp, err := client.InsertDocument(ctx, &pb.InsertDocumentRequest{
		Database:   database,
		Collection: collection,
		Document:   &pb.Document{JsonData: document},
	})
	if err != nil {
		log.Fatalf("[CLIENT ERROR] InsertDocument failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("[CLIENT ERROR] InsertDocument error: %s", resp.ErrorMessage)
	}

	log.Printf("[CLIENT SUCCESS] Document inserted successfully with ID: %s", resp.InsertedId)

	fmt.Printf("Document inserted with ID: %s\n", resp.InsertedId)
}

func findDocuments(ctx context.Context, client pb.MongoServiceClient, database, collection, filter string) {
	req := &pb.FindDocumentsRequest{
		Database:   database,
		Collection: collection,
		Limit:      10, // Limit to 10 documents
	}

	if filter != "" {
		req.Filter = &pb.Filter{JsonFilter: filter}
	}

	resp, err := client.FindDocuments(ctx, req)
	if err != nil {
		log.Fatalf("FindDocuments failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("FindDocuments error: %s", resp.ErrorMessage)
	}

	fmt.Printf("Found %d documents:\n", resp.Count)
	for i, doc := range resp.Documents {
		var prettyJSON map[string]interface{}
		if err := json.Unmarshal([]byte(doc.JsonData), &prettyJSON); err == nil {
			prettyData, _ := json.MarshalIndent(prettyJSON, "", "  ")
			fmt.Printf("Document %d:\n%s\n\n", i+1, string(prettyData))
		} else {
			fmt.Printf("Document %d: %s\n\n", i+1, doc.JsonData)
		}
	}
}

func updateDocument(ctx context.Context, client pb.MongoServiceClient, database, collection, filter, update string) {
	resp, err := client.UpdateDocument(ctx, &pb.UpdateDocumentRequest{
		Database:   database,
		Collection: collection,
		Filter:     &pb.Filter{JsonFilter: filter},
		Update:     &pb.Update{JsonUpdate: update},
		Upsert:     true, // Create if not exists
	})
	if err != nil {
		log.Fatalf("UpdateDocument failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("UpdateDocument error: %s", resp.ErrorMessage)
	}

	fmt.Printf("Update result: Matched=%d, Modified=%d, Upserted=%d\n", 
		resp.MatchedCount, resp.ModifiedCount, resp.UpsertedCount)
	if resp.UpsertedId != "" {
		fmt.Printf("Upserted ID: %s\n", resp.UpsertedId)
	}
}

func deleteDocument(ctx context.Context, client pb.MongoServiceClient, database, collection, filter string) {
	resp, err := client.DeleteDocument(ctx, &pb.DeleteDocumentRequest{
		Database:   database,
		Collection: collection,
		Filter:     &pb.Filter{JsonFilter: filter},
	})
	if err != nil {
		log.Fatalf("DeleteDocument failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("DeleteDocument error: %s", resp.ErrorMessage)
	}

	fmt.Printf("Deleted %d document(s)\n", resp.DeletedCount)
}

func countDocuments(ctx context.Context, client pb.MongoServiceClient, database, collection, filter string) {
	req := &pb.CountDocumentsRequest{
		Database:   database,
		Collection: collection,
	}

	if filter != "" {
		req.Filter = &pb.Filter{JsonFilter: filter}
	}

	resp, err := client.CountDocuments(ctx, req)
	if err != nil {
		log.Fatalf("CountDocuments failed: %v", err)
	}

	if !resp.Success {
		log.Fatalf("CountDocuments error: %s", resp.ErrorMessage)
	}

	fmt.Printf("Document count: %d\n", resp.Count)
}