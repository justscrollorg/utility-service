package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/justscrollorg/gflow-etl/internal/config"
	"github.com/justscrollorg/gflow-etl/internal/pipeline"
	"github.com/justscrollorg/gflow-etl/internal/server"
	"github.com/spf13/viper"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Starting GFlow ETL Service...")
	log.Printf("Kafka brokers: %v", cfg.Kafka.Brokers)
	log.Printf("ClickHouse host: %s", cfg.ClickHouse.Host)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create pipeline manager
	pipelineManager := pipeline.NewManager(cfg)

	// Start HTTP server for management
	httpServer := server.NewHTTPServer(cfg, pipelineManager)
	go func() {
		if err := httpServer.Start(); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start demo pipelines if enabled
	if cfg.Demo.Enabled {
		log.Println("Starting demo pipelines...")
		if err := startDemoPipelines(ctx, pipelineManager); err != nil {
			log.Printf("Failed to start demo pipelines: %v", err)
		}
	}

	<-sigChan
	log.Println("Shutting down gracefully...")

	// Shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := pipelineManager.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}

	log.Println("Shutdown complete")
}

func startDemoPipelines(ctx context.Context, manager *pipeline.Manager) error {
	// Demo pipeline 1: User session deduplication
	sessionPipeline := &pipeline.Config{
		Name:        "user-session-dedup",
		Description: "Deduplicate user session events by session_id",
		Source: pipeline.SourceConfig{
			Type:  "kafka",
			Topic: "user-sessions",
		},
		Processing: pipeline.ProcessingConfig{
			Deduplication: &pipeline.DeduplicationConfig{
				Enabled:    true,
				Key:        "session_id",
				TimeWindow: time.Hour * 24, // 24 hour window
			},
		},
		Sink: pipeline.SinkConfig{
			Type:  "clickhouse",
			Table: "user_sessions_clean",
		},
	}

	// Demo pipeline 2: Transaction + User profile join
	joinPipeline := &pipeline.Config{
		Name:        "transaction-user-join",
		Description: "Join transactions with user profiles",
		Source: pipeline.SourceConfig{
			Type: "kafka-join",
			Join: &pipeline.JoinConfig{
				LeftTopic:  "transactions",
				RightTopic: "user-profiles",
				JoinKey:    "user_id",
				JoinType:   "temporal",
				TimeWindow: time.Minute * 5, // 5 minute join window
			},
		},
		Processing: pipeline.ProcessingConfig{
			Deduplication: &pipeline.DeduplicationConfig{
				Enabled:    true,
				Key:        "transaction_id",
				TimeWindow: time.Hour * 12,
			},
		},
		Sink: pipeline.SinkConfig{
			Type:  "clickhouse",
			Table: "enriched_transactions",
		},
	}

	// Start pipelines
	if err := manager.CreatePipeline(ctx, sessionPipeline); err != nil {
		return fmt.Errorf("failed to create session pipeline: %w", err)
	}

	if err := manager.CreatePipeline(ctx, joinPipeline); err != nil {
		return fmt.Errorf("failed to create join pipeline: %w", err)
	}

	return nil
}

func init() {
	// Set default configuration
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("clickhouse.host", "localhost")
	viper.SetDefault("clickhouse.port", 9000)
	viper.SetDefault("clickhouse.database", "default")
	viper.SetDefault("demo.enabled", true)
}