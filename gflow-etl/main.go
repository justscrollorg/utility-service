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
	log.Printf("Configuration loaded:")
	log.Printf("  - Kafka brokers: %v", cfg.Kafka.Brokers)
	log.Printf("  - ClickHouse host: %s", cfg.ClickHouse.Host)
	log.Printf("  - Server port: %d", cfg.Server.Port)
	log.Printf("  - Demo enabled: %t", cfg.Demo.Enabled)
	log.Printf("  - Log level: %s", cfg.Monitoring.LogLevel)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create pipeline manager
	log.Printf("Creating pipeline manager...")
	pipelineManager := pipeline.NewManager(cfg)

	// Start HTTP server for management
	log.Printf("Starting HTTP server on port %d...", cfg.Server.Port)
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
		log.Println("Demo mode enabled - Starting demo pipelines...")
		if err := startDemoPipelines(ctx, pipelineManager); err != nil {
			log.Printf("Failed to start demo pipelines: %v", err)
		} else {
			log.Printf("Demo pipelines started successfully")
		}
	} else {
		log.Println("Demo mode disabled - Waiting for pipeline creation via API")
	}

	<-sigChan
	log.Println("Shutdown signal received - initiating graceful shutdown...")

	// Shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	log.Println("Stopping all pipelines...")
	if err := pipelineManager.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during pipeline shutdown: %v", err)
	} else {
		log.Printf("All pipelines stopped successfully")
	}

	log.Println("Stopping HTTP server...")
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	} else {
		log.Printf("HTTP server stopped successfully")
	}

	log.Println("Shutdown complete - GFlow ETL Service terminated")
}

func startDemoPipelines(ctx context.Context, manager *pipeline.Manager) error {
	log.Printf("Creating demo ETL pipelines...")
	
	// Demo pipeline 1: User session deduplication
	log.Printf("Configuring Pipeline 1: User Session Deduplication")
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
	log.Printf("  Source: Kafka topic 'user-sessions'")
	log.Printf("  Transform: Deduplication on 'session_id' (24h window)")
	log.Printf("  Sink: ClickHouse table 'user_sessions_clean'")

	// Demo pipeline 2: Transaction + User profile join
	log.Printf("Configuring Pipeline 2: Transaction-User Join")
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
	log.Printf("  Source: Kafka join 'transactions' + 'user-profiles' on 'user_id'")
	log.Printf("  Transform: Temporal join (5min window) + Dedup on 'transaction_id' (12h)")
	log.Printf("  Sink: ClickHouse table 'enriched_transactions'")

	// Start pipelines
	log.Printf("Starting Pipeline 1: user-session-dedup...")
	if err := manager.CreatePipeline(ctx, sessionPipeline); err != nil {
		log.Printf("Failed to create session pipeline: %v", err)
		return fmt.Errorf("failed to create session pipeline: %w", err)
	}
	log.Printf("Pipeline 1 started successfully")

	log.Printf("Starting Pipeline 2: transaction-user-join...")
	if err := manager.CreatePipeline(ctx, joinPipeline); err != nil {
		log.Printf("Failed to create join pipeline: %v", err)
		return fmt.Errorf("failed to create join pipeline: %w", err)
	}
	log.Printf("Pipeline 2 started successfully")

	log.Printf("All demo pipelines are now running and ready for data")
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
