package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/justscrollorg/gflow-etl/internal/config"
	"github.com/justscrollorg/gflow-etl/internal/demo"
	"github.com/sirupsen/logrus"
)

type MessagingServer struct {
	router  *gin.Engine
	logger  *logrus.Logger
	msgDemo *demo.MessagingDemo
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewMessagingServer(cfg *config.Config) (*MessagingServer, error) {
	logger := logrus.New()

	// Initialize demos
	msgDemo, err := demo.NewMessagingDemo(cfg.Kafka.Brokers)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	router := gin.Default()
	server := &MessagingServer{
		router:  router,
		logger:  logger,
		msgDemo: msgDemo,
		ctx:     ctx,
		cancel:  cancel,
	}

	server.setupRoutes()
	return server, nil
}

func (s *MessagingServer) setupRoutes() {
	// Health check
	s.router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "healthy",
			"service":   "gflow-messaging-demo",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	// Streaming ETL demo (refers to main service)
	s.router.GET("/streaming/info", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message":      "Streaming ETL demo available on main service",
			"main_service": "http://localhost:8080",
			"endpoints": gin.H{
				"start_demo":      "POST /demo/start",
				"pipeline_status": "GET /pipeline/status",
				"metrics":         "GET /metrics",
			},
			"pattern":     "real-time event streaming and processing",
			"description": "Real-time ETL with deduplication and temporal joins",
		})
	})

	// Message Queue demo
	s.router.POST("/messaging/queue/start", func(c *gin.Context) {
		go s.msgDemo.StartMessageQueueDemo(s.ctx)
		s.logger.Info("Started message queue demo")

		c.JSON(http.StatusOK, gin.H{
			"message":     "Message queue demo started",
			"pattern":     "async task processing",
			"use_cases":   []string{"send-email", "process-payment", "generate-report", "resize-image"},
			"description": "Demonstrates asynchronous task processing with work queues",
		})
	})

	// Pub-Sub demo
	s.router.POST("/messaging/pubsub/start", func(c *gin.Context) {
		go s.msgDemo.StartPubSubDemo(s.ctx)
		s.logger.Info("Started pub-sub demo")

		c.JSON(http.StatusOK, gin.H{
			"message":     "Pub-Sub demo started",
			"pattern":     "event publishing and subscription",
			"use_cases":   []string{"user.created", "order.placed", "payment.completed", "inventory.updated"},
			"description": "Demonstrates event-driven architecture with publish-subscribe pattern",
		})
	})

	// Saga demo
	s.router.POST("/messaging/saga/start", func(c *gin.Context) {
		go s.msgDemo.StartSagaDemo(s.ctx)
		s.logger.Info("Started saga demo")

		c.JSON(http.StatusOK, gin.H{
			"message":     "Saga demo started",
			"pattern":     "distributed transaction coordination",
			"use_cases":   []string{"order-processing", "user-onboarding", "payment-flow"},
			"description": "Demonstrates distributed transaction management with compensation",
		})
	})

	// Stop all messaging demos
	s.router.POST("/messaging/stop", func(c *gin.Context) {
		s.msgDemo.Stop()
		s.logger.Info("Stopped all demos")

		c.JSON(http.StatusOK, gin.H{
			"message": "All messaging demos stopped",
		})
	})

	// Get messaging statistics
	s.router.GET("/messaging/stats", func(c *gin.Context) {
		msgStats := s.msgDemo.GetDemoStats()

		c.JSON(http.StatusOK, gin.H{
			"messaging": msgStats,
			"kafka_patterns": gin.H{
				"event_streaming": gin.H{
					"description": "Real-time data processing and analytics",
					"examples":    []string{"user sessions", "transactions", "analytics events"},
					"service":     "Main ETL service (port 8080)",
				},
				"message_queues": gin.H{
					"description": "Asynchronous task processing",
					"examples":    []string{"email sending", "payment processing", "report generation"},
					"active":      msgStats["message_queue"].(map[string]interface{})["active"],
				},
				"pub_sub": gin.H{
					"description": "Event-driven microservices communication",
					"examples":    []string{"user events", "order events", "inventory updates"},
					"active":      msgStats["pub_sub"].(map[string]interface{})["active"],
				},
				"saga_orchestration": gin.H{
					"description": "Distributed transaction management",
					"examples":    []string{"order processing", "payment flows", "user onboarding"},
					"active":      msgStats["saga"].(map[string]interface{})["active"],
				},
			},
		})
	})

	// Kafka use cases overview
	s.router.GET("/kafka/patterns", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"patterns": gin.H{
				"event_streaming": gin.H{
					"description": "Real-time event processing and analytics",
					"use_cases":   []string{"User activity tracking", "Real-time analytics", "Change data capture", "Log aggregation"},
					"example":     "Process user session events in real-time for analytics dashboard",
					"endpoint":    "GET /streaming/info (refers to main service)",
				},
				"message_queues": gin.H{
					"description": "Asynchronous task processing with reliable delivery",
					"use_cases":   []string{"Background job processing", "Email sending", "File processing", "Batch operations"},
					"example":     "Queue email sending tasks for background processing",
					"endpoint":    "POST /messaging/queue/start",
				},
				"pub_sub": gin.H{
					"description": "Event-driven microservices communication",
					"use_cases":   []string{"Service decoupling", "Event notifications", "State synchronization", "Audit logging"},
					"example":     "Publish user creation events to multiple subscribers",
					"endpoint":    "POST /messaging/pubsub/start",
				},
				"saga_orchestration": gin.H{
					"description": "Distributed transaction management with compensation",
					"use_cases":   []string{"Multi-service transactions", "Order processing", "Payment flows", "Rollback handling"},
					"example":     "Coordinate order processing across inventory, payment, and shipping services",
					"endpoint":    "POST /messaging/saga/start",
				},
			},
			"comparison": gin.H{
				"event_streaming":    "Best for real-time processing of continuous data streams",
				"message_queues":     "Best for reliable asynchronous task processing",
				"pub_sub":            "Best for loosely coupled event-driven architectures",
				"saga_orchestration": "Best for complex multi-service transactions requiring coordination",
			},
		})
	})
}

func (s *MessagingServer) Start(port string) error {
	s.logger.Infof("Starting messaging demo server on port %s", port)
	return s.router.Run(":" + port)
}

func (s *MessagingServer) Stop() {
	s.logger.Info("Stopping messaging demo server")
	s.cancel()
	s.msgDemo.Stop()
}

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create messaging server
	server, err := NewMessagingServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create messaging server: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "8081" // Different port from main ETL service
		}

		if err := server.Start(port); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}()

	log.Println("Messaging demo server started. Available endpoints:")
	log.Println("  GET  /health - Health check")
	log.Println("  GET  /kafka/patterns - Overview of Kafka patterns")
	log.Println("  GET  /streaming/info - Streaming ETL info (main service)")
	log.Println("  POST /messaging/queue/start - Start message queue demo")
	log.Println("  POST /messaging/pubsub/start - Start pub-sub demo")
	log.Println("  POST /messaging/saga/start - Start saga demo")
	log.Println("  POST /messaging/stop - Stop all demos")
	log.Println("  GET  /messaging/stats - Get demo statistics")

	<-sigChan
	log.Println("Shutting down gracefully...")
	server.Stop()
	log.Println("Shutdown complete")
}
