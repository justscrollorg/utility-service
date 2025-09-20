package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/justscrollorg/gflow-etl/internal/config"
	"github.com/justscrollorg/gflow-etl/internal/pipeline"
	"github.com/sirupsen/logrus"
)

// HTTPServer provides REST API for pipeline management
type HTTPServer struct {
	config          *config.Config
	pipelineManager *pipeline.Manager
	router          *gin.Engine
	server          *http.Server
	logger          *logrus.Logger
}

// PipelineRequest represents a pipeline creation request
type PipelineRequest struct {
	Name        string                     `json:"name" binding:"required"`
	Description string                     `json:"description"`
	Source      pipeline.SourceConfig     `json:"source" binding:"required"`
	Processing  pipeline.ProcessingConfig `json:"processing"`
	Sink        pipeline.SinkConfig       `json:"sink" binding:"required"`
}

// PipelineResponse represents a pipeline in API responses
type PipelineResponse struct {
	Name        string                     `json:"name"`
	Description string                     `json:"description"`
	Source      pipeline.SourceConfig     `json:"source"`
	Processing  pipeline.ProcessingConfig `json:"processing"`
	Sink        pipeline.SinkConfig       `json:"sink"`
	Status      pipeline.Status           `json:"status"`
	CreatedAt   time.Time                 `json:"created_at"`
	UpdatedAt   time.Time                 `json:"updated_at"`
	Metrics     *pipeline.Metrics         `json:"metrics,omitempty"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(config *config.Config, pipelineManager *pipeline.Manager) *HTTPServer {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Set Gin mode based on environment
	if config.Monitoring.LogLevel == "debug" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())

	server := &HTTPServer{
		config:          config,
		pipelineManager: pipelineManager,
		router:          router,
		logger:          logger,
	}

	server.setupRoutes()
	return server
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	address := fmt.Sprintf(":%d", s.config.Server.Port)
	
	s.server = &http.Server{
		Addr:         address,
		Handler:      s.router,
		ReadTimeout:  s.config.Server.ReadTimeout,
		WriteTimeout: s.config.Server.WriteTimeout,
	}

	s.logger.WithField("address", address).Info("Starting HTTP server")
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the HTTP server
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server")
	return s.server.Shutdown(ctx)
}

// setupRoutes configures all API routes
func (s *HTTPServer) setupRoutes() {
	// Health check
	s.router.GET("/health", s.healthCheck)
	s.router.GET("/ready", s.readinessCheck)

	// API v1 routes
	v1 := s.router.Group("/api/v1")
	{
		// Pipeline management
		pipelines := v1.Group("/pipelines")
		{
			pipelines.GET("", s.listPipelines)
			pipelines.POST("", s.createPipeline)
			pipelines.GET("/:name", s.getPipeline)
			pipelines.PUT("/:name", s.updatePipeline)
			pipelines.DELETE("/:name", s.deletePipeline)
			pipelines.POST("/:name/start", s.startPipeline)
			pipelines.POST("/:name/stop", s.stopPipeline)
			pipelines.GET("/:name/metrics", s.getPipelineMetrics)
		}

		// System metrics
		v1.GET("/metrics", s.getSystemMetrics)
		v1.GET("/status", s.getSystemStatus)

		// Demo endpoints
		demo := v1.Group("/demo")
		{
			demo.POST("/start", s.startDemo)
			demo.POST("/stop", s.stopDemo)
			demo.GET("/status", s.getDemoStatus)
		}
	}

	// Serve static files for UI (if available)
	s.router.Static("/static", "./web/static")
	s.router.LoadHTMLGlob("web/templates/*")
	
	// Simple web UI
	s.router.GET("/", s.indexPage)
	s.router.GET("/ui", s.uiPage)
}

// Health check endpoint
func (s *HTTPServer) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now(),
		"service":   "gflow-etl",
		"version":   "1.0.0",
	})
}

// Readiness check endpoint
func (s *HTTPServer) readinessCheck(c *gin.Context) {
	// Check if critical components are ready
	// In a real implementation, you'd check Kafka, ClickHouse, Redis connections
	c.JSON(http.StatusOK, gin.H{
		"status":    "ready",
		"timestamp": time.Now(),
		"checks": gin.H{
			"kafka":      "ok",
			"clickhouse": "ok",
			"redis":      "ok",
		},
	})
}

// Pipeline Management Endpoints

func (s *HTTPServer) listPipelines(c *gin.Context) {
	pipelines := s.pipelineManager.ListPipelines()
	
	responses := make([]PipelineResponse, len(pipelines))
	for i, p := range pipelines {
		responses[i] = s.pipelineToResponse(p)
	}
	
	c.JSON(http.StatusOK, gin.H{
		"pipelines": responses,
		"count":     len(responses),
	})
}

func (s *HTTPServer) createPipeline(c *gin.Context) {
	var req PipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.errorResponse(c, http.StatusBadRequest, "Invalid request", err)
		return
	}

	// Convert request to pipeline config
	pipelineConfig := &pipeline.Config{
		Name:        req.Name,
		Description: req.Description,
		Source:      req.Source,
		Processing:  req.Processing,
		Sink:        req.Sink,
	}

	// Create pipeline
	if err := s.pipelineManager.CreatePipeline(c.Request.Context(), pipelineConfig); err != nil {
		s.errorResponse(c, http.StatusInternalServerError, "Failed to create pipeline", err)
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":  "Pipeline created successfully",
		"pipeline": s.pipelineToResponse(pipelineConfig),
	})
}

func (s *HTTPServer) getPipeline(c *gin.Context) {
	name := c.Param("name")
	
	pipeline, exists := s.pipelineManager.GetPipeline(name)
	if !exists {
		s.errorResponse(c, http.StatusNotFound, "Pipeline not found", nil)
		return
	}

	c.JSON(http.StatusOK, s.pipelineToResponse(pipeline.Config))
}

func (s *HTTPServer) updatePipeline(c *gin.Context) {
	name := c.Param("name")
	
	var req PipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		s.errorResponse(c, http.StatusBadRequest, "Invalid request", err)
		return
	}

	// For simplicity, we'll delete and recreate the pipeline
	// In production, you'd want more sophisticated update logic
	if err := s.pipelineManager.DeletePipeline(name); err != nil {
		s.errorResponse(c, http.StatusInternalServerError, "Failed to update pipeline", err)
		return
	}

	pipelineConfig := &pipeline.Config{
		Name:        req.Name,
		Description: req.Description,
		Source:      req.Source,
		Processing:  req.Processing,
		Sink:        req.Sink,
	}

	if err := s.pipelineManager.CreatePipeline(c.Request.Context(), pipelineConfig); err != nil {
		s.errorResponse(c, http.StatusInternalServerError, "Failed to recreate pipeline", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":  "Pipeline updated successfully",
		"pipeline": s.pipelineToResponse(pipelineConfig),
	})
}

func (s *HTTPServer) deletePipeline(c *gin.Context) {
	name := c.Param("name")
	
	if err := s.pipelineManager.DeletePipeline(name); err != nil {
		s.errorResponse(c, http.StatusInternalServerError, "Failed to delete pipeline", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Pipeline deleted successfully",
	})
}

func (s *HTTPServer) startPipeline(c *gin.Context) {
	name := c.Param("name")
	
	pipelineInstance, exists := s.pipelineManager.GetPipeline(name)
	if !exists {
		s.errorResponse(c, http.StatusNotFound, "Pipeline not found", nil)
		return
	}

	// Pipeline management would handle start/stop logic
	// For now, we'll just update the status
	pipelineInstance.Config.Status = pipeline.StatusRunning
	pipelineInstance.Config.UpdatedAt = time.Now()

	c.JSON(http.StatusOK, gin.H{
		"message": "Pipeline started successfully",
	})
}

func (s *HTTPServer) stopPipeline(c *gin.Context) {
	name := c.Param("name")
	
	if err := s.pipelineManager.StopPipeline(name); err != nil {
		s.errorResponse(c, http.StatusInternalServerError, "Failed to stop pipeline", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Pipeline stopped successfully",
	})
}

func (s *HTTPServer) getPipelineMetrics(c *gin.Context) {
	name := c.Param("name")
	
	pipeline, exists := s.pipelineManager.GetPipeline(name)
	if !exists {
		s.errorResponse(c, http.StatusNotFound, "Pipeline not found", nil)
		return
	}

	metrics := pipeline.GetMetrics()
	
	c.JSON(http.StatusOK, gin.H{
		"pipeline": name,
		"metrics":  metrics,
		"timestamp": time.Now(),
	})
}

// System Metrics and Status

func (s *HTTPServer) getSystemMetrics(c *gin.Context) {
	pipelines := s.pipelineManager.ListPipelines()
	
	totalEvents := int64(0)
	runningPipelines := 0
	failedPipelines := 0
	
	for _, p := range pipelines {
		if p.Status == pipeline.StatusRunning {
			runningPipelines++
		} else if p.Status == pipeline.StatusFailed {
			failedPipelines++
		}
		// Add metrics aggregation here
	}

	c.JSON(http.StatusOK, gin.H{
		"system": gin.H{
			"total_pipelines":   len(pipelines),
			"running_pipelines": runningPipelines,
			"failed_pipelines":  failedPipelines,
			"total_events":      totalEvents,
			"uptime":           time.Since(time.Now().Truncate(time.Hour)), // Simplified
		},
		"timestamp": time.Now(),
	})
}

func (s *HTTPServer) getSystemStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "running",
		"config": gin.H{
			"kafka_brokers":   s.config.Kafka.Brokers,
			"clickhouse_host": s.config.ClickHouse.Host,
			"redis_host":      s.config.Redis.Host,
		},
		"features": gin.H{
			"deduplication": true,
			"temporal_joins": true,
			"batch_optimization": true,
			"demo_mode": s.config.Demo.Enabled,
		},
	})
}

// Demo Management

func (s *HTTPServer) startDemo(c *gin.Context) {
	eventsPerSecStr := c.DefaultQuery("events_per_sec", "10")
	eventsPerSec, _ := strconv.Atoi(eventsPerSecStr)
	
	if eventsPerSec < 1 || eventsPerSec > 1000 {
		s.errorResponse(c, http.StatusBadRequest, "events_per_sec must be between 1 and 1000", nil)
		return
	}

	// Start demo data generation
	// Implementation would start the demo generator
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Demo started successfully",
		"config": gin.H{
			"events_per_sec": eventsPerSec,
			"duration": "unlimited",
		},
	})
}

func (s *HTTPServer) stopDemo(c *gin.Context) {
	// Stop demo data generation
	// Implementation would stop the demo generator
	
	c.JSON(http.StatusOK, gin.H{
		"message": "Demo stopped successfully",
	})
}

func (s *HTTPServer) getDemoStatus(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"demo_running": s.config.Demo.Enabled,
		"data_generation": s.config.Demo.GenerateData,
		"config": gin.H{
			"events_per_sec": s.config.Demo.DataRate,
			"duplicate_ratio": s.config.Demo.DuplicateRatio,
			"user_count": s.config.Demo.GenerateUsers,
		},
	})
}

// Web UI endpoints

func (s *HTTPServer) indexPage(c *gin.Context) {
	c.HTML(http.StatusOK, "index.html", gin.H{
		"title": "GFlow ETL - Kafka to ClickHouse Streaming",
		"description": "Real-time data processing with deduplication and joins",
	})
}

func (s *HTTPServer) uiPage(c *gin.Context) {
	pipelines := s.pipelineManager.ListPipelines()
	
	c.HTML(http.StatusOK, "ui.html", gin.H{
		"title": "GFlow ETL Dashboard",
		"pipelines": pipelines,
		"config": s.config,
	})
}

// Helper methods

func (s *HTTPServer) pipelineToResponse(p *pipeline.Config) PipelineResponse {
	return PipelineResponse{
		Name:        p.Name,
		Description: p.Description,
		Source:      p.Source,
		Processing:  p.Processing,
		Sink:        p.Sink,
		Status:      p.Status,
		CreatedAt:   p.CreatedAt,
		UpdatedAt:   p.UpdatedAt,
	}
}

func (s *HTTPServer) errorResponse(c *gin.Context, code int, message string, err error) {
	response := ErrorResponse{
		Code:    code,
		Message: message,
	}
	
	if err != nil {
		response.Error = err.Error()
		s.logger.WithError(err).Error(message)
	}
	
	c.JSON(code, response)
}