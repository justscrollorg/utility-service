package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/justscrollorg/gflow-etl/internal/config"
)

// Config represents a pipeline configuration
type Config struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Source      SourceConfig     `json:"source"`
	Processing  ProcessingConfig `json:"processing"`
	Sink        SinkConfig       `json:"sink"`
	Status      Status           `json:"status"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// SourceConfig defines the data source configuration
type SourceConfig struct {
	Type  string      `json:"type"` // "kafka", "kafka-join"
	Topic string      `json:"topic,omitempty"`
	Join  *JoinConfig `json:"join,omitempty"`
}

// JoinConfig defines how to join two Kafka topics
type JoinConfig struct {
	LeftTopic  string        `json:"left_topic"`
	RightTopic string        `json:"right_topic"`
	JoinKey    string        `json:"join_key"`
	JoinType   string        `json:"join_type"` // "temporal", "inner"
	TimeWindow time.Duration `json:"time_window"`
}

// ProcessingConfig defines data processing steps
type ProcessingConfig struct {
	Deduplication  *DeduplicationConfig `json:"deduplication,omitempty"`
	Filtering      *FilterConfig        `json:"filtering,omitempty"`
	Transformation *TransformConfig     `json:"transformation,omitempty"`
}

// DeduplicationConfig defines deduplication parameters
type DeduplicationConfig struct {
	Enabled    bool          `json:"enabled"`
	Key        string        `json:"key"`         // Field name to deduplicate on
	TimeWindow time.Duration `json:"time_window"` // Window size (max 7 days like GlassFlow)
}

// FilterConfig defines filtering rules
type FilterConfig struct {
	Enabled bool         `json:"enabled"`
	Rules   []FilterRule `json:"rules"`
}

// FilterRule represents a single filter rule
type FilterRule struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // "eq", "ne", "gt", "lt", "contains"
	Value    interface{} `json:"value"`
}

// TransformConfig defines data transformations
type TransformConfig struct {
	Enabled bool            `json:"enabled"`
	Rules   []TransformRule `json:"rules"`
}

// TransformRule represents a transformation rule
type TransformRule struct {
	Field string `json:"field"`
	Type  string `json:"type"` // "rename", "cast", "default", "compute"
	Value string `json:"value"`
}

// SinkConfig defines the data destination
type SinkConfig struct {
	Type  string `json:"type"` // "clickhouse", "kafka"
	Table string `json:"table,omitempty"`
	Topic string `json:"topic,omitempty"`
}

// Status represents pipeline status
type Status string

const (
	StatusCreated Status = "created"
	StatusRunning Status = "running"
	StatusStopped Status = "stopped"
	StatusFailed  Status = "failed"
)

// Manager manages all pipelines
type Manager struct {
	config    *config.Config
	pipelines map[string]*Pipeline
	mutex     sync.RWMutex
}

// Pipeline represents a running pipeline instance
type Pipeline struct {
	Config    *Config
	ctx       context.Context
	cancel    context.CancelFunc
	processor Processor
	metrics   *Metrics
}

// Processor interface for different pipeline processors
type Processor interface {
	Start(ctx context.Context) error
	Stop() error
	GetMetrics() *Metrics
}

// Metrics holds pipeline metrics
type Metrics struct {
	EventsProcessed   int64     `json:"events_processed"`
	EventsDeduped     int64     `json:"events_deduped"`
	EventsJoined      int64     `json:"events_joined"`
	EventsSunk        int64     `json:"events_sunk"`
	ErrorCount        int64     `json:"error_count"`
	ThroughputPerSec  float64   `json:"throughput_per_sec"`
	LastProcessedTime time.Time `json:"last_processed_time"`
	StartTime         time.Time `json:"start_time"`
}

// GetMetrics returns the current metrics from the pipeline processor
func (p *Pipeline) GetMetrics() *Metrics {
	if p.processor != nil {
		return p.processor.GetMetrics()
	}
	return &Metrics{}
}

// NewManager creates a new pipeline manager
func NewManager(config *config.Config) *Manager {
	return &Manager{
		config:    config,
		pipelines: make(map[string]*Pipeline),
	}
}

// CreatePipeline creates and starts a new pipeline
func (m *Manager) CreatePipeline(ctx context.Context, config *Config) error {
	fmt.Printf("Creating pipeline '%s'...\n", config.Name)
	fmt.Printf("   Description: %s\n", config.Description)
	
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if pipeline already exists
	if _, exists := m.pipelines[config.Name]; exists {
		fmt.Printf("Pipeline '%s' already exists\n", config.Name)
		return fmt.Errorf("pipeline %s already exists", config.Name)
	}

	// Create pipeline context
	pipelineCtx, cancel := context.WithCancel(ctx)

	// Set timestamps
	config.CreatedAt = time.Now()
	config.UpdatedAt = time.Now()
	config.Status = StatusCreated
	fmt.Printf("Pipeline '%s' created at %s\n", config.Name, config.CreatedAt.Format("15:04:05"))

	// Create processor based on configuration
	fmt.Printf("Creating processor for pipeline '%s'...\n", config.Name)
	processor, err := m.createProcessor(config)
	if err != nil {
		cancel()
		fmt.Printf("Failed to create processor for '%s': %v\n", config.Name, err)
		return fmt.Errorf("failed to create processor: %w", err)
	}
	fmt.Printf("Processor created for pipeline '%s'\n", config.Name)

	// Create pipeline
	pipeline := &Pipeline{
		Config:    config,
		ctx:       pipelineCtx,
		cancel:    cancel,
		processor: processor,
		metrics: &Metrics{
			StartTime: time.Now(),
		},
	}
	fmt.Printf("Pipeline '%s' structure created\n", config.Name)

	// Start pipeline
	go func() {
		fmt.Printf("Starting processor for pipeline '%s'...\n", config.Name)

		config.Status = StatusRunning
		config.UpdatedAt = time.Now()
		fmt.Printf("Pipeline '%s' status: %s\n", config.Name, config.Status)

		// processor.Start() will block until the processor is stopped
		if err := processor.Start(pipelineCtx); err != nil {
			config.Status = StatusFailed
			config.UpdatedAt = time.Now()
			fmt.Printf("Pipeline '%s' failed: %v\n", config.Name, err)
		} else {
			config.Status = StatusStopped
			config.UpdatedAt = time.Now()
			fmt.Printf("Pipeline '%s' stopped normally\n", config.Name)
		}
		cancel()
	}()

	m.pipelines[config.Name] = pipeline
	fmt.Printf("Pipeline '%s' registered and started successfully\n", config.Name)
	return nil
}

// GetPipeline returns a pipeline by name
func (m *Manager) GetPipeline(name string) (*Pipeline, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	pipeline, exists := m.pipelines[name]
	return pipeline, exists
}

// ListPipelines returns all pipelines
func (m *Manager) ListPipelines() []*Config {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	configs := make([]*Config, 0, len(m.pipelines))
	for _, pipeline := range m.pipelines {
		configs = append(configs, pipeline.Config)
	}
	return configs
}

// StartPipeline starts a stopped pipeline
func (m *Manager) StartPipeline(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	pipeline, exists := m.pipelines[name]
	if !exists {
		return fmt.Errorf("pipeline %s not found", name)
	}

	if pipeline.Config.Status == StatusRunning {
		return fmt.Errorf("pipeline %s is already running", name)
	}

	// Create new context for the pipeline
	pipelineCtx, cancel := context.WithCancel(context.Background())
	pipeline.cancel = cancel

	// Update status
	pipeline.Config.Status = StatusRunning
	pipeline.Config.UpdatedAt = time.Now()

	// Start processor in goroutine
	go func() {
		defer func() {
			pipeline.Config.Status = StatusStopped
			pipeline.Config.UpdatedAt = time.Now()
		}()

		if err := pipeline.processor.Start(pipelineCtx); err != nil {
			pipeline.Config.Status = StatusFailed
			pipeline.Config.UpdatedAt = time.Now()
			fmt.Printf("Pipeline '%s' failed: %v\n", name, err)
			return
		}
	}()

	fmt.Printf("Pipeline '%s' started successfully\n", name)
	return nil
}

// StopPipeline stops a pipeline
func (m *Manager) StopPipeline(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	pipeline, exists := m.pipelines[name]
	if !exists {
		return fmt.Errorf("pipeline %s not found", name)
	}

	pipeline.cancel()
	pipeline.Config.Status = StatusStopped
	pipeline.Config.UpdatedAt = time.Now()

	return pipeline.processor.Stop()
}

// DeletePipeline stops and deletes a pipeline
func (m *Manager) DeletePipeline(name string) error {
	if err := m.StopPipeline(name); err != nil {
		return err
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.pipelines, name)
	return nil
}

// Shutdown stops all pipelines
func (m *Manager) Shutdown(ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for name := range m.pipelines {
		if err := m.StopPipeline(name); err != nil {
			fmt.Printf("Error stopping pipeline %s: %v\n", name, err)
		}
	}

	return nil
}

// createProcessor creates a processor based on pipeline configuration
func (m *Manager) createProcessor(config *Config) (Processor, error) {
	switch config.Source.Type {
	case "kafka":
		return NewKafkaProcessor(m.config, config)
	case "kafka-join":
		return NewTemporalJoinProcessor(m.config, config)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", config.Source.Type)
	}
}


