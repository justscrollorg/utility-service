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
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if pipeline already exists
	if _, exists := m.pipelines[config.Name]; exists {
		return fmt.Errorf("pipeline %s already exists", config.Name)
	}

	// Create pipeline context
	pipelineCtx, cancel := context.WithCancel(ctx)

	// Set timestamps
	config.CreatedAt = time.Now()
	config.UpdatedAt = time.Now()
	config.Status = StatusCreated

	// Create processor based on configuration
	processor, err := m.createProcessor(config)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to create processor: %w", err)
	}

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

	// Start pipeline
	go func() {
		defer cancel()

		config.Status = StatusRunning
		config.UpdatedAt = time.Now()

		if err := processor.Start(pipelineCtx); err != nil {
			config.Status = StatusFailed
			config.UpdatedAt = time.Now()
			fmt.Printf("Pipeline %s failed: %v\n", config.Name, err)
			return
		}

		config.Status = StatusStopped
		config.UpdatedAt = time.Now()
	}()

	m.pipelines[config.Name] = pipeline
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
		return NewJoinProcessor(m.config, config)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", config.Source.Type)
	}
}

// NewKafkaProcessor creates a new Kafka processor
func NewKafkaProcessor(globalConfig *config.Config, pipelineConfig *Config) (Processor, error) {
	// TODO: Implement Kafka processor
	// For now, return a mock processor
	return &MockProcessor{
		name:   pipelineConfig.Name,
		status: StatusStopped,
	}, nil
}

// NewJoinProcessor creates a new join processor
func NewJoinProcessor(globalConfig *config.Config, pipelineConfig *Config) (Processor, error) {
	// TODO: Implement join processor
	// For now, return a mock processor
	return &MockProcessor{
		name:   pipelineConfig.Name,
		status: StatusStopped,
	}, nil
}

// MockProcessor is a temporary implementation for testing
type MockProcessor struct {
	name   string
	status Status
	mutex  sync.RWMutex
}

func (p *MockProcessor) Start(ctx context.Context) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.status = StatusRunning
	return nil
}

func (p *MockProcessor) Stop() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.status = StatusStopped
	return nil
}

func (p *MockProcessor) GetStatus() Status {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.status
}

func (p *MockProcessor) GetMetrics() *Metrics {
	return &Metrics{
		EventsProcessed:   0,
		EventsDeduped:     0,
		EventsJoined:      0,
		EventsSunk:        0,
		ErrorCount:        0,
		ThroughputPerSec:  0,
		LastProcessedTime: time.Now(),
		StartTime:         time.Now(),
	}
}
