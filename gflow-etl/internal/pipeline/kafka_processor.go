package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chsink "github.com/justscrollorg/gflow-etl/internal/clickhouse"
	"github.com/justscrollorg/gflow-etl/internal/config"
	"github.com/justscrollorg/gflow-etl/internal/deduplication"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// KafkaProcessor implements Kafka stream processing with deduplication and transformations
type KafkaProcessor struct {
	config         *config.Config
	pipelineConfig *Config
	logger         *logrus.Logger

	// Kafka components
	reader *kafka.Reader
	writer *kafka.Writer

	// Processing components
	dedupEngine *deduplication.Engine
	clickSink   *chsink.BatchSink

	// State management
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	isRunning  int32
	
	// Metrics
	metrics *Metrics
	mutex   sync.RWMutex
}

// NewKafkaProcessor creates a Kafka processor with full ETL capabilities
func NewKafkaProcessor(globalConfig *config.Config, pipelineConfig *Config) (*KafkaProcessor, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	processor := &KafkaProcessor{
		config:         globalConfig,
		pipelineConfig: pipelineConfig,
		logger:         logger,
		metrics: &Metrics{
			StartTime: time.Now(),
		},
	}

	// Initialize Kafka reader for consuming
	if err := processor.initKafkaReader(); err != nil {
		return nil, fmt.Errorf("failed to initialize Kafka reader: %w", err)
	}

	// Initialize deduplication engine if enabled
	if pipelineConfig.Processing.Deduplication != nil && pipelineConfig.Processing.Deduplication.Enabled {
		if err := processor.initDeduplicationEngine(); err != nil {
			return nil, fmt.Errorf("failed to initialize deduplication engine: %w", err)
		}
	}

	// Initialize ClickHouse sink if configured
	if pipelineConfig.Sink.Type == "clickhouse" {
		if err := processor.initClickHouseSink(); err != nil {
			return nil, fmt.Errorf("failed to initialize ClickHouse sink: %w", err)
		}
	}

	logger.WithFields(logrus.Fields{
		"pipeline":         pipelineConfig.Name,
		"source_topic":     pipelineConfig.Source.Topic,
		"sink_type":        pipelineConfig.Sink.Type,
		"dedup_enabled":    pipelineConfig.Processing.Deduplication != nil && pipelineConfig.Processing.Deduplication.Enabled,
		"kafka_brokers":    globalConfig.Kafka.Brokers,
	}).Info("Kafka processor initialized successfully")

	return processor, nil
}

// initKafkaReader initializes the Kafka reader with proper configuration
func (p *KafkaProcessor) initKafkaReader() error {
	readerConfig := kafka.ReaderConfig{
		Brokers: p.config.Kafka.Brokers,
		Topic:   p.pipelineConfig.Source.Topic,
		GroupID: fmt.Sprintf("gflow-etl-%s", p.pipelineConfig.Name),
		
		// Consumer configuration for reliability
		MinBytes:    1,
		MaxBytes:    10e6, // 10MB
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
		
		// Error handling
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			p.logger.Errorf("Kafka reader error: "+msg, args...)
		}),
		
		// Partition handling
		Partition: 0, // Will be auto-assigned in consumer group
	}

	p.reader = kafka.NewReader(readerConfig)

	p.logger.WithFields(logrus.Fields{
		"topic":       p.pipelineConfig.Source.Topic,
		"group_id":    readerConfig.GroupID,
		"brokers":     p.config.Kafka.Brokers,
		"start_offset": "latest",
	}).Info("Kafka reader initialized")

	return nil
}

// initDeduplicationEngine initializes Redis-based deduplication
func (p *KafkaProcessor) initDeduplicationEngine() error {
	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     p.config.Redis.Host + ":" + strconv.Itoa(p.config.Redis.Port),
		Password: p.config.Redis.Password,
		DB:       p.config.Redis.Database,
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	p.dedupEngine = deduplication.NewEngine(
		redisClient,
		p.pipelineConfig.Processing.Deduplication.Key,
		p.pipelineConfig.Processing.Deduplication.TimeWindow,
	)

	p.logger.WithFields(logrus.Fields{
		"redis_host":    p.config.Redis.Host,
		"dedup_key":     p.pipelineConfig.Processing.Deduplication.Key,
		"time_window":   p.pipelineConfig.Processing.Deduplication.TimeWindow,
	}).Info("Deduplication engine initialized")

	return nil
}

// initClickHouseSink initializes ClickHouse batch sink
func (p *KafkaProcessor) initClickHouseSink() error {
	// Connect to ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{p.config.ClickHouse.Host + ":" + strconv.Itoa(p.config.ClickHouse.Port)},
		Auth: clickhouse.Auth{
			Database: p.config.ClickHouse.Database,
			Username: p.config.ClickHouse.Username,
			Password: p.config.ClickHouse.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// Initialize batch sink
	batchConfig := chsink.BatchConfig{
		MaxBatchSize:  1000,
		FlushInterval: 10 * time.Second,
		MaxRetries:    3,
		RetryDelay:    1 * time.Second,
	}

	p.clickSink = chsink.NewBatchSink(conn, p.pipelineConfig.Sink.Table, batchConfig)

	p.logger.WithFields(logrus.Fields{
		"clickhouse_host":  p.config.ClickHouse.Host,
		"database":         p.config.ClickHouse.Database,
		"table":            p.pipelineConfig.Sink.Table,
		"batch_size":       batchConfig.MaxBatchSize,
		"flush_interval":   batchConfig.FlushInterval,
	}).Info("ClickHouse sink initialized")

	return nil
}

// Start begins processing Kafka messages
func (p *KafkaProcessor) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&p.isRunning, 0, 1) {
		return fmt.Errorf("processor is already running")
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.logger.WithFields(logrus.Fields{
		"pipeline":     p.pipelineConfig.Name,
		"topic":        p.pipelineConfig.Source.Topic,
		"group_id":     fmt.Sprintf("gflow-etl-%s", p.pipelineConfig.Name),
	}).Info("Starting Kafka processor - beginning message consumption")

	// Start message processing loop and wait for it to complete
	p.wg.Add(1)
	go p.messageProcessingLoop()

	// Wait for context cancellation or processing to complete
	p.wg.Wait()
	
	return nil
}

// messageProcessingLoop is the main processing loop
func (p *KafkaProcessor) messageProcessingLoop() {
	defer p.wg.Done()
	defer func() {
		atomic.StoreInt32(&p.isRunning, 0)
	}()

	p.logger.Info("Kafka message processing loop started")

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Message processing loop stopped due to context cancellation")
			return
		default:
			// Read message from Kafka
			message, err := p.reader.FetchMessage(p.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				p.logger.WithError(err).Error("Failed to fetch Kafka message")
				p.incrementErrorCount()
				continue
			}

			// Process the message
			if err := p.processMessage(p.ctx, message); err != nil {
				p.logger.WithFields(logrus.Fields{
					"topic":     message.Topic,
					"partition": message.Partition,
					"offset":    message.Offset,
					"error":     err,
				}).Error("Failed to process message")
				p.incrementErrorCount()
				// Don't commit offset on error
				continue
			}

			// Commit offset after successful processing
			if err := p.reader.CommitMessages(p.ctx, message); err != nil {
				p.logger.WithError(err).Error("Failed to commit Kafka offset")
			}

			p.incrementProcessedCount()
		}
	}
}

// processMessage processes a single Kafka message
func (p *KafkaProcessor) processMessage(ctx context.Context, message kafka.Message) error {
	startTime := time.Now()

	p.logger.WithFields(logrus.Fields{
		"topic":        message.Topic,
		"partition":    message.Partition,
		"offset":       message.Offset,
		"key":          string(message.Key),
		"message_size": len(message.Value),
		"timestamp":    message.Time,
	}).Debug("Processing Kafka message")

	// Parse JSON message
	var eventData map[string]interface{}
	if err := json.Unmarshal(message.Value, &eventData); err != nil {
		return fmt.Errorf("failed to unmarshal message JSON: %w", err)
	}

	// Add Kafka metadata to event
	eventData["_kafka_topic"] = message.Topic
	eventData["_kafka_partition"] = message.Partition
	eventData["_kafka_offset"] = message.Offset
	eventData["_kafka_timestamp"] = message.Time
	eventData["_processed_at"] = time.Now().UTC()

	// Apply deduplication if enabled
	if p.dedupEngine != nil {
		isDuplicate, err := p.processDeduplication(ctx, eventData)
		if err != nil {
			return fmt.Errorf("deduplication failed: %w", err)
		}
		if isDuplicate {
			p.logger.WithFields(logrus.Fields{
				"event_id": eventData["id"],
				"dedup_key": p.pipelineConfig.Processing.Deduplication.Key,
			}).Info("Event blocked as duplicate")
			p.incrementDedupedCount()
			return nil // Successfully processed (blocked)
		}
	}

	// Apply transformations (if any)
	p.applyTransformations(eventData)

	// Send to sink
	if err := p.sendToSink(ctx, eventData); err != nil {
		return fmt.Errorf("failed to send to sink: %w", err)
	}

	processingDuration := time.Since(startTime)
	p.logger.WithFields(logrus.Fields{
		"event_id":           eventData["id"],
		"processing_duration": processingDuration,
		"sink_type":          p.pipelineConfig.Sink.Type,
	}).Debug("Message processed successfully")

	return nil
}

// processDeduplication handles deduplication logic
func (p *KafkaProcessor) processDeduplication(ctx context.Context, eventData map[string]interface{}) (bool, error) {
	// Create deduplication event
	event := &deduplication.Event{
		ID:        fmt.Sprintf("%v", eventData["id"]),
		Timestamp: time.Now(),
		Data:      eventData,
		Source:    p.pipelineConfig.Source.Topic,
	}

	result, err := p.dedupEngine.ProcessEvent(ctx, event)
	if err != nil {
		return false, err
	}

	p.logger.WithFields(logrus.Fields{
		"event_id":       event.ID,
		"is_duplicate":   result.IsDuplicate,
		"first_seen":     result.FirstSeen,
		"dedup_key":      result.KeyValue,
	}).Debug("Deduplication result")

	return result.IsDuplicate, nil
}

// applyTransformations applies configured transformations
func (p *KafkaProcessor) applyTransformations(eventData map[string]interface{}) {
	// Add pipeline metadata
	eventData["_pipeline_name"] = p.pipelineConfig.Name
	eventData["_pipeline_version"] = "1.0"

	// Apply any configured transformation rules
	if p.pipelineConfig.Processing.Transformation != nil && p.pipelineConfig.Processing.Transformation.Enabled {
		for _, rule := range p.pipelineConfig.Processing.Transformation.Rules {
			p.applyTransformRule(eventData, rule)
		}
	}
}

// applyTransformRule applies a single transformation rule
func (p *KafkaProcessor) applyTransformRule(eventData map[string]interface{}, rule TransformRule) {
	switch rule.Type {
	case "rename":
		if value, exists := eventData[rule.Field]; exists {
			eventData[rule.Value] = value
			delete(eventData, rule.Field)
		}
	case "default":
		if _, exists := eventData[rule.Field]; !exists {
			eventData[rule.Field] = rule.Value
		}
	case "cast":
		// Implement type casting logic
		p.castFieldType(eventData, rule.Field, rule.Value)
	}
}

// castFieldType handles type casting
func (p *KafkaProcessor) castFieldType(eventData map[string]interface{}, field, targetType string) {
	value, exists := eventData[field]
	if !exists {
		return
	}

	switch targetType {
	case "string":
		eventData[field] = fmt.Sprintf("%v", value)
	case "int":
		if str, ok := value.(string); ok {
			if intVal, err := json.Number(str).Int64(); err == nil {
				eventData[field] = intVal
			}
		}
	case "float":
		if str, ok := value.(string); ok {
			if floatVal, err := json.Number(str).Float64(); err == nil {
				eventData[field] = floatVal
			}
		}
	}
}

// sendToSink sends processed event to the configured sink
func (p *KafkaProcessor) sendToSink(ctx context.Context, eventData map[string]interface{}) error {
	switch p.pipelineConfig.Sink.Type {
	case "clickhouse":
		if p.clickSink == nil {
			return fmt.Errorf("ClickHouse sink not initialized")
		}
		return p.clickSink.WriteEvent(ctx, eventData)
	case "kafka":
		return p.sendToKafka(ctx, eventData)
	default:
		return fmt.Errorf("unsupported sink type: %s", p.pipelineConfig.Sink.Type)
	}
}

// sendToKafka sends event to another Kafka topic
func (p *KafkaProcessor) sendToKafka(ctx context.Context, eventData map[string]interface{}) error {
	if p.writer == nil {
		// Initialize Kafka writer for output
		p.writer = &kafka.Writer{
			Addr:     kafka.TCP(p.config.Kafka.Brokers...),
			Topic:    p.pipelineConfig.Sink.Topic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	messageBytes, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event for Kafka: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%v", eventData["id"])),
		Value: messageBytes,
	})
}

// Stop stops the processor
func (p *KafkaProcessor) Stop() error {
	if !atomic.CompareAndSwapInt32(&p.isRunning, 1, 0) {
		return nil // Already stopped
	}

	p.logger.Info("Stopping Kafka processor")

	// Cancel context
	if p.cancel != nil {
		p.cancel()
	}

	// Wait for processing to finish
	p.wg.Wait()

	// Close Kafka connections
	if p.reader != nil {
		if err := p.reader.Close(); err != nil {
			p.logger.WithError(err).Error("Failed to close Kafka reader")
		}
	}

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			p.logger.WithError(err).Error("Failed to close Kafka writer")
		}
	}

	// Close ClickHouse sink
	if p.clickSink != nil {
		if err := p.clickSink.Close(context.Background()); err != nil {
			p.logger.WithError(err).Error("Failed to close ClickHouse sink")
		}
	}

	p.logger.Info("Kafka processor stopped successfully")
	return nil
}

// GetMetrics returns current processor metrics
func (p *KafkaProcessor) GetMetrics() *Metrics {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	// Calculate throughput
	duration := time.Since(p.metrics.StartTime)
	if duration > 0 {
		p.metrics.ThroughputPerSec = float64(p.metrics.EventsProcessed) / duration.Seconds()
	}

	return &Metrics{
		EventsProcessed:   p.metrics.EventsProcessed,
		EventsDeduped:     p.metrics.EventsDeduped,
		EventsJoined:      p.metrics.EventsJoined,
		EventsSunk:        p.metrics.EventsSunk,
		ErrorCount:        p.metrics.ErrorCount,
		ThroughputPerSec:  p.metrics.ThroughputPerSec,
		LastProcessedTime: p.metrics.LastProcessedTime,
		StartTime:         p.metrics.StartTime,
	}
}

// Metric helper functions
func (p *KafkaProcessor) incrementProcessedCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.EventsProcessed++
	p.metrics.EventsSunk++
	p.metrics.LastProcessedTime = time.Now()
}

func (p *KafkaProcessor) incrementDedupedCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.EventsDeduped++
}

func (p *KafkaProcessor) incrementErrorCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.ErrorCount++
}