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
	"github.com/justscrollorg/gflow-etl/internal/join"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// JoinProcessor implements temporal stream joining with configurable time windows
type JoinProcessor struct {
	config         *config.Config
	pipelineConfig *Config
	logger         *logrus.Logger

	// Kafka components
	leftReader  *kafka.Reader
	rightReader *kafka.Reader
	writer      *kafka.Writer

	// Processing components
	joinEngine  *join.TemporalJoinEngine
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

// NewTemporalJoinProcessor creates a temporal join processor with state management
func NewTemporalJoinProcessor(globalConfig *config.Config, pipelineConfig *Config) (*JoinProcessor, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	if pipelineConfig.Source.Join == nil {
		return nil, fmt.Errorf("join configuration is required for kafka-join source type")
	}

	processor := &JoinProcessor{
		config:         globalConfig,
		pipelineConfig: pipelineConfig,
		logger:         logger,
		metrics: &Metrics{
			StartTime: time.Now(),
		},
	}

	// Initialize Kafka readers for both streams
	if err := processor.initKafkaReaders(); err != nil {
		return nil, fmt.Errorf("failed to initialize Kafka readers: %w", err)
	}

	// Initialize join engine
	if err := processor.initJoinEngine(); err != nil {
		return nil, fmt.Errorf("failed to initialize join engine: %w", err)
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
		"pipeline":        pipelineConfig.Name,
		"left_topic":      pipelineConfig.Source.Join.LeftTopic,
		"right_topic":     pipelineConfig.Source.Join.RightTopic,
		"join_key":        pipelineConfig.Source.Join.JoinKey,
		"join_type":       pipelineConfig.Source.Join.JoinType,
		"time_window":     pipelineConfig.Source.Join.TimeWindow,
		"sink_type":       pipelineConfig.Sink.Type,
		"dedup_enabled":   pipelineConfig.Processing.Deduplication != nil && pipelineConfig.Processing.Deduplication.Enabled,
	}).Info("Temporal join processor initialized successfully")

	return processor, nil
}

// initKafkaReaders initializes both left and right stream readers
func (p *JoinProcessor) initKafkaReaders() error {
	joinConfig := p.pipelineConfig.Source.Join

	// Left stream reader
	leftReaderConfig := kafka.ReaderConfig{
		Brokers: p.config.Kafka.Brokers,
		Topic:   joinConfig.LeftTopic,
		GroupID: fmt.Sprintf("gflow-etl-%s-left", p.pipelineConfig.Name),
		
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
		
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			p.logger.Errorf("Kafka left reader error: "+msg, args...)
		}),
	}

	p.leftReader = kafka.NewReader(leftReaderConfig)

	// Right stream reader
	rightReaderConfig := kafka.ReaderConfig{
		Brokers: p.config.Kafka.Brokers,
		Topic:   joinConfig.RightTopic,
		GroupID: fmt.Sprintf("gflow-etl-%s-right", p.pipelineConfig.Name),
		
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     1 * time.Second,
		StartOffset: kafka.LastOffset,
		
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			p.logger.Errorf("Kafka right reader error: "+msg, args...)
		}),
	}

	p.rightReader = kafka.NewReader(rightReaderConfig)

	p.logger.WithFields(logrus.Fields{
		"left_topic":   joinConfig.LeftTopic,
		"right_topic":  joinConfig.RightTopic,
		"left_group":   leftReaderConfig.GroupID,
		"right_group":  rightReaderConfig.GroupID,
		"brokers":      p.config.Kafka.Brokers,
	}).Info("Kafka readers for join streams initialized")

	return nil
}

// initJoinEngine initializes the temporal join engine
func (p *JoinProcessor) initJoinEngine() error {
	joinConfig := p.pipelineConfig.Source.Join

	// Connect to Redis for join state storage
	redisClient := redis.NewClient(&redis.Options{
		Addr:     p.config.Redis.Host + ":" + strconv.Itoa(p.config.Redis.Port),
		Password: p.config.Redis.Password,
		DB:       p.config.Redis.Database,
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis for join engine: %w", err)
	}

	p.joinEngine = join.NewTemporalJoinEngine(
		redisClient,
		joinConfig.JoinKey,
		joinConfig.TimeWindow,
		joinConfig.LeftTopic,
		joinConfig.RightTopic,
	)

	p.logger.WithFields(logrus.Fields{
		"join_key":     joinConfig.JoinKey,
		"join_type":    joinConfig.JoinType,
		"time_window":  joinConfig.TimeWindow,
		"redis_host":   p.config.Redis.Host,
	}).Info("Temporal join engine initialized")

	return nil
}

// initDeduplicationEngine initializes Redis-based deduplication
func (p *JoinProcessor) initDeduplicationEngine() error {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     p.config.Redis.Host + ":" + strconv.Itoa(p.config.Redis.Port),
		Password: p.config.Redis.Password,
		DB:       p.config.Redis.Database,
	})

	p.dedupEngine = deduplication.NewEngine(
		redisClient,
		p.pipelineConfig.Processing.Deduplication.Key,
		p.pipelineConfig.Processing.Deduplication.TimeWindow,
	)

	p.logger.WithFields(logrus.Fields{
		"dedup_key":   p.pipelineConfig.Processing.Deduplication.Key,
		"time_window": p.pipelineConfig.Processing.Deduplication.TimeWindow,
	}).Info("Deduplication engine for join processor initialized")

	return nil
}

// initClickHouseSink initializes ClickHouse batch sink
func (p *JoinProcessor) initClickHouseSink() error {
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

	if err := conn.Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	batchConfig := chsink.BatchConfig{
		MaxBatchSize:  1000,
		FlushInterval: 10 * time.Second,
		MaxRetries:    3,
		RetryDelay:    1 * time.Second,
	}

	p.clickSink = chsink.NewBatchSink(conn, p.pipelineConfig.Sink.Table, batchConfig)

	p.logger.WithFields(logrus.Fields{
		"clickhouse_host": p.config.ClickHouse.Host,
		"database":        p.config.ClickHouse.Database,
		"table":           p.pipelineConfig.Sink.Table,
	}).Info("ClickHouse sink for join processor initialized")

	return nil
}

// Start begins processing both Kafka streams for joining
func (p *JoinProcessor) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&p.isRunning, 0, 1) {
		return fmt.Errorf("join processor is already running")
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.logger.WithFields(logrus.Fields{
		"pipeline":     p.pipelineConfig.Name,
		"left_topic":   p.pipelineConfig.Source.Join.LeftTopic,
		"right_topic":  p.pipelineConfig.Source.Join.RightTopic,
	}).Info("Starting join processor - beginning stream consumption")

	// Start processing loops for both streams
	p.wg.Add(2)
	go p.processLeftStream()
	go p.processRightStream()

	return nil
}

// processLeftStream processes messages from the left stream
func (p *JoinProcessor) processLeftStream() {
	defer p.wg.Done()
	p.logger.Info("Left stream processing loop started")

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Left stream processing stopped due to context cancellation")
			return
		default:
			message, err := p.leftReader.FetchMessage(p.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				p.logger.WithError(err).Error("Failed to fetch message from left stream")
				p.incrementErrorCount()
				continue
			}

			if err := p.processJoinMessage(p.ctx, message, "left"); err != nil {
				p.logger.WithFields(logrus.Fields{
					"topic":     message.Topic,
					"partition": message.Partition,
					"offset":    message.Offset,
					"stream":    "left",
					"error":     err,
				}).Error("Failed to process left stream message")
				p.incrementErrorCount()
				continue
			}

			if err := p.leftReader.CommitMessages(p.ctx, message); err != nil {
				p.logger.WithError(err).Error("Failed to commit left stream offset")
			}
		}
	}
}

// processRightStream processes messages from the right stream
func (p *JoinProcessor) processRightStream() {
	defer p.wg.Done()
	p.logger.Info("Right stream processing loop started")

	for {
		select {
		case <-p.ctx.Done():
			p.logger.Info("Right stream processing stopped due to context cancellation")
			return
		default:
			message, err := p.rightReader.FetchMessage(p.ctx)
			if err != nil {
				if err == context.Canceled {
					return
				}
				p.logger.WithError(err).Error("Failed to fetch message from right stream")
				p.incrementErrorCount()
				continue
			}

			if err := p.processJoinMessage(p.ctx, message, "right"); err != nil {
				p.logger.WithFields(logrus.Fields{
					"topic":     message.Topic,
					"partition": message.Partition,
					"offset":    message.Offset,
					"stream":    "right",
					"error":     err,
				}).Error("Failed to process right stream message")
				p.incrementErrorCount()
				continue
			}

			if err := p.rightReader.CommitMessages(p.ctx, message); err != nil {
				p.logger.WithError(err).Error("Failed to commit right stream offset")
			}
		}
	}
}

// processJoinMessage processes a message for joining
func (p *JoinProcessor) processJoinMessage(ctx context.Context, message kafka.Message, streamSide string) error {
	startTime := time.Now()

	p.logger.WithFields(logrus.Fields{
		"topic":        message.Topic,
		"partition":    message.Partition,
		"offset":       message.Offset,
		"stream":       streamSide,
		"message_size": len(message.Value),
		"timestamp":    message.Time,
	}).Debug("Processing join message")

	// Parse JSON message
	var eventData map[string]interface{}
	if err := json.Unmarshal(message.Value, &eventData); err != nil {
		return fmt.Errorf("failed to unmarshal message JSON: %w", err)
	}

	// Create stream event for join engine
	streamEvent := &join.StreamEvent{
		ID:        fmt.Sprintf("%v", eventData["id"]),
		Timestamp: message.Time,
		Data:      eventData,
		Source:    streamSide,
	}

	// Process join based on stream side
	var joinResult *join.JoinResult
	var err error
	
	if streamSide == "left" {
		joinResult, err = p.joinEngine.ProcessLeftEvent(ctx, streamEvent)
	} else {
		joinResult, err = p.joinEngine.ProcessRightEvent(ctx, streamEvent)
	}
	
	if err != nil {
		return fmt.Errorf("join processing failed: %w", err)
	}

	p.incrementProcessedCount()

	// If join was successful, process the joined event
	if joinResult.IsJoined {
		p.logger.WithFields(logrus.Fields{
			"join_key":    streamEvent.JoinKey,
			"left_time":   joinResult.JoinedEvent.LeftTime,
			"right_time":  joinResult.JoinedEvent.RightTime,
			"join_delay":  joinResult.JoinedEvent.JoinDelay,
		}).Info("Successful join - processing joined event")

		if err := p.processJoinedEvent(ctx, joinResult.JoinedEvent); err != nil {
			return fmt.Errorf("failed to process joined event: %w", err)
		}

		p.incrementJoinedCount()
	} else {
		p.logger.WithFields(logrus.Fields{
			"event_id":       streamEvent.ID,
			"stream":         streamSide,
			"is_orphaned":    joinResult.IsOrphaned,
			"is_late_arrival": joinResult.IsLateArrival,
		}).Debug("Event stored for future join")
	}

	processingDuration := time.Since(startTime)
	p.logger.WithFields(logrus.Fields{
		"event_id":           streamEvent.ID,
		"stream":             streamSide,
		"processing_duration": processingDuration,
		"join_success":       joinResult.IsJoined,
	}).Debug("Join message processed")

	return nil
}

// processJoinedEvent processes a successfully joined event
func (p *JoinProcessor) processJoinedEvent(ctx context.Context, joinedEvent *join.JoinedEvent) error {
	// Create final event data
	eventData := map[string]interface{}{
		"joined_id":    joinedEvent.ID,
		"join_key":     joinedEvent.JoinKey,
		"left_data":    joinedEvent.LeftData,
		"right_data":   joinedEvent.RightData,
		"left_time":    joinedEvent.LeftTime,
		"right_time":   joinedEvent.RightTime,
		"join_delay":   joinedEvent.JoinDelay,
		"joined_at":    time.Now().UTC(),
		"pipeline":     p.pipelineConfig.Name,
	}

	// Apply deduplication if enabled
	if p.dedupEngine != nil {
		isDuplicate, err := p.processDeduplication(ctx, eventData)
		if err != nil {
			return fmt.Errorf("deduplication failed: %w", err)
		}
		if isDuplicate {
			p.logger.WithField("joined_id", joinedEvent.ID).Info("Joined event blocked as duplicate")
			p.incrementDedupedCount()
			return nil
		}
	}

	// Apply transformations
	p.applyTransformations(eventData)

	// Send to sink
	if err := p.sendToSink(ctx, eventData); err != nil {
		return fmt.Errorf("failed to send joined event to sink: %w", err)
	}

	return nil
}

// processDeduplication handles deduplication logic for joined events
func (p *JoinProcessor) processDeduplication(ctx context.Context, eventData map[string]interface{}) (bool, error) {
	event := &deduplication.Event{
		ID:        fmt.Sprintf("%v", eventData["joined_id"]),
		Timestamp: time.Now(),
		Data:      eventData,
		Source:    "join_result",
	}

	result, err := p.dedupEngine.ProcessEvent(ctx, event)
	if err != nil {
		return false, err
	}

	return result.IsDuplicate, nil
}

// applyTransformations applies configured transformations to joined events
func (p *JoinProcessor) applyTransformations(eventData map[string]interface{}) {
	eventData["_pipeline_name"] = p.pipelineConfig.Name
	eventData["_pipeline_type"] = "join"
	eventData["_processed_at"] = time.Now().UTC()

	if p.pipelineConfig.Processing.Transformation != nil && p.pipelineConfig.Processing.Transformation.Enabled {
		for _, rule := range p.pipelineConfig.Processing.Transformation.Rules {
			p.applyTransformRule(eventData, rule)
		}
	}
}

// applyTransformRule applies a single transformation rule
func (p *JoinProcessor) applyTransformRule(eventData map[string]interface{}, rule TransformRule) {
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
	}
}

// sendToSink sends processed joined event to the configured sink
func (p *JoinProcessor) sendToSink(ctx context.Context, eventData map[string]interface{}) error {
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

// sendToKafka sends joined event to another Kafka topic
func (p *JoinProcessor) sendToKafka(ctx context.Context, eventData map[string]interface{}) error {
	if p.writer == nil {
		p.writer = &kafka.Writer{
			Addr:     kafka.TCP(p.config.Kafka.Brokers...),
			Topic:    p.pipelineConfig.Sink.Topic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	messageBytes, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal joined event for Kafka: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(fmt.Sprintf("%v", eventData["joined_id"])),
		Value: messageBytes,
	})
}

// Stop stops the join processor
func (p *JoinProcessor) Stop() error {
	if !atomic.CompareAndSwapInt32(&p.isRunning, 1, 0) {
		return nil
	}

	p.logger.Info("Stopping join processor")

	if p.cancel != nil {
		p.cancel()
	}

	p.wg.Wait()

	// Close all connections
	if p.leftReader != nil {
		if err := p.leftReader.Close(); err != nil {
			p.logger.WithError(err).Error("Failed to close left stream reader")
		}
	}

	if p.rightReader != nil {
		if err := p.rightReader.Close(); err != nil {
			p.logger.WithError(err).Error("Failed to close right stream reader")
		}
	}

	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			p.logger.WithError(err).Error("Failed to close Kafka writer")
		}
	}

	if p.clickSink != nil {
		if err := p.clickSink.Close(context.Background()); err != nil {
			p.logger.WithError(err).Error("Failed to close ClickHouse sink")
		}
	}

	p.logger.Info("Join processor stopped successfully")
	return nil
}

// GetMetrics returns current join processor metrics
func (p *JoinProcessor) GetMetrics() *Metrics {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

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
func (p *JoinProcessor) incrementProcessedCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.EventsProcessed++
	p.metrics.LastProcessedTime = time.Now()
}

func (p *JoinProcessor) incrementJoinedCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.EventsJoined++
	p.metrics.EventsSunk++
}

func (p *JoinProcessor) incrementDedupedCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.EventsDeduped++
}

func (p *JoinProcessor) incrementErrorCount() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.metrics.ErrorCount++
}