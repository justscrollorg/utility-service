package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"
)

// BatchSink handles optimized batch inserts to ClickHouse
// Similar to GlassFlow's ClickHouse sink with batching
type BatchSink struct {
	conn       clickhouse.Conn
	logger     *logrus.Logger
	tableName  string
	batchSize  int
	flushTimer time.Duration
	maxRetries int

	// Batching state
	batch       []map[string]interface{}
	batchMutex  sync.Mutex
	flushTicker *time.Ticker
	stopChan    chan struct{}

	// Metrics
	metrics      *SinkMetrics
	metricsMutex sync.RWMutex
}

// SinkMetrics tracks ClickHouse sink performance
type SinkMetrics struct {
	EventsReceived   int64
	EventsBatched    int64
	BatchesFlushed   int64
	EventsInserted   int64
	InsertErrors     int64
	RetryCount       int64
	FlushTimeouts    int64
	AvgBatchSize     float64
	AvgFlushDuration time.Duration
	LastFlushTime    time.Time
	ConnectionErrors int64
}

// BatchConfig configures batching behavior
type BatchConfig struct {
	MaxBatchSize  int           `json:"max_batch_size"`
	FlushInterval time.Duration `json:"flush_interval"`
	MaxRetries    int           `json:"max_retries"`
	RetryDelay    time.Duration `json:"retry_delay"`
}

// TableSchema represents the ClickHouse table structure
type TableSchema struct {
	Name    string                `json:"name"`
	Columns map[string]ColumnType `json:"columns"`
	Engine  string                `json:"engine"`
	OrderBy []string              `json:"order_by"`
	TTL     string                `json:"ttl,omitempty"`
}

// ColumnType represents a ClickHouse column type
type ColumnType struct {
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default,omitempty"`
}

// NewBatchSink creates a new ClickHouse batch sink
func NewBatchSink(conn clickhouse.Conn, tableName string, config BatchConfig) *BatchSink {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	sink := &BatchSink{
		conn:       conn,
		logger:     logger,
		tableName:  tableName,
		batchSize:  config.MaxBatchSize,
		flushTimer: config.FlushInterval,
		maxRetries: config.MaxRetries,
		batch:      make([]map[string]interface{}, 0, config.MaxBatchSize),
		stopChan:   make(chan struct{}),
		metrics:    &SinkMetrics{},
	}

	// Start flush timer
	sink.startFlushTimer()

	return sink
}

// WriteEvent adds an event to the batch
func (s *BatchSink) WriteEvent(ctx context.Context, event map[string]interface{}) error {
	s.batchMutex.Lock()
	defer s.batchMutex.Unlock()

	// Log event details for tracing
	if eventID, exists := event["id"]; exists {
		s.logger.WithFields(logrus.Fields{
			"table":      s.tableName,
			"event_id":   eventID,
			"batch_size": len(s.batch),
			"max_batch":  s.batchSize,
		}).Debug("Received event for batching")
	}

	// Add event to batch
	s.batch = append(s.batch, event)

	s.metricsMutex.Lock()
	s.metrics.EventsReceived++
	s.metrics.EventsBatched++
	s.metricsMutex.Unlock()

	// Check if batch is full
	if len(s.batch) >= s.batchSize {
		s.logger.WithFields(logrus.Fields{
			"table":      s.tableName,
			"batch_size": len(s.batch),
		}).Info("Batch full - triggering immediate flush")
		
		go s.flushBatch(ctx)
	}

	return nil
}

// WriteBatch writes multiple events at once
func (s *BatchSink) WriteBatch(ctx context.Context, events []map[string]interface{}) error {
	for _, event := range events {
		if err := s.WriteEvent(ctx, event); err != nil {
			return fmt.Errorf("failed to write event to batch: %w", err)
		}
	}
	return nil
}

// flushBatch flushes the current batch to ClickHouse
func (s *BatchSink) flushBatch(ctx context.Context) error {
	s.batchMutex.Lock()
	if len(s.batch) == 0 {
		s.batchMutex.Unlock()
		s.logger.Debug("Flush called but batch is empty")
		return nil
	}

	// Copy batch and reset
	batchToFlush := make([]map[string]interface{}, len(s.batch))
	copy(batchToFlush, s.batch)
	s.batch = s.batch[:0] // Reset slice but keep capacity
	batchSize := len(batchToFlush)
	s.batchMutex.Unlock()

	s.logger.WithFields(logrus.Fields{
		"table":      s.tableName,
		"batch_size": batchSize,
		"max_retries": s.maxRetries,
	}).Info("Starting batch flush to ClickHouse")

	startTime := time.Now()

	// Retry logic with exponential backoff
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoffDelay := time.Duration(attempt*attempt) * time.Second
			s.logger.WithFields(logrus.Fields{
				"attempt": attempt,
				"delay":   backoffDelay,
			}).Warn("Retrying batch flush after failure")
			
			time.Sleep(backoffDelay)

			s.metricsMutex.Lock()
			s.metrics.RetryCount++
			s.metricsMutex.Unlock()
		}

		err := s.executeBatchInsert(ctx, batchToFlush)
		if err == nil {
			// Success
			flushDuration := time.Since(startTime)

			s.metricsMutex.Lock()
			s.metrics.BatchesFlushed++
			s.metrics.EventsInserted += int64(batchSize)
			s.metrics.LastFlushTime = time.Now()
			s.metrics.AvgFlushDuration = (s.metrics.AvgFlushDuration + flushDuration) / 2
			s.metrics.AvgBatchSize = (s.metrics.AvgBatchSize + float64(batchSize)) / 2
			s.metricsMutex.Unlock()

			s.logger.WithFields(logrus.Fields{
				"table":          s.tableName,
				"batch_size":     batchSize,
				"flush_duration": flushDuration,
				"attempt":        attempt + 1,
				"events_total":   s.metrics.EventsInserted,
			}).Info("Successfully flushed batch to ClickHouse")

			return nil
		}

		lastErr = err
		s.logger.WithFields(logrus.Fields{
			"table":   s.tableName,
			"attempt": attempt + 1,
			"error":   err,
		}).Warn("Failed to flush batch, retrying...")
	}

	// All retries failed
	s.metricsMutex.Lock()
	s.metrics.InsertErrors++
	s.metricsMutex.Unlock()

	s.logger.WithFields(logrus.Fields{
		"batch_size": batchSize,
		"attempts":   s.maxRetries + 1,
		"error":      lastErr,
	}).Error("Failed to flush batch after all retries")

	return fmt.Errorf("failed to flush batch after %d attempts: %w", s.maxRetries+1, lastErr)
}

// executeBatchInsert performs the actual batch insert to ClickHouse
func (s *BatchSink) executeBatchInsert(ctx context.Context, batch []map[string]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	// Build dynamic insert query based on first event structure
	columns := make([]string, 0)
	placeholders := make([]string, 0)

	for column := range batch[0] {
		columns = append(columns, column)
		placeholders = append(placeholders, "?")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		s.tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// Prepare batch insert
	batchInsert, err := s.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch insert: %w", err)
	}
	defer batchInsert.Abort()

	// Add rows to batch
	for _, event := range batch {
		values := make([]interface{}, len(columns))
		for i, column := range columns {
			if value, exists := event[column]; exists {
				values[i] = s.convertValue(value)
			} else {
				values[i] = nil
			}
		}

		if err := batchInsert.Append(values...); err != nil {
			return fmt.Errorf("failed to append row to batch: %w", err)
		}
	}

	// Execute batch
	if err := batchInsert.Send(); err != nil {
		s.metricsMutex.Lock()
		s.metrics.ConnectionErrors++
		s.metricsMutex.Unlock()
		return fmt.Errorf("failed to send batch insert: %w", err)
	}

	return nil
}

// convertValue converts Go values to ClickHouse-compatible types
func (s *BatchSink) convertValue(value interface{}) interface{} {
	switch v := value.(type) {
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	case map[string]interface{}:
		// Convert nested objects to JSON strings for ClickHouse JSON columns
		return fmt.Sprintf("%v", v)
	case []interface{}:
		// Convert arrays to ClickHouse array format
		return v
	default:
		return v
	}
}

// startFlushTimer starts the periodic flush timer
func (s *BatchSink) startFlushTimer() {
	s.flushTicker = time.NewTicker(s.flushTimer)

	go func() {
		for {
			select {
			case <-s.flushTicker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := s.flushBatch(ctx); err != nil {
					s.logger.WithError(err).Error("Timer-triggered batch flush failed")
					s.metricsMutex.Lock()
					s.metrics.FlushTimeouts++
					s.metricsMutex.Unlock()
				}
				cancel()
			case <-s.stopChan:
				return
			}
		}
	}()
}

// ForceFlush immediately flushes any pending events
func (s *BatchSink) ForceFlush(ctx context.Context) error {
	return s.flushBatch(ctx)
}

// CreateTable creates a ClickHouse table with the given schema
func (s *BatchSink) CreateTable(ctx context.Context, schema TableSchema) error {
	// Build column definitions
	columnDefs := make([]string, 0, len(schema.Columns))
	for name, colType := range schema.Columns {
		colDef := fmt.Sprintf("%s %s", name, colType.Type)
		if !colType.Nullable {
			colDef += " NOT NULL"
		}
		if colType.Default != "" {
			colDef += fmt.Sprintf(" DEFAULT %s", colType.Default)
		}
		columnDefs = append(columnDefs, colDef)
	}

	// Build CREATE TABLE query
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = %s",
		schema.Name,
		strings.Join(columnDefs, ", "),
		schema.Engine,
	)

	// Add ORDER BY clause
	if len(schema.OrderBy) > 0 {
		query += fmt.Sprintf(" ORDER BY (%s)", strings.Join(schema.OrderBy, ", "))
	}

	// Add TTL if specified
	if schema.TTL != "" {
		query += fmt.Sprintf(" TTL %s", schema.TTL)
	}

	s.logger.WithField("query", query).Info("Creating ClickHouse table")

	if err := s.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// GetMetrics returns current sink metrics
func (s *BatchSink) GetMetrics() *SinkMetrics {
	s.metricsMutex.RLock()
	defer s.metricsMutex.RUnlock()

	return &SinkMetrics{
		EventsReceived:   s.metrics.EventsReceived,
		EventsBatched:    s.metrics.EventsBatched,
		BatchesFlushed:   s.metrics.BatchesFlushed,
		EventsInserted:   s.metrics.EventsInserted,
		InsertErrors:     s.metrics.InsertErrors,
		RetryCount:       s.metrics.RetryCount,
		FlushTimeouts:    s.metrics.FlushTimeouts,
		AvgBatchSize:     s.metrics.AvgBatchSize,
		AvgFlushDuration: s.metrics.AvgFlushDuration,
		LastFlushTime:    s.metrics.LastFlushTime,
		ConnectionErrors: s.metrics.ConnectionErrors,
	}
}

// GetStats returns summary statistics
func (s *BatchSink) GetStats() map[string]interface{} {
	metrics := s.GetMetrics()

	successRate := float64(100)
	if metrics.EventsReceived > 0 {
		successRate = float64(metrics.EventsInserted) / float64(metrics.EventsReceived) * 100
	}

	return map[string]interface{}{
		"events_received":      metrics.EventsReceived,
		"events_inserted":      metrics.EventsInserted,
		"batches_flushed":      metrics.BatchesFlushed,
		"success_rate_percent": successRate,
		"avg_batch_size":       metrics.AvgBatchSize,
		"avg_flush_duration":   metrics.AvgFlushDuration.String(),
		"last_flush_time":      metrics.LastFlushTime,
		"error_count":          metrics.InsertErrors,
		"retry_count":          metrics.RetryCount,
		"pending_events":       len(s.batch),
	}
}

// ResetMetrics resets all metrics counters
func (s *BatchSink) ResetMetrics() {
	s.metricsMutex.Lock()
	defer s.metricsMutex.Unlock()

	s.metrics = &SinkMetrics{}
}

// Close gracefully shuts down the sink
func (s *BatchSink) Close(ctx context.Context) error {
	s.logger.Info("Shutting down ClickHouse sink...")

	// Stop flush timer
	if s.flushTicker != nil {
		s.flushTicker.Stop()
	}
	close(s.stopChan)

	// Flush any remaining events
	if err := s.ForceFlush(ctx); err != nil {
		s.logger.WithError(err).Error("Failed to flush remaining events during shutdown")
	}

	// Close ClickHouse connection
	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("failed to close ClickHouse connection: %w", err)
	}

	s.logger.Info("ClickHouse sink shutdown complete")
	return nil
}

// CreateDemoTables creates tables for demo scenarios
func CreateDemoTables(ctx context.Context, conn clickhouse.Conn) error {
	tables := []TableSchema{
		{
			Name: "user_sessions_clean",
			Columns: map[string]ColumnType{
				"session_id":       {Type: "String", Nullable: false},
				"user_id":          {Type: "String", Nullable: false},
				"timestamp":        {Type: "DateTime", Nullable: false},
				"page_url":         {Type: "String", Nullable: true},
				"user_agent":       {Type: "String", Nullable: true},
				"ip_address":       {Type: "String", Nullable: true},
				"session_duration": {Type: "UInt32", Nullable: true},
				"page_views":       {Type: "UInt16", Nullable: true, Default: "1"},
			},
			Engine:  "MergeTree()",
			OrderBy: []string{"timestamp", "session_id"},
			TTL:     "timestamp + INTERVAL 90 DAY",
		},
		{
			Name: "enriched_transactions",
			Columns: map[string]ColumnType{
				"transaction_id": {Type: "String", Nullable: false},
				"user_id":        {Type: "String", Nullable: false},
				"timestamp":      {Type: "DateTime", Nullable: false},
				"amount":         {Type: "Decimal(10,2)", Nullable: false},
				"currency":       {Type: "String", Nullable: false, Default: "'USD'"},
				"merchant_name":  {Type: "String", Nullable: true},
				"category":       {Type: "String", Nullable: true},
				// User profile data from join
				"user_name":    {Type: "String", Nullable: true},
				"user_email":   {Type: "String", Nullable: true},
				"user_tier":    {Type: "String", Nullable: true},
				"user_country": {Type: "String", Nullable: true},
			},
			Engine:  "MergeTree()",
			OrderBy: []string{"timestamp", "user_id"},
			TTL:     "timestamp + INTERVAL 7 YEAR", // Financial data retention
		},
		{
			Name: "real_time_analytics",
			Columns: map[string]ColumnType{
				"event_id":    {Type: "String", Nullable: false},
				"event_type":  {Type: "String", Nullable: false},
				"timestamp":   {Type: "DateTime", Nullable: false},
				"user_id":     {Type: "String", Nullable: true},
				"session_id":  {Type: "String", Nullable: true},
				"properties":  {Type: "String", Nullable: true}, // JSON string
				"device_type": {Type: "String", Nullable: true},
				"platform":    {Type: "String", Nullable: true},
				"country":     {Type: "String", Nullable: true},
				"value":       {Type: "Float64", Nullable: true},
			},
			Engine:  "MergeTree()",
			OrderBy: []string{"timestamp", "event_type"},
			TTL:     "timestamp + INTERVAL 30 DAY",
		},
	}

	for _, table := range tables {
		sink := &BatchSink{conn: conn, logger: logrus.New()}
		if err := sink.CreateTable(ctx, table); err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.Name, err)
		}
	}

	return nil
}
