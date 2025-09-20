package deduplication

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Engine handles deduplication logic similar to GlassFlow
type Engine struct {
	redis      *redis.Client
	logger     *logrus.Logger
	timeWindow time.Duration
	keyField   string
	metrics    *Metrics
	mutex      sync.RWMutex
}

// Metrics tracks deduplication performance
type Metrics struct {
	EventsProcessed   int64
	EventsDeduped     int64
	DuplicatesBlocked int64
	CacheHits         int64
	CacheMisses       int64
	ErrorCount        int64
	WindowCleanups    int64
}

// Event represents a data event with metadata
type Event struct {
	ID        string                 `json:"id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	Source    string                 `json:"source"`
}

// DedupResult represents the result of deduplication processing
type DedupResult struct {
	Event       *Event    `json:"event"`
	IsDuplicate bool      `json:"is_duplicate"`
	FirstSeen   time.Time `json:"first_seen"`
	KeyValue    string    `json:"key_value"`
}

// NewEngine creates a new deduplication engine
func NewEngine(redisClient *redis.Client, keyField string, timeWindow time.Duration) *Engine {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	return &Engine{
		redis:      redisClient,
		logger:     logger,
		timeWindow: timeWindow,
		keyField:   keyField,
		metrics:    &Metrics{},
	}
}

// ProcessEvent processes an event for deduplication
func (e *Engine) ProcessEvent(ctx context.Context, event *Event) (*DedupResult, error) {
	e.mutex.Lock()
	e.metrics.EventsProcessed++
	e.mutex.Unlock()

	// Extract the deduplication key from the event
	keyValue, err := e.extractKeyValue(event)
	if err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("failed to extract deduplication key: %w", err)
	}

	// Generate Redis key with time-based expiration
	redisKey := e.generateRedisKey(keyValue)

	// Check if we've seen this key within the time window
	isDuplicate, firstSeen, err := e.checkDuplicate(ctx, redisKey, event.Timestamp)
	if err != nil {
		e.incrementErrorCount()
		return nil, fmt.Errorf("failed to check for duplicate: %w", err)
	}

	result := &DedupResult{
		Event:       event,
		IsDuplicate: isDuplicate,
		FirstSeen:   firstSeen,
		KeyValue:    keyValue,
	}

	// Update metrics
	e.mutex.Lock()
	if isDuplicate {
		e.metrics.EventsDeduped++
		e.metrics.DuplicatesBlocked++
		e.metrics.CacheHits++
	} else {
		e.metrics.CacheMisses++
	}
	e.mutex.Unlock()

	e.logger.WithFields(logrus.Fields{
		"key_value":    keyValue,
		"is_duplicate": isDuplicate,
		"first_seen":   firstSeen,
		"event_time":   event.Timestamp,
	}).Debug("Processed deduplication event")

	return result, nil
}

// ProcessBatch processes multiple events in batch for better performance
func (e *Engine) ProcessBatch(ctx context.Context, events []*Event) ([]*DedupResult, error) {
	results := make([]*DedupResult, 0, len(events))

	// Process events in parallel for better throughput
	resultsChan := make(chan *DedupResult, len(events))
	errorsChan := make(chan error, len(events))

	// Use worker pool pattern similar to GlassFlow
	numWorkers := 10
	if len(events) < numWorkers {
		numWorkers = len(events)
	}

	eventsChan := make(chan *Event, len(events))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for event := range eventsChan {
				result, err := e.ProcessEvent(ctx, event)
				if err != nil {
					errorsChan <- err
					continue
				}
				resultsChan <- result
			}
		}()
	}

	// Send events to workers
	go func() {
		defer close(eventsChan)
		for _, event := range events {
			eventsChan <- event
		}
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultsChan)
		close(errorsChan)
	}()

	// Collect results
	for result := range resultsChan {
		results = append(results, result)
	}

	// Check for errors
	select {
	case err := <-errorsChan:
		return results, err
	default:
		return results, nil
	}
}

// extractKeyValue extracts the deduplication key value from the event
func (e *Engine) extractKeyValue(event *Event) (string, error) {
	value, exists := event.Data[e.keyField]
	if !exists {
		return "", fmt.Errorf("deduplication key field '%s' not found in event data", e.keyField)
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case int, int64, float64:
		return fmt.Sprintf("%v", v), nil
	default:
		// For complex types, marshal to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal key value to string: %w", err)
		}
		return string(jsonBytes), nil
	}
}

// generateRedisKey creates a time-aware Redis key for deduplication
func (e *Engine) generateRedisKey(keyValue string) string {
	// Use time bucket approach for efficient cleanup
	// Similar to GlassFlow's sliding window implementation
	timeBucket := time.Now().Truncate(time.Minute).Unix()
	return fmt.Sprintf("dedup:%s:%d:%s", e.keyField, timeBucket, keyValue)
}

// checkDuplicate checks if the key exists within the time window
func (e *Engine) checkDuplicate(ctx context.Context, redisKey string, eventTime time.Time) (bool, time.Time, error) {
	// First, try to get existing timestamp
	existingTimeStr, err := e.redis.Get(ctx, redisKey).Result()
	if err == redis.Nil {
		// Key doesn't exist, store it with expiration
		timeValue := eventTime.Format(time.RFC3339Nano)
		err = e.redis.SetEx(ctx, redisKey, timeValue, e.timeWindow).Err()
		if err != nil {
			return false, time.Time{}, fmt.Errorf("failed to store deduplication key: %w", err)
		}
		return false, eventTime, nil
	} else if err != nil {
		return false, time.Time{}, fmt.Errorf("failed to check deduplication key: %w", err)
	}

	// Key exists, parse the stored timestamp
	firstSeen, err := time.Parse(time.RFC3339Nano, existingTimeStr)
	if err != nil {
		return false, time.Time{}, fmt.Errorf("failed to parse stored timestamp: %w", err)
	}

	// Check if the event is within the time window
	if eventTime.Sub(firstSeen) <= e.timeWindow {
		return true, firstSeen, nil
	}

	// Event is outside the window, update the key
	timeValue := eventTime.Format(time.RFC3339Nano)
	err = e.redis.SetEx(ctx, redisKey, timeValue, e.timeWindow).Err()
	if err != nil {
		return false, time.Time{}, fmt.Errorf("failed to update deduplication key: %w", err)
	}

	return false, eventTime, nil
}

// CleanupExpiredKeys removes old deduplication keys (similar to GlassFlow's cleanup)
func (e *Engine) CleanupExpiredKeys(ctx context.Context) error {
	// Get all keys matching our pattern
	pattern := fmt.Sprintf("dedup:%s:*", e.keyField)
	keys, err := e.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get deduplication keys: %w", err)
	}

	// Use pipeline for efficient batch operations
	pipe := e.redis.Pipeline()
	cleanupCount := 0

	for _, key := range keys {
		// Check TTL, remove if expired
		ttl, err := e.redis.TTL(ctx, key).Result()
		if err != nil {
			continue
		}

		if ttl <= 0 {
			pipe.Del(ctx, key)
			cleanupCount++
		}
	}

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired keys: %w", err)
	}

	e.mutex.Lock()
	e.metrics.WindowCleanups++
	e.mutex.Unlock()

	e.logger.WithFields(logrus.Fields{
		"cleaned_keys": cleanupCount,
		"total_keys":   len(keys),
	}).Info("Cleaned up expired deduplication keys")

	return nil
}

// GetMetrics returns current deduplication metrics
func (e *Engine) GetMetrics() *Metrics {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// Return a copy to avoid race conditions
	return &Metrics{
		EventsProcessed:   e.metrics.EventsProcessed,
		EventsDeduped:     e.metrics.EventsDeduped,
		DuplicatesBlocked: e.metrics.DuplicatesBlocked,
		CacheHits:         e.metrics.CacheHits,
		CacheMisses:       e.metrics.CacheMisses,
		ErrorCount:        e.metrics.ErrorCount,
		WindowCleanups:    e.metrics.WindowCleanups,
	}
}

// ResetMetrics resets all metrics counters
func (e *Engine) ResetMetrics() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.metrics = &Metrics{}
}

// SetTimeWindow updates the deduplication time window
func (e *Engine) SetTimeWindow(timeWindow time.Duration) {
	// Enforce GlassFlow's 7-day maximum
	maxWindow := time.Hour * 24 * 7
	if timeWindow > maxWindow {
		timeWindow = maxWindow
	}

	e.mutex.Lock()
	e.timeWindow = timeWindow
	e.mutex.Unlock()

	e.logger.WithField("time_window", timeWindow).Info("Updated deduplication time window")
}

// SetLogLevel updates the logging level
func (e *Engine) SetLogLevel(level string) {
	switch level {
	case "debug":
		e.logger.SetLevel(logrus.DebugLevel)
	case "info":
		e.logger.SetLevel(logrus.InfoLevel)
	case "warn":
		e.logger.SetLevel(logrus.WarnLevel)
	case "error":
		e.logger.SetLevel(logrus.ErrorLevel)
	default:
		e.logger.SetLevel(logrus.InfoLevel)
	}
}

// incrementErrorCount safely increments the error counter
func (e *Engine) incrementErrorCount() {
	e.mutex.Lock()
	e.metrics.ErrorCount++
	e.mutex.Unlock()
}
