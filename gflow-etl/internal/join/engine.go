package join

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// TemporalJoinEngine handles temporal joins between two data streams
// Similar to GlassFlow's stateful join functionality
type TemporalJoinEngine struct {
	redis           *redis.Client
	logger          *logrus.Logger
	joinKey         string
	timeWindow      time.Duration
	leftStreamName  string
	rightStreamName string
	metrics         *JoinMetrics
	mutex           sync.RWMutex
}

// JoinMetrics tracks join performance
type JoinMetrics struct {
	LeftEventsProcessed   int64
	RightEventsProcessed  int64
	SuccessfulJoins       int64
	OrphanedLeftEvents    int64
	OrphanedRightEvents   int64
	LateArrivals          int64
	WindowCleanups        int64
	ErrorCount            int64
	StateStoreSize        int64
}

// StreamEvent represents an event from either left or right stream
type StreamEvent struct {
	ID        string                 `json:"id"`
	Timestamp time.Time             `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	Source    string                 `json:"source"` // "left" or "right"
	JoinKey   string                 `json:"join_key"`
}

// JoinedEvent represents the result of joining two events
type JoinedEvent struct {
	ID         string                 `json:"id"`
	Timestamp  time.Time             `json:"timestamp"`
	LeftData   map[string]interface{} `json:"left_data"`
	RightData  map[string]interface{} `json:"right_data"`
	JoinKey    string                 `json:"join_key"`
	LeftTime   time.Time             `json:"left_time"`
	RightTime  time.Time             `json:"right_time"`
	JoinDelay  time.Duration         `json:"join_delay"`
}

// JoinResult represents the result of processing a join
type JoinResult struct {
	JoinedEvent *JoinedEvent `json:"joined_event,omitempty"`
	IsJoined    bool         `json:"is_joined"`
	IsOrphaned  bool         `json:"is_orphaned"`
	IsLateArrival bool       `json:"is_late_arrival"`
}

// NewTemporalJoinEngine creates a new temporal join engine
func NewTemporalJoinEngine(
	redisClient *redis.Client,
	joinKey string,
	timeWindow time.Duration,
	leftStreamName, rightStreamName string,
) *TemporalJoinEngine {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	return &TemporalJoinEngine{
		redis:           redisClient,
		logger:          logger,
		joinKey:         joinKey,
		timeWindow:      timeWindow,
		leftStreamName:  leftStreamName,
		rightStreamName: rightStreamName,
		metrics:         &JoinMetrics{},
	}
}

// ProcessLeftEvent processes an event from the left stream
func (j *TemporalJoinEngine) ProcessLeftEvent(ctx context.Context, event *StreamEvent) (*JoinResult, error) {
	j.mutex.Lock()
	j.metrics.LeftEventsProcessed++
	j.mutex.Unlock()

	event.Source = "left"
	return j.processEvent(ctx, event, "right")
}

// ProcessRightEvent processes an event from the right stream
func (j *TemporalJoinEngine) ProcessRightEvent(ctx context.Context, event *StreamEvent) (*JoinResult, error) {
	j.mutex.Lock()
	j.metrics.RightEventsProcessed++
	j.mutex.Unlock()

	event.Source = "right"
	return j.processEvent(ctx, event, "left")
}

// processEvent handles the core join logic
func (j *TemporalJoinEngine) processEvent(ctx context.Context, event *StreamEvent, lookupSide string) (*JoinResult, error) {
	// Extract join key value
	joinKeyValue, err := j.extractJoinKeyValue(event)
	if err != nil {
		j.incrementErrorCount()
		return nil, fmt.Errorf("failed to extract join key: %w", err)
	}
	event.JoinKey = joinKeyValue

	// Check for existing matching event in the other stream
	matchingEvent, err := j.findMatchingEvent(ctx, joinKeyValue, lookupSide, event.Timestamp)
	if err != nil {
		j.incrementErrorCount()
		return nil, fmt.Errorf("failed to find matching event: %w", err)
	}

	if matchingEvent != nil {
		// We found a match, create joined event
		joinedEvent := j.createJoinedEvent(event, matchingEvent)
		
		// Remove the matching event from store since it's been joined
		if err := j.removeEventFromStore(ctx, joinKeyValue, lookupSide, matchingEvent.ID); err != nil {
			j.logger.WithError(err).Warn("Failed to remove joined event from store")
		}

		j.mutex.Lock()
		j.metrics.SuccessfulJoins++
		j.mutex.Unlock()

		j.logger.WithFields(logrus.Fields{
			"join_key":    joinKeyValue,
			"left_time":   joinedEvent.LeftTime,
			"right_time":  joinedEvent.RightTime,
			"join_delay":  joinedEvent.JoinDelay,
		}).Debug("Successfully joined events")

		return &JoinResult{
			JoinedEvent: joinedEvent,
			IsJoined:    true,
		}, nil
	}

	// No match found, store this event for future joins
	if err := j.storeEventForJoin(ctx, event); err != nil {
		j.incrementErrorCount()
		return nil, fmt.Errorf("failed to store event for join: %w", err)
	}

	// Check if this is a late arrival (outside time window)
	isLateArrival := j.isLateArrival(event.Timestamp)
	if isLateArrival {
		j.mutex.Lock()
		j.metrics.LateArrivals++
		j.mutex.Unlock()
	}

	// Update orphaned count
	j.mutex.Lock()
	if event.Source == "left" {
		j.metrics.OrphanedLeftEvents++
	} else {
		j.metrics.OrphanedRightEvents++
	}
	j.mutex.Unlock()

	return &JoinResult{
		IsJoined:      false,
		IsOrphaned:    true,
		IsLateArrival: isLateArrival,
	}, nil
}

// extractJoinKeyValue extracts the join key value from the event
func (j *TemporalJoinEngine) extractJoinKeyValue(event *StreamEvent) (string, error) {
	value, exists := event.Data[j.joinKey]
	if !exists {
		return "", fmt.Errorf("join key field '%s' not found in event data", j.joinKey)
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case int, int64, float64:
		return fmt.Sprintf("%v", v), nil
	default:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal join key value: %w", err)
		}
		return string(jsonBytes), nil
	}
}

// findMatchingEvent looks for a matching event in the other stream's buffer
func (j *TemporalJoinEngine) findMatchingEvent(ctx context.Context, joinKeyValue, streamSide string, eventTime time.Time) (*StreamEvent, error) {
	pattern := j.generateEventKey(joinKeyValue, streamSide, "*")
	keys, err := j.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to search for matching events: %w", err)
	}

	for _, key := range keys {
		eventData, err := j.redis.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var storedEvent StreamEvent
		if err := json.Unmarshal([]byte(eventData), &storedEvent); err != nil {
			continue
		}

		// Check if the event is within the time window
		timeDiff := eventTime.Sub(storedEvent.Timestamp)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		if timeDiff <= j.timeWindow {
			return &storedEvent, nil
		}
	}

	return nil, nil
}

// storeEventForJoin stores an event in Redis for future joins
func (j *TemporalJoinEngine) storeEventForJoin(ctx context.Context, event *StreamEvent) error {
	eventKey := j.generateEventKey(event.JoinKey, event.Source, event.ID)
	
	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event for storage: %w", err)
	}

	// Store with expiration based on time window + buffer
	expiration := j.timeWindow + time.Minute*5
	err = j.redis.SetEx(ctx, eventKey, eventData, expiration).Err()
	if err != nil {
		return fmt.Errorf("failed to store event in Redis: %w", err)
	}

	j.mutex.Lock()
	j.metrics.StateStoreSize++
	j.mutex.Unlock()

	return nil
}

// removeEventFromStore removes a joined event from the store
func (j *TemporalJoinEngine) removeEventFromStore(ctx context.Context, joinKeyValue, streamSide, eventID string) error {
	eventKey := j.generateEventKey(joinKeyValue, streamSide, eventID)
	err := j.redis.Del(ctx, eventKey).Err()
	if err != nil {
		return fmt.Errorf("failed to remove event from store: %w", err)
	}

	j.mutex.Lock()
	j.metrics.StateStoreSize--
	j.mutex.Unlock()

	return nil
}

// generateEventKey creates a Redis key for storing events
func (j *TemporalJoinEngine) generateEventKey(joinKeyValue, streamSide, eventID string) string {
	return fmt.Sprintf("join:%s:%s:%s:%s", j.joinKey, joinKeyValue, streamSide, eventID)
}

// createJoinedEvent creates a joined event from left and right events
func (j *TemporalJoinEngine) createJoinedEvent(currentEvent, matchingEvent *StreamEvent) *JoinedEvent {
	var leftEvent, rightEvent *StreamEvent
	
	if currentEvent.Source == "left" {
		leftEvent = currentEvent
		rightEvent = matchingEvent
	} else {
		leftEvent = matchingEvent
		rightEvent = currentEvent
	}

	joinDelay := rightEvent.Timestamp.Sub(leftEvent.Timestamp)
	if joinDelay < 0 {
		joinDelay = -joinDelay
	}

	return &JoinedEvent{
		ID:        fmt.Sprintf("joined_%s_%s", leftEvent.ID, rightEvent.ID),
		Timestamp: time.Now(),
		LeftData:  leftEvent.Data,
		RightData: rightEvent.Data,
		JoinKey:   leftEvent.JoinKey,
		LeftTime:  leftEvent.Timestamp,
		RightTime: rightEvent.Timestamp,
		JoinDelay: joinDelay,
	}
}

// isLateArrival checks if an event arrived too late
func (j *TemporalJoinEngine) isLateArrival(eventTime time.Time) bool {
	return time.Since(eventTime) > j.timeWindow
}

// CleanupExpiredEvents removes old events from the join buffer
func (j *TemporalJoinEngine) CleanupExpiredEvents(ctx context.Context) error {
	pattern := fmt.Sprintf("join:%s:*", j.joinKey)
	keys, err := j.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return fmt.Errorf("failed to get join buffer keys: %w", err)
	}

	pipe := j.redis.Pipeline()
	cleanupCount := 0

	for _, key := range keys {
		ttl, err := j.redis.TTL(ctx, key).Result()
		if err != nil {
			continue
		}

		if ttl <= 0 {
			pipe.Del(ctx, key)
			cleanupCount++
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired join events: %w", err)
	}

	j.mutex.Lock()
	j.metrics.WindowCleanups++
	j.metrics.StateStoreSize -= int64(cleanupCount)
	j.mutex.Unlock()

	j.logger.WithFields(logrus.Fields{
		"cleaned_events": cleanupCount,
		"total_keys":     len(keys),
		"join_key":       j.joinKey,
	}).Info("Cleaned up expired join events")

	return nil
}

// GetMetrics returns current join metrics
func (j *TemporalJoinEngine) GetMetrics() *JoinMetrics {
	j.mutex.RLock()
	defer j.mutex.RUnlock()
	
	return &JoinMetrics{
		LeftEventsProcessed:  j.metrics.LeftEventsProcessed,
		RightEventsProcessed: j.metrics.RightEventsProcessed,
		SuccessfulJoins:      j.metrics.SuccessfulJoins,
		OrphanedLeftEvents:   j.metrics.OrphanedLeftEvents,
		OrphanedRightEvents:  j.metrics.OrphanedRightEvents,
		LateArrivals:         j.metrics.LateArrivals,
		WindowCleanups:       j.metrics.WindowCleanups,
		ErrorCount:           j.metrics.ErrorCount,
		StateStoreSize:       j.metrics.StateStoreSize,
	}
}

// ResetMetrics resets all metrics counters
func (j *TemporalJoinEngine) ResetMetrics() {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	
	j.metrics = &JoinMetrics{}
}

// SetTimeWindow updates the join time window
func (j *TemporalJoinEngine) SetTimeWindow(timeWindow time.Duration) {
	// Enforce GlassFlow's 7-day maximum
	maxWindow := time.Hour * 24 * 7
	if timeWindow > maxWindow {
		timeWindow = maxWindow
	}
	
	j.mutex.Lock()
	j.timeWindow = timeWindow
	j.mutex.Unlock()
	
	j.logger.WithField("time_window", timeWindow).Info("Updated join time window")
}

// GetJoinStats returns summary statistics about the join performance
func (j *TemporalJoinEngine) GetJoinStats() map[string]interface{} {
	metrics := j.GetMetrics()
	
	totalEvents := metrics.LeftEventsProcessed + metrics.RightEventsProcessed
	joinRate := float64(0)
	if totalEvents > 0 {
		joinRate = float64(metrics.SuccessfulJoins) / float64(totalEvents) * 100
	}
	
	return map[string]interface{}{
		"total_events_processed": totalEvents,
		"successful_joins":       metrics.SuccessfulJoins,
		"join_rate_percent":      joinRate,
		"orphaned_events":        metrics.OrphanedLeftEvents + metrics.OrphanedRightEvents,
		"late_arrivals":          metrics.LateArrivals,
		"state_store_size":       metrics.StateStoreSize,
		"error_count":           metrics.ErrorCount,
		"time_window":           j.timeWindow.String(),
	}
}

// incrementErrorCount safely increments the error counter
func (j *TemporalJoinEngine) incrementErrorCount() {
	j.mutex.Lock()
	j.metrics.ErrorCount++
	j.mutex.Unlock()
}