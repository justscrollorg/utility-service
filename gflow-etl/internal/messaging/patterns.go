package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// MessageQueue implements async task queue pattern
type MessageQueue struct {
	writer *kafka.Writer
	reader *kafka.Reader
	logger *logrus.Logger
	topic  string
}

// TaskMessage represents a queued task
type TaskMessage struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Payload     map[string]interface{} `json:"payload"`
	Priority    int                    `json:"priority"`
	Timestamp   time.Time              `json:"timestamp"`
	Retries     int                    `json:"retries"`
	MaxRetries  int                    `json:"max_retries"`
	ScheduledAt *time.Time             `json:"scheduled_at,omitempty"`
}

// PublishMessage sends a task to the queue
func (mq *MessageQueue) PublishMessage(ctx context.Context, taskType string, payload map[string]interface{}) error {
	task := TaskMessage{
		ID:         uuid.New().String(),
		Type:       taskType,
		Payload:    payload,
		Priority:   1,
		Timestamp:  time.Now(),
		Retries:    0,
		MaxRetries: 3,
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(task.ID),
		Value: data,
		Headers: []kafka.Header{
			{Key: "task-type", Value: []byte(taskType)},
			{Key: "priority", Value: []byte(fmt.Sprintf("%d", task.Priority))},
		},
	}

	return mq.writer.WriteMessages(ctx, message)
}

// ConsumeMessages processes tasks from the queue
func (mq *MessageQueue) ConsumeMessages(ctx context.Context, handler TaskHandler) error {
	mq.logger.Info("Starting message queue consumer")
	messageCount := 0
	
	for {
		select {
		case <-ctx.Done():
			mq.logger.WithField("messages_processed", messageCount).Info("Message queue consumer shutting down")
			return ctx.Err()
		default:
			message, err := mq.reader.ReadMessage(ctx)
			if err != nil {
				mq.logger.WithError(err).Error("Failed to read message from queue")
				continue
			}

			messageCount++
			mq.logger.WithFields(logrus.Fields{
				"message_offset": message.Offset,
				"message_key":    string(message.Key),
				"message_size":   len(message.Value),
				"total_processed": messageCount,
			}).Debug("Received message from queue")

			var task TaskMessage
			if err := json.Unmarshal(message.Value, &task); err != nil {
				mq.logger.WithFields(logrus.Fields{
					"message_offset": message.Offset,
					"error":          err,
				}).Error("Failed to unmarshal task message")
				continue
			}

			mq.logger.WithFields(logrus.Fields{
				"task_id":   task.ID,
				"task_type": task.Type,
				"priority":  task.Priority,
			}).Info("Processing task from message queue")

			startTime := time.Now()
			if err := handler.Handle(ctx, task); err != nil {
				duration := time.Since(startTime)
				mq.logger.WithFields(logrus.Fields{
					"task_id":        task.ID,
					"task_type":      task.Type,
					"processing_time": duration,
					"error":          err,
				}).Error("Task processing failed")
				// Could implement retry logic here
			} else {
				duration := time.Since(startTime)
				mq.logger.WithFields(logrus.Fields{
					"task_id":         task.ID,
					"task_type":       task.Type,
					"processing_time": duration,
				}).Info("Task processed successfully")
			}
		}
	}
}

// TaskHandler interface for processing tasks
type TaskHandler interface {
	Handle(ctx context.Context, task TaskMessage) error
}

// PubSubBroker implements publish-subscribe pattern
type PubSubBroker struct {
	writer *kafka.Writer
	logger *logrus.Logger
	topic  string
}

// Event represents a published event
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Source    string                 `json:"source"`
	Subject   string                 `json:"subject"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	Version   string                 `json:"version"`
}

// PublishEvent publishes an event to subscribers
func (ps *PubSubBroker) PublishEvent(ctx context.Context, eventType, source, subject string, data map[string]interface{}) error {
	ps.logger.WithFields(logrus.Fields{
		"event_type": eventType,
		"source":     source,
		"subject":    subject,
		"data_keys":  getMapKeys(data),
	}).Info("Publishing event to subscribers")

	event := Event{
		ID:        uuid.New().String(),
		Type:      eventType,
		Source:    source,
		Subject:   subject,
		Timestamp: time.Now(),
		Data:      data,
		Version:   "1.0",
	}

	ps.logger.WithFields(logrus.Fields{
		"event_id":    event.ID,
		"event_type":  event.Type,
		"timestamp":   event.Timestamp,
	}).Debug("Event structure created")

	eventData, err := json.Marshal(event)
	if err != nil {
		ps.logger.WithFields(logrus.Fields{
			"event_id": event.ID,
			"error":    err,
		}).Error("Failed to marshal event to JSON")
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	message := kafka.Message{
		Key:   []byte(subject), // Partition by subject for ordering
		Value: eventData,
		Headers: []kafka.Header{
			{Key: "event-type", Value: []byte(eventType)},
			{Key: "source", Value: []byte(source)},
			{Key: "version", Value: []byte("1.0")},
		},
	}

	ps.logger.WithFields(logrus.Fields{
		"event_id":     event.ID,
		"message_size": len(eventData),
		"partition_key": subject,
		"headers_count": len(message.Headers),
	}).Debug("Publishing message to Kafka topic")

	startTime := time.Now()
	err = ps.writer.WriteMessages(ctx, message)
	duration := time.Since(startTime)

	if err != nil {
		ps.logger.WithFields(logrus.Fields{
			"event_id": event.ID,
			"topic":    ps.topic,
			"duration": duration,
			"error":    err,
		}).Error("Failed to publish event to Kafka")
		return err
	}

	ps.logger.WithFields(logrus.Fields{
		"event_id": event.ID,
		"topic":    ps.topic,
		"duration": duration,
	}).Info("Event published successfully")

	return nil
}

// Helper function to get map keys for logging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// EventSubscriber represents a subscriber to events
type EventSubscriber struct {
	reader        *kafka.Reader
	logger        *logrus.Logger
	eventHandlers map[string]EventHandler
}

// EventHandler interface for handling events
type EventHandler interface {
	Handle(ctx context.Context, event Event) error
}

// Subscribe starts consuming events with registered handlers
func (es *EventSubscriber) Subscribe(ctx context.Context) error {
	es.logger.WithField("handlers_count", len(es.eventHandlers)).Info("Starting event subscriber")
	eventsProcessed := 0

	for {
		select {
		case <-ctx.Done():
			es.logger.WithField("events_processed", eventsProcessed).Info("Event subscriber shutting down")
			return ctx.Err()
		default:
			message, err := es.reader.ReadMessage(ctx)
			if err != nil {
				es.logger.WithError(err).Error("Failed to read event from topic")
				continue
			}

			es.logger.WithFields(logrus.Fields{
				"message_offset": message.Offset,
				"message_size":   len(message.Value),
				"headers_count":  len(message.Headers),
			}).Debug("Received event message")

			var event Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				es.logger.WithFields(logrus.Fields{
					"message_offset": message.Offset,
					"error":          err,
				}).Error("Failed to unmarshal event")
				continue
			}

			eventsProcessed++
			es.logger.WithFields(logrus.Fields{
				"event_id":        event.ID,
				"event_type":      event.Type,
				"event_source":    event.Source,
				"event_subject":   event.Subject,
				"events_processed": eventsProcessed,
			}).Info("Processing event")

			// Route to appropriate handler
			if handler, exists := es.eventHandlers[event.Type]; exists {
				startTime := time.Now()
				if err := handler.Handle(ctx, event); err != nil {
					duration := time.Since(startTime)
					es.logger.WithFields(logrus.Fields{
						"event_id":        event.ID,
						"event_type":      event.Type,
						"processing_time": duration,
						"error":           err,
					}).Error("Event handling failed")
				} else {
					duration := time.Since(startTime)
					es.logger.WithFields(logrus.Fields{
						"event_id":        event.ID,
						"event_type":      event.Type,
						"processing_time": duration,
					}).Info("Event handled successfully")
				}
			} else {
				es.logger.WithFields(logrus.Fields{
					"event_id":   event.ID,
					"event_type": event.Type,
					"available_handlers": getMapKeysString(es.eventHandlers),
				}).Warn("No handler registered for event type")
			}
		}
	}
}

// Helper function to get handler types for logging
func getMapKeysString(m map[string]EventHandler) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// RegisterHandler registers an event handler for a specific event type
func (es *EventSubscriber) RegisterHandler(eventType string, handler EventHandler) {
	if es.eventHandlers == nil {
		es.eventHandlers = make(map[string]EventHandler)
	}
	es.eventHandlers[eventType] = handler
	es.logger.WithFields(logrus.Fields{
		"event_type":     eventType,
		"handlers_count": len(es.eventHandlers),
	}).Info("Event handler registered")
}

// SagaOrchestrator implements distributed transaction patterns
type SagaOrchestrator struct {
	publisher *PubSubBroker
	logger    *logrus.Logger
	sagas     map[string]*SagaInstance
}

// SagaInstance represents a running saga
type SagaInstance struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Status      string                 `json:"status"` // "running", "completed", "failed", "compensating"
	CurrentStep int                    `json:"current_step"`
	Steps       []SagaStep             `json:"steps"`
	Context     map[string]interface{} `json:"context"`
	StartTime   time.Time              `json:"start_time"`
	LastUpdated time.Time              `json:"last_updated"`
}

// SagaStep represents a step in a saga
type SagaStep struct {
	Name           string                 `json:"name"`
	Command        string                 `json:"command"`
	CompensateWith string                 `json:"compensate_with"`
	Parameters     map[string]interface{} `json:"parameters"`
	Status         string                 `json:"status"` // "pending", "completed", "failed"
}

// StartSaga initiates a new saga transaction
func (so *SagaOrchestrator) StartSaga(ctx context.Context, sagaType string, steps []SagaStep, initialContext map[string]interface{}) (*SagaInstance, error) {
	so.logger.WithFields(logrus.Fields{
		"saga_type":   sagaType,
		"steps_count": len(steps),
		"context_keys": getMapKeys(initialContext),
	}).Info("Starting new saga transaction")

	saga := &SagaInstance{
		ID:          uuid.New().String(),
		Type:        sagaType,
		Status:      "running",
		CurrentStep: 0,
		Steps:       steps,
		Context:     initialContext,
		StartTime:   time.Now(),
		LastUpdated: time.Now(),
	}

	so.sagas[saga.ID] = saga

	so.logger.WithFields(logrus.Fields{
		"saga_id":      saga.ID,
		"saga_type":    saga.Type,
		"steps_count":  len(saga.Steps),
		"active_sagas": len(so.sagas),
	}).Info("Saga instance created and registered")

	// Publish saga started event
	err := so.publisher.PublishEvent(ctx, "saga.started", "saga-orchestrator", saga.ID, map[string]interface{}{
		"saga_id":   saga.ID,
		"saga_type": sagaType,
		"steps":     len(steps),
	})

	if err != nil {
		so.logger.WithFields(logrus.Fields{
			"saga_id": saga.ID,
			"error":   err,
		}).Error("Failed to publish saga started event")
		return saga, err
	}

	so.logger.WithField("saga_id", saga.ID).Info("Saga started event published successfully")
	return saga, nil
}

// Example task handlers
type EmailTaskHandler struct {
	logger *logrus.Logger
}

func (h *EmailTaskHandler) Handle(ctx context.Context, task TaskMessage) error {
	h.logger.WithFields(logrus.Fields{
		"task_id":   task.ID,
		"task_type": task.Type,
		"priority":  task.Priority,
	}).Info("Starting email task processing")

	startTime := time.Now()
	// Simulate email sending
	time.Sleep(100 * time.Millisecond)
	duration := time.Since(startTime)

	h.logger.WithFields(logrus.Fields{
		"task_id":         task.ID,
		"processing_time": duration,
		"operation":       "email_send",
	}).Info("Email task completed successfully")
	
	return nil
}

type PaymentTaskHandler struct {
	logger *logrus.Logger
}

func (h *PaymentTaskHandler) Handle(ctx context.Context, task TaskMessage) error {
	h.logger.WithFields(logrus.Fields{
		"task_id":   task.ID,
		"task_type": task.Type,
		"priority":  task.Priority,
	}).Info("Starting payment task processing")

	startTime := time.Now()
	// Simulate payment processing
	time.Sleep(200 * time.Millisecond)
	duration := time.Since(startTime)

	h.logger.WithFields(logrus.Fields{
		"task_id":         task.ID,
		"processing_time": duration,
		"operation":       "payment_process",
	}).Info("Payment task completed successfully")
	
	return nil
}

// Example event handlers
type UserEventHandler struct {
	logger *logrus.Logger
}

func (h *UserEventHandler) Handle(ctx context.Context, event Event) error {
	h.logger.WithFields(logrus.Fields{
		"event_id":      event.ID,
		"event_type":    event.Type,
		"event_subject": event.Subject,
		"event_source":  event.Source,
		"data_keys":     getMapKeys(event.Data),
	}).Info("Processing user event")

	startTime := time.Now()
	// Process user events
	duration := time.Since(startTime)

	h.logger.WithFields(logrus.Fields{
		"event_id":        event.ID,
		"processing_time": duration,
		"handler_type":    "user_event",
	}).Info("User event processed successfully")
	
	return nil
}

type OrderEventHandler struct {
	logger *logrus.Logger
}

func (h *OrderEventHandler) Handle(ctx context.Context, event Event) error {
	h.logger.WithFields(logrus.Fields{
		"event_id":      event.ID,
		"event_type":    event.Type,
		"event_subject": event.Subject,
		"event_source":  event.Source,
		"data_keys":     getMapKeys(event.Data),
	}).Info("Processing order event")

	startTime := time.Now()
	// Process order events
	duration := time.Since(startTime)

	h.logger.WithFields(logrus.Fields{
		"event_id":        event.ID,
		"processing_time": duration,
		"handler_type":    "order_event",
	}).Info("Order event processed successfully")
	
	return nil
}
