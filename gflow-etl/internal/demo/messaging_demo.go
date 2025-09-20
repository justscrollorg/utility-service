package demo

import (
	"context"
	"fmt"
	"time"

	"github.com/justscrollorg/gflow-etl/internal/messaging"
	"github.com/sirupsen/logrus"
)

// Helper function to get map keys for logging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// MessagingDemo showcases different Kafka patterns
type MessagingDemo struct {
	logger       *logrus.Logger
	messageQueue *messaging.MessageQueue
	pubSub       *messaging.PubSubBroker
	subscriber   *messaging.EventSubscriber
	saga         *messaging.SagaOrchestrator
	isRunning    bool
	stopChan     chan struct{}
}

// NewMessagingDemo creates a new messaging patterns demo
func NewMessagingDemo(kafkaBrokers []string) (*MessagingDemo, error) {
	logger := logrus.New()

	// Initialize messaging components with basic configurations
	messageQueue := &messaging.MessageQueue{}
	pubSub := &messaging.PubSubBroker{}
	subscriber := &messaging.EventSubscriber{}
	saga := &messaging.SagaOrchestrator{}

	return &MessagingDemo{
		logger:       logger,
		messageQueue: messageQueue,
		pubSub:       pubSub,
		subscriber:   subscriber,
		saga:         saga,
		stopChan:     make(chan struct{}),
	}, nil
}

// StartMessageQueueDemo demonstrates async task processing
func (md *MessagingDemo) StartMessageQueueDemo(ctx context.Context) error {
	md.logger.WithField("pattern", "message_queue").Info("Starting Message Queue Demo - Async Task Processing")

	// Simulate various task types
	tasks := []struct {
		taskType string
		payload  map[string]interface{}
	}{
		{"send-email", map[string]interface{}{
			"to":       "user@example.com",
			"subject":  "Welcome to our platform",
			"template": "welcome",
		}},
		{"process-payment", map[string]interface{}{
			"amount":   99.99,
			"currency": "USD",
			"method":   "credit_card",
		}},
		{"generate-report", map[string]interface{}{
			"type":    "monthly",
			"user_id": "12345",
			"format":  "pdf",
		}},
		{"resize-image", map[string]interface{}{
			"image_url": "https://example.com/image.jpg",
			"width":     800,
			"height":    600,
		}},
	}

	md.logger.WithField("task_types", len(tasks)).Info("Configured task types for message queue demo")
	md.isRunning = true

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	tasksPublished := 0
	for {
		select {
		case <-ctx.Done():
			md.logger.WithField("tasks_published", tasksPublished).Info("Message queue demo stopping due to context cancellation")
			return ctx.Err()
		case <-md.stopChan:
			md.logger.WithField("tasks_published", tasksPublished).Info("Message queue demo stopped by user request")
			return nil
		case <-ticker.C:
			// Publish random task
			task := tasks[time.Now().Unix()%int64(len(tasks))]
			
			md.logger.WithFields(logrus.Fields{
				"task_type": task.taskType,
				"payload_keys": getMapKeys(task.payload),
			}).Debug("Publishing task to message queue")

			if err := md.messageQueue.PublishMessage(ctx, task.taskType, task.payload); err != nil {
				md.logger.WithFields(logrus.Fields{
					"task_type": task.taskType,
					"error":     err,
				}).Error("Failed to publish task to message queue")
			} else {
				tasksPublished++
				md.logger.WithFields(logrus.Fields{
					"task_type":       task.taskType,
					"tasks_published": tasksPublished,
				}).Info("Task published to message queue successfully")
			}
		}
	}
}

// StartPubSubDemo demonstrates event publishing and subscription
func (md *MessagingDemo) StartPubSubDemo(ctx context.Context) error {
	md.logger.WithField("pattern", "pub_sub").Info("Starting Pub-Sub Demo - Event-Driven Architecture")

	events := []struct {
		eventType string
		source    string
		subject   string
		data      map[string]interface{}
	}{
		{"user.created", "user-service", "user-123", map[string]interface{}{
			"name":  "John Doe",
			"email": "john@example.com",
			"tier":  "premium",
		}},
		{"order.placed", "order-service", "order-456", map[string]interface{}{
			"amount":  199.99,
			"items":   3,
			"user_id": "user-123",
		}},
		{"payment.completed", "payment-service", "payment-789", map[string]interface{}{
			"amount":   199.99,
			"order_id": "order-456",
			"status":   "success",
		}},
		{"inventory.updated", "inventory-service", "product-101", map[string]interface{}{
			"product_id": "product-101",
			"quantity":   150,
			"location":   "warehouse-A",
		}},
	}

	md.logger.WithField("event_types", len(events)).Info("Configured event types for pub-sub demo")
	md.isRunning = true

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	eventsPublished := 0
	for {
		select {
		case <-ctx.Done():
			md.logger.WithField("events_published", eventsPublished).Info("Pub-sub demo stopping due to context cancellation")
			return ctx.Err()
		case <-md.stopChan:
			md.logger.WithField("events_published", eventsPublished).Info("Pub-sub demo stopped by user request")
			return nil
		case <-ticker.C:
			// Publish random event
			event := events[time.Now().Unix()%int64(len(events))]
			
			md.logger.WithFields(logrus.Fields{
				"event_type":   event.eventType,
				"event_source": event.source,
				"event_subject": event.subject,
				"data_keys":    getMapKeys(event.data),
			}).Debug("Publishing event to pub-sub")

			if err := md.pubSub.PublishEvent(ctx, event.eventType, event.source, event.subject, event.data); err != nil {
				md.logger.WithFields(logrus.Fields{
					"event_type": event.eventType,
					"subject":    event.subject,
					"error":      err,
				}).Error("Failed to publish event")
			} else {
				eventsPublished++
				md.logger.WithFields(logrus.Fields{
					"event_type":       event.eventType,
					"subject":          event.subject,
					"events_published": eventsPublished,
				}).Info("Event published successfully")
			}
		}
	}
}

// StartSagaDemo demonstrates distributed transaction management
func (md *MessagingDemo) StartSagaDemo(ctx context.Context) error {
	md.logger.WithField("pattern", "saga").Info("Starting Saga Demo - Distributed Transaction Management")

	sagaTemplates := []struct {
		sagaID      string
		transaction string
		steps       []string
		complexity  string
	}{
		{
			"order-payment-saga-001",
			"order_fulfillment",
			[]string{"reserve_inventory", "charge_payment", "create_shipment", "send_notification"},
			"complex",
		},
		{
			"user-onboarding-saga-002",
			"user_registration",
			[]string{"create_account", "send_welcome_email", "setup_preferences"},
			"simple",
		},
		{
			"refund-saga-003",
			"order_refund",
			[]string{"validate_refund", "process_payment_reversal", "update_inventory", "notify_customer"},
			"moderate",
		},
	}

	md.logger.WithField("saga_templates", len(sagaTemplates)).Info("Loaded saga orchestration templates")
	md.isRunning = true

	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()

	sagasExecuted := 0
	for {
		select {
		case <-ctx.Done():
			md.logger.WithField("sagas_executed", sagasExecuted).Info("Saga demo stopping due to context cancellation")
			return ctx.Err()
		case <-md.stopChan:
			md.logger.WithField("sagas_executed", sagasExecuted).Info("Saga demo stopped by user request")
			return nil
		case <-ticker.C:
			// Start random saga
			saga := sagaTemplates[time.Now().Unix()%int64(len(sagaTemplates))]
			
			sagaData := map[string]interface{}{
				"transaction_id": fmt.Sprintf("tx-%d", time.Now().UnixNano()),
				"user_id":       fmt.Sprintf("user-%d", time.Now().Unix()%1000),
				"amount":        float64(time.Now().Unix()%500 + 100),
				"timestamp":     time.Now().UTC(),
			}

			// Create saga steps for this demo
			sagaSteps := []messaging.SagaStep{
				{Name: "step1", Command: "reserve_inventory", CompensateWith: "release_inventory", Status: "pending"},
				{Name: "step2", Command: "charge_payment", CompensateWith: "refund_payment", Status: "pending"},
				{Name: "step3", Command: "create_shipment", CompensateWith: "cancel_shipment", Status: "pending"},
				{Name: "step4", Command: "send_notification", CompensateWith: "send_cancellation", Status: "pending"},
			}

			md.logger.WithFields(logrus.Fields{
				"saga_id":      saga.sagaID,
				"transaction":  saga.transaction,
				"complexity":   saga.complexity,
				"steps_count":  len(saga.steps),
				"steps":        saga.steps,
				"data_keys":    getMapKeys(sagaData),
			}).Debug("Starting saga orchestration")

			sagaInstance, err := md.saga.StartSaga(ctx, saga.sagaID, sagaSteps, sagaData)
			if err != nil {
				md.logger.WithFields(logrus.Fields{
					"saga_id":     saga.sagaID,
					"transaction": saga.transaction,
					"error":       err,
				}).Error("Failed to start saga")
			} else {
				sagasExecuted++
				md.logger.WithFields(logrus.Fields{
					"saga_id":        saga.sagaID,
					"saga_instance":  sagaInstance.ID,
					"transaction":    saga.transaction,
					"complexity":     saga.complexity,
					"sagas_executed": sagasExecuted,
				}).Info("Saga orchestration started successfully")
			}
		}
	}
}

func (md *MessagingDemo) startOrderSaga(ctx context.Context, steps []messaging.SagaStep) (*messaging.SagaInstance, error) {
	// This would integrate with the SagaOrchestrator
	// For demo purposes, we'll just log the saga initiation
	sagaContext := map[string]interface{}{
		"order_id":   fmt.Sprintf("order-%d", time.Now().Unix()),
		"user_id":    "user-123",
		"total":      199.99,
		"start_time": time.Now(),
	}

	md.logger.WithFields(logrus.Fields{
		"steps":   len(steps),
		"context": sagaContext,
	}).Info("Saga orchestration started")

	// Return mock saga instance
	return &messaging.SagaInstance{
		ID:          fmt.Sprintf("saga-%d", time.Now().Unix()),
		Type:        "order-processing",
		Status:      "running",
		CurrentStep: 0,
		Steps:       steps,
		Context:     sagaContext,
		StartTime:   time.Now(),
		LastUpdated: time.Now(),
	}, nil
}

// Stop stops all running demos
func (md *MessagingDemo) Stop() {
	if md.isRunning {
		close(md.stopChan)
		md.isRunning = false
		md.logger.Info("Messaging demos stopped")
	}
}

// GetDemoStats returns statistics about the running demos
func (md *MessagingDemo) GetDemoStats() map[string]interface{} {
	return map[string]interface{}{
		"message_queue": map[string]interface{}{
			"active":          md.isRunning,
			"supported_tasks": []string{"send-email", "process-payment", "generate-report", "resize-image"},
		},
		"pub_sub": map[string]interface{}{
			"active":           md.isRunning,
			"supported_events": []string{"user.created", "order.placed", "payment.completed", "inventory.updated"},
		},
		"saga": map[string]interface{}{
			"active":     md.isRunning,
			"patterns":   []string{"order-processing", "user-onboarding", "payment-flow"},
			"step_types": []string{"reserve-inventory", "charge-payment", "create-shipment", "send-confirmation"},
		},
	}
}
