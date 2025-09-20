package demo

import (
	"context"
	"fmt"
	"time"

	"github.com/justscrollorg/gflow-etl/internal/messaging"
	"github.com/sirupsen/logrus"
)

// MessagingDemo showcases different Kafka patterns
type MessagingDemo struct {
	logger       *logrus.Logger
	messageQueue *messaging.MessageQueue
	pubSub       *messaging.PubSubBroker
	subscriber   *messaging.EventSubscriber
	isRunning    bool
	stopChan     chan struct{}
}

// NewMessagingDemo creates a new messaging patterns demo
func NewMessagingDemo(kafkaBrokers []string) (*MessagingDemo, error) {
	logger := logrus.New()
	
	return &MessagingDemo{
		logger:   logger,
		stopChan: make(chan struct{}),
	}, nil
}

// StartMessageQueueDemo demonstrates async task processing
func (md *MessagingDemo) StartMessageQueueDemo(ctx context.Context) error {
	md.logger.Info("Starting Message Queue Demo...")
	
	// Simulate various task types
	tasks := []struct {
		taskType string
		payload  map[string]interface{}
	}{
		{"send-email", map[string]interface{}{
			"to":      "user@example.com",
			"subject": "Welcome to our platform",
			"template": "welcome",
		}},
		{"process-payment", map[string]interface{}{
			"amount":   99.99,
			"currency": "USD",
			"method":   "credit_card",
		}},
		{"generate-report", map[string]interface{}{
			"type":     "monthly",
			"user_id":  "12345",
			"format":   "pdf",
		}},
		{"resize-image", map[string]interface{}{
			"image_url": "https://example.com/image.jpg",
			"width":     800,
			"height":    600,
		}},
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-md.stopChan:
			return nil
		case <-ticker.C:
			// Publish random task
			task := tasks[time.Now().Unix()%int64(len(tasks))]
			if err := md.messageQueue.PublishMessage(ctx, task.taskType, task.payload); err != nil {
				md.logger.Errorf("Failed to publish task: %v", err)
			} else {
				md.logger.Infof("Published task: %s", task.taskType)
			}
		}
	}
}

// StartPubSubDemo demonstrates event publishing and subscription
func (md *MessagingDemo) StartPubSubDemo(ctx context.Context) error {
	md.logger.Info("Starting Pub-Sub Demo...")
	
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
			"amount":    199.99,
			"items":     3,
			"user_id":   "user-123",
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

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-md.stopChan:
			return nil
		case <-ticker.C:
			// Publish random event
			event := events[time.Now().Unix()%int64(len(events))]
			if err := md.pubSub.PublishEvent(ctx, event.eventType, event.source, event.subject, event.data); err != nil {
				md.logger.Errorf("Failed to publish event: %v", err)
			} else {
				md.logger.Infof("Published event: %s for %s", event.eventType, event.subject)
			}
		}
	}
}

// StartSagaDemo demonstrates distributed transaction patterns
func (md *MessagingDemo) StartSagaDemo(ctx context.Context) error {
	md.logger.Info("Starting Saga Demo...")
	
	// Example: E-commerce order processing saga
	orderSagaSteps := []messaging.SagaStep{
		{
			Name:           "reserve-inventory",
			Command:        "inventory.reserve",
			CompensateWith: "inventory.release",
			Parameters: map[string]interface{}{
				"product_id": "product-123",
				"quantity":   2,
			},
		},
		{
			Name:           "charge-payment",
			Command:        "payment.charge",
			CompensateWith: "payment.refund",
			Parameters: map[string]interface{}{
				"amount":     199.99,
				"payment_id": "payment-456",
			},
		},
		{
			Name:           "create-shipment",
			Command:        "shipping.create",
			CompensateWith: "shipping.cancel",
			Parameters: map[string]interface{}{
				"address": "123 Main St, City, State",
				"method":  "standard",
			},
		},
		{
			Name:           "send-confirmation",
			Command:        "notification.send",
			CompensateWith: "notification.cancel",
			Parameters: map[string]interface{}{
				"type":    "order_confirmation",
				"user_id": "user-789",
			},
		},
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-md.stopChan:
			return nil
		case <-ticker.C:
			// Start a new saga
			saga, err := md.startOrderSaga(ctx, orderSagaSteps)
			if err != nil {
				md.logger.Errorf("Failed to start saga: %v", err)
			} else {
				md.logger.Infof("Started order saga: %s", saga.ID)
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