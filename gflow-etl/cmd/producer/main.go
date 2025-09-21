package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/google/uuid"
)

// EventProducer generates and sends events to Kafka for testing the ETL pipeline
type EventProducer struct {
	writer    *kafka.Writer
	userIDs   []string
	sessionIDs []string
}

// NewEventProducer creates a new Kafka event producer
func NewEventProducer(brokers []string, topic string) *EventProducer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	// Generate some test user IDs
	userIDs := make([]string, 100)
	for i := range userIDs {
		userIDs[i] = fmt.Sprintf("user_%d", i+1)
	}

	// Generate some session IDs (will be reused to create duplicates)
	sessionIDs := make([]string, 50)
	for i := range sessionIDs {
		sessionIDs[i] = fmt.Sprintf("sess_%s_%d", time.Now().Format("20060102"), i+1)
	}

	return &EventProducer{
		writer:     writer,
		userIDs:    userIDs,
		sessionIDs: sessionIDs,
	}
}

// GenerateUserSessionEvents produces realistic user session events
func (p *EventProducer) GenerateUserSessionEvents(ctx context.Context, eventsPerSecond int) error {
	ticker := time.NewTicker(time.Second / time.Duration(eventsPerSecond))
	defer ticker.Stop()

	log.Printf("Starting user session event generation: %d events/sec", eventsPerSecond)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			event := p.createUserSessionEvent()
			
			eventJSON, err := json.Marshal(event)
			if err != nil {
				log.Printf("Failed to marshal event: %v", err)
				continue
			}

			msg := kafka.Message{
				Key:   []byte(event["session_id"].(string)),
				Value: eventJSON,
				Time:  time.Now(),
			}

			if err := p.writer.WriteMessages(ctx, msg); err != nil {
				log.Printf("Failed to write message: %v", err)
			} else {
				log.Printf("Sent event: %s", event["id"])
			}
		}
	}
}

// GenerateTransactionEvents produces transaction events for join testing
func (p *EventProducer) GenerateTransactionEvents(ctx context.Context, eventsPerSecond int) error {
	ticker := time.NewTicker(time.Second / time.Duration(eventsPerSecond))
	defer ticker.Stop()

	log.Printf("Starting transaction event generation: %d events/sec", eventsPerSecond)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			event := p.createTransactionEvent()
			
			eventJSON, err := json.Marshal(event)
			if err != nil {
				log.Printf("Failed to marshal transaction event: %v", err)
				continue
			}

			msg := kafka.Message{
				Key:   []byte(event["user_id"].(string)),
				Value: eventJSON,
				Time:  time.Now(),
			}

			if err := p.writer.WriteMessages(ctx, msg); err != nil {
				log.Printf("Failed to write transaction message: %v", err)
			} else {
				log.Printf("Sent transaction: %s", event["id"])
			}
		}
	}
}

// createUserSessionEvent creates a realistic user session event
func (p *EventProducer) createUserSessionEvent() map[string]interface{} {
	now := time.Now()
	
	// 15% chance of creating a duplicate (reusing existing session)
	var sessionID string
	if rand.Float32() < 0.15 {
		sessionID = p.sessionIDs[rand.Intn(len(p.sessionIDs))]
	} else {
		sessionID = fmt.Sprintf("sess_%s_%s", now.Format("20060102"), uuid.New().String()[:8])
	}

	pages := []string{"/home", "/products", "/checkout", "/cart", "/profile", "/search"}
	actions := []string{"page_view", "click", "scroll", "form_submit", "add_to_cart"}
	devices := []string{"desktop", "mobile", "tablet"}
	browsers := []string{"chrome", "firefox", "safari", "edge"}
	
	return map[string]interface{}{
		"id":          fmt.Sprintf("evt_%d", now.UnixNano()),
		"session_id":  sessionID,
		"user_id":     p.userIDs[rand.Intn(len(p.userIDs))],
		"timestamp":   now.UTC().Format(time.RFC3339),
		"page":        pages[rand.Intn(len(pages))],
		"action":      actions[rand.Intn(len(actions))],
		"device":      devices[rand.Intn(len(devices))],
		"browser":     browsers[rand.Intn(len(browsers))],
		"ip_address":  fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)),
		"duration_ms": rand.Intn(30000) + 1000, // 1-30 seconds
		"referrer":    "https://google.com",
	}
}

// createTransactionEvent creates a realistic transaction event for join testing
func (p *EventProducer) createTransactionEvent() map[string]interface{} {
	now := time.Now()
	
	amounts := []float64{9.99, 19.99, 29.99, 49.99, 99.99, 199.99, 299.99}
	currencies := []string{"USD", "EUR", "GBP"}
	statuses := []string{"pending", "completed", "failed"}
	
	return map[string]interface{}{
		"id":         fmt.Sprintf("txn_%d", now.UnixNano()),
		"user_id":    p.userIDs[rand.Intn(len(p.userIDs))],
		"timestamp":  now.UTC().Format(time.RFC3339),
		"amount":     amounts[rand.Intn(len(amounts))],
		"currency":   currencies[rand.Intn(len(currencies))],
		"status":     statuses[rand.Intn(len(statuses))],
		"items":      rand.Intn(5) + 1,
		"payment_method": "credit_card",
		"merchant_id": fmt.Sprintf("merchant_%d", rand.Intn(10)+1),
	}
}

// createUserProfileEvent creates user profile events for join testing
func (p *EventProducer) createUserProfileEvent() map[string]interface{} {
	now := time.Now()
	
	tiers := []string{"basic", "premium", "enterprise"}
	countries := []string{"US", "UK", "DE", "FR", "CA"}
	
	return map[string]interface{}{
		"id":         fmt.Sprintf("profile_%d", now.UnixNano()),
		"user_id":    p.userIDs[rand.Intn(len(p.userIDs))],
		"timestamp":  now.UTC().Format(time.RFC3339),
		"tier":       tiers[rand.Intn(len(tiers))],
		"country":    countries[rand.Intn(len(countries))],
		"age":        rand.Intn(60) + 18,
		"signup_date": now.AddDate(0, 0, -rand.Intn(365)).Format("2006-01-02"),
	}
}

// Close closes the Kafka writer
func (p *EventProducer) Close() error {
	return p.writer.Close()
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run producer.go <topic> [events_per_second]")
	}

	topic := os.Args[1]
	eventsPerSecond := 10
	
	if len(os.Args) > 2 {
		if rate, err := strconv.Atoi(os.Args[2]); err == nil {
			eventsPerSecond = rate
		}
	}

	brokers := []string{"localhost:9092"}
	
	producer := NewEventProducer(brokers, topic)
	defer producer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interruption
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("Shutting down producer...")
		cancel()
	}()

	switch topic {
	case "user-sessions":
		if err := producer.GenerateUserSessionEvents(ctx, eventsPerSecond); err != nil {
			log.Printf("Error generating user session events: %v", err)
		}
	case "transactions":
		if err := producer.GenerateTransactionEvents(ctx, eventsPerSecond); err != nil {
			log.Printf("Error generating transaction events: %v", err)
		}
	default:
		log.Printf("Unknown topic: %s. Supported: user-sessions, transactions", topic)
	}
}