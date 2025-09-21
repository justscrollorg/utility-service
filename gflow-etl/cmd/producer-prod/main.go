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

// ProductionEventProducer generates realistic events for production ETL testing
type ProductionEventProducer struct {
	sessionWriter *kafka.Writer
	transactionWriter *kafka.Writer
	profileWriter *kafka.Writer
	userIDs       []string
	sessionIDs    []string
}

// NewProductionEventProducer creates a new production Kafka event producer
func NewProductionEventProducer(brokers []string) *ProductionEventProducer {
	// Configure writers with production-grade settings
	sessionWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "user-sessions",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	transactionWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "transactions",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	profileWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "user-profiles",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    50,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	// Generate realistic test user IDs
	userIDs := make([]string, 1000)
	for i := range userIDs {
		userIDs[i] = fmt.Sprintf("user_%04d", i+1)
	}

	// Generate session IDs that will be reused to create duplicates
	sessionIDs := make([]string, 200)
	for i := range sessionIDs {
		sessionIDs[i] = fmt.Sprintf("sess_%s_%04d", time.Now().Format("20060102"), i+1)
	}

	return &ProductionEventProducer{
		sessionWriter:     sessionWriter,
		transactionWriter: transactionWriter,
		profileWriter:     profileWriter,
		userIDs:          userIDs,
		sessionIDs:       sessionIDs,
	}
}

// GenerateProductionWorkload generates a realistic production workload
func (p *ProductionEventProducer) GenerateProductionWorkload(ctx context.Context, sessionRate, transactionRate, profileRate int) error {
	log.Printf("Starting production workload generation:")
	log.Printf("  - User sessions: %d events/sec", sessionRate)
	log.Printf("  - Transactions: %d events/sec", transactionRate)
	log.Printf("  - User profiles: %d events/sec", profileRate)

	// Start session event generation
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(sessionRate))
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.sendUserSessionEvent(ctx); err != nil {
					log.Printf("Failed to send session event: %v", err)
				}
			}
		}
	}()

	// Start transaction event generation
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(transactionRate))
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.sendTransactionEvent(ctx); err != nil {
					log.Printf("Failed to send transaction event: %v", err)
				}
			}
		}
	}()

	// Start profile event generation (slower rate)
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(profileRate))
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.sendUserProfileEvent(ctx); err != nil {
					log.Printf("Failed to send profile event: %v", err)
				}
			}
		}
	}()

	// Keep the main function running
	<-ctx.Done()
	return ctx.Err()
}

// sendUserSessionEvent sends a realistic user session event
func (p *ProductionEventProducer) sendUserSessionEvent(ctx context.Context) error {
	event := p.createUserSessionEvent()
	
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal session event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event["session_id"].(string)),
		Value: eventJSON,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("user_session")},
			{Key: "source", Value: []byte("production_generator")},
		},
	}

	if err := p.sessionWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write session message: %w", err)
	}

	log.Printf("📊 Session event: user=%s, session=%s, action=%s", 
		event["user_id"], event["session_id"], event["action"])
	return nil
}

// sendTransactionEvent sends a realistic transaction event
func (p *ProductionEventProducer) sendTransactionEvent(ctx context.Context) error {
	event := p.createTransactionEvent()
	
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event["user_id"].(string)),
		Value: eventJSON,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("transaction")},
			{Key: "source", Value: []byte("production_generator")},
		},
	}

	if err := p.transactionWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write transaction message: %w", err)
	}

	log.Printf("💰 Transaction: user=%s, amount=$%.2f, status=%s", 
		event["user_id"], event["amount"], event["status"])
	return nil
}

// sendUserProfileEvent sends a user profile update event
func (p *ProductionEventProducer) sendUserProfileEvent(ctx context.Context) error {
	event := p.createUserProfileEvent()
	
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal profile event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event["user_id"].(string)),
		Value: eventJSON,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("user_profile")},
			{Key: "source", Value: []byte("production_generator")},
		},
	}

	if err := p.profileWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write profile message: %w", err)
	}

	log.Printf("👤 Profile update: user=%s, tier=%s, country=%s", 
		event["user_id"], event["tier"], event["country"])
	return nil
}

// createUserSessionEvent creates a realistic user session event with duplicates
func (p *ProductionEventProducer) createUserSessionEvent() map[string]interface{} {
	now := time.Now()
	
	// 20% chance of creating a duplicate session (reusing existing session)
	var sessionID string
	if rand.Float32() < 0.20 {
		sessionID = p.sessionIDs[rand.Intn(len(p.sessionIDs))]
	} else {
		sessionID = fmt.Sprintf("sess_%s_%s", now.Format("20060102"), uuid.New().String()[:8])
	}

	pages := []string{"/home", "/products", "/checkout", "/cart", "/profile", "/search", "/categories", "/deals", "/support"}
	actions := []string{"page_view", "click", "scroll", "form_submit", "add_to_cart", "remove_from_cart", "search", "filter"}
	devices := []string{"desktop", "mobile", "tablet", "smart_tv"}
	browsers := []string{"chrome", "firefox", "safari", "edge", "opera"}
	countries := []string{"US", "UK", "DE", "FR", "CA", "AU", "JP", "BR"}
	
	return map[string]interface{}{
		"id":             fmt.Sprintf("evt_%d_%s", now.UnixNano(), uuid.New().String()[:8]),
		"session_id":     sessionID,
		"user_id":        p.userIDs[rand.Intn(len(p.userIDs))],
		"timestamp":      now.UTC().Format(time.RFC3339),
		"page":           pages[rand.Intn(len(pages))],
		"action":         actions[rand.Intn(len(actions))],
		"device":         devices[rand.Intn(len(devices))],
		"browser":        browsers[rand.Intn(len(browsers))],
		"ip_address":     fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)),
		"duration_ms":    rand.Intn(60000) + 500, // 0.5-60 seconds
		"referrer":       getReferrer(),
		"country":        countries[rand.Intn(len(countries))],
		"user_agent":     getUserAgent(),
		"screen_width":   getScreenWidth(),
		"screen_height":  getScreenHeight(),
	}
}

// createTransactionEvent creates a realistic transaction event
func (p *ProductionEventProducer) createTransactionEvent() map[string]interface{} {
	now := time.Now()
	
	amounts := []float64{5.99, 9.99, 19.99, 29.99, 49.99, 99.99, 199.99, 299.99, 499.99, 999.99}
	currencies := []string{"USD", "EUR", "GBP", "CAD", "AUD"}
	paymentMethods := []string{"credit_card", "debit_card", "paypal", "apple_pay", "google_pay"}
	
	// Weight the statuses (most should be completed)
	statusWeights := map[string]float32{
		"pending":   0.15,
		"completed": 0.75,
		"failed":    0.08,
		"refunded":  0.02,
	}
	
	status := weightedChoice(statusWeights)
	
	return map[string]interface{}{
		"id":             fmt.Sprintf("txn_%d_%s", now.UnixNano(), uuid.New().String()[:8]),
		"user_id":        p.userIDs[rand.Intn(len(p.userIDs))],
		"timestamp":      now.UTC().Format(time.RFC3339),
		"amount":         amounts[rand.Intn(len(amounts))],
		"currency":       currencies[rand.Intn(len(currencies))],
		"status":         status,
		"items":          rand.Intn(8) + 1,
		"payment_method": paymentMethods[rand.Intn(len(paymentMethods))],
		"merchant_id":    fmt.Sprintf("merchant_%03d", rand.Intn(50)+1),
		"order_id":       fmt.Sprintf("order_%d", now.UnixNano()),
		"discount":       rand.Float64() * 20.0, // 0-20% discount
		"tax":            rand.Float64() * 15.0, // 0-15% tax
	}
}

// createUserProfileEvent creates user profile events for join testing
func (p *ProductionEventProducer) createUserProfileEvent() map[string]interface{} {
	now := time.Now()
	
	tiers := []string{"basic", "premium", "enterprise", "vip"}
	countries := []string{"US", "UK", "DE", "FR", "CA", "AU", "JP", "BR", "IT", "ES"}
	
	return map[string]interface{}{
		"id":             fmt.Sprintf("profile_%d_%s", now.UnixNano(), uuid.New().String()[:8]),
		"user_id":        p.userIDs[rand.Intn(len(p.userIDs))],
		"timestamp":      now.UTC().Format(time.RFC3339),
		"tier":           tiers[rand.Intn(len(tiers))],
		"country":        countries[rand.Intn(len(countries))],
		"age":            rand.Intn(65) + 18,
		"signup_date":    now.AddDate(0, 0, -rand.Intn(1095)).Format("2006-01-02"), // 0-3 years ago
		"last_login":     now.AddDate(0, 0, -rand.Intn(30)).Format(time.RFC3339),   // 0-30 days ago
		"email_verified": rand.Float32() < 0.85, // 85% verified
		"phone_verified": rand.Float32() < 0.65, // 65% verified
	}
}

// Helper functions
func getReferrer() string {
	referrers := []string{
		"https://google.com",
		"https://facebook.com",
		"https://instagram.com",
		"https://twitter.com",
		"https://linkedin.com",
		"direct",
		"email",
		"organic",
	}
	return referrers[rand.Intn(len(referrers))]
}

func getUserAgent() string {
	userAgents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15",
		"Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0",
	}
	return userAgents[rand.Intn(len(userAgents))]
}

func getScreenWidth() int {
	widths := []int{1920, 1366, 1536, 1440, 1024, 768, 414, 375}
	return widths[rand.Intn(len(widths))]
}

func getScreenHeight() int {
	heights := []int{1080, 768, 864, 900, 768, 1024, 896, 667}
	return heights[rand.Intn(len(heights))]
}

func weightedChoice(weights map[string]float32) string {
	r := rand.Float32()
	var cumulative float32
	
	for choice, weight := range weights {
		cumulative += weight
		if r <= cumulative {
			return choice
		}
	}
	
	// Fallback
	for choice := range weights {
		return choice
	}
	return ""
}

// Close closes all Kafka writers
func (p *ProductionEventProducer) Close() error {
	var errs []error
	
	if err := p.sessionWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("session writer: %w", err))
	}
	
	if err := p.transactionWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("transaction writer: %w", err))
	}
	
	if err := p.profileWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("profile writer: %w", err))
	}
	
	if len(errs) > 0 {
		return fmt.Errorf("errors closing writers: %v", errs)
	}
	
	return nil
}

func main() {
	log.Println("🚀 GFlow Production Event Producer")
	log.Println("==================================")
	
	// Parse command line arguments
	sessionRate := 50   // events per second
	transactionRate := 20
	profileRate := 5
	
	if len(os.Args) > 1 {
		if rate, err := strconv.Atoi(os.Args[1]); err == nil {
			sessionRate = rate
		}
	}
	
	if len(os.Args) > 2 {
		if rate, err := strconv.Atoi(os.Args[2]); err == nil {
			transactionRate = rate
		}
	}
	
	if len(os.Args) > 3 {
		if rate, err := strconv.Atoi(os.Args[3]); err == nil {
			profileRate = rate
		}
	}

	// Use production Kafka brokers
	brokers := []string{"kafka.kafka.svc.cluster.local:9092"}
	
	// Check for environment override
	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); kafkaBrokers != "" {
		brokers = []string{kafkaBrokers}
	}
	
	log.Printf("Connecting to Kafka brokers: %v", brokers)
	
	producer := NewProductionEventProducer(brokers)
	defer func() {
		log.Println("Closing Kafka connections...")
		if err := producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Println("\n🛑 Shutdown signal received - stopping producer...")
		cancel()
	}()

	log.Println("Starting production workload generation...")
	log.Printf("Usage: %s [session_rate] [transaction_rate] [profile_rate]", os.Args[0])
	log.Printf("Current rates: sessions=%d/s, transactions=%d/s, profiles=%d/s", 
		sessionRate, transactionRate, profileRate)
	log.Println()
	
	if err := producer.GenerateProductionWorkload(ctx, sessionRate, transactionRate, profileRate); err != nil {
		if err != context.Canceled {
			log.Printf("Error generating workload: %v", err)
		}
	}
	
	log.Println("✅ Producer shutdown complete")
}