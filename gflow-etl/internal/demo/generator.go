package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Generator generates realistic demo data for GlassFlow-style use cases
type Generator struct {
	writer       *kafka.Writer
	logger       *logrus.Logger
	userPool     []User
	sessionPool  []string
	merchantPool []string
	stopChan     chan struct{}
	isRunning    bool
}

// User represents a demo user
type User struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Email      string    `json:"email"`
	Tier       string    `json:"tier"`
	Country    string    `json:"country"`
	SignupDate time.Time `json:"signup_date"`
}

// SessionEvent represents a user session event
type SessionEvent struct {
	SessionID       string    `json:"session_id"`
	UserID          string    `json:"user_id"`
	Timestamp       time.Time `json:"timestamp"`
	PageURL         string    `json:"page_url"`
	UserAgent       string    `json:"user_agent"`
	IPAddress       string    `json:"ip_address"`
	SessionDuration int       `json:"session_duration"`
	PageViews       int       `json:"page_views"`
	EventType       string    `json:"event_type"`
}

// TransactionEvent represents a financial transaction
type TransactionEvent struct {
	TransactionID string    `json:"transaction_id"`
	UserID        string    `json:"user_id"`
	Timestamp     time.Time `json:"timestamp"`
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	MerchantName  string    `json:"merchant_name"`
	Category      string    `json:"category"`
	Status        string    `json:"status"`
	PaymentMethod string    `json:"payment_method"`
}

// UserProfileEvent represents user profile updates
type UserProfileEvent struct {
	UserID    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Tier      string    `json:"tier"`
	Country   string    `json:"country"`
	EventType string    `json:"event_type"`
}

// AnalyticsEvent represents general analytics events
type AnalyticsEvent struct {
	EventID    string                 `json:"event_id"`
	EventType  string                 `json:"event_type"`
	Timestamp  time.Time              `json:"timestamp"`
	UserID     string                 `json:"user_id"`
	SessionID  string                 `json:"session_id"`
	Properties map[string]interface{} `json:"properties"`
	DeviceType string                 `json:"device_type"`
	Platform   string                 `json:"platform"`
	Country    string                 `json:"country"`
	Value      float64                `json:"value"`
}

// GeneratorConfig configures the demo data generator
type GeneratorConfig struct {
	KafkaBrokers   []string      `json:"kafka_brokers"`
	EventsPerSec   int           `json:"events_per_sec"`
	UserCount      int           `json:"user_count"`
	DuplicateRatio float64       `json:"duplicate_ratio"` // 0.0 to 1.0
	SessionWindow  time.Duration `json:"session_window"`
	EnableJitter   bool          `json:"enable_jitter"`
}

// NewGenerator creates a new demo data generator
func NewGenerator(config GeneratorConfig) (*Generator, error) {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Create Kafka writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.KafkaBrokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
	}

	gen := &Generator{
		writer:   writer,
		logger:   logger,
		stopChan: make(chan struct{}),
	}

	// Generate user pool
	gen.generateUserPool(config.UserCount)
	gen.generateSessionPool(config.UserCount * 3) // 3 sessions per user on average
	gen.generateMerchantPool()

	return gen, nil
}

// Start begins generating demo data
func (g *Generator) Start(ctx context.Context, config GeneratorConfig) error {
	g.isRunning = true
	g.logger.Info("Starting demo data generator...")

	// Calculate sleep duration between events
	sleepDuration := time.Second / time.Duration(config.EventsPerSec)

	// Start generators for different event types
	go g.generateSessionEvents(ctx, config, sleepDuration)
	go g.generateTransactionEvents(ctx, config, sleepDuration)
	go g.generateUserProfileEvents(ctx, config, sleepDuration*10) // Less frequent
	go g.generateAnalyticsEvents(ctx, config, sleepDuration/2)    // More frequent

	// Wait for stop signal
	<-g.stopChan
	g.isRunning = false
	g.logger.Info("Demo data generator stopped")

	return g.writer.Close()
}

// Stop stops the data generator
func (g *Generator) Stop() {
	if g.isRunning {
		close(g.stopChan)
	}
}

// generateSessionEvents generates user session events with controlled duplicates
func (g *Generator) generateSessionEvents(ctx context.Context, config GeneratorConfig, sleepDuration time.Duration) {
	topic := "user-sessions"
	eventCounter := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopChan:
			return
		default:
			user := g.getRandomUser()
			sessionID := g.getRandomSession()

			// Create base event
			event := SessionEvent{
				SessionID:       sessionID,
				UserID:          user.ID,
				Timestamp:       time.Now(),
				PageURL:         g.getRandomPageURL(),
				UserAgent:       g.getRandomUserAgent(),
				IPAddress:       g.getRandomIPAddress(),
				SessionDuration: rand.Intn(3600) + 60, // 1 minute to 1 hour
				PageViews:       rand.Intn(20) + 1,    // 1 to 20 page views
				EventType:       "session_event",
			}

			// Introduce duplicates based on configuration
			if rand.Float64() < config.DuplicateRatio {
				// Generate duplicate by reusing a recent session ID
				// This simulates real-world duplicate scenarios
				event.SessionID = fmt.Sprintf("dup_%s_%d", sessionID, eventCounter%10)
			}

			if err := g.sendEvent(topic, event); err != nil {
				g.logger.WithError(err).Error("Failed to send session event")
			}

			eventCounter++
			time.Sleep(g.applyJitter(sleepDuration, config.EnableJitter))
		}
	}
}

// generateTransactionEvents generates financial transaction events
func (g *Generator) generateTransactionEvents(ctx context.Context, config GeneratorConfig, sleepDuration time.Duration) {
	topic := "transactions"
	eventCounter := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopChan:
			return
		default:
			user := g.getRandomUser()
			transactionID := uuid.New().String()

			// Create base event
			event := TransactionEvent{
				TransactionID: transactionID,
				UserID:        user.ID,
				Timestamp:     time.Now(),
				Amount:        g.getRandomAmount(),
				Currency:      g.getRandomCurrency(),
				MerchantName:  g.getRandomMerchant(),
				Category:      g.getRandomCategory(),
				Status:        g.getRandomTransactionStatus(),
				PaymentMethod: g.getRandomPaymentMethod(),
			}

			// Introduce transaction duplicates (retries, double-charges)
			if rand.Float64() < config.DuplicateRatio {
				event.TransactionID = fmt.Sprintf("retry_%s", transactionID)
			}

			if err := g.sendEvent(topic, event); err != nil {
				g.logger.WithError(err).Error("Failed to send transaction event")
			}

			eventCounter++
			time.Sleep(g.applyJitter(sleepDuration*2, config.EnableJitter)) // Transactions less frequent
		}
	}
}

// generateUserProfileEvents generates user profile update events
func (g *Generator) generateUserProfileEvents(ctx context.Context, config GeneratorConfig, sleepDuration time.Duration) {
	topic := "user-profiles"

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopChan:
			return
		default:
			user := g.getRandomUser()

			event := UserProfileEvent{
				UserID:    user.ID,
				Timestamp: time.Now(),
				Name:      user.Name,
				Email:     user.Email,
				Tier:      user.Tier,
				Country:   user.Country,
				EventType: "profile_update",
			}

			if err := g.sendEvent(topic, event); err != nil {
				g.logger.WithError(err).Error("Failed to send user profile event")
			}

			time.Sleep(g.applyJitter(sleepDuration, config.EnableJitter))
		}
	}
}

// generateAnalyticsEvents generates general analytics events
func (g *Generator) generateAnalyticsEvents(ctx context.Context, config GeneratorConfig, sleepDuration time.Duration) {
	topic := "analytics-events"

	eventTypes := []string{"page_view", "click", "purchase", "signup", "login", "logout", "search", "add_to_cart"}

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopChan:
			return
		default:
			user := g.getRandomUser()
			sessionID := g.getRandomSession()
			eventType := eventTypes[rand.Intn(len(eventTypes))]

			event := AnalyticsEvent{
				EventID:    uuid.New().String(),
				EventType:  eventType,
				Timestamp:  time.Now(),
				UserID:     user.ID,
				SessionID:  sessionID,
				Properties: g.getEventProperties(eventType),
				DeviceType: g.getRandomDeviceType(),
				Platform:   g.getRandomPlatform(),
				Country:    user.Country,
				Value:      g.getEventValue(eventType),
			}

			if err := g.sendEvent(topic, event); err != nil {
				g.logger.WithError(err).Error("Failed to send analytics event")
			}

			time.Sleep(g.applyJitter(sleepDuration, config.EnableJitter))
		}
	}
}

// sendEvent sends an event to the specified Kafka topic
func (g *Generator) sendEvent(topic string, event interface{}) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	message := kafka.Message{
		Topic: topic,
		Value: eventBytes,
		Time:  time.Now(),
	}

	// Add key for partitioning (helps with ordering and performance)
	switch e := event.(type) {
	case SessionEvent:
		message.Key = []byte(e.UserID)
	case TransactionEvent:
		message.Key = []byte(e.UserID)
	case UserProfileEvent:
		message.Key = []byte(e.UserID)
	case AnalyticsEvent:
		message.Key = []byte(e.UserID)
	}

	return g.writer.WriteMessages(context.Background(), message)
}

// Helper methods for generating realistic data

func (g *Generator) generateUserPool(count int) {
	g.userPool = make([]User, count)

	firstNames := []string{"John", "Jane", "Mike", "Sarah", "David", "Lisa", "Chris", "Emma", "Ryan", "Ashley"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}
	countries := []string{"US", "CA", "UK", "DE", "FR", "AU", "JP", "BR", "IN", "MX"}
	tiers := []string{"bronze", "silver", "gold", "platinum"}

	for i := 0; i < count; i++ {
		firstName := firstNames[rand.Intn(len(firstNames))]
		lastName := lastNames[rand.Intn(len(lastNames))]

		g.userPool[i] = User{
			ID:         fmt.Sprintf("user_%d", i+1),
			Name:       fmt.Sprintf("%s %s", firstName, lastName),
			Email:      fmt.Sprintf("%s.%s@example.com", firstName, lastName),
			Tier:       tiers[rand.Intn(len(tiers))],
			Country:    countries[rand.Intn(len(countries))],
			SignupDate: time.Now().AddDate(0, -rand.Intn(12), -rand.Intn(30)),
		}
	}
}

func (g *Generator) generateSessionPool(count int) {
	g.sessionPool = make([]string, count)
	for i := 0; i < count; i++ {
		g.sessionPool[i] = fmt.Sprintf("session_%s", uuid.New().String()[:8])
	}
}

func (g *Generator) generateMerchantPool() {
	g.merchantPool = []string{
		"Amazon", "Netflix", "Spotify", "Uber", "Airbnb", "Starbucks", "McDonald's",
		"Best Buy", "Target", "Walmart", "Apple Store", "Google Play", "Steam",
		"Adobe", "Microsoft", "Dropbox", "GitHub", "Slack", "Zoom", "Salesforce",
	}
}

func (g *Generator) getRandomUser() User {
	return g.userPool[rand.Intn(len(g.userPool))]
}

func (g *Generator) getRandomSession() string {
	return g.sessionPool[rand.Intn(len(g.sessionPool))]
}

func (g *Generator) getRandomMerchant() string {
	return g.merchantPool[rand.Intn(len(g.merchantPool))]
}

func (g *Generator) getRandomPageURL() string {
	pages := []string{"/home", "/products", "/cart", "/checkout", "/profile", "/settings", "/help", "/about"}
	return pages[rand.Intn(len(pages))]
}

func (g *Generator) getRandomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15",
		"Mozilla/5.0 (Android 11; Mobile; rv:89.0) Gecko/89.0 Firefox/89.0",
	}
	return agents[rand.Intn(len(agents))]
}

func (g *Generator) getRandomIPAddress() string {
	return fmt.Sprintf("%d.%d.%d.%d",
		rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
}

func (g *Generator) getRandomAmount() float64 {
	// Generate realistic transaction amounts
	amounts := []float64{9.99, 19.99, 49.99, 99.99, 199.99, 299.99, 499.99, 999.99}
	return amounts[rand.Intn(len(amounts))] + rand.Float64()
}

func (g *Generator) getRandomCurrency() string {
	currencies := []string{"USD", "EUR", "GBP", "CAD", "AUD", "JPY"}
	return currencies[rand.Intn(len(currencies))]
}

func (g *Generator) getRandomCategory() string {
	categories := []string{"food", "entertainment", "shopping", "travel", "software", "gaming", "books", "music"}
	return categories[rand.Intn(len(categories))]
}

func (g *Generator) getRandomTransactionStatus() string {
	statuses := []string{"completed", "pending", "failed", "refunded"}
	// Weight towards completed
	if rand.Float64() < 0.8 {
		return "completed"
	}
	return statuses[rand.Intn(len(statuses))]
}

func (g *Generator) getRandomPaymentMethod() string {
	methods := []string{"credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"}
	return methods[rand.Intn(len(methods))]
}

func (g *Generator) getRandomDeviceType() string {
	devices := []string{"desktop", "mobile", "tablet", "tv"}
	return devices[rand.Intn(len(devices))]
}

func (g *Generator) getRandomPlatform() string {
	platforms := []string{"web", "ios", "android", "windows", "macos", "linux"}
	return platforms[rand.Intn(len(platforms))]
}

func (g *Generator) getEventProperties(eventType string) map[string]interface{} {
	switch eventType {
	case "purchase":
		return map[string]interface{}{
			"product_id": fmt.Sprintf("prod_%d", rand.Intn(1000)),
			"quantity":   rand.Intn(5) + 1,
			"discount":   rand.Float64() * 0.3,
		}
	case "search":
		queries := []string{"laptop", "phone", "headphones", "shoes", "books", "games"}
		return map[string]interface{}{
			"query":         queries[rand.Intn(len(queries))],
			"results_count": rand.Intn(100),
		}
	case "add_to_cart":
		return map[string]interface{}{
			"product_id": fmt.Sprintf("prod_%d", rand.Intn(1000)),
			"price":      g.getRandomAmount(),
		}
	default:
		return map[string]interface{}{
			"source": "demo_generator",
		}
	}
}

func (g *Generator) getEventValue(eventType string) float64 {
	switch eventType {
	case "purchase":
		return g.getRandomAmount()
	case "signup":
		return 50.0 // Customer acquisition value
	case "login":
		return 1.0 // Engagement value
	default:
		return 0.0
	}
}

func (g *Generator) applyJitter(duration time.Duration, enableJitter bool) time.Duration {
	if !enableJitter {
		return duration
	}

	// Add ±20% jitter to make timing more realistic
	jitter := time.Duration(float64(duration) * (rand.Float64()*0.4 - 0.2))
	return duration + jitter
}
