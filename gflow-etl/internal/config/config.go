package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	Kafka      KafkaConfig      `mapstructure:"kafka"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	Redis      RedisConfig      `mapstructure:"redis"`
	Demo       DemoConfig       `mapstructure:"demo"`
	Monitoring MonitoringConfig `mapstructure:"monitoring"`
}

// ServerConfig contains HTTP server configuration
type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

// KafkaConfig contains Kafka connection settings
type KafkaConfig struct {
	Brokers       []string      `mapstructure:"brokers"`
	ConsumerGroup string        `mapstructure:"consumer_group"`
	BatchSize     int           `mapstructure:"batch_size"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
	SASL          SASLConfig    `mapstructure:"sasl"`
	TLS           TLSConfig     `mapstructure:"tls"`
}

// SASLConfig for Kafka SASL authentication
type SASLConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	Mechanism string `mapstructure:"mechanism"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

// TLSConfig for Kafka TLS configuration
type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	CAFile             string `mapstructure:"ca_file"`
}

// ClickHouseConfig contains ClickHouse connection settings
type ClickHouseConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	Database     string        `mapstructure:"database"`
	Username     string        `mapstructure:"username"`
	Password     string        `mapstructure:"password"`
	TLS          bool          `mapstructure:"tls"`
	BatchSize    int           `mapstructure:"batch_size"`
	FlushTimeout time.Duration `mapstructure:"flush_timeout"`
	MaxRetries   int           `mapstructure:"max_retries"`
}

// RedisConfig for state management (deduplication and joins)
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	Database int    `mapstructure:"database"`
	PoolSize int    `mapstructure:"pool_size"`
}

// DemoConfig for demo data generation
type DemoConfig struct {
	Enabled        bool          `mapstructure:"enabled"`
	GenerateData   bool          `mapstructure:"generate_data"`
	DataRate       int           `mapstructure:"data_rate"` // events per second
	GenerateUsers  int           `mapstructure:"generate_users"`
	SessionWindow  time.Duration `mapstructure:"session_window"`
	DuplicateRatio float64       `mapstructure:"duplicate_ratio"` // 0.0 to 1.0
}

// MonitoringConfig for metrics and health checks
type MonitoringConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	MeticsPort int    `mapstructure:"metrics_port"`
	LogLevel   string `mapstructure:"log_level"`
}

// Load loads configuration from various sources
func Load() (*Config, error) {
	viper.SetConfigName("gflow-etl")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/etc/gflow-etl/")
	viper.AddConfigPath("$HOME/.gflow-etl")

	// Set defaults
	setDefaults()

	// Enable environment variable support
	viper.SetEnvPrefix("GFLOW")
	viper.AutomaticEnv()

	// Try to read config file
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found, using defaults and env vars
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &config, nil
}

// setDefaults sets default configuration values
func setDefaults() {
	// Server defaults
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.read_timeout", "30s")
	viper.SetDefault("server.write_timeout", "30s")

	// Kafka defaults
	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("kafka.consumer_group", "gflow-etl")
	viper.SetDefault("kafka.batch_size", 1000)
	viper.SetDefault("kafka.flush_interval", "5s")

	// ClickHouse defaults
	viper.SetDefault("clickhouse.host", "localhost")
	viper.SetDefault("clickhouse.port", 9000)
	viper.SetDefault("clickhouse.database", "default")
	viper.SetDefault("clickhouse.username", "default")
	viper.SetDefault("clickhouse.password", "")
	viper.SetDefault("clickhouse.tls", false)
	viper.SetDefault("clickhouse.batch_size", 10000)
	viper.SetDefault("clickhouse.flush_timeout", "10s")
	viper.SetDefault("clickhouse.max_retries", 3)

	// Redis defaults
	viper.SetDefault("redis.host", "localhost")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.password", "")
	viper.SetDefault("redis.database", 0)
	viper.SetDefault("redis.pool_size", 10)

	// Demo defaults
	viper.SetDefault("demo.enabled", true)
	viper.SetDefault("demo.generate_data", true)
	viper.SetDefault("demo.data_rate", 100)
	viper.SetDefault("demo.generate_users", 1000)
	viper.SetDefault("demo.session_window", "1h")
	viper.SetDefault("demo.duplicate_ratio", 0.1)

	// Monitoring defaults
	viper.SetDefault("monitoring.enabled", true)
	viper.SetDefault("monitoring.metrics_port", 9090)
	viper.SetDefault("monitoring.log_level", "info")
}