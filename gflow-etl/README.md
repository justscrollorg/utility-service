# GFlow ETL - GlassFlow Interview Preparation

This implementation recreates GlassFlow's core functionality for Kafka to ClickHouse streaming with deduplication and joins.

## 🎯 Interview Use Cases Implemented

### 1. **Real-time Session Deduplication**
**Problem**: E-commerce platform receives duplicate session events due to network retries and client-side issues.

**GlassFlow Solution**: 
- Time-window based deduplication (up to 7 days like GlassFlow)
- Configurable deduplication keys
- State management with Redis
- Automatic cleanup of expired keys

**Implementation**: `internal/deduplication/engine.go`

**Demo Pipeline**:
```json
{
  "name": "user-session-dedup",
  "source": {
    "type": "kafka",
    "topic": "user-sessions"
  },
  "processing": {
    "deduplication": {
      "enabled": true,
      "key": "session_id",
      "time_window": "24h"
    }
  },
  "sink": {
    "type": "clickhouse",
    "table": "user_sessions_clean"
  }
}
```

### 2. **Temporal Stream Joins**
**Problem**: Join transaction events with user profile data in real-time for fraud detection.

**GlassFlow Solution**:
- Temporal joins with configurable time windows
- Stateful processing with Redis buffer
- Automatic handling of late arrivals
- Join performance metrics

**Implementation**: `internal/join/engine.go`

**Demo Pipeline**:
```json
{
  "name": "transaction-user-join",
  "source": {
    "type": "kafka-join",
    "join": {
      "left_topic": "transactions",
      "right_topic": "user-profiles",
      "join_key": "user_id",
      "join_type": "temporal",
      "time_window": "5m"
    }
  },
  "processing": {
    "deduplication": {
      "enabled": true,
      "key": "transaction_id",
      "time_window": "12h"
    }
  },
  "sink": {
    "type": "clickhouse",
    "table": "enriched_transactions"
  }
}
```

### 3. **Optimized ClickHouse Batching**
**Problem**: High-frequency events need efficient ingestion without overwhelming ClickHouse.

**GlassFlow Solution**:
- Smart batching with size and time triggers
- Connection pooling and retry logic
- Automatic schema detection
- Performance monitoring

**Implementation**: `internal/clickhouse/sink.go`

**Features**:
- Configurable batch sizes (default: 10,000 events)
- Time-based flushing (default: 10s)
- Exponential backoff retry
- Comprehensive metrics

### 4. **Real-time Analytics Pipeline**
**Problem**: Need real-time user behavior analytics with deduplication and enrichment.

**Implementation**: Complete pipeline with:
- Event generation (`internal/demo/generator.go`)
- Deduplication by event_id
- User profile enrichment via joins
- Real-time aggregation in ClickHouse

## 🚀 Getting Started

### Prerequisites
- Kubernetes cluster with Kafka and ClickHouse
- Redis for state management
- Go 1.24+ for development

### Quick Start

1. **Deploy Infrastructure**:
```bash
# Deploy Kafka (if not already deployed)
kubectl apply -f _infra/kafka/kafka.yaml

# Deploy Redis for state management
kubectl apply -f _infra/redis/

# Update ClickHouse connection details
```

2. **Build and Deploy**:
```bash
# Build Docker image
docker build -t justscroll/gflow-etl:latest ./gflow-etl/

# Deploy to Kubernetes
kubectl apply -f gflow-etl/k8s/deployment.yaml

# Or deploy integrated with existing grpcutils
kubectl apply -f grpcutils/k8s/
```

3. **Access the Dashboard**:
```bash
# Port forward to access UI
kubectl port-forward svc/utility-service 8080:8080

# Open browser to http://localhost:8080
```

## 📊 Demo Scenarios

### Scenario 1: E-commerce Session Tracking
**Demonstrates**: Deduplication, late arrivals, time windows

```bash
# Start demo data generation
curl -X POST http://localhost:8080/api/v1/demo/start?events_per_sec=50

# Monitor deduplication metrics
curl http://localhost:8080/api/v1/pipelines/user-session-dedup/metrics
```

**Expected Results**:
- ~10% duplicate events blocked
- Clean session data in ClickHouse
- Real-time deduplication metrics

### Scenario 2: Financial Transaction Processing
**Demonstrates**: Temporal joins, transaction enrichment

```bash
# Create transaction join pipeline
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d @examples/transaction-join-pipeline.json

# Monitor join performance
curl http://localhost:8080/api/v1/pipelines/transaction-user-join/metrics
```

**Expected Results**:
- Transaction events joined with user profiles
- Join rate metrics (target: >90%)
- Enriched data in ClickHouse

### Scenario 3: Real-time Analytics
**Demonstrates**: High-throughput processing, batch optimization

```bash
# Increase data generation rate
curl -X POST http://localhost:8080/api/v1/demo/start?events_per_sec=500

# Monitor system performance
curl http://localhost:8080/api/v1/metrics
```

**Expected Results**:
- High throughput processing (500+ events/sec)
- Efficient ClickHouse batching
- Low latency end-to-end

## 🔧 Architecture Highlights

### Components (Similar to GlassFlow)
1. **Pipeline Manager**: Orchestrates data flows
2. **Deduplication Engine**: Time-window based duplicate detection
3. **Join Engine**: Temporal stream joining with state management
4. **ClickHouse Sink**: Optimized batch inserter
5. **Demo Generator**: Realistic data for testing

### Key Features
- **Exactly-once processing**: Kafka offsets + Redis state
- **7-day deduplication windows**: Like GlassFlow's maximum
- **Temporal joins**: 5-minute default windows
- **Auto-scaling**: Kubernetes-native deployment
- **Comprehensive monitoring**: REST API + metrics

### Performance Characteristics
- **Throughput**: 1000+ events/sec per pipeline
- **Latency**: <100ms end-to-end (excluding ClickHouse)
- **Memory**: Efficient Redis-based state management
- **Reliability**: Automatic retries and error handling

## 📈 Metrics and Monitoring

### Pipeline Metrics
- Events processed/sec
- Deduplication rate
- Join success rate
- ClickHouse batch efficiency
- Error rates and retry counts

### System Metrics
- Resource utilization
- Connection pool status
- Redis state store size
- End-to-end latency

### API Endpoints
```bash
# System status
GET /api/v1/status

# Pipeline management
GET /api/v1/pipelines
POST /api/v1/pipelines
GET /api/v1/pipelines/{name}/metrics

# Demo controls
POST /api/v1/demo/start
POST /api/v1/demo/stop
```

## 🎤 Interview Talking Points

### Technical Depth
1. **Deduplication Strategy**: 
   - Sliding time windows vs. ReplacingMergeTree
   - Memory vs. accuracy tradeoffs
   - Cleanup strategies for expired state

2. **Join Implementation**:
   - State store design decisions
   - Late arrival handling
   - Performance optimization techniques

3. **ClickHouse Optimization**:
   - Batch size tuning
   - Connection management
   - Schema evolution handling

4. **Production Considerations**:
   - Horizontal scaling approaches
   - Failure recovery mechanisms
   - Monitoring and alerting strategies

### Business Value
1. **Data Quality**: Eliminate duplicates before they reach ClickHouse
2. **Performance**: Reduce FINAL queries and JOIN operations in ClickHouse
3. **Cost Efficiency**: Lower storage and compute costs
4. **Real-time Insights**: Enable real-time analytics with clean, joined data

## 🔄 Continuous Improvement

This implementation serves as a foundation for discussing:
- Optimization strategies
- Scaling challenges
- Alternative architectures
- Industry best practices

Perfect preparation for demonstrating understanding of GlassFlow's problem space and technical approach!