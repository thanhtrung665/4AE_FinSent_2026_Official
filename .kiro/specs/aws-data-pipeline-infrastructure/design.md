# Design Document - AWS Data Pipeline Infrastructure

## Overview

The AWS Data Pipeline Infrastructure is a production-ready, containerized financial data streaming system specifically engineered for AWS EC2 t3.large instances (2 vCPU, 8GB RAM). This system provides a resilient real-time pipeline utilizing a single Apache Kafka broker with four dedicated topics for multi-source financial data ingestion and ChromaDB for vector-indexed macroeconomic policy storage. The infrastructure is optimized to support comparative analysis between standard market baselines (SHB baseline period: 01/01/2026 to 01/06/2026) and systemic crisis events (SCB crisis period: 01/09/2022 to 31/12/2022).

## Architecture

The system implements a single-node containerized architecture managed via Docker Compose, specifically optimized for AWS EC2 t3.large hardware specifications. The design prioritizes memory efficiency, chronological data ordering, and zero-loss persistence while providing clear engineering paths for future multi-node production scaling.

**Key Architectural Principles:**
- **AWS t3.large Optimization:** Memory allocation tuned for 8GB RAM with 2GB JVM heap for Kafka (`KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"`) to prevent OOM failures
- **Strict Chronological Ordering:** Single-partition Kafka topics ensure absolute sequence integrity for time-series analysis
- **Isolated Custom Bridge Network:** Secure inter-container communication with AWS Security Group integration
- **Standard JSON Format:** Universal `Standard_JSON` serialization across all transport layers, explicitly forbidding JSONL variants
- **Independent Data Persistence:** Separate volume mounts for Kafka logs and ChromaDB collections for independent recovery

---

## Components and Interfaces

### Core System Components

**1. Single Kafka Broker**
- **Purpose**: High-throughput message streaming for four core financial data topics: `news_rss_data`, `f319_data`, `fb_mock_data`, and `market_stock_data`
- **AWS Configuration**: 2GB JVM Heap allocation (`KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"`) optimized for t3.large instances
- **Topics Architecture**: Single-partition per topic for guaranteed chronological ordering
- **Interface**: Kafka Native Binary Protocol on external port `9092` with dynamic AWS Elastic IP binding via `KAFKA_ADVERTISED_LISTENERS`
- **Dependencies**: Zookeeper coordination service with strict health check dependencies
- **Data Storage**: Persistent Docker volume mapped to `/opt/data-pipeline/kafka`
- **Log Retention**: Development-optimized settings (`log.retention.hours=72`, `log.segment.bytes=1073741824`) for EBS storage efficiency

**2. ChromaDB Vector Database**
- **Purpose**: Vector storage and semantic search for macroeconomic policy documents with timeline-based indexing
- **Configuration**: HTTP REST API hosting `macro_policies` collection with cosine similarity metrics
- **Interface**: HTTP REST API on external port `8000` with AWS Security Group protection
- **Data Format**: Strict `Standard_JSON` structure with required fields: `source_url`, `ticker_context`, `publish_date` (ISO 8601)
- **Dependencies**: Standalone container architecture
- **Data Storage**: Isolated persistent Docker volume mapped to `/opt/data-pipeline/chromadb`
- **Access Control**: Restricted to authorized ingestion agents within AWS network boundary

**3. Zookeeper Coordination Service**
- **Purpose**: Kafka broker metadata management and cluster state coordination
- **Configuration**: Single-node instance with minimal resource overhead
- **Interface**: Zookeeper Protocol on port `2181` (internal bridge network only)
- **Dependencies**: None (base service)
- **Data Storage**: Persistent volume mapped to `/opt/data-pipeline/zookeeper`

---

### Interface Specifications

#### Kafka Data Ingestion Interface

**Topic Configuration:**
```yaml
Topics:
  news_rss_data:
    partitions: 1
    replication-factor: 1
    purpose: RSS financial news feeds
    
  f319_data:
    partitions: 1  
    replication-factor: 1
    purpose: Vietnamese F319 forum discussions
    required_fields: ["ticker_context", "content", "timestamp"]
    
  fb_mock_data:
    partitions: 1
    replication-factor: 1  
    purpose: Facebook mock social media data
    required_fields: ["ticker_context", "content", "timestamp"]
    
  market_stock_data:
    partitions: 1
    replication-factor: 1
    purpose: VNStock API market data
    supported_periods:
      - SHB_baseline: "01/01/2026 to 01/06/2026"
      - SCB_crisis: "01/09/2022 to 31/12/2022"
      - proxies: ["VNINDEX", "STB"]
```

**Message Format (Standard_JSON):**
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "ticker_context": "SHB|SCB",
  "source": "rss|f319|facebook|vnstock",
  "content": {
    "title": "string",
    "body": "string",
    "metadata": {
      "publish_date": "ISO 8601",
      "source_url": "string"
    }
  }
}
```

#### ChromaDB Vector Storage Interface

**Collection Schema:**
```yaml
Collection: macro_policies
Similarity Metric: cosine
Metadata Structure:
  source_url: string (required)
  ticker_context: "SHB" | "SCB" (required)
  publish_date: ISO 8601 string (required)
  document_type: "policy" | "regulation" | "announcement"
  relevance_score: float
```

**REST API Endpoints:**
- `GET /api/v1/collections/macro_policies` - Collection info
- `POST /api/v1/collections/macro_policies/add` - Document ingestion
- `POST /api/v1/collections/macro_policies/query` - Semantic search
- `GET /api/v1/heartbeat` - Health check

---

## Data Models

### Financial Message Schema

```python
class FinancialMessage:
    timestamp: datetime  # ISO 8601 format
    ticker_context: Literal["SHB", "SCB"]
    source: Literal["rss", "f319", "facebook", "vnstock"]
    content: MessageContent
    
class MessageContent:
    title: str
    body: str
    metadata: MessageMetadata
    
class MessageMetadata:
    publish_date: datetime
    source_url: str
    sentiment_score: Optional[float]
    topic_tags: List[str]
```

### Policy Document Schema

```python
class PolicyDocument:
    document_id: str
    source_url: str
    ticker_context: Literal["SHB", "SCB"]
    publish_date: datetime
    document_type: Literal["policy", "regulation", "announcement"]
    content: str
    embedding_vector: List[float]
    relevance_score: float
```

### Case Study Time Ranges

```python
class CaseStudyPeriods:
    SHB_BASELINE = {
        "start": "2026-01-01T00:00:00Z",
        "end": "2026-06-01T00:00:00Z", 
        "description": "Standard market baseline conditions",
        "primary_ticker": "SHB"
    }
    
    SCB_CRISIS = {
        "start": "2022-09-01T00:00:00Z",
        "end": "2022-12-31T23:59:59Z",
        "description": "Systemic banking crisis period",
        "primary_ticker": "SCB",
        "proxy_tickers": ["VNINDEX", "STB"]
    }
```

---

## AWS Infrastructure Configuration

### EC2 Instance Specifications

```yaml
Instance Type: t3.large
vCPU: 2
Memory: 8GB RAM
Storage: 20GB+ EBS GP3
Operating System: Ubuntu Server 22.04 LTS
Network: VPC with custom Security Groups
```

### Security Group Configuration

```yaml
Inbound Rules:
  - Port 22 (SSH): Restricted to development team IPs
  - Port 9092 (Kafka): Restricted to authorized producer IPs  
  - Port 8000 (ChromaDB): Restricted to internal network
  
Outbound Rules:
  - Port 80/443 (HTTP/HTTPS): All destinations (for package updates)
  - Port 53 (DNS): All destinations
```

### Docker Compose Architecture

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 30s
      timeout: 10s
      retries: 3
    
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      zookeeper:
        condition: service_healthy
    restart: unless-stopped
    environment:
      KAFKA_HEAP_OPTS: "-Xmx2G -Xms2G"
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://${AWS_ELASTIC_IP}:9092
    healthcheck:
      test: ["CMD", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
      
  chromadb:
    image: chromadb/chroma:latest
    restart: unless-stopped
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/v1/heartbeat"]

networks:
  pipeline-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16

volumes:
  kafka-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /opt/data-pipeline/kafka
  chromadb-data:
    driver: local  
    driver_opts:
      type: none
      o: bind
      device: /opt/data-pipeline/chromadb
  zookeeper-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /opt/data-pipeline/zookeeper
```

---

## Error Handling and Resilience

### Container Restart Policies

All core services implement `restart: unless-stopped` policies with health check dependencies to ensure automatic recovery from failures while maintaining data integrity.

### Data Persistence Strategy

```yaml
Persistence Guarantees:
  kafka-data: 
    - Message retention: 72 hours
    - Segment size: 1GB
    - Replication: Single-node (development)
    - Recovery: Full message log persistence through container restarts
    
  chromadb-data:
    - Vector embeddings: Persistent across restarts
    - Collection metadata: Fully recoverable
    - Backup strategy: Independent volume mounts
    
  zookeeper-data:
    - Metadata persistence: Critical for Kafka recovery
    - Log retention: Standard Zookeeper configuration
```

### AWS-Specific Resilience

```yaml
EBS Storage:
  Type: GP3 (General Purpose SSD)
  IOPS: 3000 baseline
  Throughput: 125 MB/s baseline
  Backup: Daily snapshots recommended

Network Resilience:
  Elastic IP: Static IP binding for external producers
  VPC: Isolated network with custom route tables
  Multi-AZ: Single-AZ deployment (development)
```

---

## Deployment Configuration

### Automated Deployment Script

```bash
#!/bin/bash
# deploy-aws-pipeline.sh

# Pre-flight checks for t3.large compatibility
check_system_requirements() {
    MEMORY_GB=$(free -g | awk '/^Mem:/{print $2}')
    if [ $MEMORY_GB -lt 8 ]; then
        echo "ERROR: Insufficient memory. t3.large requires 8GB, found ${MEMORY_GB}GB"
        echo "RECOMMENDATION: Use t3.large or larger instance type"
        exit 1
    fi
}

# Docker installation for AWS Ubuntu
install_docker_aws() {
    if ! command -v docker &> /dev/null; then
        echo "Installing Docker Engine..."
        curl -fsSL https://get.docker.com -o get-docker.sh
        sudo sh get-docker.sh
        sudo usermod -aG docker $USER
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo "Installing Docker Compose..."
        sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
    fi
}

# Service initialization with health checks
deploy_pipeline() {
    echo "Creating data directories..."
    sudo mkdir -p /opt/data-pipeline/{kafka,chromadb,zookeeper}
    sudo chown -R $USER:$USER /opt/data-pipeline
    
    echo "Starting services..."
    docker-compose up -d
    
    echo "Waiting for services..."
    wait_for_port 9092 "Kafka Broker"
    wait_for_port 8000 "ChromaDB API"
    
    echo "Deployment complete!"
    echo "Kafka endpoint: ${AWS_ELASTIC_IP}:9092"
    echo "ChromaDB endpoint: ${AWS_ELASTIC_IP}:8000"
}
```

### Environment Configuration

```bash
# .env file for AWS deployment
AWS_ELASTIC_IP=<your-elastic-ip>
KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
CHROMADB_HOST=0.0.0.0
CHROMADB_PORT=8000
LOG_LEVEL=INFO
DEVELOPMENT_MODE=true
```

---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: VNStock Data Format Compatibility

*For any* valid VNStock API payload within the supported periods (SHB baseline 01/01/2026-01/06/2026 or SCB crisis 01/09/2022-31/12/2022), the system SHALL successfully ingest the payload into the `market_stock_data` topic without data corruption or format errors.

**Validates: Requirements 2.4**

### Property 2: Ticker Context Validation

*For any* JSON payload submitted to `f319_data` or `fb_mock_data` topics, the payload SHALL be accepted if and only if it contains a valid `ticker_context` field with value "SHB" or "SCB" in Standard_JSON format.

**Validates: Requirements 2.5**

### Property 3: ChromaDB Metadata Structure Enforcement  

*For any* document ingestion attempt to the `macro_policies` collection, the system SHALL accept the document if and only if the metadata contains all required fields (`source_url`, `ticker_context`, `publish_date`) in valid Standard_JSON format.

**Validates: Requirements 5.3**

### Property 4: JSONL Format Rejection

*For any* data submission attempt to ChromaDB vector indexes, if the data is in line-delimited JSONL format, the system SHALL reject the submission and return a format validation error.

**Validates: Requirements 5.4**

### Property 5: Data Persistence Across Container Restarts

*For any* ingested data in both Kafka topics and ChromaDB collections, after simulating container crashes or system reboots, the system SHALL preserve 100% of the historical data for both SHB and SCB context frames without data loss.

**Validates: Requirements 6.3**

### Property 6: Environment Configuration Hot-Reload

*For any* valid environment variable update via `.env` files, the system SHALL apply the configuration changes without requiring database purges or losing existing historical data streams.

**Validates: Requirements 6.4**

---

## Testing Strategy

### Property-Based Testing Framework

**Test Configuration:**
- Minimum 100 iterations per property test
- Property test tags reference design document properties
- Tag format: **Feature: aws-data-pipeline-infrastructure, Property {number}: {property_text}**

**Unit Testing Focus:**
- AWS Security Group configuration validation
- Docker Compose service dependency verification  
- Port mapping and network configuration checks
- Deployment script error condition handling
- Documentation completeness validation

**Integration Testing:**
- End-to-end data flow through all four Kafka topics
- ChromaDB vector search performance with policy documents
- AWS EC2 deployment validation on t3.large instances
- Cross-container communication via bridge network
- Disaster recovery and data persistence scenarios

### Development Validation Tools

```bash
# Health check utilities
./scripts/check-kafka-connectivity.sh
./scripts/verify-chromadb-collections.sh  
./scripts/validate-security-groups.sh
./scripts/test-topic-ingestion.sh

# Performance monitoring
./scripts/monitor-memory-usage.sh  # t3.large memory optimization
./scripts/track-ebs-usage.sh       # Storage efficiency tracking
```

This design provides a comprehensive, AWS-optimized architecture specifically tailored for the SHB/SCB comparative analysis case studies while ensuring robust data integrity and scalable performance on t3.large infrastructure.