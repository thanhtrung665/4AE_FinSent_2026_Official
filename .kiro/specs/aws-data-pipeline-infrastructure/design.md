# Design Document - AWS Data Pipeline Infrastructure

## Overview

The AWS Data Pipeline Infrastructure is a containerized data processing system designed for deployment on AWS EC2 instances. This system provides a complete streaming data pipeline using a single Apache Kafka broker for message streaming, ChromaDB for vector storage, and comprehensive monitoring capabilities optimized for development and testing environments on t3.medium instances.

## Architecture

The system follows a single-node containerized architecture optimized for development and testing environments. The architecture prioritizes resource efficiency and simplicity while maintaining production-ready patterns for easy migration to multi-broker configurations when needed.

**Key Architectural Principles:**
- Single-node deployment optimized for t3.medium EC2 instances
- Container-based isolation with Docker Compose orchestration
- Persistent data storage with volume mapping
- Network security through AWS Security Groups
- Resource-optimized JVM configurations
- Development-friendly configuration with production migration path

## Components and Interfaces

### Core System Components

**1. Single Kafka Broker**
- **Purpose**: Primary message streaming service optimized for single-node deployment
- **Configuration**: 1GB JVM heap with development-optimized parameters
- **Interface**: Kafka Protocol on port 9092
- **Dependencies**: Zookeeper for coordination
- **Data Storage**: Persistent volume at `/opt/data-pipeline/kafka`

**2. ChromaDB Vector Database**
- **Purpose**: Vector storage and similarity search service
- **Configuration**: REST API enabled with persistent storage
- **Interface**: HTTP REST API on port 8000
- **Dependencies**: None (standalone service)
- **Data Storage**: Persistent volume at `/opt/data-pipeline/chromadb`

**3. Zookeeper Coordination Service**
- **Purpose**: Kafka broker coordination and metadata management
- **Configuration**: Single-node setup with minimal resource allocation
- **Interface**: Zookeeper Protocol on port 2181 (internal only)
- **Dependencies**: None
- **Data Storage**: Persistent volume at `/opt/data-pipeline/zookeeper`

**4. Monitoring Stack**
- **Purpose**: Service health monitoring and log aggregation
- **Configuration**: Lightweight monitoring with Docker log integration
- **Interface**: Log files and health check endpoints
- **Dependencies**: All core services
- **Data Storage**: Log aggregation at `/opt/data-pipeline/logs`

### Interface Specifications

**Kafka Producer/Consumer Interface:**
```
Protocol: Kafka Native Protocol
Port: 9092
Authentication: None (development mode)
Serialization: JSON/String
Throughput Target: 500 messages/second
Connection Limit: 25 concurrent clients
```

**ChromaDB REST Interface:**
```
Protocol: HTTP/1.1
Port: 8000  
Authentication: None (development mode)
Content-Type: application/json
Response Time: <100ms for 1M documents
Connection Pooling: 10 connections
```

**Health Check Interfaces:**
```
Kafka: kafka-topics.sh --bootstrap-server localhost:9092 --list
ChromaDB: GET http://localhost:8000/api/v1/heartbeat  
Zookeeper: echo ruok | nc localhost 2181
```

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS EC2 Instance                         │
│                       (t3.medium - 2vCPU, 4GB)                │
├─────────────────────────────────────────────────────────────────┤
│                     Docker Compose Network                      │
│  ┌─────────────┐  ┌─────────────────────────────────────────┐   │
│  │ Zookeeper   │  │         Single Kafka Broker             │   │
│  │   :2181     │◄─┤  ┌─────────────────────────────────────┐ │   │
│  │             │  │  │           kafka                     │ │   │
│  └─────────────┘  │  │          :9092                      │ │   │
│                   │  │     (1GB JVM Heap)                  │ │   │
│                   │  └─────────────────────────────────────┘ │   │
│                   └─────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                    ChromaDB Service                         │ │
│  │                      :8000                                  │ │
│  └─────────────────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                  Monitoring Stack                           │ │
│  │            Health Checks & Log Aggregation                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Network Architecture

```
AWS Security Group Configuration:
┌─────────────────────┐
│  Inbound Rules      │
├─────────────────────┤
│ SSH      22/tcp     │ ← Admin Access
│ Kafka    9092/tcp   │ ← Client Connections
│ ChromaDB 8000/tcp   │ ← API Access
└─────────────────────┘

┌─────────────────────┐
│ Outbound Rules      │
├─────────────────────┤
│ All Traffic         │ ← Service Dependencies
└─────────────────────┘
```

## Component Design

### 1. Docker Compose Infrastructure

#### Core Services Configuration

**Zookeeper Service:**
- Single instance coordination service
- Port: 2181 (internal)
- Persistent volume: `zookeeper-data`
- Memory limit: 512MB
- Restart policy: `unless-stopped`

**Kafka Single Broker:**
- Single instance deployment optimized for t3.medium
- Port: 9092 (external client access)
- JVM Heap: 1GB (KAFKA_HEAP_OPTS="-Xmx1G -Xms1G")
- Persistent volume: `kafka-data`
- Single partition default for topics
- Auto-topic creation: enabled
- Log retention: 168 hours (7 days) for development
- Optimized for development and testing environments

**ChromaDB Service:**
- Port: 8000 (REST API)
- Persistent volume: `chromadb-data`
- Memory limit: 1GB
- REST API enabled for development tools

#### Network Configuration

```yaml
networks:
  data-pipeline:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

### 2. Deployment Script (deploy.sh)

#### Installation Flow

```
┌─────────────────┐
│ System Check    │ → Validate OS, CPU, Memory
│                 │
└─────┬───────────┘
      │
┌─────▼───────────┐
│ Docker Install  │ → Install Engine if missing
│                 │ → Install Compose if missing
└─────┬───────────┘
      │
┌─────▼───────────┐
│ Service Deploy  │ → Pull images
│                 │ → Start containers
│                 │ → Verify health
└─────┬───────────┘
      │
┌─────▼───────────┐
│ Status Report   │ → Service connectivity
│                 │ → Resource usage
│                 │ → Access URLs
└─────────────────┘
```

#### System Requirements Validation
- OS: Linux (Ubuntu/CentOS/Amazon Linux)
- CPU: Minimum 2 cores
- Memory: Minimum 4GB RAM
- Disk: Minimum 20GB available
- Docker: Version 20.10+
- Docker Compose: Version 2.0+

#### Error Handling
- Pre-installation validation with clear error messages
- Graceful rollback on failure
- Service-specific health check timeouts
- Resource constraint warnings and recommendations

### 3. AWS Infrastructure Setup

#### EC2 Instance Specification
- **Instance Type:** t3.medium
- **vCPUs:** 2
- **Memory:** 4GB
- **Storage:** 30GB GP3 SSD (minimum)
- **Network:** Enhanced networking enabled
- **Operating System:** Amazon Linux 2 or Ubuntu 20.04 LTS

#### Security Group Configuration

```
Inbound Rules:
- Type: SSH, Protocol: TCP, Port: 22, Source: {ADMIN_IP}/32
- Type: Custom TCP, Protocol: TCP, Port: 9092, Source: {CLIENT_IPS}/32
- Type: Custom TCP, Protocol: TCP, Port: 8000, Source: {CLIENT_IPS}/32

Outbound Rules:
- Type: All Traffic, Protocol: All, Port Range: All, Destination: 0.0.0.0/0
```

### 4. Monitoring and Health Checks

#### Health Check Endpoints
- **Kafka:** `kafka-topics.sh --bootstrap-server localhost:9092 --list`
- **ChromaDB:** `HTTP GET http://localhost:8000/api/v1/heartbeat`
- **Zookeeper:** `echo ruok | nc localhost 2181`

#### Monitoring Stack Components
- Service health monitoring with 30-second intervals
- Resource usage monitoring (CPU, Memory, Disk)
- Log aggregation from all services
- Automated alerts for service failures
- Performance metrics collection

#### Log Management
- Centralized logging via Docker log drivers
- Log rotation: 10MB files, 5 files maximum
- Log levels: INFO for production, DEBUG for development
- Structured JSON logging for parsing

### 5. Data Persistence Strategy

#### Volume Mapping Strategy

```
Host Path Mapping:
├── /opt/data-pipeline/
│   ├── zookeeper/          → zookeeper-data volume
│   ├── kafka/              → kafka-data volume  
│   ├── chromadb/           → chromadb-data volume
│   └── logs/               → log aggregation
```

#### Backup Strategy
- Daily automated snapshots of persistent volumes
- Point-in-time recovery capability
- Cross-region backup replication (optional)
- Backup retention: 7 days for development

## Data Models

### Kafka Topic Schema
```json
{
  "topic_name": "string",
  "partitions": "integer (default: 1)",
  "replication_factor": "integer (1)",
  "retention_ms": "integer (604800000)", // 7 days
  "cleanup_policy": "string (delete)"
}
```

### ChromaDB Collection Schema
```json
{
  "collection_id": "uuid",
  "name": "string",
  "metadata": {
    "dimension": "integer",
    "distance_function": "string (cosine|euclidean|manhattan)"
  },
  "documents": [
    {
      "id": "string",
      "embedding": "float[]",
      "metadata": "object",
      "document": "string"
    }
  ]
}
```

## API Interfaces

### Kafka Producer/Consumer API

**Producer Configuration:**
```properties
bootstrap.servers=<EC2_PUBLIC_IP>:9092
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.JsonSerializer
acks=1
retries=3
```

**Consumer Configuration:**
```properties
bootstrap.servers=<EC2_PUBLIC_IP>:9092
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.JsonDeserializer
group.id=data-pipeline-consumer
auto.offset.reset=earliest
```

### ChromaDB REST API

**Base URL:** `http://<EC2_PUBLIC_IP>:8000`

**Key Endpoints:**
- `GET /api/v1/heartbeat` - Health check
- `POST /api/v1/collections` - Create collection
- `GET /api/v1/collections/{collection_id}` - Get collection
- `POST /api/v1/collections/{collection_id}/add` - Add documents
- `POST /api/v1/collections/{collection_id}/query` - Query similar documents

## Configuration Management

### Environment Variables
```bash
# Kafka Configuration
KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
KAFKA_LOG_RETENTION_HOURS=168
KAFKA_NUM_PARTITIONS=1
KAFKA_DEFAULT_REPLICATION_FACTOR=1

# ChromaDB Configuration  
CHROMA_SERVER_HOST=0.0.0.0
CHROMA_SERVER_HTTP_PORT=8000
CHROMA_PERSIST_DIRECTORY=/chroma/chroma

# System Configuration
DOCKER_COMPOSE_PROJECT_NAME=data-pipeline
DATA_PERSISTENCE_PATH=/opt/data-pipeline
```

### Dynamic Configuration Updates
- Kafka configuration via JMX and Admin API
- ChromaDB configuration via REST API
- Security group updates via AWS CLI/Console
- No service restart required for most configuration changes

## Security Considerations

### Network Security
- IP-based access control via AWS Security Groups
- VPC isolation for enhanced security
- TLS encryption for client connections (production deployment)
- Inter-service communication over isolated Docker network

### Data Security
- Persistent volume encryption at rest
- Network traffic encryption in transit
- Access logs for audit trails
- Regular security updates via automated patching

### Authentication & Authorization
- AWS IAM roles for EC2 instance permissions
- Security group rules for network access control
- Docker container isolation
- Optional: Kafka SASL authentication for production use

## Error Handling

### Service Failure Recovery
```
Service Failure Detection:
├── Health Check Timeout (30s)
├── Resource Exhaustion Detection  
├── Network Connectivity Issues
└── Container Exit Codes

Recovery Actions:
├── Automatic Restart (restart: unless-stopped)
├── Service Dependencies (depends_on)
├── Health Check Retries (3 attempts)
└── Manual Intervention Alerts
```

### Common Error Scenarios
1. **Insufficient Memory:** Monitoring alerts + resource recommendations
2. **Network Connectivity:** Security group validation + connectivity tests
3. **Service Startup Failures:** Detailed error logging + troubleshooting guide
4. **Data Corruption:** Backup restoration procedures
5. **Performance Degradation:** Resource scaling recommendations

## Performance Optimization

### Resource Allocation Strategy
```
t3.medium (4GB RAM) Distribution:
├── Operating System: ~512MB
├── Kafka Single Broker: ~1GB (optimized JVM heap)
├── ChromaDB: ~1GB
├── Zookeeper: ~256MB
├── Monitoring: ~128MB
└── System Buffer: ~1GB (significant improvement from cluster)
```

### Performance Tuning Parameters

**Kafka Optimization:**
- `num.network.threads=2` (optimized for single broker)
- `num.io.threads=4` (reduced for single node)
- `socket.send.buffer.bytes=102400`
- `socket.receive.buffer.bytes=102400`
- `log.flush.interval.messages=10000`
- `log.segment.bytes=1073741824` (1GB segments)
- `log.retention.check.interval.ms=300000` (5 minutes)

**ChromaDB Optimization:**
- Connection pooling: 10 connections
- Batch insert size: 1000 documents
- Query timeout: 30 seconds
- Memory mapping for large datasets

## Development Support

### Development Mode Features
- Reduced resource requirements
- Faster startup times (development profile)
- Debug logging enabled
- Auto-reload on configuration changes
- Sample data generators for testing

### Sample Configurations

**Sample Kafka Producer (Python):**
```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['<EC2_IP>:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

producer.send('test-topic', {'message': 'Hello World'})
```

**Sample ChromaDB Client (Python):**
```python
import chromadb

client = chromadb.HttpClient(host='<EC2_IP>', port=8000)
collection = client.create_collection("test-collection")
collection.add(
    documents=["Sample document"],
    metadatas=[{"source": "test"}],
    ids=["id1"]
)
```

## Testing Strategy

### Integration Testing Approach
- Service connectivity verification
- End-to-end data flow testing
- Failure recovery testing
- Performance benchmarking
- Security validation

### Load Testing Parameters
- Kafka throughput: 500 messages/second (optimized for single broker)
- ChromaDB query latency: <100ms for 1M documents
- Concurrent connections: 25 clients (adjusted for single broker)
- Data retention testing: 7-day cycles

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system—essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Single Broker Stability

*For any* single Kafka broker deployment on t3.medium, the broker SHALL maintain stable operation under development workloads without memory exhaustion or service failures.

**Validates: Requirements 2.1, 2.2, 2.7**

### Property 2: Data Persistence Across Restarts

*For any* service with persistent volumes (Kafka, ChromaDB, Zookeeper), when containers are recreated, all previously stored data SHALL be retained and accessible.

**Validates: Requirements 7.1, 7.2, 7.3, 7.7**

### Property 3: Service Health Verification

*For any* deployed service in the data pipeline, health check endpoints SHALL respond with valid status information within the configured timeout period.

**Validates: Requirements 6.1, 6.7**

### Property 4: Resource Constraint Compliance

*For any* t3.medium EC2 instance, the single-broker data pipeline system SHALL start all services within 300 seconds and maintain stable operation under typical development workloads.

**Validates: Requirements 8.4, 8.5**

### Property 5: Security Group Access Control

*For any* IP address not in the configured whitelist, connection attempts to Kafka (port 9092) and ChromaDB (port 8000) SHALL be rejected by the AWS Security Group.

**Validates: Requirements 5.1, 5.2, 5.3, 5.5**

### Property 6: Configuration Change Isolation

*For any* configuration update that doesn't require service restart, the change SHALL be applied without interrupting active connections or causing data loss.

**Validates: Requirements 5.8, 9.4**

### Property 7: Deployment Script Validation

*For any* target system that meets minimum requirements, the deploy script SHALL successfully install all dependencies and start all services with appropriate health verification.

**Validates: Requirements 3.1, 3.2, 3.5, 3.6**

### Property 8: Monitoring Event Logging

*For any* service failure or health check failure, the monitoring stack SHALL log the failure event with sufficient detail for troubleshooting.

**Validates: Requirements 6.4, 6.8**

## Implementation Roadmap

### Phase 1: Core Infrastructure (Week 1)
- Docker Compose configuration
- Basic service definitions
- Network setup
- Volume configuration

### Phase 2: Service Configuration (Week 2)  
- Kafka cluster configuration
- ChromaDB setup
- Zookeeper coordination
- Health checks implementation

### Phase 3: Deployment Automation (Week 3)
- Deploy script development
- System requirement validation
- Error handling implementation
- Status reporting

### Phase 4: AWS Integration (Week 4)
- Security group configuration
- EC2 optimization
- Documentation completion
- Performance tuning

### Phase 5: Monitoring & Testing (Week 5)
- Monitoring stack implementation
- Integration testing
- Performance benchmarking
- Documentation finalization

## Maintenance and Operations

### Regular Maintenance Tasks
- Weekly log rotation and cleanup
- Monthly security updates
- Quarterly performance reviews
- Backup validation testing

### Scaling Considerations
- Horizontal scaling: Upgrade to larger EC2 instances (t3.large, t3.xlarge)
- Vertical scaling: Increase JVM heap size up to 2GB on larger instances  
- Multi-broker deployment: Add additional brokers when moving to production
- Auto-scaling based on resource utilization
- Migration path: Single broker → Multi-broker cluster when needed

### Troubleshooting Guide

**Common Issues:**
1. **Services won't start:** Check resource availability and logs
2. **Connection timeouts:** Verify security group and network configuration  
3. **Data loss:** Restore from backups, check replication settings
4. **Performance issues:** Monitor resource usage, tune JVM settings
5. **Deployment failures:** Validate system requirements, check Docker status

**Debug Commands:**
```bash
# Service status
docker-compose ps
docker-compose logs <service_name>

# Resource usage  
docker stats
df -h /opt/data-pipeline

# Network connectivity
telnet <EC2_IP> 9092
curl http://<EC2_IP>:8000/api/v1/heartbeat

# Kafka testing
kafka-topics.sh --bootstrap-server localhost:9092 --list
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

This design provides a comprehensive, production-ready data pipeline infrastructure optimized for AWS deployment with strong emphasis on reliability, monitoring, and development productivity.