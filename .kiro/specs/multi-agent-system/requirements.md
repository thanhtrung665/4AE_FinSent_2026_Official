# Requirements Document

## Multi-Agent System for Vietnam Market Sentiment Index (VMSI)

## Introduction

The Multi-Agent System implements a distributed, fault-tolerant architecture for calculating the Vietnam Market Sentiment Index (VMSI) in real-time. This system orchestrates specialized agents using modern async patterns and robust error handling to process social media sentiment data and macroeconomic policies, producing reliable financial market indicators.

The system integrates social media sentiment analysis (via PhoBERT processing) with policy sentiment evaluation from central bank communications to generate a composite market sentiment index. It features comprehensive error handling, connection pooling, dead letter queues, and automated risk assessment with Vietnamese-language warnings for extreme market conditions.

## Glossary

- **Multi_Agent_System**: Distributed system architecture coordinating specialized agents for VMSI calculation using modern async patterns and error handling
- **VMSI_Engine**: Mathematical computation module implementing vectorized NumPy operations for sentiment index calculations with comprehensive validation
- **Social_Agent**: Asynchronous Kafka consumer agent processing PhoBERT sentiment data with connection pooling and dead letter queue management
- **Macro_Agent**: ChromaDB semantic query agent with connection pooling, caching, and retry logic for policy sentiment analysis
- **Risk_Synthesis_Agent**: Central coordination agent implementing risk assessment, Vietnamese warning generation, and atomic file operations
- **MAC_System**: Multi-Agent Controller orchestrating agent lifecycles and workflow execution using deterministic routing
- **PhoBERT_Score**: Vietnamese-language BERT sentiment score in range [-1, 1] representing negative to positive sentiment
- **Interaction_Weight**: Logarithmic compression factor `ln(1 + likes + shares + comments)` applied to social media engagement metrics
- **Credibility_Factor**: Dynamic source reputation weight ranging from 0.1 (suspicious/bot) to 1.0 (verified/institutional)
- **NHNN_Policy_Score**: State Bank of Vietnam policy sentiment score in discrete set {-1, 0, 1} for restrictive, neutral, or accommodative policies
- **Anti_FUD_Protocol**: Automated cross-validation system detecting and mitigating fake news impact on sentiment calculations
- **Connection_Pool**: Managed database connection pool with health monitoring, retry logic, and automatic failover capabilities
- **Dead_Letter_Queue**: Message queue system for handling failed or malformed Kafka messages with separate processing workflows
- **Circuit_Breaker**: Fault tolerance pattern preventing cascade failures by temporarily isolating failed external services
- **EMA_Smoothing**: Exponential Moving Average temporal filtering with λ=0.2 smoothing factor for index stability

---

## Requirements

### Requirement 1: VMSI Mathematical Engine with Comprehensive Validation

**User Story:** As a quantitative analyst, I want a robust mathematical engine implementing VMSI calculations with comprehensive input validation and error handling, so that the computed sentiment index accurately reflects market conditions without computational errors.

#### Acceptance Criteria

1. THE VMSI_Engine SHALL implement interaction weight calculation using the logarithmic formula `E_i = ln(1 + likes_i + shares_i + comments_i)` with non-negative integer validation
2. THE VMSI_Engine SHALL validate PhoBERT scores are within range [-1, 1] and credibility factors within range [0.1, 1.0] before computation
3. THE VMSI_Engine SHALL compute social sentiment score using the weighted formula `S_social(t) = Σ(s_i × E_i × R_i) / Σ(E_i × R_i)` where division by zero returns 0.0
4. THE VMSI_Engine SHALL calculate macroeconomic policy score using the weighted formula `S_macro(t) = 0.7 × S_nhnn(t) + 0.3 × S_news(t)`
5. WHEN calculating macro scores, THE VMSI_Engine SHALL validate S_nhnn is in the discrete set {-1, 0, 1}
6. THE VMSI_Engine SHALL compute raw market index using the weighted combination `I_raw(t) = 0.6 × S_macro(t) + 0.4 × S_social(t)`
7. THE VMSI_Engine SHALL transform raw index to bounded scale using `VMSI(t) = 50 × (I_raw(t) + 1)` with negative boundary condition handling returning VMSI = 0
8. THE VMSI_Engine SHALL apply exponential moving average smoothing using `VMSI_smoothed(t) = 0.2 × VMSI(t) + 0.8 × VMSI_smoothed(t-1)`
9. THE VMSI_Engine SHALL validate all intermediate calculations produce finite floating-point values and raise VMSICalculationError for non-finite results
10. THE VMSI_Engine SHALL log calculation details at DEBUG level and final results at INFO level for audit traceability

---

### Requirement 2: Social Agent with Enhanced Kafka Integration

**User Story:** As a data processing pipeline, I want a robust Social Agent with advanced Kafka integration including connection pooling, dead letter queues, and comprehensive error handling, so that social media sentiment data is reliably processed without message loss.

#### Acceptance Criteria

1. THE Social_Agent SHALL connect to the Kafka cluster using the `confluent-kafka` library with configurable connection parameters and consumer group management
2. THE Social_Agent SHALL consume messages exclusively from the `sentiment_scored_data` topic with automatic offset management and commit strategies
3. THE Social_Agent SHALL implement exponential backoff retry strategy with configurable parameters (max_retries=5, base_delay=1.0s, max_delay=60.0s, jitter_factor=0.25)
4. THE Social_Agent SHALL utilize a Dead Letter Queue (DLQ) system to redirect malformed or unparseable JSON messages away from the primary processing pipeline
5. WHEN extracting PhoBERT scores, THE Social_Agent SHALL validate message structure contains required fields `sentiment.label`, `sentiment.confidence` and convert to range [-1, 1]
6. THE Social_Agent SHALL extract interaction metrics `likes`, `shares`, `comments` with non-negative validation and default values of 0
7. THE Social_Agent SHALL extract credibility factors from message metadata with range validation [0.1, 1.0] and default fallback of 0.5
8. THE Social_Agent SHALL process messages in configurable batches (default 100) with timeout controls for efficient throughput
9. THE Social_Agent SHALL log processing statistics every 5 seconds including message throughput, error rates, and consumer lag metrics
10. WHEN Kafka connection failures occur, THE Social_Agent SHALL implement circuit breaker pattern and graceful degradation to cached or neutral values

---

### Requirement 3: Macro Agent with Advanced ChromaDB Integration

**User Story:** As a policy analysis system, I want an enhanced Macro Agent with connection pooling, query caching, and semantic similarity search capabilities, so that central bank policy sentiment analysis is performed efficiently and reliably.

#### Acceptance Criteria

1. THE Macro_Agent SHALL connect to ChromaDB using a managed connection pool with configurable pool size (default 5 connections) and health monitoring
2. THE Macro_Agent SHALL implement query result caching with configurable TTL (default 300 seconds) to optimize repeated policy lookups
3. THE Macro_Agent SHALL perform semantic similarity queries against the `macro_policies` collection with configurable similarity threshold (default 0.7)
4. THE Macro_Agent SHALL limit document retrieval to a maximum of k=5 most relevant policy documents per query
5. THE Macro_Agent SHALL analyze retrieved policy documents using Vietnamese keyword-based sentiment analysis for positive/negative/neutral classification
6. WHEN policy sentiment analysis is performed, THE Macro_Agent SHALL output discrete NHNN scores in the set {-1, 0, 1} representing restrictive, neutral, or accommodative policies
7. THE Macro_Agent SHALL generate Vietnamese-language policy summaries including document count, average similarity, and confidence metrics
8. THE Macro_Agent SHALL implement fallback strategies using cached results when ChromaDB connections fail
9. THE Macro_Agent SHALL log performance metrics including cache hit rates, query response times, and connection health status
10. WHEN no relevant policies are found above the similarity threshold, THE Macro_Agent SHALL return neutral score (0) with appropriate Vietnamese explanation

---

### Requirement 4: Risk Synthesis Agent with Automated Assessment

**User Story:** As a risk management system, I want a comprehensive Risk Synthesis Agent that orchestrates final VMSI calculation, implements automated risk assessment with Vietnamese warnings, and provides atomic file operations for reliable output, so that market risk conditions are accurately communicated to stakeholders.

#### Acceptance Criteria

1. THE Risk_Synthesis_Agent SHALL receive and validate social scores from Social_Agent with finite value checking and appropriate error handling
2. THE Risk_Synthesis_Agent SHALL receive and validate macro scores and metadata from Macro_Agent including policy summaries and confidence levels
3. THE Risk_Synthesis_Agent SHALL coordinate with VMSI_Engine to compute final index values using the complete mathematical pipeline
4. THE Risk_Synthesis_Agent SHALL assess risk levels using threshold-based classification: ≤20 (extreme panic), ≥81 (extreme euphoria), normal (21-80)
5. WHEN extreme risk conditions are detected (VMSI ≤20 or ≥81), THE Risk_Synthesis_Agent SHALL generate Vietnamese-language risk warnings with appropriate severity indicators
6. THE Risk_Synthesis_Agent SHALL save results to `live_vmsi.json` using atomic file operations with backup creation and retry logic (max 3 attempts)
7. THE Risk_Synthesis_Agent SHALL validate output JSON structure against schema including required fields: vmsi_value, timestamp, status, risk_warning, component_scores
8. THE Risk_Synthesis_Agent SHALL include comprehensive processing metadata with timing, data source availability, and calculation details
9. THE Risk_Synthesis_Agent SHALL implement state management for EMA smoothing across multiple calculation cycles
10. WHEN file write operations fail, THE Risk_Synthesis_Agent SHALL restore from backup and raise FileOperationError with detailed error context

---

### Requirement 5: Multi-Agent System Orchestration and Workflow Management

**User Story:** As a system architect, I want a robust multi-agent orchestration system with deterministic workflow execution, timeout management, and comprehensive error handling, so that the VMSI calculation pipeline operates reliably under various system conditions.

#### Acceptance Criteria

1. THE Multi_Agent_System SHALL implement deterministic sequential workflow execution: Social_Agent → Macro_Agent → Risk_Synthesis_Agent → Output Generation
2. THE Multi_Agent_System SHALL enforce configurable timeout limits for individual agent operations (default 30 seconds) with graceful degradation on timeout
3. THE Multi_Agent_System SHALL implement circuit breaker patterns for external service dependencies (Kafka, ChromaDB) with automatic recovery strategies
4. THE Multi_Agent_System SHALL provide centralized logging with structured context including agent names, operation types, and execution timing
5. WHEN individual agents encounter recoverable errors, THE Multi_Agent_System SHALL implement retry logic with exponential backoff before workflow failure
6. THE Multi_Agent_System SHALL maintain agent health monitoring with periodic status checks and automatic restart capabilities for failed agents
7. THE Multi_Agent_System SHALL implement graceful shutdown procedures ensuring proper cleanup of connections and resources
8. THE Multi_Agent_System SHALL provide workflow execution metrics including end-to-end processing time, success rates, and error classifications
9. THE Multi_Agent_System SHALL support configuration management with environment-based parameter loading and validation
10. WHEN complete workflow failures occur, THE Multi_Agent_System SHALL implement fallback strategies using cached data or neutral baseline values

---

### Requirement 6: Advanced Message Queue Integration with Fault Tolerance

**User Story:** As a data infrastructure engineer, I want sophisticated message queue integration with dead letter queues, connection pooling, and comprehensive error handling, so that data ingestion remains reliable under high load and network issues.

#### Acceptance Criteria

1. THE Multi_Agent_System SHALL integrate with Apache Kafka using the high-performance `confluent-kafka` library with connection pooling and health monitoring
2. THE Multi_Agent_System SHALL implement Dead Letter Queue (DLQ) topology to isolate malformed, unparseable, or repeatedly failing messages from the primary data pipeline
3. THE Multi_Agent_System SHALL implement configurable consumer group management with automatic rebalancing and offset commit strategies
4. THE Multi_Agent_System SHALL provide exponential backoff retry mechanisms for transient network failures with configurable parameters and maximum retry limits
5. WHEN Kafka broker connectivity is lost, THE Multi_Agent_System SHALL implement circuit breaker logic to prevent resource exhaustion and enable graceful degradation
6. THE Multi_Agent_System SHALL monitor and log consumer lag metrics, partition assignment status, and throughput statistics for operational visibility
7. THE Multi_Agent_System SHALL implement message validation pipelines with JSON schema checking and content verification before processing
8. THE Multi_Agent_System SHALL provide configurable batch processing capabilities to optimize throughput while maintaining low latency for real-time requirements
9. THE Multi_Agent_System SHALL implement graceful consumer shutdown with proper offset commits and resource cleanup on system termination
10. WHEN DLQ processing is triggered, THE Multi_Agent_System SHALL log detailed failure reasons and provide mechanisms for message reprocessing after issue resolution

---

### Requirement 7: High-Performance Vector Database Operations with Connection Management

**User Story:** As a database administrator, I want optimized vector database operations with connection pooling, query caching, and performance monitoring, so that semantic policy searches are performed efficiently without overwhelming database resources.

#### Acceptance Criteria

1. THE Multi_Agent_System SHALL connect to ChromaDB using managed connection pools with configurable pool sizes, connection timeouts, and health monitoring
2. THE Multi_Agent_System SHALL implement query result caching with TTL-based expiration (default 300 seconds) to reduce redundant vector computations
3. THE Multi_Agent_System SHALL enforce semantic similarity filtering with configurable thresholds (default 0.7) to ensure query result relevance
4. THE Multi_Agent_System SHALL limit document retrieval to configurable maximum results (default k=5) to control resource usage and response times
5. THE Multi_Agent_System SHALL implement connection health monitoring with automatic reconnection and failover capabilities for database resilience  
6. THE Multi_Agent_System SHALL provide query performance metrics including response times, cache hit rates, and connection pool utilization
7. THE Multi_Agent_System SHALL implement graceful degradation strategies when vector database services become unavailable, utilizing cached results as fallback
8. THE Multi_Agent_System SHALL validate vector search results for completeness and quality before proceeding with sentiment analysis
9. THE Multi_Agent_System SHALL implement connection cleanup and resource management to prevent memory leaks and connection exhaustion
10. WHEN database connection failures exceed threshold limits, THE Multi_Agent_System SHALL trigger circuit breaker protection and alert monitoring systems

---

### Requirement 8: Robust Output Management with Atomic File Operations

**User Story:** As an application developer, I want reliable JSON output generation with atomic file operations, comprehensive validation, and backup management, so that downstream systems can safely consume VMSI data without risk of corruption or incomplete writes.

#### Acceptance Criteria

1. THE Multi_Agent_System SHALL generate output exclusively in standard JSON format to file `live_vmsi.json` with proper UTF-8 encoding and formatting
2. THE Multi_Agent_System SHALL implement atomic file write operations using temporary files and atomic rename to prevent partial writes during system failures
3. THE Multi_Agent_System SHALL validate output JSON structure against predefined schema including required fields: vmsi_value, timestamp, status, risk_warning, component_scores
4. THE Multi_Agent_System SHALL create automatic backup files (`live_vmsi.json.backup`) before each write operation to enable recovery from corruption
5. THE Multi_Agent_System SHALL implement retry logic for file operations with exponential backoff (maximum 3 attempts) before raising FileOperationError
6. THE Multi_Agent_System SHALL include comprehensive metadata in output including processing timestamps, agent versions, data source statistics, and calculation details
7. THE Multi_Agent_System SHALL use ISO 8601 timestamp format with explicit UTC timezone specification for all temporal data
8. THE Multi_Agent_System SHALL validate numerical outputs for finite values and appropriate ranges before serialization
9. THE Multi_Agent_System SHALL implement file locking mechanisms to prevent concurrent write operations and data corruption
10. WHEN file write operations fail due to permissions or disk space, THE Multi_Agent_System SHALL restore previous valid state from backup and log detailed error context

---

### Requirement 9: Comprehensive Fault Tolerance and Circuit Breaking

**User Story:** As a system reliability engineer, I want comprehensive fault tolerance mechanisms including circuit breakers, health monitoring, and graceful degradation, so that the system remains operational even when external dependencies fail.

#### Acceptance Criteria  

1. THE Multi_Agent_System SHALL implement circuit breaker patterns for all external service dependencies including Kafka brokers, ChromaDB, and file system operations
2. THE Multi_Agent_System SHALL monitor service health with configurable thresholds for failure rates, response times, and consecutive failures before triggering circuit breakers
3. THE Multi_Agent_System SHALL provide graceful degradation strategies including cached data utilization, neutral baseline values, and reduced functionality modes
4. THE Multi_Agent_System SHALL implement exponential backoff retry strategies with jitter for transient failures while respecting circuit breaker states
5. WHEN external services become unavailable, THE Multi_Agent_System SHALL log detailed failure context and switch to appropriate fallback mechanisms
6. THE Multi_Agent_System SHALL monitor system resource utilization including memory usage, connection counts, and processing queue depths
7. THE Multi_Agent_System SHALL implement automatic recovery detection and circuit breaker reset when external services return to healthy operation
8. THE Multi_Agent_System SHALL provide operational dashboards or metrics endpoints for monitoring system health, performance, and failure patterns
9. THE Multi_Agent_System SHALL implement configurable alerting thresholds for critical failures, resource exhaustion, and service degradation
10. WHEN cascading failures are detected, THE Multi_Agent_System SHALL implement emergency shutdown procedures with proper resource cleanup and state preservation

---

### Requirement 10: Performance Monitoring and Scalability Management

**User Story:** As a performance engineer, I want comprehensive performance monitoring with configurable metrics collection and scalability benchmarks, so that the system can handle production workloads efficiently and provide operational visibility.

#### Acceptance Criteria

1. THE Multi_Agent_System SHALL achieve end-to-end processing latency of ≤10 seconds under normal load conditions from data ingestion to JSON file generation
2. THE Multi_Agent_System SHALL support processing throughput of up to 1,000 social media messages per minute without performance degradation
3. THE Multi_Agent_System SHALL implement comprehensive metrics collection including processing times, throughput rates, error counts, and resource utilization
4. THE Multi_Agent_System SHALL provide Prometheus-compatible metrics endpoints for integration with external monitoring and alerting systems
5. THE Multi_Agent_System SHALL monitor and log key performance indicators including agent execution times, database query performance, and file I/O operations
6. THE Multi_Agent_System SHALL implement configurable performance thresholds with automatic alerting when SLA targets are not met
7. THE Multi_Agent_System SHALL provide performance profiling capabilities to identify bottlenecks and optimization opportunities
8. THE Multi_Agent_System SHALL implement load balancing strategies for parallel processing when multiple instances are deployed
9. THE Multi_Agent_System SHALL monitor memory usage patterns and implement garbage collection optimization to prevent memory leaks
10. WHEN performance degradation is detected, THE Multi_Agent_System SHALL automatically adjust processing parameters including batch sizes, timeout values, and connection pool sizes

---

### Requirement 11: Comprehensive Logging and Observability

**User Story:** As a system operations engineer, I want structured logging with contextual information and comprehensive observability features, so that system behavior can be monitored, debugged, and optimized effectively in production environments.

#### Acceptance Criteria

1. THE Multi_Agent_System SHALL implement structured logging with configurable log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL) and JSON formatting for machine parsing
2. THE Multi_Agent_System SHALL include contextual information in all log entries including agent names, operation IDs, timestamps, and execution context
3. THE Multi_Agent_System SHALL log mathematical calculation steps at DEBUG level and final results at INFO level for audit traceability and debugging
4. THE Multi_Agent_System SHALL implement correlation IDs to trace requests across multiple agents and operations in the processing pipeline
5. THE Multi_Agent_System SHALL log performance metrics including execution times, resource usage, and throughput statistics at regular intervals
6. THE Multi_Agent_System SHALL capture and log error stack traces with sufficient context for debugging while sanitizing sensitive information
7. THE Multi_Agent_System SHALL implement log aggregation capabilities compatible with centralized logging systems (ELK, Splunk, etc.)
8. THE Multi_Agent_System SHALL provide log level configuration through environment variables or configuration files without code changes
9. THE Multi_Agent_System SHALL implement log rotation and retention policies to manage disk usage while preserving audit trails
10. WHEN critical errors occur that affect system availability, THE Multi_Agent_System SHALL log emergency-level messages and trigger alerting mechanisms