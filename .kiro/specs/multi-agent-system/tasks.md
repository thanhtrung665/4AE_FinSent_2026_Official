# Implementation Plan: Multi-Agent System

## Overview

This implementation plan creates the Multi-Agent Controller (MAC) and VMSI Mathematical Engine for the FinSent-Agent system. The approach follows a modular agent-based architecture using LangChain for orchestration, implementing sequential workflow patterns to calculate the Vietnam Market Sentiment Index (VMSI) with real-time risk assessment capabilities.

## Tasks

- [x] 1. Set up project structure and VMSI Mathematical Engine
  - [x] 1.1 Create core project structure and VMSI Engine module
    - Create directory structure: `multi_agent_system/` with subdirectories for agents, engines, utils
    - Implement VMSIEngine class with numpy-based mathematical calculations
    - Add input validation for all mathematical operations and numpy array handling
    - _Requirements: 1.1, 1.10, 1.11_

  - [ ]* 1.2 Write property tests for VMSI mathematical formulas
    - **Property 1: VMSI Mathematical Formula Correctness**
    - **Property 2: Interaction Weight Logarithmic Formula** 
    - **Property 3: Macro Score Weighted Average**
    - **Property 4: Raw Index Weighted Combination**
    - **Property 5: VMSI Final Transformation and Boundary Handling**
    - **Property 6: EMA Smoothing Formula**
    - **Property 7: Numpy Array Validation and Type Handling**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.10, 1.11**

  - [x] 1.3 Implement core mathematical functions in VMSI Engine
    - Implement `calculate_social_score()` with numpy vectorized operations
    - Implement `calculate_macro_score()` with weighted average formula
    - Implement `calculate_raw_index()` and `calculate_final_vmsi()` with boundary handling
    - Implement `apply_ema_smoothing()` for temporal smoothing
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9_

- [x] 2. Checkpoint - Validate mathematical engine
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Implement Social Agent with Kafka integration
  - [x] 3.1 Create Social Agent class with Kafka consumer
    - Implement SocialAgent class with confluent-kafka consumer setup
    - Add connection to existing 'sentiment_scored_data' topic
    - Implement message extraction and PhoBERT score processing
    - Add error handling for invalid message formats with logging
    - _Requirements: 2.1, 2.2, 2.3, 2.7, 2.8_

  - [ ]* 3.2 Write property tests for Social Agent message processing
    - **Property 8: Social Agent Message Processing**
    - **Property 9: Social Agent Error Handling Continuation**
    - **Validates: Requirements 2.2, 2.3, 2.7**

  - [x] 3.3 Implement Kafka error handling and retry mechanisms
    - Add exponential backoff retry strategy for Kafka connections
    - Implement dead letter queue for failed message processing
    - Add consumer group management and offset tracking
    - Add connection health monitoring and statistics logging
    - _Requirements: 2.4, 2.5, 6.1, 6.2, 6.3, 6.4, 6.6, 6.7_

- [ ] 4. Implement Macro Agent with ChromaDB integration
  - [ ] 4.1 Create Macro Agent class with ChromaDB connectivity
    - Implement MacroAgent class with ChromaDB client setup
    - Add connection to existing 'macro_policies' collection
    - Implement semantic similarity search with configurable thresholds
    - Add policy sentiment analysis returning S_nhnn scores (1, -1, or 0)
    - _Requirements: 3.1, 3.2, 3.4, 3.5, 7.1, 7.2_

  - [ ]* 4.2 Write property tests for Macro Agent policy analysis
    - **Property 10: Macro Agent Policy Sentiment Analysis**
    - **Property 11: Vietnamese Language Generation Consistency**
    - **Property 12: Confidence Level Range Validation**
    - **Validates: Requirements 3.2, 3.3, 3.4, 3.7**

  - [x] 4.3 Implement ChromaDB error handling and caching
    - Add connection timeout handling and retry logic
    - Implement query result caching for frequent policy requests
    - Add connection pooling for ChromaDB queries
    - Add performance metrics logging for policy retrieval
    - _Requirements: 3.6, 7.3, 7.4, 7.5, 7.6, 7.7_

- [ ] 5. Implement Risk Synthesis Agent
  - [x] 5.1 Create Risk Synthesis Agent with VMSI calculation orchestration
    - Implement RiskSynthesisAgent class with data aggregation from other agents
    - Add VMSI Engine integration for final calculations
    - Implement risk level assessment logic (≤20, ≥81 thresholds)
    - Add LLM integration for Vietnamese risk warning generation
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.8, 4.9_

  - [ ]* 5.2 Write property tests for risk assessment and warnings
    - **Property 13: Risk Warning Generation Thresholds**
    - **Property 14: JSON Output Format Validation**
    - **Validates: Requirements 4.3, 4.4, 4.5, 4.6, 8.1, 8.2, 8.3**

  - [x] 5.3 Implement JSON output file management
    - Add 'live_vmsi.json' file creation with standard JSON format
    - Implement file backup before overwriting existing files
    - Add retry mechanism for file write failures (up to 3 attempts)
    - Add JSON schema validation before writing
    - Include all required fields: vmsi_value, timestamp, status, risk_warning, component_scores
    - _Requirements: 4.5, 4.6, 4.7, 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8_

- [ ] 6. Checkpoint - Validate individual agents
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement LangChain Multi-Agent Controller (MAC System)
  - [ ] 7.1 Create MAC System with LangChain orchestration framework
    - Implement MACSystem class using LangChain multi-agent framework
    - Add agent initialization and health check functionality
    - Implement sequential workflow coordination (Social → Macro → Risk Synthesis)
    - Add configurable timeouts for agent operations (default 30 seconds)
    - _Requirements: 5.1, 5.5, 5.6, 5.8_

  - [ ]* 7.2 Write property tests for agent orchestration
    - **Property 15: Sequential Workflow Execution Order**
    - **Property 16: Agent Failure Graceful Degradation**
    - **Validates: Requirements 5.2, 5.3, 5.4**

  - [ ] 7.3 Implement error handling and resilience patterns
    - Add circuit breaker patterns for external service calls (Kafka, ChromaDB)
    - Implement graceful degradation when agents fail
    - Add comprehensive error logging with severity levels
    - Add health monitoring dashboard for component status
    - _Requirements: 5.3, 5.4, 9.1, 9.2, 9.3, 9.4, 9.6, 9.7, 9.8_

- [ ] 8. Implement advanced error handling and monitoring
  - [ ] 8.1 Add circuit breaker and retry mechanisms
    - Implement PyBreaker circuit breakers for Kafka and ChromaDB connections
    - Add exponential backoff retry strategies with proper validation
    - Implement automated recovery procedures for common failures
    - Add system status monitoring and alerting
    - _Requirements: 6.4, 6.5, 9.1, 9.2, 9.5, 9.8_

  - [ ]* 8.2 Write property tests for error handling patterns
    - **Property 17: Exponential Backoff Retry Pattern**
    - **Property 22: Circuit Breaker State Transition Correctness**
    - **Property 23: Error Logging Severity Classification**
    - **Validates: Requirements 6.4, 6.5, 9.1, 9.3, 9.7**

  - [ ] 8.3 Add structured logging and monitoring
    - Implement structured logging with context and correlation IDs
    - Add performance metrics collection and Prometheus endpoints
    - Implement memory usage monitoring with 80% threshold alerts
    - Add processing statistics and audit trail logging
    - _Requirements: 9.3, 9.6, 9.7, 10.3, 10.8_

- [ ] 9. Implement performance optimizations
  - [ ] 9.1 Add performance monitoring and optimization
    - Implement numpy vectorized operations for all mathematical calculations
    - Add connection pooling for Kafka and ChromaDB
    - Add memory usage monitoring and backpressure mechanisms
    - Ensure end-to-end processing within 10 seconds for normal volumes
    - _Requirements: 10.1, 10.2, 10.5, 10.6, 10.7_

  - [ ]* 9.2 Write property tests for performance requirements
    - **Property 18: Semantic Search Threshold Behavior**
    - **Property 19: Result Limiting Consistency**
    - **Property 20: File Write Retry Mechanism**
    - **Property 21: ISO 8601 Timestamp Format Consistency**
    - **Property 24: Memory Usage Monitoring and Alerting**
    - **Property 25: Numpy Vectorized Operations Optimization**
    - **Validates: Requirements 7.2, 7.5, 8.4, 8.5, 8.8, 10.3, 10.6**

  - [ ] 9.3 Add Docker containerization support
    - Create Dockerfile with Python 3.9+ and required dependencies
    - Add docker-compose configuration for development environment
    - Implement environment variable configuration for deployment
    - Add container health checks and monitoring endpoints
    - _Requirements: 10.4_

- [ ] 10. Integration and end-to-end wiring
  - [ ] 10.1 Wire all components together in main application
    - Create main application entry point integrating all agents
    - Add configuration management for Kafka brokers, ChromaDB connections
    - Implement graceful startup and shutdown procedures
    - Add command-line interface for system control and monitoring
    - _Requirements: 5.1, 5.8, 6.1, 6.7, 6.8, 7.1_

  - [ ]* 10.2 Write integration tests for complete system
    - Test end-to-end pipeline with embedded Kafka and in-memory ChromaDB
    - Test system behavior under various failure scenarios
    - Validate processing capacity of 1000 posts/minute
    - Test JSON output format and file management
    - _Requirements: 10.1, 10.2, 8.1, 8.2, 8.3_

  - [ ] 10.3 Add system configuration and deployment scripts
    - Create configuration templates for different environments
    - Add system startup scripts and service definitions
    - Implement configuration validation on startup
    - Add deployment documentation and setup guides
    - _Requirements: 10.4, 5.6_

- [ ] 11. Final checkpoint and validation
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional property-based tests and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation throughout development
- Property tests validate universal correctness properties from the design document
- Unit tests within property test tasks validate specific examples and edge cases
- All mathematical operations use numpy for performance optimization
- LangChain framework is used for agent coordination and workflow management
- System supports both sequential and parallel agent execution where applicable
- Error handling includes circuit breakers, exponential backoff, and graceful degradation
- Monitoring includes structured logging, performance metrics, and health checks

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "1.3"] },
    { "id": 2, "tasks": ["3.1", "4.1"] },
    { "id": 3, "tasks": ["3.2", "3.3", "4.2", "4.3"] },
    { "id": 4, "tasks": ["5.1"] },
    { "id": 5, "tasks": ["5.2", "5.3"] },
    { "id": 6, "tasks": ["7.1"] },
    { "id": 7, "tasks": ["7.2", "7.3", "8.1"] },
    { "id": 8, "tasks": ["8.2", "8.3", "9.1"] },
    { "id": 9, "tasks": ["9.2", "9.3"] },
    { "id": 10, "tasks": ["10.1"] },
    { "id": 11, "tasks": ["10.2", "10.3"] }
  ]
}
```