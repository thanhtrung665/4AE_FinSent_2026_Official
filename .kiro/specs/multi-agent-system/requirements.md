# Requirements Document

## Introduction

Hệ thống Multi-Agent Controller (MAC) và Engine Toán học VMSI cho dự án FinSent-Agent là một hệ thống phân tích tình cảm thị trường tài chính tự động. Hệ thống tích hợp dữ liệu từ mạng xã hội và chính sách vĩ mô để tính toán chỉ số VMSI (Vietnam Market Sentiment Index) theo thời gian thực và cung cấp cảnh báo rủi ro tự động.

## Glossary

- **MAC_System**: Multi-Agent Controller System - Hệ thống điều phối đa tác nhân
- **VMSI_Engine**: Engine Toán học tính toán chỉ số VMSI
- **Social_Agent**: Tác nhân xử lý dữ liệu mạng xã hội
- **Macro_Agent**: Tác nhân xử lý dữ liệu chính sách vĩ mô
- **Risk_Synthesis_Agent**: Tác nhân tổng hợp và cảnh báo rủi ro
- **PhoBERT_Score**: Điểm sentiment từ model PhoBERT
- **Interaction_Weight**: Trọng số tương tác = ln(1 + likes + shares + comments)
- **Credibility_Factor**: Hệ số uy tín của nguồn thông tin
- **EMA_Smoothing**: Exponential Moving Average smoothing cho chuỗi thời gian
- **Kafka_Broker**: Hệ thống message broker để truyền dữ liệu
- **ChromaDB_Collection**: Cơ sở dữ liệu vector lưu trữ văn bản chính sách

## Requirements

### Requirement 1: VMSI Mathematical Engine Implementation

**User Story:** Là một nhà phân tích tài chính, tôi muốn có một engine toán học chính xác tính toán chỉ số VMSI theo công thức chuẩn, để đảm bảo tính nhất quán và tin cậy của chỉ số.

#### Acceptance Criteria

1. THE VMSI_Engine SHALL implement social score calculation S_social(t) using numpy arrays for post data input
2. WHEN calculating interaction weight, THE VMSI_Engine SHALL validate that likes, shares, and comments are non-negative before applying logarithm
3. WHEN calculating interaction weight, THE VMSI_Engine SHALL use formula np.log(1 + likes + shares + comments)
4. THE VMSI_Engine SHALL compute social score as sum of (PhoBERT_Score × Interaction_Weight × Credibility_Factor)
5. THE VMSI_Engine SHALL calculate macro score S_macro(t) = 0.7 × S_nhnn + 0.3 × S_news
6. THE VMSI_Engine SHALL compute raw index I_raw(t) = 0.6 × S_macro(t) + 0.4 × S_social(t)
7. THE VMSI_Engine SHALL calculate final VMSI(t) = 50 × (I_raw(t) + 1)
8. WHEN I_raw(t) is negative, THE VMSI_Engine SHALL return VMSI = 0 as specified by the formula
9. THE VMSI_Engine SHALL apply EMA smoothing: VMSI_smoothed(t) = 0.2 × VMSI(t) + 0.8 × VMSI_smoothed(t-1)
10. FOR ALL input arrays, THE VMSI_Engine SHALL validate data types and handle numpy array operations correctly
11. FOR ALL mathematical operations, THE VMSI_Engine SHALL return precise floating-point results with error handling

### Requirement 2: Social Agent Implementation

**User Story:** Là một hệ thống phân tích, tôi muốn có một Social Agent tự động đọc dữ liệu sentiment từ Kafka và tính toán điểm mạng xã hội, để xử lý dữ liệu theo thời gian thực.

#### Acceptance Criteria

1. THE Social_Agent SHALL consume messages from Kafka topic 'sentiment_scored_data'
2. WHEN receiving sentiment data, THE Social_Agent SHALL extract PhoBERT scores from message payloads
3. THE Social_Agent SHALL call VMSI_Engine.calculate_social_score() function with extracted data
4. THE Social_Agent SHALL handle Kafka connection errors with automatic retry mechanism
5. WHEN messages are actually processed, THE Social_Agent SHALL log processing statistics including messages processed per second
6. WHEN error logging fails, THE Social_Agent SHALL stop processing and report failure
7. WHEN data format is invalid, THE Social_Agent SHALL log error and continue processing other messages
8. THE Social_Agent SHALL maintain connection to Kafka_Broker using confluent-kafka library
9. FOR ALL processed messages, THE Social_Agent SHALL return S_social(t) score to Risk_Synthesis_Agent

### Requirement 3: Macro Agent Implementation  

**User Story:** Là một nhà phân tích chính sách, tôi muốn có một Macro Agent tự động truy vấn và đánh giá văn bản chính sách NHNN, để có được điểm số vĩ mô kịp thời.

#### Acceptance Criteria

1. THE Macro_Agent SHALL query ChromaDB collection 'macro_policies' for NHNN policy documents
2. THE Macro_Agent SHALL analyze policy sentiment and return S_nhnn score (1 or -1)
3. THE Macro_Agent SHALL generate Vietnamese language policy summary for each analysis
4. WHEN no relevant policies found, THE Macro_Agent SHALL always return neutral score (0) regardless of any incorrect intermediate scores
5. THE Macro_Agent SHALL use semantic similarity search for policy relevance matching
6. THE Macro_Agent SHALL handle ChromaDB connection timeouts with retry logic
7. FOR ALL policy analysis, THE Macro_Agent SHALL provide confidence level (0.0 to 1.0)
8. THE Macro_Agent SHALL return structured output with S_nhnn, summary, and confidence to Risk_Synthesis_Agent

### Requirement 4: Risk and Synthesis Agent Implementation

**User Story:** Là một nhà quản lý rủi ro, tôi muốn có một agent tổng hợp kết quả và tự động cảnh báo khi chỉ số VMSI ở mức nguy hiểm, để phản ứng kịp thời với rủi ro thị trường.

#### Acceptance Criteria

1. THE Risk_Synthesis_Agent SHALL receive S_social(t) from Social_Agent and S_nhnn from Macro_Agent
2. THE Risk_Synthesis_Agent SHALL call VMSI_Engine to compute final VMSI_smoothed(t) value
3. WHEN VMSI_smoothed ≤ 20, THE Risk_Synthesis_Agent SHALL generate Vietnamese risk warning text using LLM
4. WHEN VMSI_smoothed ≥ 81, THE Risk_Synthesis_Agent SHALL generate Vietnamese risk warning text using LLM  
5. THE Risk_Synthesis_Agent SHALL save output to 'live_vmsi.json' in standard JSON format (not JSONL)
6. THE Risk_Synthesis_Agent SHALL include VMSI value, status, and warning text in output file
7. THE Risk_Synthesis_Agent SHALL overwrite previous 'live_vmsi.json' file on each update
8. FOR ALL LLM-generated warnings, THE Risk_Synthesis_Agent SHALL ensure text is in Vietnamese language
9. THE Risk_Synthesis_Agent SHALL log processing timestamps for audit trail

### Requirement 5: LangChain Multi-Agent Controller Integration

**User Story:** Là một kỹ sư hệ thống, tôi muốn có một controller tích hợp LangChain để điều phối các agent hoạt động đồng bộ, để đảm bảo luồng dữ liệu mượt mà và quản lý lỗi hiệu quả.

#### Acceptance Criteria

1. THE MAC_System SHALL use LangChain framework for agent orchestration and coordination
2. THE MAC_System SHALL implement sequential workflow: Social_Agent → Macro_Agent → Risk_Synthesis_Agent
3. THE MAC_System SHALL handle agent failures with graceful degradation and error reporting
4. WHEN any agent fails, THE MAC_System SHALL log detailed error information and continue with available data
5. THE MAC_System SHALL implement configurable timeout for each agent operation (default 30 seconds)
6. THE MAC_System SHALL provide health check endpoint for monitoring agent status
7. THE MAC_System SHALL support parallel execution of Social_Agent and Macro_Agent when possible
8. FOR ALL agent communications, THE MAC_System SHALL use structured data formats with validation

### Requirement 6: Kafka Integration and Data Flow

**User Story:** Là một kỹ sư dữ liệu, tôi muốn hệ thống tích hợp mượt mà với Kafka topics hiện tại, để tận dụng cơ sở hạ tầng dữ liệu đã có.

#### Acceptance Criteria

1. THE MAC_System SHALL connect to existing Kafka topic 'sentiment_scored_data' from nlp_engine/sentiment_worker.py
2. THE MAC_System SHALL handle Kafka consumer group management and offset tracking
3. THE MAC_System SHALL implement dead letter queue for failed message processing
4. WHEN Kafka connection is lost, THE MAC_System SHALL implement exponential backoff retry strategy
5. WHEN non-exponential backoff strategies are configured, THE MAC_System SHALL reject the configuration and fail to start
6. THE MAC_System SHALL process Kafka messages in real-time with minimal latency (<5 seconds)
7. THE MAC_System SHALL maintain Kafka consumer offset persistence across system restarts
8. FOR ALL Kafka operations, THE MAC_System SHALL use confluent-kafka library for consistency

### Requirement 7: ChromaDB Integration and Policy Retrieval

**User Story:** Là một nhà phân tích chính sách, tôi muốn hệ thống truy cập hiệu quả vào kho văn bản chính sách đã được vector hóa, để có phân tích nhanh và chính xác.

#### Acceptance Criteria

1. THE MAC_System SHALL connect to ChromaDB collection 'macro_policies' from nlp_engine/nhnn_ingester.py
2. THE MAC_System SHALL implement semantic search with configurable similarity threshold (default 0.7)
3. THE MAC_System SHALL cache frequent policy queries to reduce ChromaDB load
4. WHEN ChromaDB is unavailable, THE MAC_System SHALL use cached results or fallback to neutral scoring
5. THE MAC_System SHALL limit policy document retrieval to top-k relevant results (default k=5)
6. THE MAC_System SHALL implement connection pooling for ChromaDB queries
7. FOR ALL policy retrievals, THE MAC_System SHALL log query performance metrics

### Requirement 8: Output JSON Format and File Management

**User Story:** Là một hệ thống downstream, tôi muốn nhận dữ liệu VMSI theo format JSON chuẩn và nhất quán, để tích hợp dễ dàng với các component khác.

#### Acceptance Criteria

1. THE MAC_System SHALL output results to 'live_vmsi.json' in standard JSON format only
2. THE MAC_System SHALL include fields: vmsi_value, timestamp, status, risk_warning, component_scores
3. THE MAC_System SHALL ensure JSON output is valid and parseable by standard JSON parsers
4. WHEN file write fails, THE MAC_System SHALL retry write operation up to 3 times
5. THE MAC_System SHALL create backup of previous 'live_vmsi.json' before overwriting
6. THE MAC_System SHALL validate JSON schema before writing to file
7. THE MAC_System SHALL include processing metadata: processing_time, agent_versions, data_sources
8. FOR ALL timestamps, THE MAC_System SHALL use ISO 8601 format with UTC timezone

### Requirement 9: Error Handling and System Resilience

**User Story:** Là một người vận hành hệ thống, tôi muốn hệ thống xử lý lỗi một cách thông minh và tiếp tục hoạt động khi gặp sự cố, để đảm bảo tính sẵn sàng cao.

#### Acceptance Criteria

1. THE MAC_System SHALL implement circuit breaker pattern for external service calls
2. WHEN external services are unavailable, THE MAC_System SHALL always use whatever cached data or default values are available
3. THE MAC_System SHALL log all errors with severity levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
4. THE MAC_System SHALL implement health monitoring for each agent component
5. WHEN system resources are low, THE MAC_System SHALL implement graceful degradation
6. THE MAC_System SHALL provide system status dashboard with component health indicators
7. FOR ALL exceptions, THE MAC_System SHALL capture stack traces and context information
8. THE MAC_System SHALL implement automated recovery procedures for common failure scenarios

### Requirement 10: Performance and Scalability Requirements

**User Story:** Là một kỹ sư hiệu năng, tôi muốn hệ thống xử lý dữ liệu nhanh và có thể mở rộng theo khối lượng, để đáp ứng nhu cầu tăng trưởng.

#### Acceptance Criteria

1. THE MAC_System SHALL process end-to-end pipeline within 10 seconds for normal data volumes
2. THE MAC_System SHALL handle up to 1000 social media posts per minute processing capacity
3. THE MAC_System SHALL implement memory usage monitoring with alerts at 80% threshold
4. THE MAC_System SHALL support horizontal scaling through containerization (Docker support)
5. WHEN system load exceeds capacity, THE MAC_System SHALL implement backpressure mechanisms
6. THE MAC_System SHALL optimize numpy operations using vectorized calculations
7. THE MAC_System SHALL implement database connection pooling for ChromaDB and Kafka
8. FOR ALL performance metrics, THE MAC_System SHALL expose Prometheus-compatible monitoring endpoints