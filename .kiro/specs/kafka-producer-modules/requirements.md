# Requirements Document

## Introduction

The Kafka Producer Modules specification defines the data ingestion layer for the FinSent-Agent platform. This document specifies the engineering requirements for two distinct Python-based producer systems operating inside the `producers/` folder: the Market Data Producer and the Facebook Mock Injector. Both systems inherit from the existing BaseKafkaProducer class to ensure consistent serialization, logging, and error resilience. The primary objective is to stream multi-source financial and social media text streams into an AWS-hosted Apache Kafka broker, enforcing temporal split frames for asset baseline mapping and systemic financial crisis mapping.

## Glossary

- **Market_Data_Producer**: Python worker module designed to fetch financial indicators and stock prices using the vnstock library
- **Facebook_Mock_Injector**: Simulation engine that parses historical retail investor sentiment data from a local CSV file to simulate real-time social media streaming over Kafka
- **BaseKafkaProducer**: The base class containing standardized Apache Kafka connection pools, automated retry algorithms, and health verification loops
- **vnstock**: An open-source Python library used to interact with live and historical Vietnamese financial market APIs
- **Standard_JSON**: The JavaScript Object Notation schema enforced across all message payloads
- **Ticker_Context**: Metadata tag restricting data routing boundaries to either "SHB" (Standard Baseline) or "SCB" (Systemic Crisis Event)
- **System**: The collective Kafka Producer Modules system including both Market Data Producer and Facebook Mock Injector
- **CSV_File**: The local comma-separated values file containing Facebook mock data records
- **Kafka_Topic**: Named message channels in the Kafka broker where producers publish messages

## Requirements

### Requirement 1: Market Data Producer Implementation

**User Story:** As a quantitative financial analyst, I want to capture historical and active market price data automatically across specific timeline frameworks, so that I can establish a mathematical baseline against market volatility.

#### Acceptance Criteria

1. THE Market_Data_Producer SHALL inherit methods and attributes from the existing BaseKafkaProducer class
2. WHEN the Market_Data_Producer initializes, THE Market_Data_Producer SHALL parse configurations from environment variables to establish network connections to the AWS Kafka broker on port 9092
3. THE Market_Data_Producer SHALL fetch historical data using vnstock for specified ticker symbols configured in environment variables
4. WHEN polling sequences execute, THE Market_Data_Producer SHALL collect data at intervals specified by environment configuration with a default of 1-minute intervals
5. THE Market_Data_Producer SHALL structure collected financial data into Standard_JSON schema containing ticker, timestamp (ISO 8601 format), open, high, low, close, volume, and data_source fields
6. WHEN data is collected and structured, THE Market_Data_Producer SHALL publish JSON payloads to the Kafka_Topic named market_stock_data

### Requirement 2: Facebook Mock Data Injector Implementation

**User Story:** As a system test engineer, I want to stream historical social media comment datasets sequentially to simulate real-time crowd sentiment, so that I can validate the Multi-Agent system's ability to process social media data streams.

#### Acceptance Criteria

1. THE Facebook_Mock_Injector SHALL inherit from the existing BaseKafkaProducer class architecture
2. WHEN the Facebook_Mock_Injector initializes, THE Facebook_Mock_Injector SHALL verify the physical path, structure validation, and read permissions of the specified CSV_File, and SHALL fail initialization if any verification step fails
3. THE CSV_File SHALL contain standardized columns including comment_id, content_text, created_at (ISO 8601), and likes
4. WHEN data extraction is successful, THE Facebook_Mock_Injector SHALL transform each CSV row into Standard_JSON format and publish to the Kafka_Topic named fb_mock_data
5. THE Facebook_Mock_Injector SHALL introduce configurable runtime delay between consecutive message dispatches controlled via environment variables with a default of 1 second
6. WHEN the injector reaches end-of-file, THE Facebook_Mock_Injector SHALL reset to the initial position and continue in a continuous loop

### Requirement 3: CSV File Management and Validation

**User Story:** As a system administrator, I want automatic CSV file validation and recovery mechanisms, so that the system can handle missing or corrupted data files gracefully.

#### Acceptance Criteria

1. WHEN a CSV_File is missing at the specified path, THE System SHALL create a sample CSV_File with valid structure and sample data
2. THE System SHALL validate CSV_File encoding using automatic detection with fallback to common encodings
3. WHEN CSV schema compliance inspection detects violations, THE System SHALL execute conditional remediation actions based on specific violation types detected
4. THE System SHALL log all CSV processing operations with detailed error information for troubleshooting

### Requirement 4: Dependencies and Configuration Management

**User Story:** As a deployment engineer, I want isolated microservice configurations and structured dependency manifests, so that the application can be deployed on AWS instances without library version conflicts.

#### Acceptance Criteria

1. THE System SHALL maintain dependency specifications in requirements.txt file within the producers directory
2. THE System SHALL provide environment configuration template in .env.example file containing all required configuration parameters
3. THE System SHALL support mandatory runtime parameters including MARKET_DATA_INTERVAL_SECONDS, FB_MOCK_FILE_PATH, and FB_MOCK_STREAM_DELAY
4. WHEN configuration parameters are missing, THE System SHALL use documented default values

### Requirement 5: Error Resilience and Logging Integration

**User Story:** As a DevOps engineer, I want comprehensive exception tracking and automated recovery routines during network failures, so that the system maintains data pipeline integrity during connection issues.

#### Acceptance Criteria

1. WHEN AWS connection timeouts or broker network failures occur, THE System SHALL trigger BaseKafkaProducer exponential backoff retry mechanisms
2. THE Market_Data_Producer SHALL monitor external financial API endpoint availability and SHALL suspend operations gracefully when upstream servers become unavailable
3. THE Facebook_Mock_Injector SHALL validate data integrity for each CSV record and SHALL handle corrupted data according to configured error handling policies
4. THE System SHALL maintain consistent logging output format across all components with processing metrics for analytics tracking

### Requirement 6: Health Verification and Monitoring

**User Story:** As an orchestration system, I want programmatic health validation interfaces, so that container status can be queried automatically without manual log parsing.

#### Acceptance Criteria

1. THE Market_Data_Producer SHALL implement health_check interface that tests both Kafka broker connections and financial API network accessibility, and SHALL return false when any tested connection fails
2. THE Facebook_Mock_Injector SHALL implement health_check interface that validates CSV_File accessibility and structural consistency
3. THE health_check methods SHALL return boolean responses with detailed error information available through the logging system
4. THE System SHALL provide health status information suitable for automated monitoring systems

### Requirement 7: System Integration and Deployment Architecture

**User Story:** As a backend developer, I want the ingestion modules to follow standard design abstractions, so that they can be managed collectively by orchestration systems.

#### Acceptance Criteria

1. THE System SHALL deploy within the structural boundaries of the producers system directory
2. THE System SHALL ensure both modules can be imported and controlled from a unified main.py entry point
3. THE System SHALL maintain compatibility with existing system architecture and orchestration frameworks
4. THE System SHALL provide standardized interfaces for start, stop, and status operations