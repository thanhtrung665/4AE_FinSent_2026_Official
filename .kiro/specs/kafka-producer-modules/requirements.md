# Requirements Document

## Introduction

Phát triển hai module Python Producer mới trong hệ thống Kafka hiện có để thu thập và stream dữ liệu từ hai nguồn khác nhau: thị trường chứng khoán Việt Nam thông qua vnstock và dữ liệu mock Facebook từ file CSV. Cả hai module sẽ kế thừa từ BaseKafkaProducer hiện có để đảm bảo tính nhất quán về retry logic và error handling.

## Glossary

- **Market_Data_Producer**: Module thu thập dữ liệu VNINDEX và VN30 từ thư viện vnstock
- **Facebook_Mock_Injector**: Module đọc và stream dữ liệu từ file facebook_mock.csv
- **BaseKafkaProducer**: Class cơ sở hiện có với retry logic và error handling
- **vnstock**: Thư viện Python để truy cập dữ liệu thị trường chứng khoán Việt Nam
- **Kafka_Topic**: Kênh dữ liệu trong Apache Kafka để phân phối messages
- **JSON_Format**: Định dạng dữ liệu có cấu trúc cho việc trao đổi thông tin
- **Health_Check**: Cơ chế kiểm tra tình trạng hoạt động của producer
- **Environment_Configuration**: Hệ thống cấu hình thông qua file .env

## Requirements

### Requirement 1: Market Data Producer Implementation

**User Story:** As a data analyst, I want to collect Vietnamese stock market data automatically, so that I can analyze VNINDEX and VN30 trends in real-time.

#### Acceptance Criteria

1. THE Market_Data_Producer SHALL inherit from the existing BaseKafkaProducer class
2. WHEN the Market_Data_Producer initializes, THE system SHALL configure vnstock library connection
3. THE Market_Data_Producer SHALL collect VNINDEX and VN30 data every 1 minute
4. WHEN collecting market data, THE Market_Data_Producer SHALL format output as JSON with fields: ticker, timestamp, open, high, low, close, volume
5. THE Market_Data_Producer SHALL publish formatted data to "market_stock_data" Kafka topic
6. WHEN data collection fails, THE Market_Data_Producer SHALL use BaseKafkaProducer retry logic
7. THE Market_Data_Producer SHALL log all collection activities and errors

### Requirement 2: Facebook Mock Data Injector Implementation  

**User Story:** As a developer, I want to simulate Facebook data streaming from a CSV file, so that I can test data processing pipelines with realistic social media data.

#### Acceptance Criteria

1. THE Facebook_Mock_Injector SHALL inherit from the existing BaseKafkaProducer class
2. WHEN the Facebook_Mock_Injector initializes, THE system SHALL locate and validate facebook_mock.csv file
3. THE facebook_mock.csv file SHALL contain columns: comment_id, content_text, created_at, likes
4. THE Facebook_Mock_Injector SHALL convert CSV rows to JSON format before publishing
5. THE Facebook_Mock_Injector SHALL publish JSON data to "fb_mock_data" Kafka topic
6. WHEN streaming data, THE Facebook_Mock_Injector SHALL simulate real-time streaming with 1-second delay between messages
7. WHEN reaching end of CSV file, THE Facebook_Mock_Injector SHALL restart from beginning for continuous streaming
8. WHEN file reading fails, THE Facebook_Mock_Injector SHALL use BaseKafkaProducer retry logic

### Requirement 3: Dependencies and Configuration Management

**User Story:** As a system administrator, I want to manage new dependencies and configuration seamlessly, so that I can deploy the new producers without conflicts.

#### Acceptance Criteria

1. THE system SHALL add vnstock dependency to requirements.txt file
2. THE system SHALL add pandas dependency to requirements.txt for CSV processing
3. THE system SHALL update .env.example with necessary configuration variables
4. WHEN configuring Market_Data_Producer, THE system SHALL support VNSTOCK_UPDATE_INTERVAL environment variable
5. WHEN configuring Facebook_Mock_Injector, THE system SHALL support FB_MOCK_FILE_PATH environment variable
6. THE system SHALL support FB_MOCK_STREAM_DELAY environment variable for streaming delay configuration

### Requirement 4: Error Handling and Logging Integration

**User Story:** As a system operator, I want comprehensive error handling and logging, so that I can monitor and troubleshoot the new producers effectively.

#### Acceptance Criteria

1. WHEN any producer encounters errors, THE system SHALL use BaseKafkaProducer error handling mechanisms
2. THE Market_Data_Producer SHALL log vnstock connection status and data retrieval results
3. THE Facebook_Mock_Injector SHALL log CSV file processing status and streaming progress
4. WHEN vnstock API is unavailable, THE Market_Data_Producer SHALL log appropriate warnings and retry
5. WHEN CSV file is missing or corrupted, THE Facebook_Mock_Injector SHALL log detailed error information
6. THE system SHALL maintain consistent logging format with existing producers

### Requirement 5: Health Check Integration

**User Story:** As a monitoring system, I want to check the health status of new producers, so that I can ensure system reliability and detect issues early.

#### Acceptance Criteria

1. THE Market_Data_Producer SHALL implement health_check method inherited from BaseKafkaProducer
2. THE Facebook_Mock_Injector SHALL implement health_check method inherited from BaseKafkaProducer
3. WHEN health check runs, THE Market_Data_Producer SHALL verify vnstock library connectivity
4. WHEN health check runs, THE Facebook_Mock_Injector SHALL verify CSV file accessibility and format
5. THE health check methods SHALL return boolean status indicating producer health
6. WHEN health check fails, THE system SHALL provide specific error information in logs

### Requirement 6: Integration with Existing System

**User Story:** As a developer, I want the new producers to integrate seamlessly with the existing codebase, so that I can manage all producers through the current system architecture.

#### Acceptance Criteria

1. THE new producers SHALL follow the same directory structure as existing producers in producers/ folder
2. THE Market_Data_Producer SHALL be importable and usable in main.py alongside existing producers  
3. THE Facebook_Mock_Injector SHALL be importable and usable in main.py alongside existing producers
4. THE system SHALL support running new producers individually or together with existing ones
5. WHEN integrating with main.py, THE system SHALL maintain existing command-line interface patterns
6. THE new producers SHALL be compatible with existing DataPipelineManager class structure