# Implementation Plan: Kafka Producer Modules

## Overview

This implementation plan creates two Python Kafka producer modules for streaming Vietnamese stock market data and mock Facebook social media data. Both modules inherit from the existing `BaseKafkaProducer` class and integrate seamlessly with the current data pipeline architecture. The implementation focuses on robust data collection, transformation, and streaming capabilities with comprehensive error handling and logging.

## Tasks

- [x] 1. Set up project dependencies and configuration
  - [x] 1.1 Update requirements.txt with new dependencies (vnstock, pandas)
    - Add vnstock library for Vietnamese stock market data access
    - Add pandas for CSV processing and data manipulation
    - Verify compatibility with existing Python 3.12 environment
    - _Requirements: 3.1, 3.2_

  - [x] 1.2 Update environment configuration file
    - Add MARKET_DATA_INTERVAL_SECONDS environment variable to .env.example
    - Add VNSTOCK_TICKERS environment variable configuration
    - Add FB_MOCK_FILE_PATH and FB_MOCK_STREAM_DELAY environment variables
    - _Requirements: 3.3, 3.4, 3.5, 3.6_

- [x] 2. Implement Market Data Producer core functionality
  - [x] 2.1 Create Market_Data_Producer class with BaseKafkaProducer inheritance
    - Implement __init__ method with proper configuration loading
    - Set up vnstock library connection and ticker configuration
    - Initialize logging and error handling from base class
    - _Requirements: 1.1, 1.2_

  - [ ]* 2.2 Write property test for Market Data JSON format consistency
    - **Property 1: Market Data JSON Format Consistency**
    - **Validates: Requirements 1.4**

  - [x] 2.3 Implement market data collection methods
    - Create get_market_data() method for individual ticker data retrieval
    - Implement collect_all_tickers() method for batch data collection
    - Add data change detection logic to avoid duplicate streaming
    - _Requirements: 1.3, 1.4_

  - [ ]* 2.4 Write unit tests for market data collection
    - Test vnstock API integration with mock data
    - Test data parsing and JSON format validation
    - Test change detection algorithm accuracy
    - _Requirements: 1.4_

- [x] 3. Implement Facebook Mock Injector core functionality
  - [x] 3.1 Create Facebook_Mock_Injector class with BaseKafkaProducer inheritance
    - Implement __init__ method with CSV file path validation
    - Set up streaming delay configuration and file accessibility checks
    - Initialize processed records tracking to avoid duplicates
    - _Requirements: 2.1, 2.2_

  - [ ]* 3.2 Write property test for CSV to JSON transformation
    - **Property 4: CSV to JSON Transformation**
    - **Validates: Requirements 2.4**

  - [x] 3.3 Implement CSV data processing methods
    - Create read_csv_data() method with multiple encoding support
    - Implement stream_csv_data() generator for real-time streaming simulation
    - Add CSV format validation with specific column checking
    - _Requirements: 2.3, 2.4, 2.6_

  - [ ]* 3.4 Write property test for CSV format validation
    - **Property 5: CSV Format Validation**
    - **Validates: Requirements 2.3**

- [-] 4. Checkpoint - Core functionality validation
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement error handling and retry logic
  - [~] 5.1 Integrate BaseKafkaProducer error handling for Market_Data_Producer
    - Implement vnstock API failure handling with exponential backoff
    - Add data validation error recovery mechanisms
    - Ensure consistent logging format for all error conditions
    - _Requirements: 1.6, 1.7, 4.1, 4.2_

  - [ ]* 5.2 Write property test for error handling consistency
    - **Property 2: Error Handling Consistency**
    - **Validates: Requirements 1.6, 2.8, 4.1**

  - [~] 5.3 Integrate BaseKafkaProducer error handling for Facebook_Mock_Injector
    - Implement CSV file system error handling (missing files, permissions)
    - Add encoding detection and fallback mechanisms
    - Ensure consistent logging format for file processing errors
    - _Requirements: 2.8, 4.3, 4.6_

  - [ ]* 5.4 Write property test for logging coverage completeness
    - **Property 3: Logging Coverage Completeness**
    - **Validates: Requirements 1.7, 4.2, 4.3, 4.6**

- [ ] 6. Implement health check and monitoring features
  - [~] 6.1 Add health check methods to Market_Data_Producer
    - Implement vnstock connectivity testing method
    - Add market data availability verification
    - Ensure health check returns boolean status with specific error logging
    - _Requirements: 5.1, 5.3, 5.5_

  - [~] 6.2 Add health check methods to Facebook_Mock_Injector
    - Implement CSV file accessibility and format validation
    - Add streaming capability verification
    - Ensure health check returns boolean status with specific error logging
    - _Requirements: 5.2, 5.4, 5.5_

  - [ ]* 6.3 Write property test for health check specificity
    - **Property 6: Health Check Specificity**
    - **Validates: Requirements 5.6**

- [ ] 7. Integration with existing system architecture
  - [~] 7.1 Update main.py with new producer imports and initialization
    - Add import statements for both new producer classes
    - Integrate producers with existing DataPipelineManager structure
    - Maintain existing command-line interface patterns
    - _Requirements: 6.2, 6.3, 6.5_

  - [~] 7.2 Add individual producer execution capabilities
    - Implement standalone execution methods for Market_Data_Producer
    - Implement standalone execution methods for Facebook_Mock_Injector
    - Ensure compatibility with existing producer execution patterns
    - _Requirements: 6.4, 6.6_

  - [ ]* 7.3 Write integration tests for system compatibility
    - Test DataPipelineManager integration with new producers
    - Test concurrent execution with existing RSS and F319 producers
    - Test Kafka message delivery and topic routing
    - _Requirements: 6.1, 6.6_

- [ ] 8. Final validation and testing
  - [~] 8.1 Create comprehensive test suite execution
    - Run all unit tests for both producer modules
    - Execute property-based tests with minimum 100 iterations
    - Validate mock data generation and streaming scenarios
    - _Requirements: All testing requirements_

  - [~] 8.2 Perform end-to-end integration testing
    - Test complete data pipeline from collection to Kafka streaming
    - Validate JSON format consistency across all data sources
    - Test error recovery scenarios with real failure conditions
    - _Requirements: 1.5, 2.5, 4.1, 4.4_

- [~] 9. Final checkpoint - Complete system validation
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Property tests validate universal correctness properties from the design document
- Integration tasks ensure seamless compatibility with existing codebase
- Health check implementation provides monitoring capabilities for system reliability
- Error handling tasks leverage BaseKafkaProducer's proven retry logic and logging mechanisms

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1", "3.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "3.2", "3.3"] },
    { "id": 3, "tasks": ["2.4", "3.4", "5.1", "5.3"] },
    { "id": 4, "tasks": ["5.2", "5.4", "6.1", "6.2"] },
    { "id": 5, "tasks": ["6.3", "7.1"] },
    { "id": 6, "tasks": ["7.2", "7.3"] },
    { "id": 7, "tasks": ["8.1"] },
    { "id": 8, "tasks": ["8.2"] }
  ]
}
```