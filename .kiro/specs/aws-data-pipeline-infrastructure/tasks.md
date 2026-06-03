# Implementation Plan: AWS Data Pipeline Infrastructure

## Overview

This implementation plan covers the creation of a production-ready containerized financial data streaming system specifically engineered for AWS EC2 t3.large instances (2 vCPU, 8GB RAM). The system provides a resilient real-time pipeline utilizing a single Apache Kafka broker with four dedicated topics for multi-source financial data ingestion (`news_rss_data`, `f319_data`, `fb_mock_data`, `market_stock_data`) and ChromaDB for vector-indexed macroeconomic policy storage. The infrastructure is optimized to support comparative analysis between standard market baselines (SHB baseline period: 01/01/2026 to 01/06/2026) and systemic crisis events (SCB crisis period: 01/09/2022 to 31/12/2022).

## Tasks

- [ ] 1. Set up project structure and configuration files
  - Create project directory structure for AWS t3.large infrastructure deployment
  - Set up persistent data directories under /opt/data-pipeline
  - Create environment configuration templates with AWS Elastic IP support
  - Configure .env template with KAFKA_HEAP_OPTS="-Xmx2G -Xms2G" for t3.large
  - _Requirements: 1.1, 1.3, 1.5_

- [ ] 2. Implement Docker Compose infrastructure for t3.large
  - [ ] 2.1 Create docker-compose.yml with AWS-optimized service definitions
    - Define Zookeeper service with persistent volumes and 8GB RAM optimization
    - Configure isolated custom bridge network (172.20.0.0/16) for secure inter-container communication
    - Set up health checks and restart policies for all core services
    - Configure external port mappings: Kafka (9092), ChromaDB (8000)
    - _Requirements: 1.1, 1.2, 1.4, 1.5, 1.6_
  
  - [ ] 2.2 Configure single Kafka broker with 4 dedicated topics
    - Define Kafka service with KAFKA_HEAP_OPTS="-Xmx2G -Xms2G" for t3.large optimization
    - Configure KAFKA_ADVERTISED_LISTENERS for AWS Elastic IP dynamic binding
    - Auto-provision four core topics: news_rss_data, f319_data, fb_mock_data, market_stock_data
    - Set single-partition architecture per topic for chronological ordering
    - Configure development log retention (72 hours, 1GB segments) for EBS efficiency
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_
  
  - [ ] 2.3 Configure ChromaDB vector database service
    - Define ChromaDB service on port 8000 with macro_policies collection
    - Configure cosine similarity metrics for semantic policy search
    - Set up persistent volume for vector data storage (/opt/data-pipeline/chromadb)
    - Implement REST API access control restricted to AWS network boundary
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 3. Create automated AWS deployment script
  - [ ] 3.1 Implement deploy-aws-pipeline.sh with t3.large validation
    - Create pre-flight check for minimum 8GB RAM requirement
    - Implement Docker Engine and Docker Compose installation for AWS Ubuntu
    - Add error handling for insufficient t3.large specifications with recommendations
    - Configure data directory creation under /opt/data-pipeline
    - _Requirements: 3.1, 3.2, 3.3_
  
  - [ ] 3.2 Add service deployment with health verification
    - Implement sequential service initialization with dependency management
    - Add real-time connectivity status monitoring for ports 9092 and 8000
    - Create deployment report with AWS Elastic IP endpoint verification
    - Include container IP mapping display for debugging
    - _Requirements: 3.4, 3.5_

- [ ] 4. Create comprehensive AWS infrastructure documentation
  - [ ] 4.1 Create README_AWS.md with t3.large setup instructions
    - Document step-by-step AWS EC2 console configuration for t3.large
    - Provide VPC and subnet configuration guidelines
    - Include AWS Elastic IP allocation and DNS configuration steps
    - Add performance tuning recommendations for 8GB RAM optimization
    - _Requirements: 4.1, 4.2, 4.4, 4.5_
  
  - [ ] 4.2 Document AWS Security Group configuration
    - Specify strict port whitelist: SSH (22), Kafka (9092), ChromaDB (8000)
    - Document IP whitelist configuration for development team access
    - Include KAFKA_ADVERTISED_LISTENERS configuration for Elastic IP binding
    - Provide security validation procedures without service disruption
    - _Requirements: 4.3, 4.4, 4.6, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
  
  - [ ] 4.3 Add client connection examples and troubleshooting
    - Include sample Kafka producer/consumer configurations for all 4 topics
    - Provide ChromaDB client connection examples with Standard_JSON format
    - Document common deployment issues and resolution procedures
    - Add SHB/SCB case study data ingestion examples
    - _Requirements: 4.7_

- [ ] 5. Implement SHB/SCB case study data model support
  - [ ] 5.1 Configure Kafka topics for SHB/SCB comparative analysis
    - Implement ticker_context validation for "SHB" and "SCB" tags
    - Configure market_stock_data topic for VNStock API compatibility
    - Set up f319_data and fb_mock_data with Standard_JSON structure enforcement
    - Add message format validation for SHB baseline (01/01/2026-01/06/2026) and SCB crisis (01/09/2022-31/12/2022) periods
    - _Requirements: 2.4, 2.5_
  
  - [ ] 5.2 Configure ChromaDB for policy document indexing
    - Set up macro_policies collection with required metadata fields
    - Implement Standard_JSON enforcement (source_url, ticker_context, publish_date)
    - Configure JSONL format rejection validation
    - Add cosine similarity indexing for semantic policy search
    - _Requirements: 5.2, 5.3, 5.4_

- [ ] 6. Checkpoint - Verify core infrastructure deployment
  - Test deployment script on clean t3.large instance
  - Validate all 4 Kafka topics are auto-created correctly
  - Verify ChromaDB macro_policies collection initialization
  - Ensure AWS Security Group configuration is functional
  - Ask the user if questions arise

- [ ] 7. Implement data persistence and volume management
  - [ ] 7.1 Configure persistent volume mappings for t3.large
    - Set up independent volume paths: kafka, chromadb, zookeeper under /opt/data-pipeline
    - Configure EBS GP3 storage optimization for 8GB RAM instances
    - Ensure 100% data preservation during container restarts and system reboots
    - _Requirements: 6.3_
  
  - [ ] 7.2 Add backup and recovery procedures
    - Document EBS snapshot procedures for critical data preservation
    - Create data recovery validation for SHB and SCB context frames
    - Include cross-region backup recommendations
    - _Requirements: 6.3_

- [ ] 8. Implement development and testing support features
  - [ ] 8.1 Configure development-friendly settings
    - Enable Kafka topic auto-creation with 72-hour log retention
    - Configure ChromaDB REST API for development tool integration
    - Set up .env configuration hot-reload without database purge
    - Add development mode options for faster startup
    - _Requirements: 6.4_
  
  - [ ] 8.2 Create sample configurations for case studies
    - Create SHB baseline period data ingestion examples
    - Add SCB crisis period sample configurations
    - Include VNStock API integration examples for both periods
    - Provide Standard_JSON format templates and validation samples
    - _Requirements: 2.4, 2.5, 5.3_

- [ ] 9. Add security and network access control
  - [ ] 9.1 Implement network security configuration
    - Configure Docker bridge network isolation (172.20.0.0/16)
    - Set up AWS Security Group integration for dynamic IP updates
    - Document VPC isolation and optional TLS encryption
    - _Requirements: 4.3, 4.4, 4.6_
  
  - [ ] 9.2 Add authentication and authorization documentation
    - Document AWS IAM roles and permissions for EC2 deployment
    - Include optional SASL authentication configuration for production scaling
    - Add access logging and audit trail configuration
    - _Requirements: 4.3, 5.1_

- [ ] 10. Create property-based testing implementation
  - [ ]* 10.1 Write property test for VNStock data format compatibility
    - **Property 1: VNStock Data Format Compatibility**
    - **Validates: Requirements 2.4**
    - Test valid VNStock payloads for both SHB and SCB periods
  
  - [ ]* 10.2 Write property test for ticker context validation
    - **Property 2: Ticker Context Validation**
    - **Validates: Requirements 2.5**
    - Test Standard_JSON format enforcement for f319_data and fb_mock_data
  
  - [ ]* 10.3 Write property test for ChromaDB metadata structure
    - **Property 3: ChromaDB Metadata Structure Enforcement**
    - **Validates: Requirements 5.3**
    - Test required field validation for macro_policies collection
  
  - [ ]* 10.4 Write property test for JSONL format rejection
    - **Property 4: JSONL Format Rejection**
    - **Validates: Requirements 5.4**
    - Test JSONL format rejection in ChromaDB vector indexes
  
  - [ ]* 10.5 Write property test for data persistence across restarts
    - **Property 5: Data Persistence Across Container Restarts**
    - **Validates: Requirements 6.3**
    - Test 100% data preservation for SHB and SCB context frames
  
  - [ ]* 10.6 Write property test for environment configuration hot-reload
    - **Property 6: Environment Configuration Hot-Reload**
    - **Validates: Requirements 6.4**
    - Test .env updates without data loss

- [ ] 11. Create resource optimization and monitoring
  - [ ] 11.1 Configure t3.large resource optimization
    - Set KAFKA_HEAP_OPTS="-Xmx2G -Xms2G" for optimal 8GB RAM utilization
    - Configure memory limits and restart policies for 2 vCPU architecture
    - Add resource usage monitoring optimized for t3.large specifications
    - _Requirements: 2.1_
  
  - [ ] 11.2 Implement performance monitoring and health checks
    - Set up service startup time monitoring with health check verification
    - Configure stable operation monitoring under financial data workloads
    - Add AWS CloudWatch integration for infrastructure monitoring
    - _Requirements: 3.4, 3.5_

- [ ] 12. Final integration and deployment validation
  - [ ] 12.1 Create comprehensive end-to-end testing
    - Implement connectivity testing for all 4 Kafka topics
    - Add ChromaDB vector search validation with policy documents
    - Create AWS Security Group validation procedures
    - Test complete SHB and SCB data ingestion workflows
  
  - [ ] 12.2 Finalize documentation and deployment validation
    - Complete README_AWS.md with final t3.large configuration details
    - Validate deployment script on clean AWS EC2 t3.large instance
    - Ensure all 6 correctness properties are testable and documented
    - Verify complete requirements coverage and property traceability

- [ ] 13. Final checkpoint - Complete system validation
  - Ensure all property-based tests pass for the 6 correctness properties
  - Verify AWS t3.large deployment is fully functional
  - Confirm all 4 Kafka topics and ChromaDB collections are operational
  - Validate SHB/SCB case study data flows are working correctly
  - Ask the user if questions arise

## Notes

- Infrastructure optimized for AWS EC2 t3.large instances (2 vCPU, 8GB RAM)
- Single Kafka broker with 4 dedicated topics for financial data streaming
- Memory allocation: KAFKA_HEAP_OPTS="-Xmx2G -Xms2G" for t3.large optimization
- Support for SHB baseline period (01/01/2026-01/06/2026) and SCB crisis period (01/09/2022-31/12/2022)
- AWS Security Groups configured for ports 22, 9092, 8000 with IP whitelisting
- ChromaDB macro_policies collection with cosine similarity for policy document search
- Standard JSON format enforcement across all data pipelines, explicit JSONL rejection
- 6 correctness properties validated through property-based testing
- Data persistence guaranteed across container restarts and system reboots
- Environment configuration hot-reload support without data loss
- Tasks marked with `*` are optional property-based tests for validation
- Complete AWS deployment documentation with step-by-step setup procedures

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 2, "tasks": ["2.2", "2.3", "3.2", "4.2", "5.1"] },
    { "id": 3, "tasks": ["4.3", "5.2", "7.1", "8.1"] },
    { "id": 4, "tasks": ["7.2", "8.2", "9.1", "11.1"] },
    { "id": 5, "tasks": ["9.2", "11.2", "12.1"] },
    { "id": 6, "tasks": ["10.1", "10.2", "10.3"] },
    { "id": 7, "tasks": ["10.4", "10.5", "10.6"] },
    { "id": 8, "tasks": ["12.2"] }
  ]
}
```