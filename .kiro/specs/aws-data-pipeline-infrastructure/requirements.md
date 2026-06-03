# Requirements Document - AWS Data Pipeline Infrastructure
## Introduction

The AWS Data Pipeline Infrastructure provides a complete containerized data processing pipeline natively deployable on AWS EC2 production-ready infrastructure. The system is engineered to handle parallel historical streaming data for comparative asset analysis—specifically evaluating standard market baselines against extreme systemic crisis events. The infrastructure includes a single Apache Kafka broker optimized for low-latency message streaming, ChromaDB for high-performance vector storage, and an isolated container network, ensuring absolute data integrity under multi-source loads.

## Glossary

- **Data_Pipeline_System**: The complete containerized infrastructure including Kafka, ChromaDB, and core networking components running on AWS.
- **Kafka_Broker**: Single-broker Apache Kafka deployment optimized for streaming financial news and time-series transactional data.
- **ChromaDB_Service**: Isolated vector database service providing storage, indexing, and semantic search capabilities for macroeconomic policy documents.
- **Zookeeper_Service**: Distributed coordination service for Kafka broker metadata and cluster state management.
- **Deploy_Script**: Automated Bash deployment script for multi-component Docker installation, volume provisioning, and health validation.
- **Security_Group**: AWS virtual firewall controlling inbound and outbound instance traffic to secure data pipelines from unauthorized external mutations.
- **Docker_Compose**: Multi-container orchestration specification defining production-ready configurations, health checks, and service dependencies.
- **EC2_Instance**: AWS virtual machine host configured with t3.large hardware specifications (2 vCPU, 8GB RAM) to maintain system reliability during heavy NLP execution blocks.
- **Standard_JSON**: Strict JavaScript Object Notation format used universally across message payloads and database metadata fields, ensuring absolute compatibility with downstream systems.

---

## Requirements

### Requirement 1: Container Orchestration Infrastructure

**User Story:** As a DevOps engineer, I want a complete Docker Compose configuration optimized for AWS t3.large, so that I can deploy the entire data pipeline with automated volume isolation and strict service dependency chains.

#### Acceptance Criteria

1. **THE Data_Pipeline_System SHALL** provide a `docker-compose.yml` file defining all required services natively running on the AWS infrastructure.
2. **THE Docker_Compose SHALL** configure `Zookeeper_Service` as a strict dependency and health checkpoint for `Kafka_Broker` initialization.
3. **THE Docker_Compose SHALL** allocate isolated, persistent Docker volumes for Kafka broker logs and ChromaDB vector collections to prevent data wipe during container recreation.
4. **THE Docker_Compose SHALL** map `ChromaDB_Service` to external port `8000` and `Kafka_Broker` primary listener to external port `9092`.
5. **THE Docker_Compose SHALL** implement an isolated custom bridge network ensuring secure, internal communication between all pipeline containers.
6. **THE Docker_Compose SHALL** enforce restart-on-failure policies across all core microservices to guarantee continuous automated recovery.

---

### Requirement 2: Financial Stream & Kafka Multi-Topic Configuration

**User Story:** As a data engineer, I want a single Kafka broker configured with dedicated historical topics for SHB and SCB, so that I can stream multi-source financial texts and price data without cross-contamination.

#### Acceptance Criteria

1. **THE Kafka_Broker SHALL** run with memory allocations optimized for `t3.large`, setting `KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"` to prevent system-wide Out-Of-Memory (OOM) failures.
2. **THE Kafka_Broker SHALL** automatically provision four core messaging topics upon initialization: `news_rss_data`, `f319_data`, `fb_mock_data`, and `market_stock_data`.
3. **THE Kafka_Broker SHALL** enforce a strict single-partition architecture per topic to guarantee absolute chronological message ordering during time-series processing.
4. **THE data payloads** within `market_stock_data` **SHALL** natively support standard VNStock API inputs for the SHB baseline period (01/01/2026 to 01/06/2026) and the systemic contagion proxies (VNINDEX, STB) for the SCB crisis period (01/09/2022 to 31/12/2022).
5. **THE data payloads** within `f319_data` and `fb_mock_data` **SHALL** mandate a `Standard_JSON` structure containing explicit `ticker_context` tags (`"SHB"` or `"SCB"`) to prevent data ingestion routing errors.

---

### Requirement 3: Automated Deployment and AWS Provisioning

**User Story:** As a system administrator, I want an automated deployment script tailored for AWS Ubuntu Server, so that I can install Docker dependencies and launch the pipeline without manual setup errors.

#### Acceptance Criteria

1. **THE Deploy_Script SHALL** automatically detect and install Docker Engine and Docker Compose if missing from the host AWS instance.
2. **THE Deploy_Script SHALL** perform a pre-flight environment check to validate that the host meets the minimum `t3.large` specification (8GB RAM).
3. **WHEN** host system memory is below 8GB, **THE Deploy_Script SHALL** halt execution, issue a critical resource warning, and output architectural optimization recommendations.
4. **THE Deploy_Script SHALL** sequentially initialize all services, displaying real-time connectivity status bars for ports `9092` and `8000`.
5. **THE Deploy_Script SHALL** output a clean deployment report containing active internal container IP mappings and external AWS endpoint verification commands.

---

### Requirement 4: AWS Security Architecture & Infrastructure Documentation

**User Story:** As a cloud security engineer, I want strict AWS Security Group configurations and deployment guides, so that the pipeline endpoints remain isolated from public web-scraping botnets.

#### Acceptance Criteria

1. **THE Data_Pipeline_System SHALL** provide a comprehensive `README_AWS.md` detailing step-by-step AWS EC2 console setup parameters.
2. **THE Security_Group configurations** documented **SHALL** mandate strict port whitelist boundaries: Port `22` (SSH Administrative Access), Port `9092` (Kafka Data Ingestion), and Port `8000` (ChromaDB Cloud Access).
3. **THE README_AWS.md SHALL** provide explicit instructions on mapping AWS Security Groups to the development team's public IP address, denying all other inbound traffic by default.
4. **THE README_AWS.md SHALL** detail the structural configuration required for `KAFKA_ADVERTISED_LISTENERS` to dynamically bind the Kafka broker to the public AWS Elastic IP.
5. **THE documentation SHALL** include step-by-step validation procedures to test remote endpoint security without causing service disruption.

---

### Requirement 5: Vector Storage and Policy Ingestion Architecture

**User Story:** As an AI engineer, I want ChromaDB configured with isolated collections, so that macroeconomic policies can be vector-indexed and mapped to specific historical timeframes.

#### Acceptance Criteria

1. **THE ChromaDB_Service SHALL** expose a clean REST API on port `8000` accessible only to authorized ingestion agents running within the whitelisted AWS network boundary.
2. **THE ChromaDB_Service SHALL** host a primary collection named `macro_policies` configured with cosine similarity metrics for precise semantic text mapping.
3. **THE ingestion pipeline** feeding ChromaDB **SHALL** enforce a strict `Standard_JSON` structure for chunk metadata, embedding the fields: `source_url`, `ticker_context` (`"SHB"` or `"SCB"`), and `publish_date` (ISO 8601 format).
4. **THE ChromaDB_Service configuration SHALL** forbid the automated conversion or ingestion of line-delimited JSONL formats within the core policy vector indexes.
5. **THE Vector database storage volume mounts SHALL** be explicitly separate from the Kafka logging paths to guarantee independent data recovery lines.

---

### Requirement 6: Resilience, Integrity, and Dev-Testing Support

**User Story:** As a backend developer, I want development-focused diagnostic logs and data retention rules, so that I can reliably test edge-case stress scenarios for historical asset simulations.

#### Acceptance Criteria

1. **THE Kafka_Broker SHALL** configure specialized development log retention rules (`log.retention.hours=72` and `log.segment.bytes=1073741824`) to minimize disk space consumption on AWS EBS storage.
2. **THE Data_Pipeline_System SHALL** provide unified status check utilities allowing developers to verify internal broker and vector storage connectivity via a single terminal command.
3. **WHEN** docker containers undergo simulated crashes or host system reboots, **THE Data_Pipeline_System SHALL** preserve 100% of the ingested historical data streams for both SHB and SCB context frames.
4. **THE system SHALL** support configuration updates to individual container environment variables via standard environment files (`.env`) without requiring a complete database purge or cold-restart sequence.