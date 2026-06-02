"""
Enhanced Kafka error handling and retry mechanisms for the Multi-Agent System.

This module implements:
- Exponential backoff retry strategy
- Dead letter queue for failed message processing
- Consumer group management and offset tracking
- Connection health monitoring and statistics logging
"""

import json
import time
import random
from typing import Dict, Any, Optional, List, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
import threading
import queue
import uuid

from confluent_kafka import Consumer, Producer, KafkaException, KafkaError, TopicPartition
from pybreaker import CircuitBreaker

from .logging_config import get_logger
from .exceptions import KafkaConnectionError, ConfigurationError

logger = get_logger('kafka_error_handler')


@dataclass
class RetryConfig:
    """Configuration for exponential backoff retry mechanism."""
    max_retries: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter_factor: float = 0.25
    
    def __post_init__(self):
        """Validate retry configuration according to requirements."""
        if self.exponential_base != 2.0:
            # Requirement 6.5: Only exponential backoff allowed
            raise ConfigurationError(
                f"Only exponential backoff strategy allowed. "
                f"Exponential base must be 2.0, got {self.exponential_base}"
            )
        
        if self.base_delay <= 0 or self.max_delay <= 0:
            raise ConfigurationError("Retry delays must be positive")
        
        if self.max_retries <= 0:
            raise ConfigurationError("Max retries must be positive")


@dataclass
class KafkaHealthMetrics:
    """Health monitoring metrics for Kafka connections."""
    connection_status: str = "disconnected"  # connected, disconnected, degraded
    last_successful_operation: Optional[datetime] = None
    total_messages_processed: int = 0
    total_messages_failed: int = 0
    connection_failures: int = 0
    retry_attempts: int = 0
    circuit_breaker_trips: int = 0
    processing_rate_per_second: float = 0.0
    average_processing_latency_ms: float = 0.0
    current_consumer_lag: int = 0
    
    def update_processing_rate(self, messages_processed: int, elapsed_time: float):
        """Update processing rate statistics."""
        if elapsed_time > 0:
            self.processing_rate_per_second = messages_processed / elapsed_time


@dataclass
class DeadLetterMessage:
    """Message structure for dead letter queue."""
    original_message: bytes
    original_topic: str
    original_partition: int
    original_offset: int
    failure_reason: str
    failure_timestamp: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class DeadLetterQueue:
    """
    Dead letter queue implementation for failed Kafka message processing.
    
    Handles messages that fail processing after all retry attempts,
    allowing for later manual inspection and reprocessing.
    """
    
    def __init__(self, max_size: int = 10000, persistence_file: Optional[str] = None):
        """
        Initialize dead letter queue.
        
        Args:
            max_size: Maximum number of messages to store in memory
            persistence_file: Optional file path for persistent storage
        """
        self._queue: queue.Queue = queue.Queue(maxsize=max_size)
        self._lock = Lock()
        self._persistence_file = persistence_file
        self._statistics = {
            'total_messages': 0,
            'messages_by_reason': {},
            'oldest_message_timestamp': None,
            'newest_message_timestamp': None
        }
        
        logger.info(f"Dead letter queue initialized (max_size={max_size})")
    
    def add_message(self, message: DeadLetterMessage) -> bool:
        """
        Add a failed message to the dead letter queue.
        
        Args:
            message: Failed message with failure details
            
        Returns:
            True if message added successfully, False if queue is full
        """
        try:
            with self._lock:
                self._queue.put_nowait(message)
                
                # Update statistics
                self._statistics['total_messages'] += 1
                
                reason = message.failure_reason
                self._statistics['messages_by_reason'][reason] = (
                    self._statistics['messages_by_reason'].get(reason, 0) + 1
                )
                
                if self._statistics['oldest_message_timestamp'] is None:
                    self._statistics['oldest_message_timestamp'] = message.failure_timestamp
                
                self._statistics['newest_message_timestamp'] = message.failure_timestamp
                
                logger.warning(
                    f"Message added to dead letter queue: "
                    f"reason={reason}, message_id={message.message_id}, "
                    f"retry_count={message.retry_count}"
                )
                
                # Persist to file if configured
                if self._persistence_file:
                    self._persist_message(message)
                
                return True
                
        except queue.Full:
            logger.error("Dead letter queue is full, cannot add message")
            return False
        except Exception as e:
            logger.error(f"Error adding message to dead letter queue: {e}")
            return False
    
    def _persist_message(self, message: DeadLetterMessage):
        """Persist message to file for recovery."""
        try:
            message_data = {
                'message_id': message.message_id,
                'original_topic': message.original_topic,
                'original_partition': message.original_partition,
                'original_offset': message.original_offset,
                'failure_reason': message.failure_reason,
                'failure_timestamp': message.failure_timestamp.isoformat(),
                'retry_count': message.retry_count,
                'original_message': message.original_message.decode('utf-8', errors='replace')
            }
            
            with open(self._persistence_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(message_data) + '\n')
                
        except Exception as e:
            logger.error(f"Failed to persist message to file: {e}")
    
    def get_messages(self, max_count: int = 100) -> List[DeadLetterMessage]:
        """
        Retrieve messages from dead letter queue for inspection.
        
        Args:
            max_count: Maximum number of messages to retrieve
            
        Returns:
            List of dead letter messages
        """
        messages = []
        
        try:
            with self._lock:
                for _ in range(min(max_count, self._queue.qsize())):
                    if not self._queue.empty():
                        messages.append(self._queue.get_nowait())
                
            logger.info(f"Retrieved {len(messages)} messages from dead letter queue")
            return messages
            
        except Exception as e:
            logger.error(f"Error retrieving messages from dead letter queue: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get dead letter queue statistics."""
        with self._lock:
            return {
                'queue_size': self._queue.qsize(),
                'max_size': self._queue.maxsize,
                **self._statistics
            }


class ExponentialBackoffRetry:
    """
    Exponential backoff retry mechanism for Kafka operations.
    
    Implements requirement 6.4: Exponential backoff retry strategy
    with proper jitter and maximum delay limits.
    """
    
    def __init__(self, config: RetryConfig):
        """Initialize retry mechanism with configuration."""
        self.config = config
        logger.info(f"Exponential backoff retry initialized: {config}")
    
    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay for retry attempt.
        
        Args:
            attempt: Current retry attempt number (0-based)
            
        Returns:
            Delay in seconds with jitter applied
        """
        # Exponential backoff: base_delay * (exponential_base ^ attempt)
        delay = self.config.base_delay * (self.config.exponential_base ** attempt)
        
        # Cap at maximum delay
        delay = min(delay, self.config.max_delay)
        
        # Add jitter (±jitter_factor% randomness)
        jitter_range = delay * self.config.jitter_factor
        jitter = jitter_range * (2 * random.random() - 1)
        delay += jitter
        
        # Ensure minimum delay of 0.1 seconds
        return max(0.1, delay)
    
    def execute_with_retry(self, 
                          operation: Callable, 
                          operation_name: str = "kafka_operation",
                          **kwargs) -> Any:
        """
        Execute operation with exponential backoff retry.
        
        Args:
            operation: Function to execute
            operation_name: Name for logging
            **kwargs: Arguments to pass to operation
            
        Returns:
            Operation result
            
        Raises:
            KafkaConnectionError: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries):
            try:
                logger.debug(f"Executing {operation_name} (attempt {attempt + 1}/{self.config.max_retries})")
                result = operation(**kwargs)
                
                if attempt > 0:
                    logger.info(f"{operation_name} succeeded after {attempt + 1} attempts")
                
                return result
                
            except Exception as e:
                last_exception = e
                
                if attempt < self.config.max_retries - 1:
                    delay = self.calculate_delay(attempt)
                    logger.warning(
                        f"{operation_name} failed (attempt {attempt + 1}/{self.config.max_retries}): {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    
                    try:
                        time.sleep(delay)
                    except KeyboardInterrupt:
                        logger.info(f"{operation_name} interrupted by user")
                        raise KafkaConnectionError(f"Operation interrupted: {operation_name}")
                else:
                    logger.error(f"{operation_name} failed after all retry attempts: {e}")
        
        raise KafkaConnectionError(
            f"Operation {operation_name} failed after {self.config.max_retries} attempts: {last_exception}"
        )


class KafkaConsumerManager:
    """
    Enhanced Kafka consumer with error handling, retry mechanisms, and health monitoring.
    
    Implements:
    - Exponential backoff retry (Requirement 6.4)
    - Dead letter queue (Requirement 6.3) 
    - Consumer group management (Requirement 6.2)
    - Connection health monitoring (Requirements 6.6, 6.7)
    """
    
    def __init__(self, 
                 kafka_config: Dict[str, Any],
                 topics: List[str],
                 retry_config: Optional[RetryConfig] = None,
                 enable_dead_letter_queue: bool = True):
        """
        Initialize enhanced Kafka consumer manager.
        
        Args:
            kafka_config: Kafka consumer configuration
            topics: List of topics to subscribe to
            retry_config: Retry mechanism configuration
            enable_dead_letter_queue: Whether to enable DLQ
        """
        self.kafka_config = kafka_config
        self.topics = topics
        self.retry_config = retry_config or RetryConfig()
        
        # Initialize components
        self.retry_handler = ExponentialBackoffRetry(self.retry_config)
        self.dead_letter_queue = DeadLetterQueue() if enable_dead_letter_queue else None
        self.metrics = KafkaHealthMetrics()
        
        # Consumer instance
        self.consumer: Optional[Consumer] = None
        self._lock = Lock()
        
        # Circuit breaker for Kafka operations
        self.circuit_breaker = CircuitBreaker(
            fail_max=5,  # Trip after 5 consecutive failures
            reset_timeout=30,  # Attempt reset after 30 seconds
            exclude=[KeyboardInterrupt]
        )
        
        # Statistics tracking
        self._processing_start_time: Optional[datetime] = None
        self._last_offset_commit_time: Optional[datetime] = None
        
        logger.info(f"KafkaConsumerManager initialized for topics: {topics}")
    
    def _create_consumer(self) -> Consumer:
        """Create Kafka consumer with error handling."""
        try:
            # Validate required configuration
            if 'bootstrap.servers' not in self.kafka_config:
                raise ConfigurationError("bootstrap.servers required in Kafka config")
            
            if 'group.id' not in self.kafka_config:
                raise ConfigurationError("group.id required in Kafka config")
            
            # Set default consumer configuration for reliability
            config = {
                'enable.auto.commit': False,  # Manual offset management
                'auto.offset.reset': 'latest',
                'session.timeout.ms': 30000,
                'heartbeat.interval.ms': 10000,
                'max.poll.interval.ms': 300000,
                **self.kafka_config
            }
            
            consumer = Consumer(config)
            logger.info(f"Kafka consumer created with config: {list(config.keys())}")
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise KafkaConnectionError(f"Consumer creation failed: {e}")
    
    def _subscribe_to_topics(self, consumer: Consumer):
        """Subscribe consumer to topics."""
        try:
            consumer.subscribe(self.topics)
            logger.info(f"Subscribed to Kafka topics: {self.topics}")
        except Exception as e:
            logger.error(f"Failed to subscribe to topics: {e}")
            raise KafkaConnectionError(f"Topic subscription failed: {e}")
    
    def connect(self) -> bool:
        """
        Establish Kafka connection with retry mechanism.
        
        Returns:
            True if connection successful
            
        Raises:
            KafkaConnectionError: If connection fails after retries
        """
        def _connect_operation():
            with self._lock:
                if self.consumer:
                    try:
                        self.consumer.close()
                    except:
                        pass
                
                self.consumer = self._create_consumer()
                self._subscribe_to_topics(self.consumer)
                
                # Test connection by getting metadata
                metadata = self.consumer.list_topics(timeout=10)
                if not metadata:
                    raise KafkaConnectionError("Failed to retrieve topic metadata")
                
                self.metrics.connection_status = "connected"
                self.metrics.last_successful_operation = datetime.utcnow()
                
                return True
        
        try:
            result = self.retry_handler.execute_with_retry(
                _connect_operation,
                operation_name="kafka_connect"
            )
            
            logger.info("Kafka connection established successfully")
            return result
            
        except KafkaConnectionError as e:
            self.metrics.connection_status = "disconnected"
            self.metrics.connection_failures += 1
            logger.error(f"Kafka connection failed: {e}")
            raise
    
    def consume_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Consume a single message with error handling.
        
        Args:
            timeout: Poll timeout in seconds
            
        Returns:
            Message payload or None if no message available
            
        Raises:
            KafkaConnectionError: If connection errors occur
        """
        if not self.consumer:
            logger.warning("Consumer not initialized, attempting to connect...")
            self.connect()
        
        if self._processing_start_time is None:
            self._processing_start_time = datetime.utcnow()
        
        try:
            msg = self.consumer.poll(timeout=timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                    return None
                
                # Handle connection errors with circuit breaker
                if msg.error().code() in [
                    KafkaError._TRANSPORT, 
                    KafkaError._ALL_BROKERS_DOWN, 
                    KafkaError._NETWORK_EXCEPTION
                ]:
                    self.metrics.connection_status = "degraded"
                    # Use circuit breaker for reconnection
                    try:
                        if self.circuit_breaker.current_state == 'closed':
                            self.circuit_breaker.call(self.connect)
                    except Exception as breaker_error:
                        logger.warning(f"Circuit breaker reconnection failed: {breaker_error}")
                
                error_msg = f"Kafka consumer error: {msg.error()}"
                logger.error(error_msg)
                raise KafkaConnectionError(error_msg)
            
            # Parse message
            try:
                message_value = msg.value().decode('utf-8')
                payload = json.loads(message_value)
                
                # Update metrics
                self.metrics.total_messages_processed += 1
                self.metrics.last_successful_operation = datetime.utcnow()
                
                # Calculate processing rate
                if self._processing_start_time:
                    elapsed = (datetime.utcnow() - self._processing_start_time).total_seconds()
                    if elapsed > 0:
                        self.metrics.processing_rate_per_second = (
                            self.metrics.total_messages_processed / elapsed
                        )
                
                logger.debug(
                    f"Message consumed: topic={msg.topic()}, partition={msg.partition()}, "
                    f"offset={msg.offset()}"
                )
                
                return {
                    'payload': payload,
                    'topic': msg.topic(),
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'timestamp': msg.timestamp(),
                    'key': msg.key().decode('utf-8') if msg.key() else None
                }
                
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                # Send to dead letter queue if enabled
                if self.dead_letter_queue:
                    dlq_message = DeadLetterMessage(
                        original_message=msg.value(),
                        original_topic=msg.topic(),
                        original_partition=msg.partition(),
                        original_offset=msg.offset(),
                        failure_reason=f"Message decode error: {e}"
                    )
                    self.dead_letter_queue.add_message(dlq_message)
                
                self.metrics.total_messages_failed += 1
                logger.error(f"Failed to decode message: {e}")
                return None
                
        except KafkaConnectionError:
            raise
        except Exception as e:
            self.metrics.total_messages_failed += 1
            error_msg = f"Unexpected error consuming message: {e}"
            logger.error(error_msg)
            raise KafkaConnectionError(error_msg)
    
    def commit_offset(self, message_info: Dict[str, Any]) -> bool:
        """
        Manually commit message offset for reliable processing.
        
        Args:
            message_info: Message information with topic, partition, offset
            
        Returns:
            True if commit successful
        """
        try:
            if not self.consumer:
                logger.warning("Cannot commit offset, consumer not initialized")
                return False
            
            # Create TopicPartition for manual commit
            tp = TopicPartition(
                topic=message_info['topic'],
                partition=message_info['partition'],
                offset=message_info['offset'] + 1  # Commit next offset
            )
            
            self.consumer.commit(offsets=[tp])
            self._last_offset_commit_time = datetime.utcnow()
            
            logger.debug(f"Offset committed: {tp}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to commit offset: {e}")
            return False
    
    def get_consumer_lag(self) -> int:
        """
        Calculate current consumer lag (messages behind).
        
        Returns:
            Number of messages behind high water mark
        """
        try:
            if not self.consumer:
                return 0
            
            # Get current assignment
            assignment = self.consumer.assignment()
            if not assignment:
                return 0
            
            # Get committed positions and high water marks
            committed = self.consumer.committed(assignment, timeout=5)
            watermarks = {}
            
            total_lag = 0
            for tp in assignment:
                try:
                    low, high = self.consumer.get_watermark_offsets(tp, timeout=5)
                    watermarks[tp] = (low, high)
                    
                    # Find committed offset for this partition
                    committed_offset = None
                    for committed_tp in committed:
                        if (committed_tp.topic == tp.topic and 
                            committed_tp.partition == tp.partition):
                            committed_offset = committed_tp.offset
                            break
                    
                    if committed_offset is not None:
                        lag = high - committed_offset
                        total_lag += max(0, lag)
                        
                except Exception as e:
                    logger.debug(f"Error calculating lag for {tp}: {e}")
            
            self.metrics.current_consumer_lag = total_lag
            return total_lag
            
        except Exception as e:
            logger.error(f"Error calculating consumer lag: {e}")
            return 0
    
    def get_health_metrics(self) -> KafkaHealthMetrics:
        """Get current health and performance metrics."""
        # Update consumer lag
        self.get_consumer_lag()
        
        # Update circuit breaker statistics
        self.metrics.circuit_breaker_trips = getattr(
            self.circuit_breaker, '_failure_count', 0
        )
        
        return self.metrics
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics for monitoring."""
        metrics = self.get_health_metrics()
        
        stats = {
            'connection_status': metrics.connection_status,
            'messages_processed': metrics.total_messages_processed,
            'messages_failed': metrics.total_messages_failed,
            'connection_failures': metrics.connection_failures,
            'processing_rate_per_second': metrics.processing_rate_per_second,
            'consumer_lag': metrics.current_consumer_lag,
            'circuit_breaker_trips': metrics.circuit_breaker_trips,
            'last_successful_operation': (
                metrics.last_successful_operation.isoformat() 
                if metrics.last_successful_operation else None
            ),
            'last_offset_commit': (
                self._last_offset_commit_time.isoformat() 
                if self._last_offset_commit_time else None
            )
        }
        
        # Add dead letter queue statistics if enabled
        if self.dead_letter_queue:
            stats['dead_letter_queue'] = self.dead_letter_queue.get_statistics()
        
        return stats
    
    def close(self):
        """Close consumer and clean up resources."""
        try:
            with self._lock:
                if self.consumer:
                    # Commit any pending offsets
                    try:
                        self.consumer.commit()
                    except Exception as e:
                        logger.debug(f"Error committing final offsets: {e}")
                    
                    # Close consumer
                    self.consumer.close()
                    self.consumer = None
                    
            self.metrics.connection_status = "disconnected"
            logger.info("Kafka consumer manager closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing consumer manager: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()