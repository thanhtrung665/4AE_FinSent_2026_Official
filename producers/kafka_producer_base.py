"""
Base Kafka Producer với retry logic và error handling
Sử dụng confluent-kafka cho Python 3.12 compatibility
"""
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from confluent_kafka import Producer, KafkaError, KafkaException
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class BaseKafkaProducer:
    """Base class cho Kafka Producer với retry logic"""
    
    def __init__(self, topic: str):
        self.topic = topic
        self.kafka_broker = os.getenv('AWS_KAFKA_BROKER')
        self.max_retry_attempts = int(os.getenv('MAX_RETRY_ATTEMPTS', 5))
        self.retry_backoff = int(os.getenv('RETRY_BACKOFF_SECONDS', 2))
        
        if not self.kafka_broker:
            raise ValueError("AWS_KAFKA_BROKER environment variable not set")
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize producer
        self.producer = self._create_producer()
    
    def _create_producer(self) -> Producer:
        """Tạo Kafka Producer với cấu hình tối ưu"""
        try:
            config = {
                'bootstrap.servers': self.kafka_broker,
                'client.id': f'python-producer-{int(time.time())}',
                'acks': '1',  # Wait for leader acknowledgment
                'retries': self.max_retry_attempts,
                'retry.backoff.ms': self.retry_backoff * 1000,
                'batch.size': 16384,
                'linger.ms': 10,
                'buffer.memory': 33554432,
                'request.timeout.ms': 30000,
                'delivery.timeout.ms': 60000,
                'compression.type': 'snappy'
            }
            
            producer = Producer(config)
            self.logger.info(f"Kafka Producer created successfully for broker: {self.kafka_broker}")
            return producer
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise
    
    def _delivery_callback(self, err, msg):
        """Callback function cho delivery report"""
        if err is not None:
            self.logger.error(f'Message delivery failed: {err}')
        else:
            self.logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((KafkaException, ConnectionError, OSError))
    )
    def send_message(self, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Gửi message với retry logic
        
        Args:
            message: Dictionary chứa dữ liệu cần gửi
            key: Optional key cho message
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Add timestamp if not present
            if 'timestamp' not in message:
                message['timestamp'] = datetime.now().isoformat()
            
            # Serialize message
            message_value = json.dumps(message, default=str).encode('utf-8')
            message_key = key.encode('utf-8') if key else None
            
            # Send message asynchronously
            self.producer.produce(
                topic=self.topic,
                value=message_value,
                key=message_key,
                callback=self._delivery_callback
            )
            
            # Flush to ensure delivery
            self.producer.flush(timeout=30)
            
            self.logger.info(f"Message sent successfully to {self.topic}")
            return True
            
        except KafkaException as e:
            self.logger.error(f"Kafka error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def send_batch(self, messages: list, flush_timeout: int = 30) -> int:
        """
        Gửi batch messages
        
        Args:
            messages: List of (message, key) tuples
            flush_timeout: Timeout for flush operation
            
        Returns:
            int: Number of successful sends
        """
        success_count = 0
        
        for message_data in messages:
            if isinstance(message_data, tuple):
                message, key = message_data
            else:
                message, key = message_data, None
                
            try:
                # Add timestamp if not present
                if 'timestamp' not in message:
                    message['timestamp'] = datetime.now().isoformat()
                
                # Serialize message
                message_value = json.dumps(message, default=str).encode('utf-8')
                message_key = key.encode('utf-8') if key else None
                
                # Send message asynchronously
                self.producer.produce(
                    topic=self.topic,
                    value=message_value,
                    key=message_key,
                    callback=self._delivery_callback
                )
                success_count += 1
                
            except Exception as e:
                self.logger.error(f"Failed to send message in batch: {e}")
                continue
        
        # Flush all pending messages
        try:
            self.producer.flush(timeout=flush_timeout)
            self.logger.info(f"Batch send completed: {success_count}/{len(messages)} messages sent")
        except Exception as e:
            self.logger.error(f"Error flushing producer: {e}")
            
        return success_count
    
    def health_check(self) -> bool:
        """
        Kiểm tra kết nối Kafka
        
        Returns:
            bool: True if healthy, False otherwise
        """
        try:
            # Get cluster metadata to check connection
            metadata = self.producer.list_topics(timeout=10)
            if metadata:
                self.logger.info("Kafka connection healthy")
                return True
            else:
                self.logger.warning("Kafka connection not established")
                return False
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False
    
    def close(self):
        """Đóng producer connection"""
        try:
            if self.producer:
                self.producer.flush(timeout=10)
                # confluent-kafka doesn't have explicit close method
                # The producer will be cleaned up when the object is destroyed
                self.logger.info("Kafka Producer closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing producer: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()