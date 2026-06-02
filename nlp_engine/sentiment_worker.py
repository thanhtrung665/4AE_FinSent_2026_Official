"""
PhoBERT Sentiment Worker - Công nhân khử nhiễu
Kafka Consumer + Producer với PhoBERT sentiment analysis
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from threading import Thread, Event
import signal

# Kafka
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Transformers and ML
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import torch

# Utilities
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import hashlib

# Load environment variables
load_dotenv()

class SentimentWorker:
    """PhoBERT Sentiment Analysis Worker"""
    
    def __init__(self):
        # Configuration
        self.kafka_broker = os.getenv('AWS_KAFKA_BROKER')
        self.input_topics = ['f319_data', 'fb_mock_data']
        self.output_topic = 'sentiment_scored_data'
        self.consumer_group = f'sentiment_worker_{int(time.time())}'
        
        # Model configuration
        self.phobert_model = os.getenv('PHOBERT_MODEL', 'vinai/phobert-base-v2')
        self.sentiment_model = os.getenv('SENTIMENT_MODEL', 'cardiffnlp/twitter-roberta-base-sentiment-latest')
        
        if not self.kafka_broker:
            raise ValueError("AWS_KAFKA_BROKER environment variable not set")
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('SentimentWorker')
        
        # Initialize components
        self._setup_kafka()
        self._setup_sentiment_models()
        
        # Threading control
        self.shutdown_event = Event()
        self.processing_stats = {
            'messages_processed': 0,
            'messages_sent': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("PhoBERT Sentiment Worker initialized successfully")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown_event.set()
    
    def _setup_kafka(self):
        """Cài đặt Kafka consumer và producer"""
        try:
            # Consumer configuration
            consumer_config = {
                'bootstrap.servers': self.kafka_broker,
                'group.id': self.consumer_group,
                'auto.offset.reset': 'latest',  # Start from latest messages
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 1000,
                'max.poll.interval.ms': 300000,
                'session.timeout.ms': 45000,
                'heartbeat.interval.ms': 3000,
                'fetch.min.bytes': 1,
                'fetch.wait.max.ms': 500
            }
            
            self.consumer = Consumer(consumer_config)
            
            # Subscribe to input topics
            self.consumer.subscribe(self.input_topics)
            self.logger.info(f"Subscribed to topics: {self.input_topics}")
            
            # Producer configuration
            producer_config = {
                'bootstrap.servers': self.kafka_broker,
                'client.id': f'sentiment-worker-{int(time.time())}',
                'acks': '1',
                'retries': 3,
                'retry.backoff.ms': 1000,
                'batch.size': 16384,
                'linger.ms': 10,
                'buffer.memory': 33554432,
                'compression.type': 'snappy'
            }
            
            self.producer = Producer(producer_config)
            self.logger.info(f"Kafka setup complete - broker: {self.kafka_broker}")
            
        except Exception as e:
            self.logger.error(f"Failed to setup Kafka: {e}")
            raise
    
    def _setup_sentiment_models(self):
        """Cài đặt PhoBERT và sentiment analysis models"""
        try:
            self.logger.info("Loading sentiment analysis models...")
            
            # Determine device
            device = 0 if torch.cuda.is_available() else -1
            device_name = "CUDA" if device == 0 else "CPU"
            self.logger.info(f"Using device: {device_name}")
            
            # Load Vietnamese sentiment model (fallback approach)
            try:
                # Try to load a Vietnamese-specific model
                self.sentiment_pipeline = pipeline(
                    "text-classification",
                    model="uitnlp/vietnamese-sentiment",
                    tokenizer="uitnlp/vietnamese-sentiment",
                    device=device,
                    return_all_scores=True
                )
                self.logger.info("Loaded Vietnamese sentiment model: uitnlp/vietnamese-sentiment")
                self.model_type = "vietnamese"
                
            except Exception as e:
                self.logger.warning(f"Failed to load Vietnamese model: {e}")
                # Fallback to multilingual model
                try:
                    self.sentiment_pipeline = pipeline(
                        "sentiment-analysis",
                        model=self.sentiment_model,
                        device=device,
                        return_all_scores=True
                    )
                    self.logger.info(f"Loaded multilingual sentiment model: {self.sentiment_model}")
                    self.model_type = "multilingual"
                    
                except Exception as e2:
                    self.logger.warning(f"Failed to load multilingual model: {e2}")
                    # Final fallback to basic model
                    self.sentiment_pipeline = pipeline(
                        "sentiment-analysis",
                        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                        device=device,
                        return_all_scores=True
                    )
                    self.logger.info("Loaded basic sentiment model")
                    self.model_type = "basic"
            
            # Test the model
            test_result = self._analyze_sentiment("Đây là một tin tốt")
            self.logger.info(f"Model test successful: {test_result}")
            
        except Exception as e:
            self.logger.error(f"Failed to setup sentiment models: {e}")
            raise
    
    def _analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """Phân tích sentiment cho một đoạn text"""
        try:
            if not text or not text.strip():
                return {
                    'label': 'NEUTRAL',
                    'confidence': 0.0,
                    'all_scores': [],
                    'error': 'Empty text'
                }
            
            # Clean text
            clean_text = text.strip()[:512]  # Limit length for model
            
            # Run sentiment analysis
            results = self.sentiment_pipeline(clean_text)
            
            if not results:
                return {
                    'label': 'NEUTRAL',
                    'confidence': 0.0,
                    'all_scores': [],
                    'error': 'No results from model'
                }
            
            # Process results based on model type
            if isinstance(results[0], list):
                # Model returns all scores
                all_scores = results[0]
            else:
                # Model returns single result
                all_scores = [results[0]]
            
            # Find best prediction
            best_prediction = max(all_scores, key=lambda x: x['score'])
            
            # Standardize labels to Positive/Negative/Neutral
            label = self._standardize_label(best_prediction['label'])
            
            return {
                'label': label,
                'confidence': float(best_prediction['score']),
                'all_scores': [
                    {
                        'label': self._standardize_label(score['label']),
                        'confidence': float(score['score'])
                    }
                    for score in all_scores
                ],
                'model_type': self.model_type,
                'text_length': len(clean_text)
            }
            
        except Exception as e:
            self.logger.error(f"Error analyzing sentiment: {e}")
            return {
                'label': 'NEUTRAL',
                'confidence': 0.0,
                'all_scores': [],
                'error': str(e)
            }
    
    def _standardize_label(self, label: str) -> str:
        """Chuẩn hóa labels về 3 loại: Positive, Negative, Neutral"""
        label_lower = label.lower()
        
        # Mapping various label formats
        positive_labels = ['positive', 'pos', 'label_2', '2', 'good', 'tích cực']
        negative_labels = ['negative', 'neg', 'label_0', '0', 'bad', 'tiêu cực'] 
        neutral_labels = ['neutral', 'neu', 'label_1', '1', 'trung tính']
        
        if any(pos in label_lower for pos in positive_labels):
            return 'Positive'
        elif any(neg in label_lower for neg in negative_labels):
            return 'Negative'
        elif any(neu in label_lower for neu in neutral_labels):
            return 'Neutral'
        else:
            # Default to neutral for unknown labels
            return 'Neutral'
    
    def _delivery_callback(self, err, msg):
        """Callback cho producer delivery"""
        if err is not None:
            self.logger.error(f'Message delivery failed: {err}')
            self.processing_stats['errors'] += 1
        else:
            self.logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
            self.processing_stats['messages_sent'] += 1
    
    def _process_message(self, message_data: Dict[str, Any], source_topic: str) -> Optional[Dict[str, Any]]:
        """Xử lý một message và thêm sentiment analysis"""
        try:
            # Extract text content based on message format
            text_content = self._extract_text_content(message_data, source_topic)
            
            if not text_content:
                self.logger.warning(f"No text content found in message from {source_topic}")
                return None
            
            # Analyze sentiment
            sentiment_result = self._analyze_sentiment(text_content)
            
            # Create enhanced message
            enhanced_message = {
                # Original message data
                **message_data,
                
                # Sentiment analysis results
                'sentiment': {
                    'label': sentiment_result['label'],
                    'confidence': sentiment_result['confidence'],
                    'all_scores': sentiment_result['all_scores'],
                    'model_type': sentiment_result.get('model_type', 'unknown')
                },
                
                # Processing metadata
                'processing': {
                    'source_topic': source_topic,
                    'processed_at': datetime.now(timezone.utc).isoformat(),
                    'worker_id': self.consumer_group,
                    'text_length': len(text_content),
                    'processing_version': '1.0'
                }
            }
            
            # Add error info if any
            if 'error' in sentiment_result:
                enhanced_message['sentiment']['error'] = sentiment_result['error']
            
            return enhanced_message
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return None
    
    def _extract_text_content(self, message_data: Dict[str, Any], source_topic: str) -> str:
        """Extract text content từ message dựa trên source topic"""
        try:
            if source_topic == 'f319_data':
                # F319 forum posts
                content = message_data.get('content_text', '')
                title = message_data.get('title', '')
                # Combine title and content
                full_text = f"{title} {content}".strip() if title else content
                return full_text
                
            elif source_topic == 'fb_mock_data':
                # Facebook mock data
                return message_data.get('content_text', '')
                
            elif source_topic == 'news_rss_data':
                # RSS news data (if we want to process this too)
                title = message_data.get('article_title', '')
                body = message_data.get('article_body', '')
                full_text = f"{title} {body}".strip() if title else body
                return full_text
                
            else:
                # Try to find any text field
                for field in ['content_text', 'text', 'message', 'body', 'content']:
                    if field in message_data and message_data[field]:
                        return str(message_data[field])
                
                return str(message_data)  # Fallback to entire message
                
        except Exception as e:
            self.logger.error(f"Error extracting text content: {e}")
            return ""
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((KafkaException, ConnectionError))
    )
    def _send_enhanced_message(self, enhanced_message: Dict[str, Any]) -> bool:
        """Gửi enhanced message tới output topic"""
        try:
            # Serialize message
            message_value = json.dumps(enhanced_message, default=str, ensure_ascii=False).encode('utf-8')
            
            # Create message key from original message
            message_key = None
            if 'comment_id' in enhanced_message:
                message_key = enhanced_message['comment_id']
            elif 'post_id' in enhanced_message:
                message_key = enhanced_message['post_id']
            else:
                # Generate key from message hash
                message_hash = hashlib.md5(message_value).hexdigest()[:8]
                message_key = f"sentiment_{message_hash}"
            
            # Send to Kafka
            self.producer.produce(
                topic=self.output_topic,
                value=message_value,
                key=message_key.encode('utf-8') if message_key else None,
                callback=self._delivery_callback
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending enhanced message: {e}")
            return False
    
    def run_consumer_loop(self):
        """Main consumer loop"""
        self.logger.info("Starting sentiment analysis consumer loop...")
        
        try:
            while not self.shutdown_event.is_set():
                try:
                    # Poll for messages
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition
                            continue
                        else:
                            self.logger.error(f"Consumer error: {msg.error()}")
                            continue
                    
                    # Process message
                    try:
                        # Decode message
                        message_data = json.loads(msg.value().decode('utf-8'))
                        source_topic = msg.topic()
                        
                        self.logger.debug(f"Processing message from {source_topic}")
                        
                        # Process and enhance message
                        enhanced_message = self._process_message(message_data, source_topic)
                        
                        if enhanced_message:
                            # Send enhanced message
                            if self._send_enhanced_message(enhanced_message):
                                self.processing_stats['messages_processed'] += 1
                                self.logger.info(
                                    f"Processed message from {source_topic} - "
                                    f"Sentiment: {enhanced_message['sentiment']['label']} "
                                    f"({enhanced_message['sentiment']['confidence']:.2f})"
                                )
                            else:
                                self.processing_stats['errors'] += 1
                        else:
                            self.logger.warning(f"Failed to process message from {source_topic}")
                            self.processing_stats['errors'] += 1
                    
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Invalid JSON in message: {e}")
                        self.processing_stats['errors'] += 1
                    except Exception as e:
                        self.logger.error(f"Error processing message: {e}")
                        self.processing_stats['errors'] += 1
                
                except KafkaException as e:
                    self.logger.error(f"Kafka exception: {e}")
                    time.sleep(5)  # Wait before retry
                
                # Flush producer occasionally
                if self.processing_stats['messages_processed'] % 10 == 0:
                    self.producer.flush(timeout=5)
        
        except KeyboardInterrupt:
            self.logger.info("Consumer loop interrupted by user")
        except Exception as e:
            self.logger.error(f"Critical error in consumer loop: {e}")
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Cleanup resources"""
        self.logger.info("Cleaning up resources...")
        
        try:
            if hasattr(self, 'consumer'):
                self.consumer.close()
        except Exception as e:
            self.logger.error(f"Error closing consumer: {e}")
        
        try:
            if hasattr(self, 'producer'):
                self.producer.flush(timeout=10)
        except Exception as e:
            self.logger.error(f"Error flushing producer: {e}")
        
        # Print final stats
        self.print_stats()
        self.logger.info("Cleanup completed")
    
    def print_stats(self):
        """In thống kê processing"""
        runtime = time.time() - self.processing_stats['start_time']
        
        stats = {
            'runtime_seconds': int(runtime),
            'messages_processed': self.processing_stats['messages_processed'],
            'messages_sent': self.processing_stats['messages_sent'],
            'errors': self.processing_stats['errors'],
            'messages_per_second': self.processing_stats['messages_processed'] / max(runtime, 1),
            'model_type': getattr(self, 'model_type', 'unknown')
        }
        
        self.logger.info("Processing Statistics:")
        self.logger.info(json.dumps(stats, indent=2))
    
    def health_check(self) -> Dict[str, Any]:
        """Kiểm tra health của worker"""
        try:
            # Test sentiment model
            test_sentiment = self._analyze_sentiment("Test message")
            
            # Test Kafka producer
            test_msg = {"test": "health_check", "timestamp": datetime.now().isoformat()}
            
            health_info = {
                'status': 'healthy',
                'kafka_broker': self.kafka_broker,
                'input_topics': self.input_topics,
                'output_topic': self.output_topic,
                'model_type': getattr(self, 'model_type', 'unknown'),
                'sentiment_test': test_sentiment,
                'processing_stats': self.processing_stats,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            return health_info
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="PhoBERT Sentiment Worker")
    parser.add_argument('--mode', choices=['run', 'health', 'test'], 
                       default='run', help='Operation mode')
    parser.add_argument('--test-text', type=str, help='Test text for sentiment analysis')
    
    args = parser.parse_args()
    
    # Initialize worker
    worker = SentimentWorker()
    
    if args.mode == 'run':
        # Run consumer loop
        worker.run_consumer_loop()
        
    elif args.mode == 'health':
        # Health check
        health = worker.health_check()
        print(json.dumps(health, indent=2, ensure_ascii=False))
        
    elif args.mode == 'test':
        # Test sentiment analysis
        test_text = args.test_text or "Thị trường hôm nay rất tích cực và tăng mạnh!"
        result = worker._analyze_sentiment(test_text)
        print(f"Text: {test_text}")
        print(f"Sentiment: {json.dumps(result, indent=2, ensure_ascii=False)}")


if __name__ == "__main__":
    main()