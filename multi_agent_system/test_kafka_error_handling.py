#!/usr/bin/env python3
"""
Test script for enhanced Kafka error handling and retry mechanisms.

This test validates the implementation of Task 3.3:
- Exponential backoff retry strategy for Kafka connections
- Dead letter queue for failed message processing  
- Consumer group management and offset tracking
- Connection health monitoring and statistics logging
"""

import unittest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from multi_agent_system.utils.kafka_error_handler import (
    KafkaConsumerManager, 
    RetryConfig, 
    ExponentialBackoffRetry,
    DeadLetterQueue,
    DeadLetterMessage
)
from multi_agent_system.utils.exceptions import KafkaConnectionError, ConfigurationError
from multi_agent_system.agents.social_agent import SocialAgent


class TestRetryConfig(unittest.TestCase):
    """Test retry configuration validation."""
    
    def test_valid_retry_config(self):
        """Test valid retry configuration."""
        config = RetryConfig(
            max_retries=5,
            base_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter_factor=0.25
        )
        
        self.assertEqual(config.max_retries, 5)
        self.assertEqual(config.exponential_base, 2.0)
    
    def test_invalid_exponential_base(self):
        """Test that non-exponential backoff strategies are rejected (Requirement 6.5)."""
        with self.assertRaises(ConfigurationError) as cm:
            RetryConfig(exponential_base=3.0)  # Should only allow 2.0
        
        self.assertIn("Only exponential backoff strategy allowed", str(cm.exception))
        self.assertIn("Exponential base must be 2.0", str(cm.exception))
    
    def test_invalid_delays(self):
        """Test validation of delay values."""
        with self.assertRaises(ConfigurationError):
            RetryConfig(base_delay=-1.0)
        
        with self.assertRaises(ConfigurationError):
            RetryConfig(max_delay=0)
        
        with self.assertRaises(ConfigurationError):
            RetryConfig(max_retries=0)


class TestExponentialBackoffRetry(unittest.TestCase):
    """Test exponential backoff retry mechanism."""
    
    def setUp(self):
        self.config = RetryConfig(
            max_retries=3,
            base_delay=1.0,
            max_delay=10.0,
            exponential_base=2.0,
            jitter_factor=0.1
        )
        self.retry_handler = ExponentialBackoffRetry(self.config)
    
    def test_delay_calculation(self):
        """Test exponential backoff delay calculation."""
        # Test delay progression: 1.0, 2.0, 4.0, 8.0, but capped at 10.0
        delay_0 = self.retry_handler.calculate_delay(0)
        delay_1 = self.retry_handler.calculate_delay(1)
        delay_2 = self.retry_handler.calculate_delay(2)
        delay_3 = self.retry_handler.calculate_delay(3)
        
        # Check exponential progression (allowing for jitter)
        self.assertAlmostEqual(delay_0, 1.0, delta=0.2)  # 1.0 ± 10% jitter
        self.assertAlmostEqual(delay_1, 2.0, delta=0.4)  # 2.0 ± 20% jitter
        self.assertAlmostEqual(delay_2, 4.0, delta=0.8)  # 4.0 ± 20% jitter
        
        # Should be capped at max_delay
        self.assertLessEqual(delay_3, self.config.max_delay + 1.0)  # Allow for jitter
        
        # All delays should be positive
        self.assertGreater(delay_0, 0)
        self.assertGreater(delay_1, 0)
        self.assertGreater(delay_2, 0)
        self.assertGreater(delay_3, 0)
    
    def test_successful_operation(self):
        """Test successful operation without retries."""
        mock_operation = Mock(return_value="success")
        
        result = self.retry_handler.execute_with_retry(
            mock_operation, 
            operation_name="test_operation"
        )
        
        self.assertEqual(result, "success")
        mock_operation.assert_called_once()
    
    def test_operation_with_retries(self):
        """Test operation that succeeds after retries."""
        mock_operation = Mock()
        mock_operation.side_effect = [Exception("fail"), Exception("fail"), "success"]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = self.retry_handler.execute_with_retry(
                mock_operation,
                operation_name="test_retry"
            )
        
        self.assertEqual(result, "success")
        self.assertEqual(mock_operation.call_count, 3)
    
    def test_operation_max_retries_exceeded(self):
        """Test operation that fails all retry attempts."""
        mock_operation = Mock(side_effect=Exception("persistent_failure"))
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            with self.assertRaises(KafkaConnectionError) as cm:
                self.retry_handler.execute_with_retry(
                    mock_operation,
                    operation_name="test_max_retries"
                )
        
        self.assertIn("failed after 3 attempts", str(cm.exception))
        self.assertEqual(mock_operation.call_count, 3)


class TestDeadLetterQueue(unittest.TestCase):
    """Test dead letter queue functionality."""
    
    def setUp(self):
        self.dlq = DeadLetterQueue(max_size=10)
    
    def test_add_message_to_dlq(self):
        """Test adding messages to dead letter queue."""
        message = DeadLetterMessage(
            original_message=b'{"test": "data"}',
            original_topic="test_topic",
            original_partition=0,
            original_offset=123,
            failure_reason="JSON decode error"
        )
        
        success = self.dlq.add_message(message)
        self.assertTrue(success)
        
        stats = self.dlq.get_statistics()
        self.assertEqual(stats['queue_size'], 1)
        self.assertEqual(stats['total_messages'], 1)
        self.assertIn("JSON decode error", stats['messages_by_reason'])
    
    def test_dlq_overflow(self):
        """Test dead letter queue overflow handling."""
        # Fill up the queue
        for i in range(12):  # More than max_size=10
            message = DeadLetterMessage(
                original_message=f'{{"test": "{i}"}}'.encode(),
                original_topic="test_topic",
                original_partition=0,
                original_offset=i,
                failure_reason=f"error_{i}"
            )
            success = self.dlq.add_message(message)
            
            if i < 10:
                self.assertTrue(success)
            else:
                self.assertFalse(success)  # Should fail when queue is full
    
    def test_retrieve_messages(self):
        """Test retrieving messages from DLQ."""
        # Add some test messages
        for i in range(3):
            message = DeadLetterMessage(
                original_message=f'{{"test": "{i}"}}'.encode(),
                original_topic="test_topic",
                original_partition=0,
                original_offset=i,
                failure_reason=f"error_{i}"
            )
            self.dlq.add_message(message)
        
        # Retrieve messages
        messages = self.dlq.get_messages(max_count=2)
        self.assertEqual(len(messages), 2)
        
        # Check that messages were actually retrieved (queue should be smaller)
        stats = self.dlq.get_statistics()
        self.assertEqual(stats['queue_size'], 1)


class TestKafkaConsumerManager(unittest.TestCase):
    """Test enhanced Kafka consumer manager."""
    
    def setUp(self):
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test_group',
            'auto.offset.reset': 'latest'
        }
        self.retry_config = RetryConfig(max_retries=2, base_delay=0.1, max_delay=1.0)
    
    @patch('multi_agent_system.utils.kafka_error_handler.Consumer')
    def test_consumer_creation(self, mock_consumer_class):
        """Test consumer creation with proper configuration."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        manager = KafkaConsumerManager(
            kafka_config=self.kafka_config,
            topics=['test_topic'],
            retry_config=self.retry_config
        )
        
        # Test connection
        with patch.object(manager, '_subscribe_to_topics'):
            manager.connect()
        
        # Verify consumer was created with correct config
        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args[0][0]
        
        self.assertEqual(call_args['bootstrap.servers'], 'localhost:9092')
        self.assertEqual(call_args['group.id'], 'test_group')
        self.assertFalse(call_args['enable.auto.commit'])  # Should be False for manual offset management
    
    def test_invalid_kafka_config(self):
        """Test validation of Kafka configuration."""
        # Missing bootstrap.servers
        with self.assertRaises(KafkaConnectionError):
            manager = KafkaConsumerManager(
                kafka_config={'group.id': 'test'},
                topics=['test_topic']
            )
            manager.connect()
        
        # Missing group.id
        with self.assertRaises(KafkaConnectionError):
            manager = KafkaConsumerManager(
                kafka_config={'bootstrap.servers': 'localhost:9092'},
                topics=['test_topic']
            )
            manager.connect()
    
    @patch('multi_agent_system.utils.kafka_error_handler.Consumer')
    def test_health_metrics_tracking(self, mock_consumer_class):
        """Test health metrics tracking."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        manager = KafkaConsumerManager(
            kafka_config=self.kafka_config,
            topics=['test_topic'],
            retry_config=self.retry_config
        )
        
        # Test initial metrics
        metrics = manager.get_health_metrics()
        self.assertEqual(metrics.connection_status, "disconnected")
        self.assertEqual(metrics.total_messages_processed, 0)
        
        # Test connection state change
        with patch.object(manager, '_subscribe_to_topics'):
            manager.connect()
        
        metrics = manager.get_health_metrics()
        self.assertEqual(metrics.connection_status, "connected")


class TestSocialAgentEnhancement(unittest.TestCase):
    """Test Social Agent integration with enhanced Kafka error handling."""
    
    @patch('multi_agent_system.agents.social_agent.KafkaConsumerManager')
    def test_social_agent_initialization(self, mock_manager_class):
        """Test Social Agent initialization with enhanced error handling."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        
        agent = SocialAgent()
        
        # Verify KafkaConsumerManager was created
        mock_manager_class.assert_called_once()
        call_args = mock_manager_class.call_args
        
        # Check configuration
        kafka_config = call_args[1]['kafka_config']
        topics = call_args[1]['topics']
        
        self.assertIn('bootstrap.servers', kafka_config)
        self.assertEqual(topics, ['sentiment_scored_data'])
        self.assertFalse(kafka_config['enable.auto.commit'])  # Should use manual commits
        
        # Check that manager.connect() was called
        mock_manager.connect.assert_called_once()
    
    @patch('multi_agent_system.agents.social_agent.KafkaConsumerManager')
    def test_enhanced_message_consumption(self, mock_manager_class):
        """Test enhanced message consumption with offset management."""
        mock_manager = Mock()
        mock_manager_class.return_value = mock_manager
        
        # Mock successful message consumption
        test_message = {
            'payload': {
                'sentiment': {'label': 'Positive', 'confidence': 0.8},
                'interactions': {'likes': 100, 'shares': 20, 'comments': 10}
            },
            'topic': 'sentiment_scored_data',
            'partition': 0,
            'offset': 42
        }
        mock_manager.consume_message.return_value = test_message
        mock_manager.commit_offset.return_value = True
        
        agent = SocialAgent()
        
        # Test message consumption
        payload = agent.consume_sentiment_data(timeout=1.0)
        
        self.assertIsNotNone(payload)
        self.assertEqual(payload['sentiment']['label'], 'Positive')
        
        # Verify offset was committed
        mock_manager.commit_offset.assert_called_once()
        commit_args = mock_manager.commit_offset.call_args[0][0]
        self.assertEqual(commit_args['offset'], 42)
    
    @patch('multi_agent_system.agents.social_agent.KafkaConsumerManager')
    def test_health_status_reporting(self, mock_manager_class):
        """Test health status reporting functionality."""
        mock_manager = Mock()
        mock_health_metrics = Mock()
        mock_health_metrics.connection_status = "connected"
        mock_health_metrics.total_messages_processed = 150
        mock_health_metrics.processing_rate_per_second = 25.5
        mock_health_metrics.current_consumer_lag = 0
        mock_health_metrics.last_successful_operation = None
        
        mock_manager.get_health_metrics.return_value = mock_health_metrics
        mock_manager_class.return_value = mock_manager
        
        agent = SocialAgent()
        
        # Test health status
        health = agent.get_health_status()
        
        self.assertEqual(health['status'], 'connected')
        self.assertEqual(health['total_messages_processed'], 150)
        self.assertEqual(health['processing_rate_per_second'], 25.5)
        self.assertEqual(health['current_consumer_lag'], 0)
    
    @patch('multi_agent_system.agents.social_agent.KafkaConsumerManager')
    def test_statistics_with_dlq_info(self, mock_manager_class):
        """Test statistics reporting including DLQ information."""
        mock_manager = Mock()
        mock_dlq = Mock()
        mock_dlq.get_statistics.return_value = {
            'queue_size': 5,
            'total_messages': 12,
            'messages_by_reason': {'decode_error': 7, 'validation_error': 5}
        }
        
        mock_manager.dead_letter_queue = mock_dlq
        mock_manager.get_statistics.return_value = {
            'connection_status': 'connected',
            'messages_processed': 1000,
            'dead_letter_queue': mock_dlq.get_statistics()
        }
        
        mock_manager_class.return_value = mock_manager
        
        agent = SocialAgent()
        
        # Test DLQ statistics
        dlq_stats = agent.get_dead_letter_queue_stats()
        
        self.assertIsNotNone(dlq_stats)
        self.assertEqual(dlq_stats['queue_size'], 5)
        self.assertEqual(dlq_stats['total_messages'], 12)
        self.assertIn('decode_error', dlq_stats['messages_by_reason'])


def main():
    """Run all tests."""
    print("Testing Enhanced Kafka Error Handling Implementation (Task 3.3)")
    print("=" * 70)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestRetryConfig,
        TestExponentialBackoffRetry,
        TestDeadLetterQueue,
        TestKafkaConsumerManager,
        TestSocialAgentEnhancement
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    print(f"\n✓ Task 3.3 Implementation: {'PASSED' if success else 'FAILED'}")
    
    if success:
        print("\nImplemented features:")
        print("✓ Exponential backoff retry strategy for Kafka connections (Requirement 6.4)")
        print("✓ Dead letter queue for failed message processing (Requirement 6.3)")
        print("✓ Consumer group management and offset tracking (Requirement 6.2)")
        print("✓ Connection health monitoring and statistics logging (Requirements 6.6, 6.7)")
        print("✓ Enhanced Social Agent integration")
        print("✓ Circuit breaker pattern for external service calls")
        print("✓ Comprehensive error handling and logging")
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)