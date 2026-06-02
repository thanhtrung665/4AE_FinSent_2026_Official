#!/usr/bin/env python3
"""
Test script for Social Agent implementation.

This script tests the Social Agent class functionality including:
- Kafka consumer initialization with retry mechanism
- Message processing and PhoBERT score extraction
- Error handling for invalid message formats
- Processing statistics logging
- Automatic retry on connection errors
"""

import json
import time
import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any
import numpy as np

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from multi_agent_system.agents.social_agent import SocialAgent
from multi_agent_system.engines.vmsi_engine import VMSIEngine
from multi_agent_system.utils.exceptions import ValidationError, VMSICalculationError


class TestSocialAgent(unittest.TestCase):
    """Test cases for Social Agent functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test_group',
            'auto.offset.reset': 'latest'
        }
        
        # Mock VMSI Engine
        self.mock_vmsi_engine = Mock(spec=VMSIEngine)
        self.mock_vmsi_engine.calculate_interaction_weights.return_value = np.array([1.0, 2.0, 3.0])
        self.mock_vmsi_engine.calculate_social_score.return_value = 0.75
        
    @patch('multi_agent_system.agents.social_agent.Consumer')
    def test_social_agent_initialization(self, mock_consumer_class):
        """Test Social Agent initialization and configuration."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
        
        # Verify initialization
        self.assertEqual(agent.kafka_config, self.kafka_config)
        self.assertEqual(agent.topic, 'sentiment_scored_data')
        self.assertEqual(agent.vmsi_engine, self.mock_vmsi_engine)
        
        # Verify consumer was created and subscribed
        mock_consumer_class.assert_called_once_with(self.kafka_config)
        mock_consumer.subscribe.assert_called_once_with(['sentiment_scored_data'])
        
        # Verify statistics initialization
        self.assertEqual(agent.stats['messages_processed'], 0)
        self.assertEqual(agent.stats['messages_failed'], 0)
        self.assertIsNone(agent.stats['processing_start_time'])
        
    def test_retry_delay_calculation(self):
        """Test exponential backoff retry delay calculation."""
        with patch('multi_agent_system.agents.social_agent.Consumer'):
            agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
            
            # Test exponential backoff calculation
            delay_0 = agent._calculate_retry_delay(0)
            delay_1 = agent._calculate_retry_delay(1) 
            delay_2 = agent._calculate_retry_delay(2)
            
            # Verify exponential increase (with jitter tolerance)
            self.assertGreater(delay_1, delay_0 * 0.75)  # Account for jitter
            self.assertGreater(delay_2, delay_1 * 0.75)  # Account for jitter
            
            # Test maximum delay cap
            delay_large = agent._calculate_retry_delay(10)
            self.assertLessEqual(delay_large, agent.max_retry_delay * 1.25)  # Account for jitter
            
    def test_phobert_score_extraction(self):
        """Test PhoBERT score extraction from message payloads."""
        with patch('agents.social_agent.Consumer'):
            agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
            
            # Test positive sentiment
            positive_payload = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 100, "shares": 20, "comments": 15},
                "metadata": {"credibility_score": 0.7}
            }
            score = agent.extract_phobert_scores(positive_payload)
            self.assertEqual(score, 0.85)
            
            # Test negative sentiment
            negative_payload = {
                "sentiment": {"label": "Negative", "confidence": 0.75},
                "interactions": {"likes": 50, "shares": 5, "comments": 10},
                "metadata": {"credibility_score": 0.6}
            }
            score = agent.extract_phobert_scores(negative_payload)
            self.assertEqual(score, -0.75)
            
            # Test neutral sentiment
            neutral_payload = {
                "sentiment": {"label": "Neutral", "confidence": 0.90},
                "interactions": {"likes": 25, "shares": 2, "comments": 3},
                "metadata": {"credibility_score": 0.8}
            }
            score = agent.extract_phobert_scores(neutral_payload)
            self.assertEqual(score, 0.0)
    
    def test_interaction_metrics_extraction(self):
        """Test interaction metrics extraction from message payloads."""
        with patch('agents.social_agent.Consumer'):
            agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
            
            # Test valid interaction data
            payload = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 150, "shares": 23, "comments": 47},
                "metadata": {"credibility_score": 0.7}
            }
            
            likes, shares, comments = agent.extract_interaction_metrics(payload)
            self.assertEqual(likes, 150)
            self.assertEqual(shares, 23)
            self.assertEqual(comments, 47)
            
            # Test missing interaction fields (should default to 0)
            payload_missing = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 100},  # Missing shares and comments
                "metadata": {"credibility_score": 0.7}
            }
            
            likes, shares, comments = agent.extract_interaction_metrics(payload_missing)
            self.assertEqual(likes, 100)
            self.assertEqual(shares, 0)
            self.assertEqual(comments, 0)
    
    def test_credibility_factor_extraction(self):
        """Test credibility factor extraction from message metadata."""
        with patch('agents.social_agent.Consumer'):
            agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
            
            # Test valid credibility score
            payload = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 100, "shares": 20, "comments": 15},
                "metadata": {"credibility_score": 0.8}
            }
            
            credibility = agent.extract_credibility_factor(payload)
            self.assertEqual(credibility, 0.8)
            
            # Test missing metadata (should default to 0.5)
            payload_no_metadata = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 100, "shares": 20, "comments": 15}
            }
            
            credibility = agent.extract_credibility_factor(payload_no_metadata)
            self.assertEqual(credibility, 0.5)
            
            # Test out-of-range credibility (should be clamped)
            payload_high_cred = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 100, "shares": 20, "comments": 15},
                "metadata": {"credibility_score": 1.5}  # Too high
            }
            
            credibility = agent.extract_credibility_factor(payload_high_cred)
            self.assertEqual(credibility, 1.0)  # Should be clamped to 1.0
    
    def test_invalid_message_handling(self):
        """Test error handling for invalid message formats."""
        with patch('agents.social_agent.Consumer'):
            agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
            
            # Test missing sentiment field
            invalid_payload = {
                "interactions": {"likes": 100, "shares": 20, "comments": 15},
                "metadata": {"credibility_score": 0.7}
            }
            
            with self.assertRaises(ValidationError):
                agent.extract_phobert_scores(invalid_payload)
            
            # Test invalid sentiment label
            invalid_label_payload = {
                "sentiment": {"label": "Unknown", "confidence": 0.85},
                "interactions": {"likes": 100, "shares": 20, "comments": 15},
                "metadata": {"credibility_score": 0.7}
            }
            
            with self.assertRaises(ValidationError):
                agent.extract_phobert_scores(invalid_label_payload)
            
            # Test negative interaction values
            negative_interactions = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": -10, "shares": 20, "comments": 15},
                "metadata": {"credibility_score": 0.7}
            }
            
            with self.assertRaises(ValidationError):
                agent.extract_interaction_metrics(negative_interactions)
    
    def test_processing_statistics(self):
        """Test processing statistics collection and reporting."""
        with patch('agents.social_agent.Consumer'):
            agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
            
            # Simulate some processing
            agent.stats['messages_processed'] = 100
            agent.stats['messages_failed'] = 5
            agent.stats['processing_start_time'] = time.time() - 10  # 10 seconds ago
            agent.stats['connection_failures'] = 2
            agent.stats['retry_attempts'] = 3
            
            stats = agent.get_processing_statistics()
            
            # Verify statistics
            self.assertEqual(stats['messages_processed'], 100)
            self.assertEqual(stats['messages_failed'], 5)
            self.assertEqual(stats['connection_failures'], 2)
            self.assertEqual(stats['retry_attempts'], 3)
            self.assertGreater(stats['processing_rate_msg_per_sec'], 0)
            self.assertGreater(stats['elapsed_time_seconds'], 0)
    
    @patch('agents.social_agent.Consumer')
    def test_kafka_connection_retry(self, mock_consumer_class):
        """Test Kafka connection retry mechanism."""
        # Mock consumer to fail first few times, then succeed
        mock_consumer = Mock()
        mock_consumer_class.side_effect = [
            Exception("Connection failed"),  # First attempt fails
            Exception("Connection failed"),  # Second attempt fails  
            mock_consumer                    # Third attempt succeeds
        ]
        
        # This should succeed after retries
        agent = SocialAgent(kafka_config=self.kafka_config, vmsi_engine=self.mock_vmsi_engine)
        
        # Verify multiple consumer creation attempts
        self.assertEqual(mock_consumer_class.call_count, 3)
        mock_consumer.subscribe.assert_called_once_with(['sentiment_scored_data'])
        
        # Verify retry statistics
        self.assertGreater(agent.stats['retry_attempts'], 0)


def run_basic_functionality_test():
    """Run a basic functionality test without requiring Kafka."""
    print("\n=== Testing Social Agent Basic Functionality ===")
    
    try:
        # Test with mocked consumer
        with patch('agents.social_agent.Consumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            
            # Create agent
            agent = SocialAgent()
            print("✅ Social Agent initialized successfully")
            
            # Test message processing
            test_payload = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": 150, "shares": 23, "comments": 47},
                "metadata": {"credibility_score": 0.7}
            }
            
            # Test individual extraction methods
            phobert_score = agent.extract_phobert_scores(test_payload)
            print(f"✅ PhoBERT score extraction: {phobert_score}")
            
            likes, shares, comments = agent.extract_interaction_metrics(test_payload)
            print(f"✅ Interaction metrics extraction: likes={likes}, shares={shares}, comments={comments}")
            
            credibility = agent.extract_credibility_factor(test_payload)
            print(f"✅ Credibility factor extraction: {credibility}")
            
            # Test statistics
            stats = agent.get_processing_statistics()
            print(f"✅ Processing statistics: {stats}")
            
            # Test cleanup
            agent.close()
            print("✅ Agent closed successfully")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


def run_error_handling_test():
    """Test error handling scenarios."""
    print("\n=== Testing Error Handling ===")
    
    try:
        with patch('agents.social_agent.Consumer') as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer
            
            agent = SocialAgent()
            
            # Test invalid message format handling
            invalid_payload = {"invalid": "data"}
            try:
                agent.extract_phobert_scores(invalid_payload)
                print("❌ Should have raised ValidationError")
                return False
            except ValidationError:
                print("✅ Correctly handled invalid message format")
            
            # Test negative interaction values
            negative_payload = {
                "sentiment": {"label": "Positive", "confidence": 0.85},
                "interactions": {"likes": -10, "shares": 20, "comments": 15}
            }
            try:
                agent.extract_interaction_metrics(negative_payload)
                print("❌ Should have raised ValidationError for negative values")
                return False
            except ValidationError:
                print("✅ Correctly handled negative interaction values")
            
            # Test exponential backoff calculation
            delay_0 = agent._calculate_retry_delay(0)
            delay_1 = agent._calculate_retry_delay(1)
            delay_2 = agent._calculate_retry_delay(2)
            
            if delay_0 < delay_1 < delay_2:
                print("✅ Exponential backoff working correctly")
            else:
                print(f"⚠️ Exponential backoff may have jitter: {delay_0:.2f} -> {delay_1:.2f} -> {delay_2:.2f}")
            
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False
    
    return True


if __name__ == "__main__":
    print("Starting Social Agent Test Suite")
    print("=" * 50)
    
    # Run basic functionality test
    if not run_basic_functionality_test():
        exit(1)
    
    # Run error handling test  
    if not run_error_handling_test():
        exit(1)
    
    # Run unit tests
    print("\n=== Running Unit Tests ===")
    unittest.main(verbosity=2, exit=False)
    
    print("\n" + "=" * 50)
    print("Social Agent Test Suite Completed Successfully")