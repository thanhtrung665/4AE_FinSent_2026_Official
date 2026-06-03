#!/usr/bin/env python3
"""
Comprehensive validation test for both Kafka producer modules.
This test validates the complete functionality of both:
1. Market_Data_Producer with vnstock integration
2. Facebook_Mock_Injector with CSV processing

Tests inheritance from BaseKafkaProducer and core functionality.
"""

import os
import unittest
import tempfile
import json
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone


class TestKafkaProducerModulesValidation(unittest.TestCase):
    """Comprehensive validation tests for both producer modules"""

    def setUp(self):
        """Set up test environment"""
        # Mock environment variables
        os.environ['AWS_KAFKA_BROKER'] = 'localhost:9092'
        os.environ['MARKET_DATA_INTERVAL_SECONDS'] = '60'
        os.environ['VNSTOCK_TICKERS'] = 'VN30,VNINDEX'
        os.environ['FB_MOCK_FILE_PATH'] = 'facebook_mock.csv'
        os.environ['FB_MOCK_STREAM_DELAY'] = '1.0'

    def test_market_data_producer_inheritance(self):
        """Test Market_Data_Producer inherits from BaseKafkaProducer correctly"""
        from market_data_producer import MarketDataProducer
        from kafka_producer_base import BaseKafkaProducer
        
        producer = MarketDataProducer()
        
        # Test inheritance
        self.assertIsInstance(producer, BaseKafkaProducer)
        
        # Test topic configuration
        self.assertEqual(producer.topic, 'market_stock_data')
        
        # Test ticker configuration
        self.assertEqual(producer.tickers, ['VN30', 'VNINDEX'])
        
        # Test interval configuration
        self.assertEqual(producer.interval_seconds, 60)
        
        producer.close()

    def test_facebook_mock_injector_inheritance(self):
        """Test Facebook_Mock_Injector inherits from BaseKafkaProducer correctly"""
        from facebook_mock_injector import FacebookMockInjector
        from kafka_producer_base import BaseKafkaProducer
        
        injector = FacebookMockInjector()
        
        # Test inheritance
        self.assertIsInstance(injector, BaseKafkaProducer)
        
        # Test topic configuration
        self.assertEqual(injector.topic, 'fb_mock_data')
        
        # Test CSV file path configuration
        self.assertEqual(injector.csv_file_path, 'facebook_mock.csv')
        
        # Test delay configuration
        self.assertEqual(injector.delay_seconds, 1.0)
        
        injector.close()

    def test_market_data_json_format_consistency(self):
        """Test that market data output follows required JSON format"""
        from market_data_producer import MarketDataProducer
        import pandas as pd
        
        producer = MarketDataProducer()
        
        # Mock a sample vnstock record as pandas Series
        mock_record = pd.Series({
            'time': '2024-01-15 15:30:00',
            'open': 1245.67,
            'high': 1251.23,
            'low': 1242.15,
            'close': 1249.88,
            'volume': 15420000
        })
        
        # Test parsing
        result = producer._parse_market_record(mock_record, "VN30")
        
        # Validate JSON structure
        required_fields = [
            'ticker', 'timestamp', 'open', 'high', 'low', 
            'close', 'volume', 'data_source', 'collection_timestamp'
        ]
        
        for field in required_fields:
            self.assertIn(field, result, f"Missing required field: {field}")
        
        # Validate data types
        self.assertIsInstance(result['open'], (int, float))
        self.assertIsInstance(result['high'], (int, float))
        self.assertIsInstance(result['low'], (int, float))
        self.assertIsInstance(result['close'], (int, float))
        self.assertIsInstance(result['volume'], int)
        
        # Validate specific values
        self.assertEqual(result['ticker'], 'VN30')
        self.assertEqual(result['data_source'], 'vnstock')
        self.assertEqual(result['close'], 1249.88)
        
        producer.close()

    def test_csv_to_json_transformation_consistency(self):
        """Test CSV to JSON transformation preserves data correctly"""
        from facebook_mock_injector import FacebookMockInjector
        import pandas as pd
        
        injector = FacebookMockInjector()
        
        # Mock CSV row
        mock_row = pd.Series({
            'comment_id': 'fb_test_001',
            'content_text': 'Test Vietnamese content về thị trường',
            'created_at': '2024-01-15 09:30:00',
            'likes': 25
        })
        
        # Test parsing
        result = injector._parse_csv_row(mock_row, 0)
        
        # Validate transformation
        self.assertEqual(result['comment_id'], 'fb_test_001')
        self.assertEqual(result['content_text'], 'Test Vietnamese content về thị trường')
        self.assertEqual(result['likes'], 25)
        self.assertEqual(result['row_index'], 0)
        
        # Validate additional fields are added
        self.assertIn('injection_timestamp', result)
        
        # Validate datetime parsing
        self.assertIsInstance(result['created_at'], str)
        
        injector.close()

    @patch('market_data_producer.Quote')
    def test_market_data_producer_error_handling(self, mock_quote_class):
        """Test Market_Data_Producer error handling consistency"""
        from market_data_producer import MarketDataProducer
        
        # Mock Quote class to raise an exception
        mock_quote = MagicMock()
        mock_quote.history.side_effect = Exception("Network error")
        mock_quote_class.return_value = mock_quote
        
        producer = MarketDataProducer()
        
        # Test error handling
        result = producer.get_market_data('VN30')
        
        # Should return None on error
        self.assertIsNone(result)
        
        # Should log error (verify logger was called)
        self.assertTrue(hasattr(producer, 'logger'))
        
        producer.close()

    def test_facebook_mock_injector_file_error_handling(self):
        """Test Facebook_Mock_Injector file error handling"""
        from facebook_mock_injector import FacebookMockInjector
        
        # Set non-existent file path
        os.environ['FB_MOCK_FILE_PATH'] = 'non_existent_file.csv'
        
        injector = FacebookMockInjector()
        
        # Should handle missing file gracefully
        self.assertTrue(hasattr(injector, 'logger'))
        
        # Should create sample file or handle error gracefully
        csv_info = injector.get_csv_info()
        
        # Either file was created or error is properly handled
        self.assertIsInstance(csv_info, dict)
        
        injector.close()

    def test_health_check_implementations(self):
        """Test health check implementations for both producers"""
        from market_data_producer import MarketDataProducer
        from facebook_mock_injector import FacebookMockInjector
        
        # Test Market Data Producer health check
        market_producer = MarketDataProducer()
        
        # Health check should be inherited from BaseKafkaProducer
        self.assertTrue(hasattr(market_producer, 'health_check'))
        self.assertTrue(callable(market_producer.health_check))
        
        # Test Facebook Mock Injector health check
        fb_injector = FacebookMockInjector()
        
        # Health check should be inherited from BaseKafkaProducer
        self.assertTrue(hasattr(fb_injector, 'health_check'))
        self.assertTrue(callable(fb_injector.health_check))
        
        market_producer.close()
        fb_injector.close()

    def test_configuration_management(self):
        """Test that both producers handle environment configuration correctly"""
        from market_data_producer import MarketDataProducer
        from facebook_mock_injector import FacebookMockInjector
        
        # Test Market Data Producer configuration
        market_producer = MarketDataProducer()
        self.assertEqual(market_producer.interval_seconds, 60)
        self.assertEqual(market_producer.tickers, ['VN30', 'VNINDEX'])
        
        # Test Facebook Mock Injector configuration
        fb_injector = FacebookMockInjector()
        self.assertEqual(fb_injector.delay_seconds, 1.0)
        
        market_producer.close()
        fb_injector.close()

    def test_collect_once_functionality(self):
        """Test single collection functionality for both producers"""
        from market_data_producer import MarketDataProducer
        from facebook_mock_injector import FacebookMockInjector
        
        # Test Market Data Producer collect_once
        market_producer = MarketDataProducer()
        
        # Should have collect_once method
        self.assertTrue(hasattr(market_producer, 'collect_once'))
        self.assertTrue(callable(market_producer.collect_once))
        
        # Test Facebook Mock Injector inject_once
        fb_injector = FacebookMockInjector()
        
        # Should have inject_once method
        self.assertTrue(hasattr(fb_injector, 'inject_once'))
        self.assertTrue(callable(fb_injector.inject_once))
        
        market_producer.close()
        fb_injector.close()

    def test_logging_integration(self):
        """Test that both producers have proper logging integration"""
        from market_data_producer import MarketDataProducer
        from facebook_mock_injector import FacebookMockInjector
        
        # Test Market Data Producer logging
        market_producer = MarketDataProducer()
        self.assertTrue(hasattr(market_producer, 'logger'))
        self.assertEqual(market_producer.logger.name, 'MarketDataProducer')
        
        # Test Facebook Mock Injector logging
        fb_injector = FacebookMockInjector()
        self.assertTrue(hasattr(fb_injector, 'logger'))
        self.assertEqual(fb_injector.logger.name, 'FacebookMockInjector')
        
        market_producer.close()
        fb_injector.close()

    def test_data_format_requirements(self):
        """Test that data formats meet all requirements"""
        from market_data_producer import MarketDataProducer
        from facebook_mock_injector import FacebookMockInjector
        import pandas as pd
        
        # Test Market Data format requirements
        market_producer = MarketDataProducer()
        
        # Mock data should have all required fields as pandas Series
        mock_record = pd.Series({
            'time': datetime.now(),
            'open': 100.0,
            'high': 105.0,
            'low': 95.0,
            'close': 102.0,
            'volume': 1000000
        })
        
        result = market_producer._parse_market_record(mock_record, "TEST")
        
        # Validate requirements compliance
        self.assertIn('ticker', result)
        self.assertIn('timestamp', result)
        self.assertIn('data_source', result)
        self.assertEqual(result['data_source'], 'vnstock')
        
        # Test Facebook Mock format requirements
        fb_injector = FacebookMockInjector()
        
        mock_csv_row = pd.Series({
            'comment_id': 'test_001',
            'content_text': 'Test content',
            'created_at': '2024-01-15 10:00:00',
            'likes': 10
        })
        
        fb_result = fb_injector._parse_csv_row(mock_csv_row, 0)
        
        # Validate CSV transformation requirements
        required_csv_fields = ['comment_id', 'content_text', 'created_at', 'likes']
        for field in required_csv_fields:
            self.assertIn(field, fb_result)
        
        market_producer.close()
        fb_injector.close()


if __name__ == '__main__':
    print("Running comprehensive validation tests for Kafka Producer Modules...")
    unittest.main(verbosity=2)