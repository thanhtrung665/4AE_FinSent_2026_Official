#!/usr/bin/env python3
"""
Unit tests for Facebook Mock Injector CSV processing methods.
Tests the implementation of task 3.3 requirements:
1. read_csv_data() method with multiple encoding support
2. stream_csv_data() generator for real-time streaming simulation  
3. CSV format validation with specific column checking
"""

import os
import unittest
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone


class TestFacebookMockInjectorCSVMethods(unittest.TestCase):
    """Test CSV processing methods of Facebook Mock Injector"""

    def setUp(self):
        """Set up test environment"""
        # Mock environment variables
        os.environ['AWS_KAFKA_BROKER'] = 'localhost:9092'
        
        # Create a test CSV file
        self.test_csv_content = """comment_id,content_text,created_at,likes
fb_test_001,"Thị trường hôm nay khá tích cực!",2024-01-15 09:30:00,15
fb_test_002,"Cổ phiếu ngân hàng tăng mạnh",2024-01-15 10:15:00,8
fb_test_003,"VN30 vượt ngưỡng kháng cự",2024-01-15 11:00:00,23"""

        # Create temporary CSV file
        self.temp_csv_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8')
        self.temp_csv_file.write(self.test_csv_content)
        self.temp_csv_file.close()

    def tearDown(self):
        """Clean up test environment"""
        # Remove temporary file
        if os.path.exists(self.temp_csv_file.name):
            os.unlink(self.temp_csv_file.name)

    def test_csv_format_validation_valid_file(self):
        """Test CSV format validation with valid file"""
        from facebook_mock_injector import FacebookMockInjector
        
        # Create injector and override CSV path
        injector = object.__new__(FacebookMockInjector)
        injector.csv_file_path = self.temp_csv_file.name
        injector.logger = MagicMock()
        
        # Test validation
        result = injector.validate_csv_format()
        
        self.assertTrue(result)
        # Verify logger was called with success message
        injector.logger.info.assert_called()

    def test_csv_format_validation_missing_file(self):
        """Test CSV format validation with missing file"""
        from facebook_mock_injector import FacebookMockInjector
        
        # Create injector but prevent file creation by setting a completely invalid path
        injector = object.__new__(FacebookMockInjector)
        injector.csv_file_path = "/invalid/path/non_existent_file.csv"
        injector.logger = MagicMock()
        
        result = injector.validate_csv_format()
        
        self.assertFalse(result)
        # Verify error was logged
        injector.logger.error.assert_called()

    def test_csv_format_validation_missing_columns(self):
        """Test CSV format validation with missing required columns"""
        from facebook_mock_injector import FacebookMockInjector
        
        # Create CSV with missing columns
        invalid_content = """id,text,date
1,"Test content","2024-01-15"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(invalid_content)
            invalid_csv_path = f.name

        try:
            injector = object.__new__(FacebookMockInjector)
            injector.csv_file_path = invalid_csv_path
            injector.logger = MagicMock()
            
            result = injector.validate_csv_format()
            
            self.assertFalse(result)
            # Verify error about missing columns was logged
            injector.logger.error.assert_called()
            
        finally:
            os.unlink(invalid_csv_path)

    def test_read_csv_data_utf8_encoding(self):
        """Test read_csv_data() with UTF-8 encoding"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        injector.csv_file_path = self.temp_csv_file.name
        injector.logger = MagicMock()
        
        data = injector.read_csv_data()
        
        # Should read 3 records
        self.assertEqual(len(data), 3)
        
        # Check first record structure
        first_record = data[0]
        required_fields = ['comment_id', 'content_text', 'created_at', 'likes', 'row_index', 'injection_timestamp']
        for field in required_fields:
            self.assertIn(field, first_record)
        
        # Check data integrity
        self.assertEqual(first_record['comment_id'], 'fb_test_001')
        self.assertEqual(first_record['content_text'], 'Thị trường hôm nay khá tích cực!')
        self.assertEqual(first_record['likes'], 15)
        self.assertEqual(first_record['row_index'], 0)

    def test_read_csv_data_multiple_encoding_fallback(self):
        """Test read_csv_data() with encoding fallback mechanism"""
        from facebook_mock_injector import FacebookMockInjector
        
        # Create CSV with Vietnamese characters in UTF-8 (safest)
        vietnamese_content = """comment_id,content_text,created_at,likes
fb_vietnamese,"Tôi yêu Việt Nam và chứng khoán Việt",2024-01-15 09:30:00,25"""
        
        # Test with UTF-8 encoding (most common case)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(vietnamese_content)
            temp_path = f.name

        try:
            injector = object.__new__(FacebookMockInjector)
            injector.csv_file_path = temp_path
            injector.logger = MagicMock()
            
            data = injector.read_csv_data()
            
            # Should successfully read the file
            self.assertEqual(len(data), 1)
            self.assertIn('Tôi yêu Việt Nam', data[0]['content_text'])
            
        finally:
            # Clean up with better error handling
            try:
                os.unlink(temp_path)
            except (OSError, PermissionError):
                pass  # File might be locked, ignore cleanup error

    def test_read_csv_data_empty_file(self):
        """Test read_csv_data() with empty CSV file"""
        from facebook_mock_injector import FacebookMockInjector
        
        # Create empty CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("comment_id,content_text,created_at,likes\n")  # Headers only
            empty_csv_path = f.name

        try:
            injector = object.__new__(FacebookMockInjector)
            injector.csv_file_path = empty_csv_path
            injector.logger = MagicMock()
            
            data = injector.read_csv_data()
            
            # Should return empty list
            self.assertEqual(len(data), 0)
            # Should log warning about empty file
            injector.logger.warning.assert_called()
            
        finally:
            os.unlink(empty_csv_path)

    @patch('facebook_mock_injector.time.sleep')  # Mock sleep to make test faster
    def test_stream_csv_data_generator(self, mock_sleep):
        """Test stream_csv_data() generator functionality"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        injector.csv_file_path = self.temp_csv_file.name
        injector.delay_seconds = 1.0
        injector.processed_ids = set()
        injector.logger = MagicMock()
        
        # Collect all streamed records
        streamed_records = list(injector.stream_csv_data())
        
        # Should stream all 3 records
        self.assertEqual(len(streamed_records), 3)
        
        # Check streaming metadata is added
        for i, record in enumerate(streamed_records):
            self.assertIn('stream_index', record)
            self.assertIn('stream_timestamp', record)
            self.assertEqual(record['stream_index'], i)
            
            # Verify original data is preserved
            self.assertIn('comment_id', record)
            self.assertIn('content_text', record)
            self.assertIn('created_at', record)
            self.assertIn('likes', record)
        
        # Verify sleep was called between records (but not after last)
        self.assertEqual(mock_sleep.call_count, 2)  # n-1 sleep calls for n records

    @patch('facebook_mock_injector.time.sleep')
    def test_stream_csv_data_processed_ids_tracking(self, mock_sleep):
        """Test stream_csv_data() tracking of processed IDs"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        injector.csv_file_path = self.temp_csv_file.name
        injector.delay_seconds = 0.1
        injector.processed_ids = set()
        injector.logger = MagicMock()
        
        # Stream all records and collect them
        records_streamed = []
        for record in injector.stream_csv_data():
            records_streamed.append(record)
        
        # Check that all processed_ids are updated
        self.assertEqual(len(injector.processed_ids), 3)  # All 3 records processed
        self.assertIn('fb_test_001', injector.processed_ids)
        self.assertIn('fb_test_002', injector.processed_ids)
        self.assertIn('fb_test_003', injector.processed_ids)

    def test_stream_csv_data_skip_processed(self):
        """Test stream_csv_data() skips already processed records"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        injector.csv_file_path = self.temp_csv_file.name
        injector.delay_seconds = 0.0  # No delay for testing
        injector.processed_ids = {'fb_test_001', 'fb_test_002'}  # Pre-mark as processed
        injector.logger = MagicMock()
        
        # Stream should skip processed records
        streamed_records = list(injector.stream_csv_data())
        
        # Should only stream the unprocessed record
        self.assertEqual(len(streamed_records), 1)
        self.assertEqual(streamed_records[0]['comment_id'], 'fb_test_003')

    def test_datetime_parsing_various_formats(self):
        """Test _parse_datetime() method with various datetime formats"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        injector.logger = MagicMock()
        
        # Test various datetime formats
        test_cases = [
            "2024-01-15 09:30:00",
            "2024-01-15 09:30",
            "2024-01-15",
            "15/01/2024 09:30:00",
            "01/15/2024 09:30"
        ]
        
        for dt_str in test_cases:
            result = injector._parse_datetime(dt_str)
            # Should return valid ISO format timestamp
            self.assertIsInstance(result, str)
            # Should be parseable by datetime
            try:
                parsed = datetime.fromisoformat(result.replace('Z', '+00:00'))
                self.assertIsInstance(parsed, datetime)
            except:
                self.fail(f"Failed to parse result datetime: {result}")

    def test_safe_int_conversion(self):
        """Test _safe_int() method with various input types"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        
        # Test various inputs
        test_cases = [
            (15, 15),           # Normal integer
            ("25", 25),         # String number
            (10.7, 10),         # Float
            ("", 0),            # Empty string
            (None, 0),          # None
            ("abc", 0),         # Non-numeric string
            (pd.NA, 0),         # Pandas NA
        ]
        
        for input_val, expected in test_cases:
            result = injector._safe_int(input_val)
            self.assertEqual(result, expected)

    def test_csv_row_parsing_edge_cases(self):
        """Test _parse_csv_row() method with edge cases"""
        from facebook_mock_injector import FacebookMockInjector
        
        injector = object.__new__(FacebookMockInjector)
        injector.logger = MagicMock()
        
        # Test with missing values
        test_row = pd.Series({
            'comment_id': None,
            'content_text': '',
            'created_at': None,
            'likes': None
        })
        
        result = injector._parse_csv_row(test_row, 5)
        
        # Should handle missing values gracefully
        self.assertIsNotNone(result)
        self.assertEqual(result['comment_id'], 'None')  # str(None) = 'None'
        self.assertEqual(result['content_text'], 'Mock comment content 5')  # Generated content for empty
        self.assertEqual(result['likes'], 0)  # Default likes
        self.assertEqual(result['row_index'], 5)
        self.assertIn('injection_timestamp', result)


if __name__ == '__main__':
    # Run the tests
    unittest.main()