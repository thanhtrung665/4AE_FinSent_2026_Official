"""
Facebook Mock Injector Module - Giả lập streaming data từ CSV file
"""
import pandas as pd
import csv
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Iterator
import time
import os
from kafka_producer_base import BaseKafkaProducer
import logging
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import stat
import chardet
from confluent_kafka import KafkaException

class FacebookMockInjector(BaseKafkaProducer):
    """Facebook Mock Data Injector để giả lập streaming data"""
    
    def __init__(self):
        super().__init__(topic='fb_mock_data')
        
        # Configuration
        self.csv_file_path = os.getenv('FB_MOCK_FILE_PATH', 'facebook_mock.csv')
        self.delay_seconds = float(os.getenv('FB_MOCK_STREAM_DELAY', 1.0))
        
        # Enhanced file validation with proper error handling
        self._validate_and_initialize_csv_file()
        
        self.logger.info(f"Facebook Mock Injector initialized")
        self.logger.info(f"CSV file: {self.csv_file_path}")
        self.logger.info(f"Streaming delay: {self.delay_seconds} seconds")
        
        # Track processed records to avoid duplicates
        self.processed_ids = set()
    
    def _validate_and_initialize_csv_file(self):
        """
        Enhanced CSV file validation with comprehensive error handling
        Uses BaseKafkaProducer error handling patterns
        """
        try:
            # Check if file exists
            if not os.path.exists(self.csv_file_path):
                # Try relative path from producers directory
                alt_path = os.path.join(os.path.dirname(__file__), self.csv_file_path)
                if os.path.exists(alt_path):
                    self.csv_file_path = alt_path
                    self.logger.info(f"Using relative CSV path: {self.csv_file_path}")
                else:
                    self.logger.warning(f"CSV file not found: {self.csv_file_path}")
                    self._handle_missing_csv_file()
                    return
            
            # Check file permissions
            if not self._check_file_permissions():
                return
                
            # Validate file format and encoding
            if not self._validate_csv_encoding_and_format():
                return
                
            self.logger.info(f"CSV file validation successful: {self.csv_file_path}")
            
        except Exception as e:
            self.logger.error(f"Critical error during CSV file initialization: {e}")
            raise
    
    def _check_file_permissions(self) -> bool:
        """
        Check CSV file permissions with detailed error reporting
        
        Returns:
            bool: True if file is accessible, False otherwise
        """
        try:
            file_path = Path(self.csv_file_path)
            
            # Check if file is readable
            if not os.access(self.csv_file_path, os.R_OK):
                self.logger.error(f"CSV file is not readable: {self.csv_file_path}")
                self.logger.error(f"Current user lacks read permissions for file")
                self.logger.error(f"Suggested fix: chmod +r {self.csv_file_path}")
                return False
            
            # Get file stats for detailed logging
            file_stat = os.stat(self.csv_file_path)
            file_mode = stat.filemode(file_stat.st_mode)
            file_size = file_stat.st_size
            
            self.logger.info(f"CSV file permissions check successful")
            self.logger.info(f"File mode: {file_mode}, Size: {file_size} bytes")
            
            # Check if file is empty
            if file_size == 0:
                self.logger.warning(f"CSV file is empty: {self.csv_file_path}")
                self.logger.info("Creating sample CSV file...")
                self._create_sample_csv()
                return True
            
            return True
            
        except PermissionError as e:
            self.logger.error(f"Permission denied accessing CSV file: {e}")
            self.logger.error(f"Ensure proper read permissions for: {self.csv_file_path}")
            return False
        except FileNotFoundError as e:
            self.logger.error(f"CSV file not found during permission check: {e}")
            return False
        except OSError as e:
            self.logger.error(f"OS error checking file permissions: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error checking file permissions: {e}")
            return False
    
    def _detect_file_encoding(self) -> str:
        """
        Detect CSV file encoding using multiple methods
        
        Returns:
            str: Detected encoding or default 'utf-8'
        """
        try:
            # Method 1: Use chardet for detection
            with open(self.csv_file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB for detection
                
            if raw_data:
                detected = chardet.detect(raw_data)
                if detected and detected.get('confidence', 0) > 0.7:
                    encoding = detected['encoding']
                    self.logger.info(f"Detected encoding: {encoding} (confidence: {detected['confidence']:.2f})")
                    return encoding
            
            # Method 2: Try common encodings
            test_encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin1', 'iso-8859-1']
            
            for encoding in test_encodings:
                try:
                    with open(self.csv_file_path, 'r', encoding=encoding) as f:
                        f.read(1000)  # Try to read first 1KB
                    self.logger.info(f"Successfully validated encoding: {encoding}")
                    return encoding
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    self.logger.warning(f"Error testing encoding {encoding}: {e}")
                    continue
            
            # Fallback to utf-8
            self.logger.warning("Could not detect file encoding, defaulting to utf-8")
            return 'utf-8'
            
        except Exception as e:
            self.logger.error(f"Error detecting file encoding: {e}")
            return 'utf-8'
    
    def _validate_csv_encoding_and_format(self) -> bool:
        """
        Validate CSV file encoding and basic format
        
        Returns:
            bool: True if validation successful, False otherwise
        """
        try:
            # Detect and validate encoding
            detected_encoding = self._detect_file_encoding()
            
            # Try to read CSV with detected encoding
            try:
                df = pd.read_csv(self.csv_file_path, encoding=detected_encoding, nrows=5)
                self.logger.info(f"Successfully read CSV with {detected_encoding} encoding")
            except UnicodeDecodeError as e:
                self.logger.error(f"Encoding validation failed for {detected_encoding}: {e}")
                
                # Try fallback encodings
                fallback_encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin1']
                for encoding in fallback_encodings:
                    if encoding != detected_encoding:
                        try:
                            df = pd.read_csv(self.csv_file_path, encoding=encoding, nrows=5)
                            self.logger.info(f"Fallback encoding successful: {encoding}")
                            break
                        except UnicodeDecodeError:
                            continue
                else:
                    self.logger.error("All encoding attempts failed")
                    return False
            
            # Validate required columns
            required_columns = ['comment_id', 'content_text', 'created_at', 'likes']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self.logger.error(f"CSV file missing required columns: {missing_columns}")
                self.logger.error(f"Available columns: {list(df.columns)}")
                self.logger.error(f"Expected columns: {required_columns}")
                return False
                
            self.logger.info(f"CSV format validation successful. Columns: {list(df.columns)}")
            return True
            
        except pd.errors.EmptyDataError:
            self.logger.error("CSV file is empty or has no data")
            return False
        except pd.errors.ParserError as e:
            self.logger.error(f"CSV parsing error: {e}")
            self.logger.error("Check CSV file format and structure")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error validating CSV format: {e}")
            return False
    
    def _handle_missing_csv_file(self):
        """
        Handle missing CSV file with appropriate fallback actions
        """
        try:
            # Check if directory is writable
            csv_dir = os.path.dirname(os.path.abspath(self.csv_file_path))
            
            if not os.path.exists(csv_dir):
                self.logger.info(f"Creating directory: {csv_dir}")
                os.makedirs(csv_dir, exist_ok=True)
            
            if not os.access(csv_dir, os.W_OK):
                self.logger.error(f"Directory is not writable: {csv_dir}")
                self.logger.error(f"Cannot create sample CSV file")
                raise PermissionError(f"No write permission for directory: {csv_dir}")
            
            self.logger.info("Creating sample CSV file...")
            self._create_sample_csv()
            
        except Exception as e:
            self.logger.error(f"Failed to handle missing CSV file: {e}")
            raise
    
    def _create_sample_csv(self):
        """
        Create sample CSV file with comprehensive error handling
        Uses BaseKafkaProducer error patterns for consistency
        """
        try:
            self.logger.info(f"Creating sample CSV file at: {self.csv_file_path}")
            
            sample_data = [
                {
                    'comment_id': 'fb_001',
                    'content_text': 'Thị trường hôm nay khá tích cực, VN-Index tăng mạnh!',
                    'created_at': '2024-01-15 09:30:00',
                    'likes': 15
                },
                {
                    'comment_id': 'fb_002', 
                    'content_text': 'Cổ phiếu ngân hàng đang có xu hướng tăng trở lại.',
                    'created_at': '2024-01-15 10:15:00',
                    'likes': 8
                },
                {
                    'comment_id': 'fb_003',
                    'content_text': 'Nên mua vào lúc này hay chờ thêm? Thị trường biến động khá nhiều.',
                    'created_at': '2024-01-15 11:00:00', 
                    'likes': 23
                },
                {
                    'comment_id': 'fb_004',
                    'content_text': 'VN30 đã vượt qua ngưỡng kháng cự quan trọng!',
                    'created_at': '2024-01-15 11:45:00',
                    'likes': 31
                },
                {
                    'comment_id': 'fb_005',
                    'content_text': 'Dự báo thị trường tuần tới sẽ tiếp tục tích cực.',
                    'created_at': '2024-01-15 14:20:00',
                    'likes': 12
                },
                {
                    'comment_id': 'fb_006',
                    'content_text': 'Cần thận trọng với nhóm cổ phiếu bất động sản.',
                    'created_at': '2024-01-15 15:10:00',
                    'likes': 7
                },
                {
                    'comment_id': 'fb_007',
                    'content_text': 'Khối ngoại đang mua ròng mạnh, tín hiệu tích cực!',
                    'created_at': '2024-01-15 16:00:00',
                    'likes': 45
                },
                {
                    'comment_id': 'fb_008',
                    'content_text': 'Phiên chiều khả năng sẽ có điều chỉnh nhẹ.',
                    'created_at': '2024-01-15 16:30:00',
                    'likes': 9
                }
            ]
            
            # Create DataFrame and save with explicit encoding
            df = pd.DataFrame(sample_data)
            
            # Try to save with UTF-8 encoding
            try:
                df.to_csv(self.csv_file_path, index=False, encoding='utf-8')
                self.logger.info(f"Sample CSV created successfully with UTF-8 encoding")
            except UnicodeEncodeError as e:
                self.logger.warning(f"UTF-8 encoding failed: {e}, trying UTF-8 with BOM")
                df.to_csv(self.csv_file_path, index=False, encoding='utf-8-sig')
                self.logger.info(f"Sample CSV created with UTF-8-BOM encoding")
            
            # Verify the created file
            if os.path.exists(self.csv_file_path):
                file_size = os.path.getsize(self.csv_file_path)
                self.logger.info(f"Sample CSV file created: {len(sample_data)} records, {file_size} bytes")
                
                # Validate the created file
                if self.validate_csv_format():
                    self.logger.info("Sample CSV file validation successful")
                else:
                    self.logger.error("Sample CSV file validation failed")
                    raise ValueError("Created sample CSV file is invalid")
            else:
                raise FileNotFoundError("Sample CSV file was not created successfully")
            
        except PermissionError as e:
            self.logger.error(f"Permission denied creating sample CSV: {e}")
            self.logger.error(f"Check write permissions for directory: {os.path.dirname(self.csv_file_path)}")
            raise
        except OSError as e:
            self.logger.error(f"OS error creating sample CSV: {e}")
            self.logger.error(f"Check disk space and directory permissions")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error creating sample CSV: {e}")
            raise
    
    def validate_csv_format(self) -> bool:
        """
        Kiểm tra format của CSV file
        
        Returns:
            True if CSV format is valid
        """
        try:
            required_columns = ['comment_id', 'content_text', 'created_at', 'likes']
            
            # Read first few rows to check format
            df = pd.read_csv(self.csv_file_path, nrows=5)
            
            # Check if required columns exist
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self.logger.error(f"CSV file missing required columns: {missing_columns}")
                self.logger.error(f"Available columns: {list(df.columns)}")
                return False
            
            # Check if file has data
            if len(df) == 0:
                self.logger.error("CSV file is empty")
                return False
                
            self.logger.info(f"CSV format validation successful. Columns: {list(df.columns)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating CSV format: {e}")
            return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type((OSError, PermissionError, pd.errors.EmptyDataError))
    )
    def read_csv_data(self) -> List[Dict[str, Any]]:
        """
        Read CSV data with enhanced error handling using BaseKafkaProducer patterns
        
        Returns:
            List of parsed Facebook mock data
        """
        try:
            self.logger.info(f"Reading CSV data from: {self.csv_file_path}")
            
            # Pre-flight checks
            if not os.path.exists(self.csv_file_path):
                self.logger.error(f"CSV file not found: {self.csv_file_path}")
                raise FileNotFoundError(f"CSV file not found: {self.csv_file_path}")
            
            if not os.access(self.csv_file_path, os.R_OK):
                self.logger.error(f"No read permission for CSV file: {self.csv_file_path}")
                raise PermissionError(f"No read permission for CSV file: {self.csv_file_path}")
            
            # Detect encoding with fallback mechanism
            detected_encoding = self._detect_file_encoding()
            
            # Try reading with detected encoding first
            df = self._read_csv_with_encoding(detected_encoding)
            
            if df.empty:
                self.logger.warning("CSV file is empty")
                return []
            
            # Convert to list of dictionaries with error handling
            mock_data = []
            parse_errors = 0
            
            for index, row in df.iterrows():
                try:
                    record = self._parse_csv_row(row, index)
                    if record:
                        mock_data.append(record)
                except Exception as e:
                    parse_errors += 1
                    self.logger.warning(f"Error parsing row {index}: {e}")
                    # Continue processing other rows
                    continue
            
            success_rate = (len(mock_data) / len(df)) * 100 if len(df) > 0 else 0
            
            self.logger.info(f"CSV parsing completed: {len(mock_data)}/{len(df)} records parsed successfully")
            if parse_errors > 0:
                self.logger.warning(f"Parse errors: {parse_errors}, Success rate: {success_rate:.1f}%")
            
            if len(mock_data) == 0 and len(df) > 0:
                self.logger.error("No valid records could be parsed from CSV file")
                raise ValueError("No valid records could be parsed from CSV file")
            
            return mock_data
            
        except FileNotFoundError as e:
            self.logger.error(f"CSV file not found: {e}")
            raise
        except PermissionError as e:
            self.logger.error(f"Permission error reading CSV: {e}")
            raise
        except pd.errors.EmptyDataError as e:
            self.logger.error(f"CSV file is empty: {e}")
            raise
        except pd.errors.ParserError as e:
            self.logger.error(f"CSV parsing error: {e}")
            self.logger.error("Check CSV file format, delimiters, and structure")
            raise
        except UnicodeDecodeError as e:
            self.logger.error(f"Encoding error reading CSV: {e}")
            self.logger.error("Try specifying a different encoding in FB_MOCK_FILE_ENCODING environment variable")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error reading CSV data: {e}")
            raise
    
    def _read_csv_with_encoding(self, encoding: str) -> pd.DataFrame:
        """
        Read CSV with specific encoding and fallback logic
        
        Args:
            encoding: Primary encoding to try
            
        Returns:
            pandas.DataFrame: Parsed CSV data
        """
        fallback_encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin1', 'iso-8859-1']
        
        # Try primary encoding first
        try:
            df = pd.read_csv(self.csv_file_path, encoding=encoding)
            self.logger.info(f"Successfully read CSV with {encoding} encoding")
            return df
        except UnicodeDecodeError as e:
            self.logger.warning(f"Primary encoding {encoding} failed: {e}")
            
        # Try fallback encodings
        for fallback_encoding in fallback_encodings:
            if fallback_encoding != encoding:  # Skip if already tried
                try:
                    df = pd.read_csv(self.csv_file_path, encoding=fallback_encoding)
                    self.logger.info(f"Fallback encoding successful: {fallback_encoding}")
                    return df
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    self.logger.warning(f"Error with encoding {fallback_encoding}: {e}")
                    continue
        
        # If all encodings fail, raise the original error
        self.logger.error("All encoding attempts failed")
        raise UnicodeDecodeError(
            f"Could not decode CSV file with any of the attempted encodings: {[encoding] + fallback_encodings}"
        )
    
    def _parse_csv_row(self, row: pd.Series, row_index: int) -> Optional[Dict[str, Any]]:
        """
        Parse một row CSV thành format JSON
        
        Args:
            row: Pandas Series row
            row_index: Index của row
            
        Returns:
            Parsed Facebook mock data dictionary
        """
        try:
            # Extract comment_id
            comment_id = str(row.get('comment_id', f'fb_{row_index:06d}'))
            
            # Extract content_text
            content_text = str(row.get('content_text', '')).strip()
            if not content_text or content_text.lower() in ['nan', 'none', '']:
                content_text = f"Mock comment content {row_index}"
            
            # Parse created_at
            created_at = self._parse_datetime(row.get('created_at'))
            
            # Extract likes (safe conversion to int)
            likes = self._safe_int(row.get('likes', 0))
            
            # Create mock data record
            mock_record = {
                "comment_id": comment_id,
                "content_text": content_text,
                "created_at": created_at,
                "likes": likes,
                "row_index": row_index,
                "injection_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            return mock_record
            
        except Exception as e:
            self.logger.warning(f"Error parsing CSV row {row_index}: {e}")
            return None
    
    def _parse_datetime(self, dt_value) -> str:
        """Parse datetime từ CSV"""
        try:
            if pd.isna(dt_value) or dt_value is None:
                return datetime.now(timezone.utc).isoformat()
            
            dt_str = str(dt_value).strip()
            
            # Try different datetime formats
            datetime_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%d',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M',
                '%d/%m/%Y',
                '%m/%d/%Y %H:%M:%S',
                '%m/%d/%Y %H:%M',
                '%m/%d/%Y'
            ]
            
            for fmt in datetime_formats:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # Try pandas parsing as fallback
            try:
                dt = pd.to_datetime(dt_str)
                if dt.tzinfo is None:
                    dt = dt.tz_localize(timezone.utc)
                return dt.isoformat()
            except:
                pass
            
            # Default to current time if parsing fails
            self.logger.warning(f"Could not parse datetime: {dt_value}")
            return datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            self.logger.warning(f"Error parsing datetime {dt_value}: {e}")
            return datetime.now(timezone.utc).isoformat()
    
    def _safe_int(self, value, default=0) -> int:
        """Safely convert value to int"""
        try:
            if pd.isna(value) or value is None:
                return default
            return int(float(str(value)))
        except (ValueError, TypeError):
            return default
    
    def stream_csv_data(self) -> Iterator[Dict[str, Any]]:
        """
        Generator để stream dữ liệu từ CSV với delay
        
        Yields:
            Facebook mock data records one by one
        """
        try:
            self.logger.info("Starting CSV data streaming...")
            
            # Read all data first
            all_data = self.read_csv_data()
            
            if not all_data:
                self.logger.warning("No data to stream")
                return
            
            self.logger.info(f"Starting to stream {len(all_data)} records with {self.delay_seconds}s delay")
            
            for i, record in enumerate(all_data):
                # Check if already processed (for resume functionality)
                if record['comment_id'] in self.processed_ids:
                    self.logger.debug(f"Skipping already processed record: {record['comment_id']}")
                    continue
                
                # Add streaming metadata
                record['stream_index'] = i
                record['stream_timestamp'] = datetime.now(timezone.utc).isoformat()
                
                yield record
                
                # Mark as processed
                self.processed_ids.add(record['comment_id'])
                
                # Add delay between records (except for last one)
                if i < len(all_data) - 1:
                    self.logger.debug(f"Streaming record {i+1}/{len(all_data)}, waiting {self.delay_seconds}s...")
                    time.sleep(self.delay_seconds)
            
            self.logger.info("CSV data streaming completed")
            
        except Exception as e:
            self.logger.error(f"Error in CSV data streaming: {e}")
            return
    
    def run_mock_injection(self, loop_count: int = 1, loop_delay_minutes: int = 10) -> None:
        """
        Run mock data injection with enhanced error handling using BaseKafkaProducer patterns
        
        Args:
            loop_count: Number of loops to run CSV (0 = infinite)
            loop_delay_minutes: Delay between loops (minutes)
        """
        self.logger.info(f"Starting Facebook Mock Injection")
        self.logger.info(f"Loop count: {loop_count if loop_count > 0 else 'infinite'}")
        self.logger.info(f"Loop delay: {loop_delay_minutes} minutes")
        
        current_loop = 0
        total_errors = 0
        max_consecutive_errors = 10  # Prevent infinite error loops
        consecutive_errors = 0
        
        try:
            while loop_count == 0 or current_loop < loop_count:
                current_loop += 1
                loop_start_time = time.time()
                
                try:
                    self.logger.info(f"Starting injection loop {current_loop}")
                    
                    # Reset processed IDs for new loop
                    if current_loop > 1:
                        self.processed_ids.clear()
                    
                    # Pre-loop validation using BaseKafkaProducer health check
                    if not self.health_check():
                        self.logger.error("Kafka health check failed, skipping loop")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            self.logger.error("Too many consecutive errors, stopping injection")
                            break
                        continue
                    
                    success_count = 0
                    total_count = 0
                    loop_errors = 0
                    
                    # Stream data with enhanced error handling
                    try:
                        for record in self.stream_csv_data():
                            total_count += 1
                            try:
                                # Send to Kafka using BaseKafkaProducer retry logic
                                if self.send_message(record, key=record['comment_id']):
                                    success_count += 1
                                    consecutive_errors = 0  # Reset consecutive error count on success
                                    self.logger.debug(f"Injected record {total_count}: {record['comment_id']}")
                                else:
                                    loop_errors += 1
                                    self.logger.warning(f"Failed to inject record: {record['comment_id']}")
                                    
                            except KafkaException as e:
                                loop_errors += 1
                                total_errors += 1
                                self.logger.error(f"Kafka error injecting record {record['comment_id']}: {e}")
                                # BaseKafkaProducer's retry logic will handle this
                                continue
                            except Exception as e:
                                loop_errors += 1
                                total_errors += 1
                                self.logger.error(f"Unexpected error injecting record {record['comment_id']}: {e}")
                                continue
                        
                        # Loop completion summary with consistent logging format
                        loop_duration = time.time() - loop_start_time
                        success_rate = (success_count / total_count * 100) if total_count > 0 else 0
                        
                        self.logger.info(f"Loop {current_loop} completed in {loop_duration:.2f}s")
                        self.logger.info(f"Records processed: {success_count}/{total_count} ({success_rate:.1f}% success)")
                        
                        if loop_errors > 0:
                            self.logger.warning(f"Loop errors: {loop_errors}")
                            
                        consecutive_errors = 0  # Reset on successful loop completion
                        
                    except Exception as e:
                        loop_errors += 1
                        consecutive_errors += 1
                        total_errors += 1
                        self.logger.error(f"Critical error in loop {current_loop} data streaming: {e}")
                        
                        # Check if we should continue or stop
                        if consecutive_errors >= max_consecutive_errors:
                            self.logger.error("Too many consecutive streaming errors, stopping injection")
                            break
                
                except Exception as e:
                    consecutive_errors += 1
                    total_errors += 1
                    self.logger.error(f"Critical error in loop {current_loop}: {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error("Too many consecutive loop errors, stopping injection")
                        break
                
                # Inter-loop delay and health check
                if loop_count == 0 or current_loop < loop_count:
                    if loop_delay_minutes > 0:
                        self.logger.info(f"Waiting {loop_delay_minutes} minutes before next loop...")
                        
                        # Sleep in smaller chunks to allow for interruption
                        sleep_chunks = max(1, loop_delay_minutes)  # At least 1 minute chunks
                        chunk_duration = (loop_delay_minutes * 60) / sleep_chunks
                        
                        for chunk in range(sleep_chunks):
                            time.sleep(chunk_duration)
                            # Periodic health check during long delays
                            if chunk % 5 == 4:  # Every 5 minutes
                                if not self.health_check():
                                    self.logger.warning("Kafka health check failed during delay period")
            
            # Final summary with consistent logging format
            self.logger.info(f"Mock injection completed after {current_loop} loops")
            if total_errors > 0:
                self.logger.warning(f"Total errors encountered: {total_errors}")
            else:
                self.logger.info("Mock injection completed successfully with no errors")
                
        except KeyboardInterrupt:
            self.logger.info("Mock injection stopped by user (KeyboardInterrupt)")
        except Exception as e:
            self.logger.error(f"Critical error in mock injection: {e}")
            raise
    
    def inject_once(self) -> Dict[str, int]:
        """
        Run single injection with enhanced error handling using BaseKafkaProducer patterns
        
        Returns:
            Dictionary with injection results and detailed error information
        """
        start_time = time.time()
        
        try:
            self.logger.info("Running single Facebook mock data injection...")
            
            # Pre-injection health check using BaseKafkaProducer
            if not self.health_check():
                self.logger.error("Kafka health check failed before injection")
                return {
                    'total_records': 0,
                    'successful_injections': 0,
                    'failed_injections': 0,
                    'csv_file': self.csv_file_path,
                    'error': 'Kafka health check failed',
                    'duration_seconds': 0
                }
            
            success_count = 0
            total_count = 0
            kafka_errors = 0
            parse_errors = 0
            other_errors = 0
            
            # Stream data with comprehensive error tracking
            try:
                for record in self.stream_csv_data():
                    total_count += 1
                    try:
                        # Send to Kafka using BaseKafkaProducer retry logic
                        if self.send_message(record, key=record['comment_id']):
                            success_count += 1
                            self.logger.debug(f"Injected record {total_count}: {record['comment_id']}")
                        else:
                            kafka_errors += 1
                            self.logger.warning(f"Failed to inject record: {record['comment_id']}")
                            
                    except KafkaException as e:
                        kafka_errors += 1
                        self.logger.error(f"Kafka error injecting record {record['comment_id']}: {e}")
                        # BaseKafkaProducer's retry logic handles this automatically
                        continue
                    except ValueError as e:
                        parse_errors += 1
                        self.logger.error(f"Data parsing error for record {record['comment_id']}: {e}")
                        continue
                    except Exception as e:
                        other_errors += 1
                        self.logger.error(f"Unexpected error injecting record {record['comment_id']}: {e}")
                        continue
                
            except Exception as e:
                self.logger.error(f"Critical error during data streaming: {e}")
                parse_errors += 1
            
            # Calculate results with detailed breakdown
            duration = time.time() - start_time
            failed_injections = total_count - success_count
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            results = {
                'total_records': total_count,
                'successful_injections': success_count,
                'failed_injections': failed_injections,
                'kafka_errors': kafka_errors,
                'parse_errors': parse_errors,
                'other_errors': other_errors,
                'success_rate_percent': round(success_rate, 2),
                'csv_file': self.csv_file_path,
                'duration_seconds': round(duration, 2)
            }
            
            # Consistent logging format matching BaseKafkaProducer patterns
            self.logger.info(f"Single injection completed in {duration:.2f}s")
            self.logger.info(f"Results: {success_count}/{total_count} records injected ({success_rate:.1f}% success)")
            
            if failed_injections > 0:
                self.logger.warning(f"Error breakdown - Kafka: {kafka_errors}, Parse: {parse_errors}, Other: {other_errors}")
            
            return results
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Critical error in single mock injection: {e}")
            return {
                'total_records': 0,
                'successful_injections': 0,
                'failed_injections': 0,
                'kafka_errors': 0,
                'parse_errors': 0,
                'other_errors': 1,
                'success_rate_percent': 0.0,
                'csv_file': self.csv_file_path,
                'error': str(e),
                'duration_seconds': round(duration, 2)
            }
    
    def get_csv_info(self) -> Dict[str, Any]:
        """
        Get comprehensive CSV file information with enhanced error handling
        
        Returns:
            Dictionary containing detailed CSV file information and diagnostics
        """
        start_time = time.time()
        
        try:
            info = {
                'file_path': self.csv_file_path,
                'file_exists': False,
                'readable': False,
                'file_size_bytes': 0,
                'columns': [],
                'total_rows': 0,
                'sample_data': [],
                'delay_seconds': self.delay_seconds,
                'encoding_detected': None,
                'file_permissions': None,
                'last_modified': None,
                'validation_passed': False,
                'check_duration_seconds': 0
            }
            
            # Check file existence
            if not os.path.exists(self.csv_file_path):
                info.update({
                    'error': 'File not found',
                    'suggestions': [
                        f'Create file at: {self.csv_file_path}',
                        'Run injector to auto-create sample file',
                        'Check FB_MOCK_FILE_PATH environment variable'
                    ]
                })
                return info
            
            info['file_exists'] = True
            
            # Get file system information
            try:
                file_stat = os.stat(self.csv_file_path)
                info.update({
                    'file_size_bytes': file_stat.st_size,
                    'last_modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    'file_permissions': stat.filemode(file_stat.st_mode)
                })
                
                # Check readability
                info['readable'] = os.access(self.csv_file_path, os.R_OK)
                
            except (OSError, PermissionError) as e:
                self.logger.error(f"Error getting file stats: {e}")
                info['error'] = f'File system error: {e}'
                return info
            
            # Check if file is empty
            if info['file_size_bytes'] == 0:
                info.update({
                    'error': 'File is empty',
                    'suggestions': ['Add data to CSV file', 'Run injector to create sample data']
                })
                return info
            
            # Check file permissions
            if not info['readable']:
                info.update({
                    'error': 'No read permission',
                    'suggestions': [f'Fix permissions: chmod +r {self.csv_file_path}']
                })
                return info
            
            # Detect encoding
            try:
                info['encoding_detected'] = self._detect_file_encoding()
            except Exception as e:
                self.logger.warning(f"Encoding detection failed: {e}")
                info['encoding_detected'] = 'unknown'
            
            # Try reading CSV content
            try:
                # Read sample for column information
                df_sample = pd.read_csv(self.csv_file_path, nrows=10, encoding=info['encoding_detected'])
                info['columns'] = list(df_sample.columns)
                info['sample_data'] = df_sample.head(3).to_dict('records') if not df_sample.empty else []
                
                # Get total row count efficiently
                try:
                    with open(self.csv_file_path, 'r', encoding=info['encoding_detected']) as f:
                        info['total_rows'] = sum(1 for _ in f) - 1  # Subtract header row
                except Exception:
                    # Fallback to pandas if direct count fails
                    df_full = pd.read_csv(self.csv_file_path, encoding=info['encoding_detected'])
                    info['total_rows'] = len(df_full)
                
                # Validate format
                info['validation_passed'] = self.validate_csv_format()
                
                if info['validation_passed']:
                    info['status'] = 'Ready for injection'
                else:
                    required_columns = ['comment_id', 'content_text', 'created_at', 'likes']
                    missing_columns = [col for col in required_columns if col not in info['columns']]
                    info.update({
                        'error': 'Format validation failed',
                        'missing_columns': missing_columns,
                        'suggestions': [
                            f'Ensure CSV has required columns: {required_columns}',
                            'Check column names for typos',
                            'Verify CSV format and structure'
                        ]
                    })
                
            except pd.errors.EmptyDataError:
                info.update({
                    'error': 'CSV file contains no data',
                    'suggestions': ['Add data rows to CSV file']
                })
            except pd.errors.ParserError as e:
                info.update({
                    'error': f'CSV parsing error: {e}',
                    'suggestions': [
                        'Check CSV format and delimiters',
                        'Ensure consistent number of columns',
                        'Remove any malformed rows'
                    ]
                })
            except UnicodeDecodeError as e:
                info.update({
                    'error': f'Encoding error: {e}',
                    'suggestions': [
                        'Try saving CSV with UTF-8 encoding',
                        'Check file encoding and convert if necessary'
                    ]
                })
            except Exception as e:
                info.update({
                    'error': f'Unexpected error reading CSV: {e}',
                    'suggestions': ['Check file format and accessibility']
                })
            
            # Calculate check duration
            info['check_duration_seconds'] = round(time.time() - start_time, 3)
            
            return info
            
        except Exception as e:
            self.logger.error(f"Critical error getting CSV info: {e}")
            return {
                'file_path': self.csv_file_path,
                'file_exists': os.path.exists(self.csv_file_path) if self.csv_file_path else False,
                'error': f'Critical error: {e}',
                'check_duration_seconds': round(time.time() - start_time, 3)
            }


if __name__ == "__main__":
    # Test Facebook Mock Injector with enhanced error handling
    try:
        with FacebookMockInjector() as injector:
            print("=== Facebook Mock Injector Enhanced Error Handling Test ===")
            
            # Get comprehensive CSV info
            print("\n1. Getting CSV file information...")
            csv_info = injector.get_csv_info()
            print(f"CSV Info: {csv_info}")
            
            # Validate CSV format with detailed error reporting
            print("\n2. Validating CSV format...")
            if injector.validate_csv_format():
                print("✓ CSV format validation passed")
                
                # Check Kafka health using BaseKafkaProducer
                print("\n3. Checking Kafka connectivity...")
                if injector.health_check():
                    print("✓ Kafka connection healthy")
                    
                    # Run single injection with comprehensive error tracking
                    print("\n4. Running single injection test...")
                    results = injector.inject_once()
                    print(f"Injection results: {results}")
                    
                    if results['successful_injections'] > 0:
                        print("✓ Mock data injection test successful")
                    else:
                        print("✗ No records were successfully injected")
                        if 'error' in results:
                            print(f"Error: {results['error']}")
                    
                    # Uncomment to test continuous injection with error resilience
                    # print("\n5. Testing continuous injection (2 loops)...")
                    # injector.run_mock_injection(loop_count=2, loop_delay_minutes=1)
                    
                else:
                    print("✗ Kafka connection failed")
                    print("Check AWS_KAFKA_BROKER configuration and network connectivity")
                    
            else:
                print("✗ CSV format validation failed")
                print("Check CSV file structure and required columns")
                
    except FileNotFoundError as e:
        print(f"✗ File system error: {e}")
    except PermissionError as e:
        print(f"✗ Permission error: {e}")
    except Exception as e:
        print(f"✗ Critical error: {e}")
        import traceback
        traceback.print_exc()