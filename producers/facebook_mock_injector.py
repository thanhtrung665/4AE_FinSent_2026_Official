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

class FacebookMockInjector(BaseKafkaProducer):
    """Facebook Mock Data Injector để giả lập streaming data"""
    
    def __init__(self):
        super().__init__(topic='fb_mock_data')
        
        # Configuration
        self.csv_file_path = os.getenv('FACEBOOK_MOCK_CSV', 'facebook_mock.csv')
        self.delay_seconds = float(os.getenv('FB_MOCK_DELAY_SECONDS', 1.0))
        
        # Validate CSV file exists
        if not os.path.exists(self.csv_file_path):
            # Try relative path from producers directory
            alt_path = os.path.join(os.path.dirname(__file__), self.csv_file_path)
            if os.path.exists(alt_path):
                self.csv_file_path = alt_path
            else:
                self.logger.warning(f"CSV file not found: {self.csv_file_path}")
                self.logger.info("Creating sample CSV file...")
                self._create_sample_csv()
        
        self.logger.info(f"Facebook Mock Injector initialized")
        self.logger.info(f"CSV file: {self.csv_file_path}")
        self.logger.info(f"Streaming delay: {self.delay_seconds} seconds")
        
        # Track processed records to avoid duplicates
        self.processed_ids = set()
    
    def _create_sample_csv(self):
        """Tạo file CSV mẫu nếu không tồn tại"""
        try:
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
            
            df = pd.DataFrame(sample_data)
            df.to_csv(self.csv_file_path, index=False, encoding='utf-8')
            
            self.logger.info(f"Created sample CSV file with {len(sample_data)} records: {self.csv_file_path}")
            
        except Exception as e:
            self.logger.error(f"Error creating sample CSV: {e}")
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
    
    def read_csv_data(self) -> List[Dict[str, Any]]:
        """
        Đọc toàn bộ dữ liệu từ CSV file
        
        Returns:
            List of parsed Facebook mock data
        """
        try:
            self.logger.info(f"Reading CSV data from: {self.csv_file_path}")
            
            # Read CSV with proper encoding
            try:
                df = pd.read_csv(self.csv_file_path, encoding='utf-8')
            except UnicodeDecodeError:
                # Fallback to different encodings
                for encoding in ['utf-8-sig', 'cp1252', 'latin1']:
                    try:
                        df = pd.read_csv(self.csv_file_path, encoding=encoding)
                        self.logger.info(f"Successfully read CSV with {encoding} encoding")
                        break
                    except:
                        continue
                else:
                    raise ValueError("Could not read CSV with any supported encoding")
            
            if df.empty:
                self.logger.warning("CSV file is empty")
                return []
            
            # Convert to list of dictionaries
            mock_data = []
            
            for index, row in df.iterrows():
                try:
                    record = self._parse_csv_row(row, index)
                    if record:
                        mock_data.append(record)
                except Exception as e:
                    self.logger.warning(f"Error parsing row {index}: {e}")
                    continue
            
            self.logger.info(f"Successfully parsed {len(mock_data)}/{len(df)} records from CSV")
            return mock_data
            
        except Exception as e:
            self.logger.error(f"Error reading CSV data: {e}")
            return []
    
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
        Chạy mock data injection với streaming simulation
        
        Args:
            loop_count: Số lần lặp lại CSV (0 = infinite)
            loop_delay_minutes: Delay giữa các loops (phút)
        """
        self.logger.info(f"Starting Facebook Mock Injection")
        self.logger.info(f"Loop count: {loop_count if loop_count > 0 else 'infinite'}")
        self.logger.info(f"Loop delay: {loop_delay_minutes} minutes")
        
        try:
            current_loop = 0
            
            while loop_count == 0 or current_loop < loop_count:
                current_loop += 1
                self.logger.info(f"Starting injection loop {current_loop}")
                
                # Reset processed IDs for new loop
                if current_loop > 1:
                    self.processed_ids.clear()
                
                success_count = 0
                total_count = 0
                
                # Stream data with delays
                for record in self.stream_csv_data():
                    total_count += 1
                    try:
                        # Send to Kafka
                        if self.send_message(record, key=record['comment_id']):
                            success_count += 1
                            self.logger.info(f"Injected record {total_count}: {record['comment_id']}")
                        else:
                            self.logger.warning(f"Failed to inject record: {record['comment_id']}")
                            
                    except Exception as e:
                        self.logger.error(f"Error injecting record {record['comment_id']}: {e}")
                        continue
                
                self.logger.info(f"Loop {current_loop} completed: {success_count}/{total_count} records injected")
                
                # Wait between loops (if not the last loop)
                if loop_count == 0 or current_loop < loop_count:
                    if loop_delay_minutes > 0:
                        self.logger.info(f"Waiting {loop_delay_minutes} minutes before next loop...")
                        time.sleep(loop_delay_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info("Mock injection stopped by user")
        except Exception as e:
            self.logger.error(f"Critical error in mock injection: {e}")
            raise
    
    def inject_once(self) -> Dict[str, int]:
        """
        Chạy injection một lần và return kết quả
        
        Returns:
            Dictionary với kết quả injection
        """
        try:
            self.logger.info("Running single Facebook mock data injection...")
            
            success_count = 0
            total_count = 0
            
            # Stream data with delays
            for record in self.stream_csv_data():
                total_count += 1
                try:
                    # Send to Kafka
                    if self.send_message(record, key=record['comment_id']):
                        success_count += 1
                        self.logger.info(f"Injected record {total_count}: {record['comment_id']}")
                    else:
                        self.logger.warning(f"Failed to inject record: {record['comment_id']}")
                        
                except Exception as e:
                    self.logger.error(f"Error injecting record {record['comment_id']}: {e}")
                    continue
            
            results = {
                'total_records': total_count,
                'successful_injections': success_count,
                'failed_injections': total_count - success_count,
                'csv_file': self.csv_file_path
            }
            
            self.logger.info(f"Single injection complete: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in single mock injection: {e}")
            return {
                'total_records': 0,
                'successful_injections': 0,
                'failed_injections': 0,
                'csv_file': self.csv_file_path
            }
    
    def get_csv_info(self) -> Dict[str, Any]:
        """
        Lấy thông tin về CSV file
        
        Returns:
            Dictionary chứa thông tin CSV
        """
        try:
            if not os.path.exists(self.csv_file_path):
                return {
                    'file_exists': False,
                    'file_path': self.csv_file_path,
                    'error': 'File not found'
                }
            
            # Get file stats
            file_stat = os.stat(self.csv_file_path)
            file_size = file_stat.st_size
            
            # Read CSV info
            df = pd.read_csv(self.csv_file_path, nrows=10)  # Sample first 10 rows
            
            info = {
                'file_exists': True,
                'file_path': self.csv_file_path,
                'file_size_bytes': file_size,
                'columns': list(df.columns),
                'total_rows': len(pd.read_csv(self.csv_file_path)),  # Get actual count
                'sample_data': df.head(3).to_dict('records') if not df.empty else [],
                'delay_seconds': self.delay_seconds
            }
            
            return info
            
        except Exception as e:
            return {
                'file_exists': os.path.exists(self.csv_file_path),
                'file_path': self.csv_file_path,
                'error': str(e)
            }


if __name__ == "__main__":
    # Test Facebook Mock Injector
    with FacebookMockInjector() as injector:
        # Get CSV info
        csv_info = injector.get_csv_info()
        print(f"CSV Info: {csv_info}")
        
        # Validate CSV format
        if injector.validate_csv_format():
            print("✓ CSV format is valid")
            
            # Check Kafka health
            if injector.health_check():
                print("✓ Kafka connection healthy")
                
                # Run single injection
                results = injector.inject_once()
                print(f"Injection results: {results}")
                
                # Uncomment to run continuous injection
                # injector.run_mock_injection(loop_count=2, loop_delay_minutes=1)
            else:
                print("✗ Kafka connection failed")
        else:
            print("✗ CSV format validation failed")