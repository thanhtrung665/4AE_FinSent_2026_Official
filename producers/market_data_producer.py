"""
Market Data Producer Module - Thu thập dữ liệu VNINDEX và VN30 từ vnstock
"""
import vnstock as stock
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import time
import os
from kafka_producer_base import BaseKafkaProducer
import logging
import numpy as np

class MarketDataProducer(BaseKafkaProducer):
    """Market Data Producer cho dữ liệu chứng khoán Việt Nam"""
    
    def __init__(self):
        super().__init__(topic='market_stock_data')
        
        # Configuration
        self.interval_seconds = int(os.getenv('MARKET_DATA_INTERVAL_SECONDS', 60))
        self.tickers = os.getenv('VNSTOCK_TICKERS', 'VNINDEX,VN30').split(',')
        self.tickers = [ticker.strip() for ticker in self.tickers]
        
        # Validate tickers
        if not self.tickers:
            raise ValueError("No tickers specified in VNSTOCK_TICKERS")
        
        self.logger.info(f"Market Data Producer initialized for tickers: {self.tickers}")
        self.logger.info(f"Data collection interval: {self.interval_seconds} seconds")
        
        # Cache for previous data to detect changes
        self.previous_data = {}
    
    def get_market_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Lấy dữ liệu thị trường cho một ticker
        
        Args:
            ticker: Mã chứng khoán (VNINDEX, VN30, etc.)
            
        Returns:
            Dictionary chứa dữ liệu market hoặc None nếu lỗi
        """
        try:
            self.logger.info(f"Fetching market data for {ticker}")
            
            # Get current date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)  # Get last 5 days to ensure we have data
            
            # Format dates for vnstock
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            # Fetch data based on ticker type
            if ticker.upper() in ['VNINDEX', 'VN30', 'HNX', 'UPCOM']:
                # Index data
                df = stock.stock_historical_data(
                    symbol=ticker.upper(),
                    start_date=start_str,
                    end_date=end_str,
                    resolution='1D',
                    type='index'
                )
            else:
                # Individual stock data
                df = stock.stock_historical_data(
                    symbol=ticker.upper(),
                    start_date=start_str,
                    end_date=end_str,
                    resolution='1D',
                    type='stock'
                )
            
            if df is None or df.empty:
                self.logger.warning(f"No data returned for {ticker}")
                return None
            
            # Get the most recent record
            df = df.sort_values('time', ascending=False)
            latest_record = df.iloc[0]
            
            # Parse the data
            market_data = self._parse_market_record(latest_record, ticker)
            
            if market_data:
                self.logger.info(f"Successfully fetched data for {ticker}: Close={market_data.get('close')}")
            
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error fetching market data for {ticker}: {e}")
            return None
    
    def _parse_market_record(self, record, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Parse dữ liệu từ vnstock record thành format JSON
        
        Args:
            record: Pandas Series từ vnstock
            ticker: Ticker symbol
            
        Returns:
            Parsed market data dictionary
        """
        try:
            # Handle timestamp
            timestamp = record.get('time')
            if timestamp is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    try:
                        timestamp = pd.to_datetime(timestamp).to_pydatetime()
                        if timestamp.tzinfo is None:
                            timestamp = timestamp.replace(tzinfo=timezone.utc)
                    except:
                        timestamp = datetime.now(timezone.utc)
            elif isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.to_pydatetime()
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            
            # Extract OHLCV data with multiple possible column names
            def safe_float(value, default=0.0):
                """Safely convert value to float"""
                if pd.isna(value) or value is None:
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            def safe_int(value, default=0):
                """Safely convert value to int"""
                if pd.isna(value) or value is None:
                    return default
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return default
            
            # Try different column name variations
            open_price = None
            high_price = None
            low_price = None
            close_price = None
            volume = None
            
            # Common column names
            for col in ['open', 'Open', 'OPEN']:
                if col in record.index and open_price is None:
                    open_price = safe_float(record.get(col))
                    
            for col in ['high', 'High', 'HIGH']:
                if col in record.index and high_price is None:
                    high_price = safe_float(record.get(col))
                    
            for col in ['low', 'Low', 'LOW']:
                if col in record.index and low_price is None:
                    low_price = safe_float(record.get(col))
                    
            for col in ['close', 'Close', 'CLOSE']:
                if col in record.index and close_price is None:
                    close_price = safe_float(record.get(col))
                    
            for col in ['volume', 'Volume', 'VOLUME', 'vol']:
                if col in record.index and volume is None:
                    volume = safe_int(record.get(col))
            
            # Fallback values if not found
            if open_price is None:
                open_price = close_price or 0.0
            if high_price is None:
                high_price = max(open_price or 0, close_price or 0)
            if low_price is None:
                low_price = min(open_price or 0, close_price or 0)
            if close_price is None:
                close_price = open_price or 0.0
            if volume is None:
                volume = 0
            
            market_data = {
                "ticker": ticker.upper(),
                "timestamp": timestamp.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "data_source": "vnstock",
                "collection_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Validate data
            if close_price <= 0:
                self.logger.warning(f"Invalid close price for {ticker}: {close_price}")
                return None
                
            return market_data
            
        except Exception as e:
            self.logger.error(f"Error parsing market record for {ticker}: {e}")
            return None
    
    def get_real_time_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Lấy dữ liệu real-time cho ticker (fallback method)
        
        Args:
            ticker: Ticker symbol
            
        Returns:
            Real-time market data
        """
        try:
            # Try to get intraday data if available
            df = stock.stock_intraday_data(symbol=ticker.upper())
            
            if df is not None and not df.empty:
                df = df.sort_values('time', ascending=False)
                latest_record = df.iloc[0]
                return self._parse_market_record(latest_record, ticker)
            else:
                # Fallback to historical data
                return self.get_market_data(ticker)
                
        except Exception as e:
            self.logger.warning(f"Real-time data not available for {ticker}, falling back to historical: {e}")
            return self.get_market_data(ticker)
    
    def collect_all_tickers(self) -> List[Dict[str, Any]]:
        """
        Thu thập dữ liệu cho tất cả tickers
        
        Returns:
            List of market data for all tickers
        """
        market_data_list = []
        
        for ticker in self.tickers:
            try:
                # Try real-time first, then fallback to historical
                data = self.get_real_time_data(ticker)
                
                if data:
                    # Check if data has changed from previous collection
                    previous = self.previous_data.get(ticker)
                    if previous is None or self._has_data_changed(previous, data):
                        market_data_list.append(data)
                        self.previous_data[ticker] = data
                        self.logger.info(f"New data collected for {ticker}")
                    else:
                        self.logger.info(f"No change in data for {ticker}, skipping")
                else:
                    self.logger.warning(f"Failed to collect data for {ticker}")
                    
            except Exception as e:
                self.logger.error(f"Error collecting data for {ticker}: {e}")
                continue
        
        return market_data_list
    
    def _has_data_changed(self, previous: Dict[str, Any], current: Dict[str, Any]) -> bool:
        """
        Kiểm tra xem dữ liệu có thay đổi so với lần trước
        
        Args:
            previous: Dữ liệu lần trước
            current: Dữ liệu hiện tại
            
        Returns:
            True if data has changed
        """
        try:
            # Compare key fields
            key_fields = ['close', 'volume', 'high', 'low']
            
            for field in key_fields:
                if abs(float(previous.get(field, 0)) - float(current.get(field, 0))) > 0.001:
                    return True
            
            # Compare timestamps
            prev_time = previous.get('timestamp', '')
            curr_time = current.get('timestamp', '')
            
            if prev_time != curr_time:
                return True
                
            return False
            
        except Exception as e:
            self.logger.warning(f"Error comparing data changes: {e}")
            return True  # Assume changed if can't compare
    
    def run_market_data_collection(self) -> None:
        """
        Chạy thu thập dữ liệu market theo interval
        """
        self.logger.info(f"Starting market data collection with {self.interval_seconds} second intervals")
        
        try:
            while True:
                try:
                    self.logger.info("Starting market data collection cycle...")
                    
                    # Collect data for all tickers
                    market_data_list = self.collect_all_tickers()
                    
                    # Send to Kafka
                    success_count = 0
                    for data in market_data_list:
                        try:
                            if self.send_message(data, key=f"market_{data['ticker']}_{int(time.time())}"):
                                success_count += 1
                        except Exception as e:
                            self.logger.error(f"Failed to send market data to Kafka: {e}")
                            continue
                    
                    self.logger.info(f"Market data cycle complete: {success_count}/{len(market_data_list)} records sent")
                    
                    # Wait for next interval
                    self.logger.info(f"Waiting {self.interval_seconds} seconds for next collection...")
                    time.sleep(self.interval_seconds)
                    
                except Exception as e:
                    self.logger.error(f"Error in market data collection cycle: {e}")
                    time.sleep(30)  # Wait before retry
                    continue
                    
        except KeyboardInterrupt:
            self.logger.info("Market data collection stopped by user")
        except Exception as e:
            self.logger.error(f"Critical error in market data collection: {e}")
            raise
    
    def collect_once(self) -> Dict[str, int]:
        """
        Chạy thu thập một lần và return kết quả
        
        Returns:
            Dictionary với số lượng records thu thập được
        """
        try:
            self.logger.info("Running single market data collection...")
            
            # Collect data
            market_data_list = self.collect_all_tickers()
            
            # Send to Kafka
            success_count = 0
            for data in market_data_list:
                try:
                    if self.send_message(data, key=f"market_{data['ticker']}_{int(time.time())}"):
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to send market data to Kafka: {e}")
                    continue
            
            results = {
                'collected': len(market_data_list),
                'sent_to_kafka': success_count,
                'tickers': [data['ticker'] for data in market_data_list]
            }
            
            self.logger.info(f"Single collection complete: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in single market data collection: {e}")
            return {'collected': 0, 'sent_to_kafka': 0, 'tickers': []}
    
    def test_vnstock_connection(self) -> bool:
        """
        Test kết nối vnstock
        
        Returns:
            True if connection is working
        """
        try:
            self.logger.info("Testing vnstock connection...")
            
            # Try to get a simple stock list
            test_data = stock.listing_companies()
            
            if test_data is not None and not test_data.empty:
                self.logger.info("vnstock connection test successful")
                return True
            else:
                self.logger.warning("vnstock connection test failed - no data returned")
                return False
                
        except Exception as e:
            self.logger.error(f"vnstock connection test failed: {e}")
            return False
    
    def get_available_indices(self) -> List[str]:
        """
        Lấy danh sách các indices có sẵn
        
        Returns:
            List of available indices
        """
        try:
            # Common Vietnamese market indices
            indices = ['VNINDEX', 'VN30', 'HNX', 'UPCOM']
            
            self.logger.info(f"Available indices: {indices}")
            return indices
            
        except Exception as e:
            self.logger.error(f"Error getting available indices: {e}")
            return ['VNINDEX', 'VN30']


if __name__ == "__main__":
    # Test Market Data Producer
    with MarketDataProducer() as producer:
        # Test vnstock connection
        if producer.test_vnstock_connection():
            print("✓ vnstock connection successful")
            
            # Check Kafka health
            if producer.health_check():
                print("✓ Kafka connection healthy")
                
                # Get available indices
                indices = producer.get_available_indices()
                print(f"Available indices: {indices}")
                
                # Run single collection
                results = producer.collect_once()
                print(f"Collection results: {results}")
                
                # Uncomment to run continuous collection
                # producer.run_market_data_collection()
            else:
                print("✗ Kafka connection failed")
        else:
            print("✗ vnstock connection failed")