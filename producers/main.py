"""
Main module để chạy cả hai producers
"""
import os
import sys
import threading
import time
import signal
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import producers
from rss_feeder import RSSFeeder
from f319_scraper import F319Scraper
from market_data_producer import MarketDataProducer
from facebook_mock_injector import FacebookMockInjector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('producers.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('MainProducer')

class DataPipelineManager:
    """Manager cho cả hai producers"""
    
    def __init__(self):
        self.rss_feeder = None
        self.f319_scraper = None
        self.market_producer = None
        self.mock_injector = None
        self.threads = []
        self.shutdown_event = threading.Event()
        
        # Configuration
        self.rss_interval_minutes = int(os.getenv('RSS_INTERVAL_MINUTES', 30))
        self.f319_interval_minutes = int(os.getenv('F319_INTERVAL_MINUTES', 60))
        self.market_interval_minutes = int(os.getenv('MARKET_DATA_INTERVAL_MINUTES', 1))
        
        # Mock injector (one-time run)
        self.run_mock_injector = os.getenv('RUN_MOCK_INJECTOR', 'false').lower() == 'true'
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown_event.set()
    
    def _run_rss_feeder(self):
        """Run RSS feeder in thread"""
        try:
            logger.info("Starting RSS Feeder thread...")
            self.rss_feeder = RSSFeeder()
            
            # Health check
            if not self.rss_feeder.health_check():
                logger.error("RSS Feeder health check failed")
                return
            
            while not self.shutdown_event.is_set():
                try:
                    logger.info("Running RSS collection cycle...")
                    results = self.rss_feeder.collect_once()
                    logger.info(f"RSS collection results: {results}")
                    
                    # Wait for next interval or shutdown
                    wait_time = self.rss_interval_minutes * 60
                    if self.shutdown_event.wait(wait_time):
                        break  # Shutdown requested
                        
                except Exception as e:
                    logger.error(f"Error in RSS feeder cycle: {e}")
                    # Wait before retrying
                    if self.shutdown_event.wait(60):
                        break
                        
        except Exception as e:
            logger.error(f"Critical error in RSS feeder thread: {e}")
        finally:
            if self.rss_feeder:
                self.rss_feeder.close()
                logger.info("RSS Feeder thread stopped")
    
    def _run_f319_scraper(self):
        """Run F319 scraper in thread"""
        try:
            logger.info("Starting F319 Scraper thread...")
            self.f319_scraper = F319Scraper()
            
            # Health check
            if not self.f319_scraper.health_check():
                logger.error("F319 Scraper health check failed")
                return
            
            while not self.shutdown_event.is_set():
                try:
                    logger.info("Running F319 scraping cycle...")
                    
                    # Scrape different sections
                    sections = ["", "finance", "investment"]  # Add more sections as needed
                    results = self.f319_scraper.scrape_once(
                        sections=sections, 
                        max_pages_per_section=2
                    )
                    logger.info(f"F319 scraping results: {results}")
                    
                    # Wait for next interval or shutdown
                    wait_time = self.f319_interval_minutes * 60
                    if self.shutdown_event.wait(wait_time):
                        break  # Shutdown requested
                        
                except Exception as e:
                    logger.error(f"Error in F319 scraper cycle: {e}")
                    # Wait before retrying
                    if self.shutdown_event.wait(120):  # Longer wait for scraper errors
                        break
                        
        except Exception as e:
            logger.error(f"Critical error in F319 scraper thread: {e}")
        finally:
            if self.f319_scraper:
                self.f319_scraper.close()
    def _run_market_data_producer(self):
        """Run Market Data producer in thread"""
        try:
            logger.info("Starting Market Data Producer thread...")
            self.market_producer = MarketDataProducer()
            
            # Health check
            if not self.market_producer.health_check():
                logger.error("Market Data Producer health check failed")
                return
            
            # Test vnstock connection
            if not self.market_producer.test_vnstock_connection():
                logger.error("vnstock connection test failed")
                return
            
            while not self.shutdown_event.is_set():
                try:
                    logger.info("Running market data collection cycle...")
                    results = self.market_producer.collect_once()
                    logger.info(f"Market data collection results: {results}")
                    
                    # Wait for next interval or shutdown
                    wait_time = self.market_interval_minutes * 60
                    if self.shutdown_event.wait(wait_time):
                        break  # Shutdown requested
                        
                except Exception as e:
                    logger.error(f"Error in market data collection cycle: {e}")
                    # Wait before retrying
                    if self.shutdown_event.wait(60):
                        break
                        
        except Exception as e:
            logger.error(f"Critical error in market data producer thread: {e}")
        finally:
            if self.market_producer:
                self.market_producer.close()
                logger.info("Market Data Producer thread stopped")
    
    def _run_mock_injector_once(self):
        """Run Facebook Mock Injector once"""
        try:
            logger.info("Starting Facebook Mock Injector...")
            self.mock_injector = FacebookMockInjector()
            
            # Health check
            if not self.mock_injector.health_check():
                logger.error("Facebook Mock Injector health check failed")
                return
            
            # Validate CSV
            if not self.mock_injector.validate_csv_format():
                logger.error("Facebook Mock CSV format validation failed")
                return
            
            # Run injection once
            results = self.mock_injector.inject_once()
            logger.info(f"Facebook Mock injection results: {results}")
                        
        except Exception as e:
            logger.error(f"Error in Facebook Mock Injector: {e}")
        finally:
            if self.mock_injector:
                self.mock_injector.close()
                logger.info("Facebook Mock Injector completed")
    
    def start_all_producers(self):
        """Start all producers in separate threads"""
        try:
            logger.info("Starting Data Pipeline Manager...")
            logger.info(f"RSS interval: {self.rss_interval_minutes} minutes")
            logger.info(f"F319 interval: {self.f319_interval_minutes} minutes")
            logger.info(f"Market data interval: {self.market_interval_minutes} minutes")
            logger.info(f"Run mock injector: {self.run_mock_injector}")
            
            # Create and start threads
            rss_thread = threading.Thread(target=self._run_rss_feeder, name="RSSFeeder")
            f319_thread = threading.Thread(target=self._run_f319_scraper, name="F319Scraper")
            market_thread = threading.Thread(target=self._run_market_data_producer, name="MarketDataProducer")
            
            rss_thread.daemon = True
            f319_thread.daemon = True
            market_thread.daemon = True
            
            self.threads = [rss_thread, f319_thread, market_thread]
            
            # Start threads with delays between them
            rss_thread.start()
            logger.info("RSS Feeder thread started")
            
            time.sleep(5)
            
            f319_thread.start()
            logger.info("F319 Scraper thread started")
            
            time.sleep(5)
            
            market_thread.start()
            logger.info("Market Data Producer thread started")
            
            # Run mock injector once if enabled
            if self.run_mock_injector:
                time.sleep(10)  # Wait for other producers to initialize
                mock_thread = threading.Thread(target=self._run_mock_injector_once, name="MockInjector")
                mock_thread.daemon = True
                mock_thread.start()
                logger.info("Facebook Mock Injector started")
            
            # Wait for threads or shutdown signal
            try:
                while not self.shutdown_event.is_set():
                    # Check if threads are still alive
                    alive_threads = [t for t in self.threads if t.is_alive()]
                    if not alive_threads:
                        logger.warning("All producer threads have stopped")
                        break
                    
                    # Wait a bit before checking again
                    time.sleep(10)
                    
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                self.shutdown_event.set()
            
        except Exception as e:
            logger.error(f"Critical error in pipeline manager: {e}")
            self.shutdown_event.set()
        
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Cleanup resources"""
        logger.info("Shutting down producers...")
        
        # Set shutdown event
        self.shutdown_event.set()
        
        # Wait for threads to finish
        for thread in self.threads:
            if thread.is_alive():
                logger.info(f"Waiting for {thread.name} to finish...")
                thread.join(timeout=30)
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} did not finish gracefully")
        
        # Close producers
        if self.rss_feeder:
            try:
                self.rss_feeder.close()
            except:
                pass
                
        if self.f319_scraper:
            try:
                self.f319_scraper.close()
            except:
                pass
                
        if self.market_producer:
            try:
                self.market_producer.close()
            except:
                pass
                
        if self.mock_injector:
            try:
                self.mock_injector.close()
            except:
                pass
        
        logger.info("Data Pipeline Manager shutdown complete")
    
    def run_single_collection(self):
        """Run single collection from all sources"""
        logger.info("Running single collection from all sources...")
        
        results = {}
        
        # RSS Collection
        try:
            logger.info("Running RSS collection...")
            with RSSFeeder() as rss_feeder:
                if rss_feeder.health_check():
                    rss_results = rss_feeder.collect_once()
                    results['rss'] = rss_results
                    logger.info(f"RSS results: {rss_results}")
                else:
                    logger.error("RSS Feeder health check failed")
                    results['rss'] = {}
        except Exception as e:
            logger.error(f"Error in RSS collection: {e}")
            results['rss'] = {}
        
        # Wait between collections
        time.sleep(5)
        
        # F319 Scraping
        try:
            logger.info("Running F319 scraping...")
            with F319Scraper() as f319_scraper:
                if f319_scraper.health_check():
                    f319_results = f319_scraper.scrape_once(
                        sections=["", "finance"], 
                        max_pages_per_section=2
                    )
                    results['f319'] = f319_results
                    logger.info(f"F319 results: {f319_results}")
                else:
                    logger.error("F319 Scraper health check failed")
                    results['f319'] = {}
        except Exception as e:
            logger.error(f"Error in F319 scraping: {e}")
            results['f319'] = {}
        
        # Wait between collections
        time.sleep(5)
        
        # Market Data Collection
        try:
            logger.info("Running Market Data collection...")
            with MarketDataProducer() as market_producer:
                if market_producer.health_check() and market_producer.test_vnstock_connection():
                    market_results = market_producer.collect_once()
                    results['market_data'] = market_results
                    logger.info(f"Market Data results: {market_results}")
                else:
                    logger.error("Market Data Producer health check failed")
                    results['market_data'] = {}
        except Exception as e:
            logger.error(f"Error in Market Data collection: {e}")
            results['market_data'] = {}
        
        # Wait between collections
        time.sleep(2)
        
        # Facebook Mock Injection
        try:
            logger.info("Running Facebook Mock injection...")
            with FacebookMockInjector() as mock_injector:
                if mock_injector.health_check() and mock_injector.validate_csv_format():
                    mock_results = mock_injector.inject_once()
                    results['facebook_mock'] = mock_results
                    logger.info(f"Facebook Mock results: {mock_results}")
                else:
                    logger.error("Facebook Mock Injector health check failed")
                    results['facebook_mock'] = {}
        except Exception as e:
            logger.error(f"Error in Facebook Mock injection: {e}")
            results['facebook_mock'] = {}
        
        logger.info(f"Single collection complete. Results: {results}")
        return results
    
    def health_check_all(self):
        """Check health of all producers"""
        results = {}
        
        # Check RSS Feeder
        try:
            with RSSFeeder() as rss_feeder:
                results['rss_feeder'] = rss_feeder.health_check()
        except Exception as e:
            logger.error(f"RSS Feeder health check error: {e}")
            results['rss_feeder'] = False
        
        # Check F319 Scraper
        try:
            with F319Scraper() as f319_scraper:
                results['f319_scraper'] = f319_scraper.health_check()
        except Exception as e:
            logger.error(f"F319 Scraper health check error: {e}")
            results['f319_scraper'] = False
        
        # Check Market Data Producer
        try:
            with MarketDataProducer() as market_producer:
                kafka_health = market_producer.health_check()
                vnstock_health = market_producer.test_vnstock_connection()
                results['market_data_producer'] = kafka_health and vnstock_health
                results['market_data_kafka'] = kafka_health
                results['market_data_vnstock'] = vnstock_health
        except Exception as e:
            logger.error(f"Market Data Producer health check error: {e}")
            results['market_data_producer'] = False
            results['market_data_kafka'] = False
            results['market_data_vnstock'] = False
        
        # Check Facebook Mock Injector
        try:
            with FacebookMockInjector() as mock_injector:
                kafka_health = mock_injector.health_check()
                csv_health = mock_injector.validate_csv_format()
                results['facebook_mock_injector'] = kafka_health and csv_health
                results['facebook_mock_kafka'] = kafka_health
                results['facebook_mock_csv'] = csv_health
        except Exception as e:
            logger.error(f"Facebook Mock Injector health check error: {e}")
            results['facebook_mock_injector'] = False
            results['facebook_mock_kafka'] = False
            results['facebook_mock_csv'] = False
        
        return results


def main():
    """Main function"""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        manager = DataPipelineManager()
        
        if command == "health":
            # Health check
            results = manager.health_check_all()
            print("Health Check Results:")
            for service, status in results.items():
                print(f"  {service}: {'✓ Healthy' if status else '✗ Unhealthy'}")
                
        elif command == "single":
            # Single collection
            results = manager.run_single_collection()
            print("Single Collection Results:")
            print(f"  RSS: {results.get('rss', {})}")
            print(f"  F319: {results.get('f319', {})}")
            print(f"  Market Data: {results.get('market_data', {})}")
            print(f"  Facebook Mock: {results.get('facebook_mock', {})}")
            
        elif command == "continuous":
            # Continuous mode
            manager.start_all_producers()
            
        else:
            print("Usage: python main.py [health|single|continuous]")
            print("  health     - Check health of all producers")
            print("  single     - Run single collection from all sources")
            print("  continuous - Run continuous data collection")
            
    else:
        print("Usage: python main.py [health|single|continuous]")
        print("  health     - Check health of all producers")
        print("  single     - Run single collection from all sources")
        print("  continuous - Run continuous data collection")


if __name__ == "__main__":
    main()