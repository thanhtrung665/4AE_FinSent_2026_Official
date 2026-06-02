"""
RSS Feeder Module - Thu thập dữ liệu từ CafeF và Vietstock
"""
import feedparser
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import time
import os
from kafka_producer_base import BaseKafkaProducer
from urllib.parse import urljoin
import logging
from bs4 import BeautifulSoup
import re

class RSSFeeder(BaseKafkaProducer):
    """RSS Feed Producer cho tin tức tài chính"""
    
    def __init__(self):
        super().__init__(topic='news_rss_data')
        
        # RSS Feed URLs
        self.rss_feeds = {
            'cafef': os.getenv('CAFEF_RSS_URL', 'https://cafef.vn/thi-truong-chung-khoan.rss'),
            'vietstock': os.getenv('VIETSTOCK_RSS_URL', 'https://vietstock.vn/rss/tai-chinh.rss')
        }
        
        # Request session với retry
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Adapter cho retry
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.logger.info("RSS Feeder initialized successfully")
    
    def fetch_rss_feed(self, feed_url: str, source: str) -> List[Dict[str, Any]]:
        """
        Lấy và parse RSS feed
        
        Args:
            feed_url: URL của RSS feed
            source: Tên nguồn (cafef, vietstock)
            
        Returns:
            List of parsed articles
        """
        try:
            self.logger.info(f"Fetching RSS feed from {source}: {feed_url}")
            
            # Fetch RSS content
            response = self.session.get(feed_url, timeout=30)
            response.raise_for_status()
            
            # Parse RSS
            feed = feedparser.parse(response.content)
            
            if feed.bozo:
                self.logger.warning(f"RSS feed {source} has parsing issues: {feed.bozo_exception}")
            
            articles = []
            
            for entry in feed.entries:
                try:
                    article = self._parse_rss_entry(entry, source)
                    if article:
                        articles.append(article)
                except Exception as e:
                    self.logger.error(f"Error parsing entry from {source}: {e}")
                    continue
            
            self.logger.info(f"Successfully parsed {len(articles)} articles from {source}")
            return articles
            
        except requests.RequestException as e:
            self.logger.error(f"Network error fetching RSS from {source}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching RSS feed from {source}: {e}")
            raise
    
    def _parse_rss_entry(self, entry, source: str) -> Optional[Dict[str, Any]]:
        """
        Parse một entry RSS thành format JSON
        
        Args:
            entry: RSS entry object
            source: Tên nguồn
            
        Returns:
            Parsed article dictionary
        """
        try:
            # Extract basic info
            title = getattr(entry, 'title', '').strip()
            link = getattr(entry, 'link', '')
            
            if not title or not link:
                return None
            
            # Parse publish date
            publish_date = self._parse_publish_date(entry)
            
            # Get article content
            article_body = self._extract_article_content(entry, source)
            
            # Extract metadata tags
            metadata_tags = self._extract_metadata_tags(entry, source)
            
            article = {
                "article_title": title,
                "article_body": article_body,
                "publish_date": publish_date,
                "metadata_tags": metadata_tags,
                "source": source,
                "original_url": link,
                "feed_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            return article
            
        except Exception as e:
            self.logger.error(f"Error parsing RSS entry: {e}")
            return None
    
    def _parse_publish_date(self, entry) -> str:
        """Parse publish date từ RSS entry"""
        try:
            # Try different date fields
            date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
            
            for field in date_fields:
                if hasattr(entry, field) and getattr(entry, field):
                    time_struct = getattr(entry, field)
                    if time_struct:
                        return datetime(*time_struct[:6], tzinfo=timezone.utc).isoformat()
            
            # Fallback to string parsing
            date_strings = ['published', 'updated', 'created']
            for field in date_strings:
                if hasattr(entry, field) and getattr(entry, field):
                    date_str = getattr(entry, field)
                    try:
                        # Try parsing with feedparser's built-in parser
                        import email.utils
                        time_tuple = email.utils.parsedate_tz(date_str)
                        if time_tuple:
                            timestamp = email.utils.mktime_tz(time_tuple)
                            return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                    except:
                        continue
            
            # Default to current time if no date found
            return datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            self.logger.warning(f"Error parsing publish date: {e}")
            return datetime.now(timezone.utc).isoformat()
    
    def _extract_article_content(self, entry, source: str) -> str:
        """Extract article body content"""
        try:
            # Try different content fields
            content_fields = ['content', 'summary', 'description']
            content = ""
            
            for field in content_fields:
                if hasattr(entry, field):
                    field_content = getattr(entry, field)
                    
                    if isinstance(field_content, list) and len(field_content) > 0:
                        content = field_content[0].get('value', '')
                    elif isinstance(field_content, str):
                        content = field_content
                    
                    if content:
                        break
            
            if content:
                # Clean HTML tags
                soup = BeautifulSoup(content, 'html.parser')
                content = soup.get_text(separator=' ', strip=True)
                
                # Clean extra whitespace
                content = re.sub(r'\s+', ' ', content).strip()
            
            # If no content, try to fetch from link (be careful with rate limiting)
            if not content and hasattr(entry, 'link'):
                content = self._fetch_full_article(entry.link, source)
            
            return content or "Không có nội dung"
            
        except Exception as e:
            self.logger.warning(f"Error extracting article content: {e}")
            return "Lỗi trích xuất nội dung"
    
    def _fetch_full_article(self, url: str, source: str) -> str:
        """Fetch full article content from URL (with rate limiting)"""
        try:
            # Add delay to avoid overwhelming the server
            time.sleep(1)
            
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Source-specific content selectors
            content_selectors = {
                'cafef': ['.detail-content', '.content', 'article', '.post-content'],
                'vietstock': ['.news-content', '.content', 'article', '.post-content'],
                'default': ['article', '.content', '.post-content', '.entry-content', 'main']
            }
            
            selectors = content_selectors.get(source, content_selectors['default'])
            
            for selector in selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    # Remove script and style tags
                    for tag in content_div(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()
                    
                    text = content_div.get_text(separator=' ', strip=True)
                    text = re.sub(r'\s+', ' ', text).strip()
                    
                    if len(text) > 100:  # Minimum content length
                        return text[:2000]  # Limit content length
            
            return ""
            
        except Exception as e:
            self.logger.warning(f"Error fetching full article from {url}: {e}")
            return ""
    
    def _extract_metadata_tags(self, entry, source: str) -> List[str]:
        """Extract metadata tags from RSS entry"""
        tags = [source]  # Always include source as tag
        
        try:
            # Extract tags from different fields
            if hasattr(entry, 'tags') and entry.tags:
                for tag in entry.tags:
                    if hasattr(tag, 'term') and tag.term:
                        tags.append(tag.term.strip())
            
            if hasattr(entry, 'category') and entry.category:
                if isinstance(entry.category, list):
                    tags.extend([cat.strip() for cat in entry.category])
                else:
                    tags.append(entry.category.strip())
            
            if hasattr(entry, 'categories') and entry.categories:
                for cat in entry.categories:
                    if isinstance(cat, dict) and 'term' in cat:
                        tags.append(cat['term'].strip())
                    elif isinstance(cat, str):
                        tags.append(cat.strip())
            
            # Add publication type
            tags.append('financial_news')
            tags.append('rss_feed')
            
            # Remove duplicates and empty tags
            tags = list(set([tag for tag in tags if tag and tag.strip()]))
            
            return tags
            
        except Exception as e:
            self.logger.warning(f"Error extracting metadata tags: {e}")
            return [source, 'financial_news', 'rss_feed']
    
    def run_feed_collection(self, interval_minutes: int = 30) -> None:
        """
        Chạy thu thập RSS feeds theo interval
        
        Args:
            interval_minutes: Khoảng thời gian giữa các lần thu thập (phút)
        """
        self.logger.info(f"Starting RSS feed collection with {interval_minutes} minute intervals")
        
        try:
            while True:
                for source, feed_url in self.rss_feeds.items():
                    try:
                        self.logger.info(f"Processing feed: {source}")
                        
                        # Fetch articles
                        articles = self.fetch_rss_feed(feed_url, source)
                        
                        # Send to Kafka
                        success_count = 0
                        for article in articles:
                            try:
                                if self.send_message(article, key=f"{source}_{article.get('article_title', '')[:50]}"):
                                    success_count += 1
                            except Exception as e:
                                self.logger.error(f"Failed to send article to Kafka: {e}")
                                continue
                        
                        self.logger.info(f"Sent {success_count}/{len(articles)} articles from {source}")
                        
                        # Small delay between sources
                        time.sleep(2)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing feed {source}: {e}")
                        continue
                
                # Wait for next interval
                self.logger.info(f"Waiting {interval_minutes} minutes for next collection...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info("RSS Feed collection stopped by user")
        except Exception as e:
            self.logger.error(f"Critical error in feed collection: {e}")
            raise
    
    def collect_once(self) -> Dict[str, int]:
        """
        Chạy thu thập một lần và return kết quả
        
        Returns:
            Dictionary với số lượng articles thu thập được từ mỗi source
        """
        results = {}
        
        for source, feed_url in self.rss_feeds.items():
            try:
                self.logger.info(f"Collecting from {source}...")
                
                # Fetch articles
                articles = self.fetch_rss_feed(feed_url, source)
                
                # Send to Kafka
                success_count = 0
                for article in articles:
                    try:
                        if self.send_message(article, key=f"{source}_{article.get('article_title', '')[:50]}"):
                            success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to send article to Kafka: {e}")
                        continue
                
                results[source] = success_count
                self.logger.info(f"Successfully sent {success_count}/{len(articles)} articles from {source}")
                
            except Exception as e:
                self.logger.error(f"Error collecting from {source}: {e}")
                results[source] = 0
        
        return results


if __name__ == "__main__":
    # Test RSS Feeder
    with RSSFeeder() as feeder:
        # Check health
        if feeder.health_check():
            # Run single collection
            results = feeder.collect_once()
            print(f"Collection results: {results}")
            
            # Uncomment to run continuous collection
            # feeder.run_feed_collection(interval_minutes=30)