"""
F319 Scraper Module - Thu thập dữ liệu từ diễn đàn F319
"""
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import time
import re
import os
from kafka_producer_base import BaseKafkaProducer
from urllib.parse import urljoin, urlparse
import logging

class F319Scraper(BaseKafkaProducer):
    """F319 Forum Scraper Producer"""
    
    def __init__(self):
        super().__init__(topic='f319_data')
        
        self.base_url = os.getenv('F319_BASE_URL', 'https://www.f319.com')
        self.delay_seconds = int(os.getenv('F319_DELAY_SECONDS', 2))
        
        # Request session với retry và user agent
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Setup retry adapter
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
            backoff_factor=2
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Cache để tránh duplicate posts
        self.scraped_posts = set()
        
        self.logger.info("F319 Scraper initialized successfully")
    
    def get_forum_pages(self, forum_section: str = "", max_pages: int = 3) -> List[str]:
        """
        Lấy danh sách URLs của các trang forum
        
        Args:
            forum_section: Section của forum (để lọc theo chủ đề)
            max_pages: Số trang tối đa để scrape
            
        Returns:
            List of forum page URLs
        """
        try:
            urls = []
            
            # Main forum URL
            if forum_section:
                start_url = urljoin(self.base_url, f"/forum/{forum_section}")
            else:
                start_url = urljoin(self.base_url, "/forum")
            
            # Get first page to understand pagination
            self.logger.info(f"Fetching forum section: {start_url}")
            
            response = self.session.get(start_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Add current page
            urls.append(start_url)
            
            # Find pagination links
            pagination_selectors = [
                '.pagination a',
                '.page-numbers a',
                '.paging a',
                'a[href*="page"]'
            ]
            
            for selector in pagination_selectors:
                page_links = soup.select(selector)
                if page_links:
                    for link in page_links[:max_pages-1]:  # -1 because we already have first page
                        href = link.get('href')
                        if href and 'page' in href.lower():
                            full_url = urljoin(self.base_url, href)
                            if full_url not in urls:
                                urls.append(full_url)
                    break
            
            # If no pagination found, try to construct page URLs
            if len(urls) == 1:
                for page_num in range(2, min(max_pages + 1, 6)):  # Max 5 pages if no pagination
                    page_url = f"{start_url}?page={page_num}"
                    urls.append(page_url)
            
            self.logger.info(f"Found {len(urls)} forum pages to scrape")
            return urls[:max_pages]
            
        except Exception as e:
            self.logger.error(f"Error getting forum pages: {e}")
            return [urljoin(self.base_url, "/forum")]  # Fallback to main forum
    
    def scrape_forum_page(self, page_url: str) -> List[Dict[str, Any]]:
        """
        Scrape một trang forum để lấy danh sách posts
        
        Args:
            page_url: URL của trang forum
            
        Returns:
            List of post data
        """
        try:
            self.logger.info(f"Scraping forum page: {page_url}")
            
            # Add delay to be respectful
            time.sleep(self.delay_seconds)
            
            response = self.session.get(page_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            posts = []
            
            # Different selectors for different forum layouts
            post_selectors = [
                '.thread-item',
                '.topic-row',
                '.post-item',
                '.forum-post',
                'tr[id*="thread"]',
                '.message-main',
                'article'
            ]
            
            post_elements = []
            for selector in post_selectors:
                elements = soup.select(selector)
                if elements:
                    post_elements = elements
                    self.logger.info(f"Found posts using selector: {selector}")
                    break
            
            if not post_elements:
                # Fallback: find any links that might be posts
                post_elements = soup.select('a[href*="/thread/"], a[href*="/post/"], a[href*="/topic/"]')
                self.logger.warning("Using fallback post detection")
            
            for element in post_elements:
                try:
                    post_data = self._extract_post_data(element, page_url)
                    if post_data and post_data['post_id'] not in self.scraped_posts:
                        posts.append(post_data)
                        self.scraped_posts.add(post_data['post_id'])
                except Exception as e:
                    self.logger.warning(f"Error extracting post data: {e}")
                    continue
            
            self.logger.info(f"Extracted {len(posts)} posts from {page_url}")
            return posts
            
        except requests.RequestException as e:
            self.logger.error(f"Network error scraping page {page_url}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error scraping forum page {page_url}: {e}")
            return []
    
    def _extract_post_data(self, element, page_url: str) -> Optional[Dict[str, Any]]:
        """
        Trích xuất dữ liệu từ một post element
        
        Args:
            element: BeautifulSoup element
            page_url: URL của trang chứa post
            
        Returns:
            Post data dictionary
        """
        try:
            # Extract post URL and ID
            post_link = self._find_post_link(element)
            if not post_link:
                return None
            
            post_url = urljoin(self.base_url, post_link)
            post_id = self._extract_post_id(post_url)
            
            if not post_id:
                return None
            
            # Extract title
            title = self._extract_title(element)
            
            # Extract preview content if available
            preview_content = self._extract_preview_content(element)
            
            # Extract engagement metrics from listing page
            engagement = self._extract_engagement_metrics(element)
            
            # Extract creation time
            created_at = self._extract_creation_time(element)
            
            # Get full post content
            full_content = self._get_full_post_content(post_url)
            
            post_data = {
                "post_id": post_id,
                "content_text": full_content or preview_content or title,
                "created_at": created_at,
                "engagement_metrics": engagement,
                "title": title,
                "post_url": post_url,
                "source_page": page_url,
                "scrape_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            return post_data
            
        except Exception as e:
            self.logger.warning(f"Error extracting post data: {e}")
            return None
    
    def _find_post_link(self, element) -> Optional[str]:
        """Find the main post/thread link in element"""
        # Try different link patterns
        link_selectors = [
            'a[href*="/thread/"]',
            'a[href*="/post/"]',
            'a[href*="/topic/"]',
            '.thread-title a',
            '.topic-title a',
            'h3 a',
            'h4 a',
            '.title a'
        ]
        
        for selector in link_selectors:
            link_elem = element.select_one(selector)
            if link_elem and link_elem.get('href'):
                return link_elem.get('href')
        
        # Fallback: any link in the element
        link_elem = element.find('a', href=True)
        if link_elem:
            href = link_elem.get('href')
            if any(keyword in href for keyword in ['/thread/', '/post/', '/topic/', '/showthread']):
                return href
        
        return None
    
    def _extract_post_id(self, post_url: str) -> Optional[str]:
        """Extract post ID from URL"""
        try:
            # Common patterns for post IDs
            patterns = [
                r'/thread/(\d+)',
                r'/post/(\d+)',
                r'/topic/(\d+)',
                r'[?&]t=(\d+)',
                r'[?&]p=(\d+)',
                r'showthread\.php[?]t=(\d+)'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, post_url)
                if match:
                    return f"f319_{match.group(1)}"
            
            # Fallback: use URL hash
            return f"f319_{abs(hash(post_url)) % 1000000}"
            
        except Exception as e:
            self.logger.warning(f"Error extracting post ID from {post_url}: {e}")
            return None
    
    def _extract_title(self, element) -> str:
        """Extract post title"""
        try:
            title_selectors = [
                '.thread-title',
                '.topic-title',
                'h3',
                'h4',
                '.title',
                'a[title]'
            ]
            
            for selector in title_selectors:
                title_elem = element.select_one(selector)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    if title and len(title) > 5:
                        return title
            
            # Fallback: first link text
            link = element.find('a')
            if link:
                title = link.get_text(strip=True)
                if title and len(title) > 5:
                    return title
            
            return "Không có tiêu đề"
            
        except Exception as e:
            self.logger.warning(f"Error extracting title: {e}")
            return "Không có tiêu đề"
    
    def _extract_preview_content(self, element) -> str:
        """Extract preview content from listing"""
        try:
            content_selectors = [
                '.thread-preview',
                '.post-preview',
                '.excerpt',
                '.summary'
            ]
            
            for selector in content_selectors:
                content_elem = element.select_one(selector)
                if content_elem:
                    content = content_elem.get_text(strip=True)
                    if content and len(content) > 10:
                        return content
            
            return ""
            
        except Exception as e:
            return ""
    
    def _extract_engagement_metrics(self, element) -> Dict[str, int]:
        """Extract engagement metrics (views, replies) from listing"""
        metrics = {"views": 0, "replies": 0}
        
        try:
            # Common patterns for metrics
            stats_text = element.get_text()
            
            # Extract views
            view_patterns = [
                r'(\d+)\s*views?',
                r'(\d+)\s*lượt xem',
                r'Views:\s*(\d+)',
                r'Xem:\s*(\d+)'
            ]
            
            for pattern in view_patterns:
                match = re.search(pattern, stats_text, re.IGNORECASE)
                if match:
                    metrics["views"] = int(match.group(1))
                    break
            
            # Extract replies
            reply_patterns = [
                r'(\d+)\s*repl(?:y|ies)',
                r'(\d+)\s*trả lời',
                r'Replies:\s*(\d+)',
                r'Posts:\s*(\d+)'
            ]
            
            for pattern in reply_patterns:
                match = re.search(pattern, stats_text, re.IGNORECASE)
                if match:
                    metrics["replies"] = int(match.group(1))
                    break
            
            # Try to find in specific elements
            stats_selectors = [
                '.thread-stats',
                '.post-stats',
                '.stats',
                '.meta'
            ]
            
            for selector in stats_selectors:
                stats_elem = element.select_one(selector)
                if stats_elem:
                    stats_text = stats_elem.get_text()
                    numbers = re.findall(r'\d+', stats_text)
                    if len(numbers) >= 2:
                        metrics["replies"] = int(numbers[0])
                        metrics["views"] = int(numbers[1])
                        break
            
        except Exception as e:
            self.logger.warning(f"Error extracting engagement metrics: {e}")
        
        return metrics
    
    def _extract_creation_time(self, element) -> str:
        """Extract post creation time"""
        try:
            # Look for time elements
            time_selectors = [
                'time[datetime]',
                '.post-time',
                '.created-at',
                '.date',
                '.timestamp'
            ]
            
            for selector in time_selectors:
                time_elem = element.select_one(selector)
                if time_elem:
                    # Try datetime attribute
                    datetime_attr = time_elem.get('datetime')
                    if datetime_attr:
                        return datetime_attr
                    
                    # Try parsing text
                    time_text = time_elem.get_text(strip=True)
                    if time_text:
                        parsed_time = self._parse_relative_time(time_text)
                        if parsed_time:
                            return parsed_time
            
            # Look for common time patterns in text
            text = element.get_text()
            time_patterns = [
                r'(\d{1,2}/\d{1,2}/\d{4})',
                r'(\d{4}-\d{2}-\d{2})',
                r'(\d{1,2}\s+\w+\s+ago)',
                r'(Yesterday|Today|Hôm qua|Hôm nay)'
            ]
            
            for pattern in time_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    parsed_time = self._parse_relative_time(match.group(1))
                    if parsed_time:
                        return parsed_time
            
        except Exception as e:
            self.logger.warning(f"Error extracting creation time: {e}")
        
        # Default to current time
        return datetime.now(timezone.utc).isoformat()
    
    def _parse_relative_time(self, time_str: str) -> Optional[str]:
        """Parse relative time strings like 'X hours ago'"""
        try:
            time_str = time_str.lower().strip()
            now = datetime.now(timezone.utc)
            
            # Handle various patterns
            if any(word in time_str for word in ['hour', 'giờ']) and 'ago' in time_str:
                hours = re.search(r'(\d+)', time_str)
                if hours:
                    target_time = now - timedelta(hours=int(hours.group(1)))
                    return target_time.isoformat()
            
            if any(word in time_str for word in ['minute', 'phút']) and 'ago' in time_str:
                minutes = re.search(r'(\d+)', time_str)
                if minutes:
                    target_time = now - timedelta(minutes=int(minutes.group(1)))
                    return target_time.isoformat()
            
            if any(word in time_str for word in ['day', 'ngày']) and 'ago' in time_str:
                days = re.search(r'(\d+)', time_str)
                if days:
                    target_time = now - timedelta(days=int(days.group(1)))
                    return target_time.isoformat()
            
            if 'yesterday' in time_str or 'hôm qua' in time_str:
                target_time = now - timedelta(days=1)
                return target_time.isoformat()
            
            if 'today' in time_str or 'hôm nay' in time_str:
                return now.isoformat()
            
            # Try direct date parsing
            from dateutil import parser
            try:
                parsed_date = parser.parse(time_str)
                return parsed_date.isoformat()
            except:
                pass
            
        except Exception as e:
            self.logger.warning(f"Error parsing time '{time_str}': {e}")
        
        return None
    
    def _get_full_post_content(self, post_url: str) -> Optional[str]:
        """Fetch full post content from individual post page"""
        try:
            # Add delay
            time.sleep(self.delay_seconds)
            
            response = self.session.get(post_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Content selectors for different forum layouts
            content_selectors = [
                '.post-content',
                '.message-content',
                '.thread-content',
                '.post-body',
                '.message-main .message-body',
                '.postbody',
                '.content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove unwanted elements
                    for tag in content_elem(['script', 'style', 'nav', 'footer', 'aside', '.signature']):
                        tag.decompose()
                    
                    content = content_elem.get_text(separator='\n', strip=True)
                    content = re.sub(r'\n\s*\n', '\n', content)  # Clean multiple newlines
                    
                    if content and len(content) > 50:
                        return content[:2000]  # Limit content length
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Error fetching full content from {post_url}: {e}")
            return None
    
    def scrape_forum_section(self, section: str = "", max_pages: int = 3) -> List[Dict[str, Any]]:
        """
        Scrape một section của forum
        
        Args:
            section: Tên section (optional)
            max_pages: Số trang tối đa
            
        Returns:
            List of all posts from the section
        """
        all_posts = []
        
        try:
            # Get forum pages
            page_urls = self.get_forum_pages(section, max_pages)
            
            for page_url in page_urls:
                try:
                    posts = self.scrape_forum_page(page_url)
                    all_posts.extend(posts)
                    
                    self.logger.info(f"Collected {len(posts)} posts from {page_url}")
                    
                except Exception as e:
                    self.logger.error(f"Error scraping page {page_url}: {e}")
                    continue
            
            self.logger.info(f"Total posts collected: {len(all_posts)}")
            return all_posts
            
        except Exception as e:
            self.logger.error(f"Error scraping forum section: {e}")
            return all_posts
    
    def run_scraping_session(self, sections: List[str] = [""], interval_minutes: int = 60, max_pages_per_section: int = 2) -> None:
        """
        Chạy scraping session liên tục
        
        Args:
            sections: List of forum sections to scrape
            interval_minutes: Khoảng thời gian giữa các lần scrape
            max_pages_per_section: Số trang tối đa cho mỗi section
        """
        self.logger.info(f"Starting F319 scraping session with {interval_minutes} minute intervals")
        
        try:
            while True:
                total_sent = 0
                
                for section in sections:
                    try:
                        self.logger.info(f"Scraping section: {section or 'main'}")
                        
                        # Scrape posts
                        posts = self.scrape_forum_section(section, max_pages_per_section)
                        
                        # Send to Kafka
                        for post in posts:
                            try:
                                if self.send_message(post, key=post['post_id']):
                                    total_sent += 1
                            except Exception as e:
                                self.logger.error(f"Failed to send post to Kafka: {e}")
                                continue
                        
                        self.logger.info(f"Sent {len(posts)} posts from section {section or 'main'}")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing section {section}: {e}")
                        continue
                
                self.logger.info(f"Scraping session complete. Total posts sent: {total_sent}")
                
                # Wait for next interval
                self.logger.info(f"Waiting {interval_minutes} minutes for next scraping session...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            self.logger.info("F319 scraping stopped by user")
        except Exception as e:
            self.logger.error(f"Critical error in scraping session: {e}")
            raise
    
    def scrape_once(self, sections: List[str] = [""], max_pages_per_section: int = 2) -> Dict[str, int]:
        """
        Chạy scraping một lần và return kết quả
        
        Args:
            sections: List of sections to scrape
            max_pages_per_section: Max pages per section
            
        Returns:
            Dictionary with results per section
        """
        results = {}
        
        for section in sections:
            try:
                self.logger.info(f"Scraping section: {section or 'main'}")
                
                # Scrape posts
                posts = self.scrape_forum_section(section, max_pages_per_section)
                
                # Send to Kafka
                success_count = 0
                for post in posts:
                    try:
                        if self.send_message(post, key=post['post_id']):
                            success_count += 1
                    except Exception as e:
                        self.logger.error(f"Failed to send post to Kafka: {e}")
                        continue
                
                results[section or 'main'] = success_count
                self.logger.info(f"Successfully sent {success_count}/{len(posts)} posts from section {section or 'main'}")
                
            except Exception as e:
                self.logger.error(f"Error scraping section {section}: {e}")
                results[section or 'main'] = 0
        
        return results


if __name__ == "__main__":
    # Test F319 Scraper
    from datetime import timedelta  # Import needed for time parsing
    
    with F319Scraper() as scraper:
        # Check health
        if scraper.health_check():
            # Run single scraping session
            results = scraper.scrape_once(sections=["", "finance"], max_pages_per_section=2)
            print(f"Scraping results: {results}")
            
            # Uncomment to run continuous scraping
            # scraper.run_scraping_session(sections=["", "finance"], interval_minutes=60)