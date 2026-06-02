# Data Pipeline Producers

Bốn module Python Producer thu thập dữ liệu thời gian thực và gửi vào Kafka topics.

## Cấu Trúc

```
producers/
├── .env.example              # Template cấu hình environment
├── requirements.txt          # Python dependencies
├── kafka_producer_base.py    # Base class với retry logic
├── rss_feeder.py            # RSS Feed Producer (CafeF, Vietstock)
├── f319_scraper.py          # F319 Forum Scraper
├── market_data_producer.py   # Market Data Producer (vnstock)
├── facebook_mock_injector.py # Facebook Mock Data Injector
├── facebook_mock.csv        # Sample CSV data
├── main.py                  # Main controller chạy tất cả producers
└── README.md                # Documentation này
```

## Cài Đặt

### Giải Pháp cho Python 3.12+ (Khuyến Nghị)

Nếu bạn gặp lỗi `AttributeError: module 'pkgutil' has no attribute 'ImpImporter'`, hãy làm theo các bước này:

**Cách 1: Sử dụng Setup Script (Khuyến Nghị)**

```bash
cd producers
python setup.py
```

**Cách 2: Cài Đặt Thủ Công**

```bash
cd producers

# Upgrade pip trước
python -m pip install --upgrade pip setuptools wheel

# Cài từng package một
pip install requests>=2.31.0
pip install python-dotenv>=1.0.0
pip install tenacity>=8.2.0
pip install confluent-kafka>=2.3.0
pip install beautifulsoup4>=4.12.0
pip install feedparser>=6.0.10
pip install pandas>=1.5.0
pip install numpy>=1.21.0
pip install python-dateutil>=2.8.0

# Cài vnstock (có thể lâu)
pip install vnstock>=0.2.8.0
```

**Cách 3: Requirements Minimal (Fallback)**

```bash
cd producers
pip install -r requirements-minimal.txt
# Sau đó cài thêm vnstock và lxml riêng
pip install vnstock lxml
```

### Cài đặt dependencies (Cách cũ - có thể lỗi với Python 3.12)

```bash
cd producers
pip install -r requirements.txt  # Có thể lỗi với Python 3.12
```

### 2. Cấu hình Environment Variables

Sao chép `.env.example` thành `.env` và cập nhật các giá trị:

```bash
cp .env.example .env
```

Chỉnh sửa file `.env`:

```bash
# AWS Kafka Configuration
AWS_KAFKA_BROKER=your-ec2-ip:9092

# RSS Feed URLs
CAFEF_RSS_URL=https://cafef.vn/thi-truong-chung-khoan.rss
VIETSTOCK_RSS_URL=https://vietstock.vn/rss/tai-chinh.rss

# F319 Configuration
F319_BASE_URL=https://www.f319.com
F319_DELAY_SECONDS=2

# Retry Configuration
MAX_RETRY_ATTEMPTS=5
RETRY_BACKOFF_SECONDS=2

# Market Data Configuration
MARKET_DATA_INTERVAL_SECONDS=60
VNSTOCK_TICKERS=VNINDEX,VN30

# Facebook Mock Configuration
FACEBOOK_MOCK_CSV=facebook_mock.csv
FB_MOCK_DELAY_SECONDS=1

# Intervals (phút)
RSS_INTERVAL_MINUTES=30
F319_INTERVAL_MINUTES=60
MARKET_DATA_INTERVAL_MINUTES=1

# Mock Injector (true/false)
RUN_MOCK_INJECTOR=false
```

## Modules

### 1. RSS Feeder (`rss_feeder.py`)

Thu thập tin tức từ CafeF và Vietstock RSS feeds.

**Output Topic:** `news_rss_data`

**JSON Format:**
```json
{
    "article_title": "Tiêu đề bài viết",
    "article_body": "Nội dung bài viết đầy đủ",
    "publish_date": "2024-01-15T10:30:00+00:00",
    "metadata_tags": ["cafef", "financial_news", "rss_feed", "chung-khoan"],
    "source": "cafef",
    "original_url": "https://cafef.vn/article...",
    "feed_timestamp": "2024-01-15T10:35:00+00:00"
}
```

**Features:**
- Parse RSS feeds từ CafeF và Vietstock
- Trích xuất nội dung đầy đủ từ link gốc
- Xử lý metadata tags và publish date
- Retry logic với tenacity
- Rate limiting để tránh overload server

### 2. F319 Scraper (`f319_scraper.py`)

Thu thập dữ liệu từ diễn đàn F319.

**Output Topic:** `f319_data`

**JSON Format:**
```json
{
    "post_id": "f319_123456",
    "content_text": "Nội dung bài post đầy đủ",
    "created_at": "2024-01-15T09:15:00+00:00",
    "engagement_metrics": {
        "views": 150,
        "replies": 12
    },
    "title": "Tiêu đề bài post",
    "post_url": "https://www.f319.com/thread/123456",
    "source_page": "https://www.f319.com/forum",
    "scrape_timestamp": "2024-01-15T10:00:00+00:00"
}
```

**Features:**
- Scrape multiple forum sections
- Trích xuất engagement metrics (views, replies)
- Parse thời gian relative ("X hours ago")
- Anti-detection với user agent rotation
- Respectful scraping với delay
- Duplicate post detection

### 3. Market Data Producer (`market_data_producer.py`)

Thu thập dữ liệu chứng khoán từ vnstock API.

**Output Topic:** `market_stock_data`

**JSON Format:**
```json
{
    "ticker": "VNINDEX",
    "timestamp": "2024-01-15T15:30:00+00:00",
    "open": 1245.67,
    "high": 1251.23,
    "low": 1242.15,
    "close": 1249.88,
    "volume": 15420000,
    "data_source": "vnstock",
    "collection_timestamp": "2024-01-15T15:31:00+00:00"
}
```

**Features:**
- Thu thập VNINDEX và VN30 data mỗi 1 phút
- Sử dụng vnstock library chính thức
- Fallback từ real-time sang historical data
- Data validation và change detection
- Retry logic cho API calls
- Support multiple indices

### 4. Facebook Mock Injector (`facebook_mock_injector.py`)

Giả lập streaming data từ CSV file.

**Output Topic:** `fb_mock_data`

**JSON Format:**
```json
{
    "comment_id": "fb_001",
    "content_text": "Thị trường hôm nay khá tích cực, VN-Index tăng mạnh!",
    "created_at": "2024-01-15T09:30:00+00:00",
    "likes": 15,
    "row_index": 0,
    "stream_index": 0,
    "injection_timestamp": "2024-01-15T16:00:00+00:00",
    "stream_timestamp": "2024-01-15T16:00:01+00:00"
}
```

**Features:**
- Đọc dữ liệu từ CSV file
- Streaming simulation với time.sleep(1)
- Auto-generate sample CSV nếu không tồn tại
- Support multiple encodings (UTF-8, CP1252, Latin1)
- Duplicate detection và resume capability
- Flexible datetime parsing

### 5. Base Producer (`kafka_producer_base.py`)

### 5. Base Producer (`kafka_producer_base.py`)

Base class chung cho tất cả producers với retry logic.

**Features:**
- **Retry Logic:** Tự động retry tối đa 5 lần khi gặp lỗi network
- **Error Handling:** Xử lý các loại lỗi Kafka khác nhau
- **Health Check:** Kiểm tra kết nối Kafka
- **Batch Sending:** Gửi nhiều messages cùng lúc
- **Logging:** Chi tiết logs cho debugging
- **Context Manager:** Tự động cleanup resources

## Sử Dụng

### Chạy Health Check

Kiểm tra kết nối Kafka và tình trạng producers:

```bash
python main.py health
```

### Chạy Single Collection

Thu thập dữ liệu một lần từ tất cả sources:

```bash
python main.py single
```

### Chạy Continuous Mode

Chạy liên tục thu thập dữ liệu từ tất cả sources:

```bash
python main.py continuous
```

### Chạy Riêng Từng Module

**RSS Feeder:**
```bash
python rss_feeder.py
```

**F319 Scraper:**
```bash
python f319_scraper.py
```

**Market Data Producer:**
```bash
python market_data_producer.py
```

**Facebook Mock Injector:**
```bash
python facebook_mock_injector.py
```

## Retry Logic

Cả hai producers đều có retry logic tự động:

### Network Retry
- **Max Attempts:** 5 lần
- **Backoff Strategy:** Exponential backoff (1s, 2s, 4s, 8s, 16s)
- **Retry Conditions:** 
  - KafkaTimeoutError
  - NoBrokersAvailable
  - ConnectionError
  - HTTP 5xx errors

### Error Recovery
- Tự động reconnect Kafka khi mất kết nối
- Graceful handling khi source websites không khả dụng
- Continue processing ngay cả khi một số items bị lỗi

## Monitoring & Logging

### Log Files
- `producers.log` - Tất cả logs từ cả hai producers
- Console output với real-time status

### Log Levels
- **INFO:** Normal operations, thành công
- **WARNING:** Non-critical issues, fallbacks
- **ERROR:** Failures cần attention
- **CRITICAL:** System-level failures

### Metrics Tracked
- Số articles/posts/market records thành công
- Retry attempts và failures
- Processing time cho mỗi source
- Kafka connection health
- vnstock API response times
- CSV file processing stats

## Troubleshooting

### Common Issues

**1. Python 3.12 Compatibility Error**
```
AttributeError: module 'pkgutil' has no attribute 'ImpImporter'
```
- Sử dụng `python setup.py` thay vì `pip install -r requirements.txt`
- Hoặc cài từng package riêng như hướng dẫn trên
- Sử dụng confluent-kafka thay vì kafka-python

**2. Kafka Connection Failed**
```
ERROR - No Kafka brokers available
```
- Kiểm tra AWS_KAFKA_BROKER trong .env
- Verify EC2 instance và port 9092
- Check Security Group rules

**2. RSS Feed Parse Error**
```
WARNING - RSS feed has parsing issues
```
- Source website có thể thay đổi format
- Network timeout hoặc rate limiting
- Update RSS URLs nếu cần

**3. F319 Scraping Failed**
```
ERROR - Error scraping forum page
```
- Website có thể block requests
- Tăng F319_DELAY_SECONDS
- Check user agent và headers

**4. Memory/Performance Issues**
- Giảm max_pages_per_section
- Tăng intervals giữa các cycles
- Limit content length trong scrapers

**5. vnstock Connection Failed**
```
ERROR - vnstock connection test failed
```
- Kiểm tra internet connection
- Verify vnstock library version
- Check if vnstock API đang maintenance

**6. CSV File Not Found**
```
WARNING - CSV file not found: facebook_mock.csv
```
- Tạo file facebook_mock.csv theo format mẫu
- Check đường dẫn file trong FACEBOOK_MOCK_CSV
- Verify file permissions

**7. Market Data Validation Error**
```
WARNING - Invalid close price for VNINDEX: 0.0
```
- vnstock API trả về dữ liệu không hợp lệ
- Market có thể đóng cửa
- Check thời gian giao dịch

### Debug Commands

**Test Kafka Connection:**
```python
from kafka_producer_base import BaseKafkaProducer
producer = BaseKafkaProducer('test_topic')
print(producer.health_check())
```

**Test RSS Parser:**
```python
from rss_feeder import RSSFeeder
feeder = RSSFeeder()
articles = feeder.fetch_rss_feed('https://cafef.vn/rss', 'test')
print(f"Found {len(articles)} articles")
```

**Test F319 Scraper:**
```python
from f319_scraper import F319Scraper
scraper = F319Scraper()
posts = scraper.scrape_forum_section("", 1)
print(f"Found {len(posts)} posts")
```

**Test Market Data Producer:**
```python
from market_data_producer import MarketDataProducer
producer = MarketDataProducer()
print(producer.test_vnstock_connection())
results = producer.collect_once()
print(f"Market data: {results}")
```

**Test Facebook Mock Injector:**
```python
from facebook_mock_injector import FacebookMockInjector
injector = FacebookMockInjector()
csv_info = injector.get_csv_info()
print(f"CSV info: {csv_info}")
results = injector.inject_once()
print(f"Injection results: {results}")
```

## Configuration Tips

### Production Settings
```bash
# Tăng intervals để giảm load
RSS_INTERVAL_MINUTES=60
F319_INTERVAL_MINUTES=120
MARKET_DATA_INTERVAL_MINUTES=5

# Conservative retry settings
MAX_RETRY_ATTEMPTS=3
RETRY_BACKOFF_SECONDS=5

# Respectful delays
F319_DELAY_SECONDS=3
FB_MOCK_DELAY_SECONDS=2

# Disable mock injector in production
RUN_MOCK_INJECTOR=false
```

### Development Settings
```bash
# Nhanh hơn cho testing
RSS_INTERVAL_MINUTES=5
F319_INTERVAL_MINUTES=10
MARKET_DATA_INTERVAL_MINUTES=1

# Aggressive retry
MAX_RETRY_ATTEMPTS=5
RETRY_BACKOFF_SECONDS=1

# Faster processing
F319_DELAY_SECONDS=1
FB_MOCK_DELAY_SECONDS=0.5

# Enable mock injector for testing
RUN_MOCK_INJECTOR=true
```

## Security & Ethics

### Respectful Scraping
- Implement delays giữa requests
- Respect robots.txt
- Don't overload target servers
- Use appropriate user agents

### Data Privacy
- Chỉ thu thập public data
- Không lưu trữ personal information
- Follow website terms of service

### Error Handling
- Graceful degradation khi services unavailable
- Logging không expose sensitive data
- Proper cleanup of resources

## Extension & Customization

### Thêm RSS Sources Mới
1. Update RSS_FEEDS dictionary trong `rss_feeder.py`
2. Add custom content selectors nếu cần
3. Test với `collect_once()`

### Thêm Market Indices Mới
1. Update VNSTOCK_TICKERS trong .env
2. Add tickers vào list: "VNINDEX,VN30,HNX,UPCOM"
3. Test với `collect_once()`

### Thêm CSV Data Sources Mới
1. Create new CSV file theo format chuẩn
2. Update FACEBOOK_MOCK_CSV path
3. Verify với `validate_csv_format()`

### Custom Data Processing
1. Extend parsing functions trong các producers
2. Add custom validation rules
3. Implement custom retry strategies

Chúc may mắn với data pipeline! 🚀