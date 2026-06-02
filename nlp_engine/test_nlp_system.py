"""
Test script cho toàn bộ NLP Engine system
"""
import os
import json
import time
from pathlib import Path
from datetime import datetime

def test_document_ingester():
    """Test NHNN Document Ingester"""
    print("=" * 50)
    print("Testing NHNN Document Ingester")
    print("=" * 50)
    
    try:
        from nhnn_ingester import NHNNDocumentIngester
        
        # Initialize ingester
        ingester = NHNNDocumentIngester()
        
        # Test connection
        print("1. Testing ChromaDB connection...")
        stats = ingester.get_collection_stats()
        if 'error' not in stats:
            print(f"✅ ChromaDB connection successful")
            print(f"   Collection: {stats['collection_name']}")
            print(f"   Documents: {stats['document_count']}")
        else:
            print(f"❌ ChromaDB connection failed: {stats['error']}")
            return False
        
        # Test document processing
        print("\n2. Testing document processing...")
        
        # Create a test document
        test_doc_path = Path("nhnn_docs/test_document.txt")
        test_content = """
        Ngân hàng Nhà nước Việt Nam
        Thông tư số 01/2024/TT-NHNN
        
        Về việc điều hành chính sách tiền tệ năm 2024
        
        Căn cứ Luật Ngân hàng Nhà nước Việt Nam;
        Căn cứ nghị quyết của Chính phủ về chính sách tiền tệ;
        
        Ngân hàng Nhà nước quyết định:
        
        Điều 1. Mục tiêu chính sách tiền tệ
        - Kiểm soát lạm phát dưới 4%
        - Hỗ trợ tăng trưởng kinh tế bền vững
        - Ổn định tỷ giá và thị trường ngoại hối
        
        Điều 2. Công cụ chính sách tiền tệ
        - Lãi suất điều hành
        - Tỷ lệ dự trữ bắt buộc
        - Nghiệp vụ thị trường mở
        """
        
        with open(test_doc_path, 'w', encoding='utf-8') as f:
            f.write(test_content)
        
        # Process the test document
        if ingester.process_file(test_doc_path):
            print("✅ Document processing successful")
        else:
            print("❌ Document processing failed")
            return False
        
        # Test search functionality
        print("\n3. Testing document search...")
        search_results = ingester.search_documents("chính sách tiền tệ", k=3)
        if search_results:
            print(f"✅ Search successful - found {len(search_results)} results")
            print(f"   Sample result: {search_results[0]['doc_name']}")
        else:
            print("❌ Search failed")
            return False
        
        # Cleanup test document
        if test_doc_path.exists():
            test_doc_path.unlink()
        
        print("\n✅ Document Ingester test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Document Ingester test failed: {e}")
        return False

def test_sentiment_worker():
    """Test Sentiment Worker"""
    print("\n" + "=" * 50)
    print("Testing PhoBERT Sentiment Worker")
    print("=" * 50)
    
    try:
        from sentiment_worker import SentimentWorker
        
        # Initialize worker
        print("1. Initializing Sentiment Worker...")
        worker = SentimentWorker()
        print("✅ Sentiment Worker initialized successfully")
        
        # Test health check
        print("\n2. Testing health check...")
        health = worker.health_check()
        if health['status'] == 'healthy':
            print("✅ Health check passed")
            print(f"   Model type: {health.get('model_type', 'unknown')}")
        else:
            print(f"❌ Health check failed: {health.get('error', 'Unknown error')}")
            return False
        
        # Test sentiment analysis
        print("\n3. Testing sentiment analysis...")
        test_texts = [
            "Thị trường chứng khoán hôm nay tăng mạnh, VN-Index vượt mốc 1250 điểm!",
            "Tình hình kinh tế đang gặp nhiều khó khăn và thách thức.",
            "Ngân hàng Nhà nước giữ nguyên lãi suất điều hành.",
            "Cổ phiếu ngân hàng có dấu hiệu phục hồi tích cực."
        ]
        
        for text in test_texts:
            result = worker._analyze_sentiment(text)
            print(f"   Text: {text[:50]}...")
            print(f"   Sentiment: {result['label']} (confidence: {result['confidence']:.2f})")
        
        print("\n✅ Sentiment Worker test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Sentiment Worker test failed: {e}")
        return False

def test_integration():
    """Test integration between modules"""
    print("\n" + "=" * 50)
    print("Testing System Integration")
    print("=" * 50)
    
    try:
        # Test message format compatibility
        print("1. Testing message format compatibility...")
        
        # Sample messages from different sources
        sample_messages = {
            'f319_data': {
                'post_id': 'f319_test_001',
                'content_text': 'Thị trường hôm nay khá tích cực với VN-Index tăng điểm.',
                'created_at': datetime.now().isoformat(),
                'engagement_metrics': {'views': 100, 'replies': 5},
                'title': 'Nhận định thị trường'
            },
            'fb_mock_data': {
                'comment_id': 'fb_test_001',
                'content_text': 'Tôi nghĩ nên mua thêm cổ phiếu ngân hàng lúc này.',
                'created_at': datetime.now().isoformat(),
                'likes': 15
            }
        }
        
        # Test sentiment processing for each message type
        from sentiment_worker import SentimentWorker
        worker = SentimentWorker()
        
        for topic, message in sample_messages.items():
            print(f"\n   Testing {topic} message format...")
            text_content = worker._extract_text_content(message, topic)
            if text_content:
                sentiment = worker._analyze_sentiment(text_content)
                enhanced = worker._process_message(message, topic)
                if enhanced and 'sentiment' in enhanced:
                    print(f"   ✅ {topic} format processed successfully")
                    print(f"      Sentiment: {enhanced['sentiment']['label']}")
                else:
                    print(f"   ❌ {topic} format processing failed")
                    return False
            else:
                print(f"   ❌ No text content extracted from {topic}")
                return False
        
        print("\n✅ Integration test completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        return False

def test_configuration():
    """Test system configuration"""
    print("\n" + "=" * 50)
    print("Testing System Configuration")
    print("=" * 50)
    
    # Check environment variables
    print("1. Checking environment variables...")
    
    required_vars = [
        'AWS_CHROMA_HOST',
        'AWS_KAFKA_BROKER'
    ]
    
    from dotenv import load_dotenv
    load_dotenv()
    
    for var in required_vars:
        value = os.getenv(var)
        if value and value != f'your-ec2-ip:8000' and value != f'your-ec2-ip:9092':
            print(f"   ✅ {var}: {value}")
        else:
            print(f"   ❌ {var}: Not configured or using default value")
            print(f"      Please update {var} in .env file")
    
    # Check directories
    print("\n2. Checking directories...")
    required_dirs = ['nhnn_docs', 'nhnn_docs/pdf', 'nhnn_docs/docx', 'nhnn_docs/txt']
    
    for dir_path in required_dirs:
        if Path(dir_path).exists():
            print(f"   ✅ {dir_path}")
        else:
            print(f"   ❌ {dir_path} - Creating...")
            Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    print("\n✅ Configuration check completed")
    return True

def main():
    """Main test function"""
    print("=" * 60)
    print("NLP ENGINE SYSTEM TEST")
    print("=" * 60)
    
    # Configuration test
    config_ok = test_configuration()
    
    # Document ingester test
    ingester_ok = test_document_ingester()
    
    # Sentiment worker test  
    sentiment_ok = test_sentiment_worker()
    
    # Integration test
    integration_ok = test_integration()
    
    # Final summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    tests = {
        'Configuration': config_ok,
        'Document Ingester': ingester_ok,
        'Sentiment Worker': sentiment_ok,
        'System Integration': integration_ok
    }
    
    all_passed = True
    for test_name, passed in tests.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name:20} {status}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 ALL TESTS PASSED! NLP Engine is ready to use.")
        print("\nNext steps:")
        print("1. Add documents to nhnn_docs/ directory")
        print("2. Run: python nhnn_ingester.py --mode scan")
        print("3. Run: python sentiment_worker.py --mode run")
    else:
        print("⚠️  Some tests failed. Please check configuration and dependencies.")
    print("=" * 60)

if __name__ == "__main__":
    main()