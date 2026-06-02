"""
Quick Start Script để test producers sau khi cài đặt
"""
import sys
import os

def test_imports():
    """Test xem các dependencies đã cài đúng chưa"""
    print("Testing imports...")
    
    try:
        import requests
        print("✅ requests")
    except ImportError as e:
        print(f"❌ requests: {e}")
        return False
    
    try:
        from dotenv import load_dotenv
        print("✅ python-dotenv")
    except ImportError as e:
        print(f"❌ python-dotenv: {e}")
        return False
    
    try:
        from confluent_kafka import Producer
        print("✅ confluent-kafka")
    except ImportError as e:
        print(f"❌ confluent-kafka: {e}")
        return False
    
    try:
        import pandas as pd
        print("✅ pandas")
    except ImportError as e:
        print(f"❌ pandas: {e}")
        return False
    
    try:
        import feedparser
        print("✅ feedparser")
    except ImportError as e:
        print(f"❌ feedparser: {e}")
        return False
    
    try:
        from bs4 import BeautifulSoup
        print("✅ beautifulsoup4")
    except ImportError as e:
        print(f"❌ beautifulsoup4: {e}")
        return False
    
    try:
        import vnstock
        print("✅ vnstock")
    except ImportError as e:
        print(f"❌ vnstock: {e}")
        print("   Note: vnstock is optional, producers will work without it")
        
    return True

def test_env_file():
    """Test .env file"""
    print("\nTesting .env configuration...")
    
    if not os.path.exists('.env'):
        if os.path.exists('.env.example'):
            print("❌ .env file not found")
            print("💡 Run: cp .env.example .env")
            return False
        else:
            print("❌ .env.example not found")
            return False
    
    print("✅ .env file exists")
    
    from dotenv import load_dotenv
    load_dotenv()
    
    broker = os.getenv('AWS_KAFKA_BROKER')
    if not broker or broker == 'your-ec2-ip:9092':
        print("❌ AWS_KAFKA_BROKER not configured")
        print("💡 Update AWS_KAFKA_BROKER in .env file")
        return False
    
    print(f"✅ AWS_KAFKA_BROKER: {broker}")
    return True

def test_kafka_connection():
    """Test Kafka connection"""
    print("\nTesting Kafka connection...")
    
    try:
        from kafka_producer_base import BaseKafkaProducer
        
        # Test với dummy topic
        producer = BaseKafkaProducer('test-topic')
        if producer.health_check():
            print("✅ Kafka connection successful")
            producer.close()
            return True
        else:
            print("❌ Kafka connection failed")
            print("💡 Check if Kafka is running and AWS_KAFKA_BROKER is correct")
            producer.close()
            return False
            
    except Exception as e:
        print(f"❌ Kafka connection error: {e}")
        return False

def main():
    """Main quick start function"""
    print("=" * 60)
    print("PYTHON DATA PIPELINE PRODUCERS - QUICK START")
    print("=" * 60)
    
    # Test imports
    if not test_imports():
        print("\n❌ Some imports failed. Run setup.py first:")
        print("   python setup.py")
        return False
    
    # Test .env
    if not test_env_file():
        print("\n❌ Environment configuration incomplete")
        return False
    
    # Test Kafka connection (optional)
    print("\nTesting Kafka connection (optional)...")
    test_kafka_connection()
    
    print("\n" + "=" * 60)
    print("QUICK START COMPLETE!")
    print("=" * 60)
    print("Next steps:")
    print("1. python main.py health       # Check all producers")
    print("2. python main.py single       # Run single collection")
    print("3. python main.py continuous   # Run continuous mode")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)