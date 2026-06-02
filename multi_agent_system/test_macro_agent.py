#!/usr/bin/env python3
"""
Test script for MacroAgent implementation.

This script tests the MacroAgent class functionality including:
- ChromaDB connection and collection access
- Semantic similarity search
- Policy sentiment analysis
- S_nhnn score generation
"""

import os
import sys
import traceback
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

from agents.macro_agent import MacroAgent, PolicyDocument
from utils.logging_config import get_logger

logger = get_logger('test_macro_agent')


def test_macro_agent_initialization():
    """Test MacroAgent initialization and connection."""
    print("\n=== Testing MacroAgent Initialization ===")
    
    try:
        # Test with default configuration
        agent = MacroAgent()
        print("✅ MacroAgent initialized successfully")
        
        # Test health check
        health = agent.health_check()
        print(f"Health status: {health}")
        
        if health['status'] == 'healthy':
            print("✅ MacroAgent health check passed")
            return agent
        else:
            print("❌ MacroAgent health check failed")
            return None
            
    except Exception as e:
        print(f"❌ MacroAgent initialization failed: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return None


def test_policy_query(agent: MacroAgent):
    """Test policy querying functionality."""
    print("\n=== Testing Policy Query ===")
    
    try:
        # Test with default threshold
        policies = agent.query_nhnn_policies(similarity_threshold=0.5)
        print(f"✅ Retrieved {len(policies)} policies with threshold 0.5")
        
        # Display sample policies
        for i, policy in enumerate(policies[:3]):
            print(f"Policy {i+1}:")
            print(f"  Similarity: {policy.similarity_score:.3f}")
            print(f"  Content preview: {policy.content[:100]}...")
            print(f"  Metadata: {policy.metadata}")
            print()
        
        # Test with higher threshold
        strict_policies = agent.query_nhnn_policies(similarity_threshold=0.8)
        print(f"✅ Retrieved {len(strict_policies)} policies with threshold 0.8")
        
        return policies
        
    except Exception as e:
        print(f"❌ Policy query failed: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return []


def test_sentiment_analysis(agent: MacroAgent, policies):
    """Test policy sentiment analysis."""
    print("\n=== Testing Sentiment Analysis ===")
    
    try:
        if not policies:
            print("No policies to analyze, testing with empty list")
            s_nhnn, summary, confidence = agent.analyze_policy_sentiment([])
            print(f"Empty analysis: S_nhnn={s_nhnn}, confidence={confidence:.2f}")
            print(f"Summary: {summary}")
            return
        
        # Analyze retrieved policies
        s_nhnn, summary, confidence = agent.analyze_policy_sentiment(policies)
        
        print(f"✅ Sentiment analysis completed")
        print(f"S_nhnn score: {s_nhnn}")
        print(f"Confidence: {confidence:.2f}")
        print(f"Summary: {summary}")
        
        # Validate score range
        if s_nhnn in [-1, 0, 1]:
            print("✅ S_nhnn score is valid (-1, 0, or 1)")
        else:
            print(f"❌ Invalid S_nhnn score: {s_nhnn}")
            
        # Validate confidence range
        if 0.0 <= confidence <= 1.0:
            print("✅ Confidence level is valid (0.0 to 1.0)")
        else:
            print(f"❌ Invalid confidence level: {confidence}")
            
    except Exception as e:
        print(f"❌ Sentiment analysis failed: {e}")
        print(f"Traceback: {traceback.format_exc()}")


def test_macro_score_generation(agent: MacroAgent):
    """Test complete macro score generation."""
    print("\n=== Testing Macro Score Generation ===")
    
    try:
        # Test with default threshold
        result = agent.get_macro_score()
        
        print("✅ Macro score generation completed")
        print(f"Result: {result}")
        
        # Validate required fields
        required_fields = ['s_nhnn', 'summary', 'confidence', 'num_policies', 'processing_time']
        for field in required_fields:
            if field in result:
                print(f"✅ Field '{field}' present: {result[field]}")
            else:
                print(f"❌ Missing field: {field}")
                
        # Test with different threshold
        strict_result = agent.get_macro_score(similarity_threshold=0.8)
        print(f"\nStrict threshold result: {strict_result}")
        
    except Exception as e:
        print(f"❌ Macro score generation failed: {e}")
        print(f"Traceback: {traceback.format_exc()}")


def test_error_handling():
    """Test error handling scenarios."""
    print("\n=== Testing Error Handling ===")
    
    try:
        # Test with invalid host
        print("Testing invalid ChromaDB host...")
        try:
            invalid_agent = MacroAgent(chroma_host="invalid_host:9999")
            print("❌ Should have failed with invalid host")
        except Exception as e:
            print(f"✅ Correctly handled invalid host: {type(e).__name__}")
        
        # Test with invalid threshold
        print("Testing invalid similarity threshold...")
        agent = MacroAgent()
        try:
            agent.query_nhnn_policies(similarity_threshold=1.5)
            print("❌ Should have failed with invalid threshold")
        except Exception as e:
            print(f"✅ Correctly handled invalid threshold: {type(e).__name__}")
            
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")


def main():
    """Run all MacroAgent tests."""
    print("Starting MacroAgent Test Suite")
    print("=" * 50)
    
    # Check environment variables
    chroma_host = os.getenv('AWS_CHROMA_HOST')
    collection_name = os.getenv('COLLECTION_NAME', 'macro_policies')
    
    if not chroma_host:
        print("Warning: AWS_CHROMA_HOST not set, using default localhost:8000")
        print("Note: For full testing, ensure ChromaDB is running and configured")
    
    print(f"ChromaDB Host: {chroma_host or 'localhost:8000'}")
    print(f"Collection: {collection_name}")
    
    # Run tests
    agent = test_macro_agent_initialization()
    
    if agent:
        policies = test_policy_query(agent)
        test_sentiment_analysis(agent, policies)
        test_macro_score_generation(agent)
    
    test_error_handling()
    
    print("\n" + "=" * 50)
    print("MacroAgent Test Suite Completed")


if __name__ == "__main__":
    main()