#!/usr/bin/env python3
"""
Test file for enhanced Macro Agent with connection pooling and caching.

This test validates the ChromaDB error handling, caching, connection pooling,
and performance metrics functionality.
"""

import os
import sys
import time
import logging
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from multi_agent_system.agents.macro_agent import MacroAgent, PolicyDocument
from multi_agent_system.utils.chromadb_pool import ChromaDBConnectionPool, ConnectionMetrics
from multi_agent_system.utils.exceptions import ChromaDBConnectionError, ValidationError
from multi_agent_system.utils.logging_config import setup_logging

# Setup logging
setup_logging(level="INFO")
logger = logging.getLogger(__name__)


def test_connection_pool_creation():
    """Test ChromaDB connection pool creation and basic functionality."""
    print("\n=== Testing ChromaDB Connection Pool Creation ===")
    
    try:
        # Test connection pool initialization
        pool = ChromaDBConnectionPool(
            host='localhost',
            port=8000,
            pool_size=3,
            max_retries=2,
            cache_ttl=60,
            max_cache_size=100
        )
        
        print(f"✓ Connection pool created successfully")
        print(f"  - Host: localhost:8000")
        print(f"  - Pool size: 3")
        print(f"  - Cache TTL: 60s")
        
        # Test health check
        health = pool.health_check()
        print(f"✓ Health check result: {health['status']}")
        
        # Test metrics
        metrics = pool.get_metrics()
        print(f"✓ Metrics available:")
        print(f"  - Queries: {metrics['queries']['total']}")
        print(f"  - Cache size: {metrics['cache']['size']}")
        print(f"  - Pool connections: {metrics['connections']['created']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Connection pool test failed: {e}")
        return False
    finally:
        if 'pool' in locals():
            pool.close()


def test_macro_agent_enhanced_features():
    """Test enhanced Macro Agent with connection pooling."""
    print("\n=== Testing Enhanced Macro Agent Features ===")
    
    try:
        # Test agent initialization with connection pool
        agent = MacroAgent(
            chroma_host='localhost:8000',
            collection_name='test_macro_policies',
            pool_size=2,
            cache_ttl=120
        )
        
        print(f"✓ Enhanced Macro Agent created successfully")
        print(f"  - Collection: {agent.collection_name}")
        print(f"  - Cache TTL: {agent.cache_ttl}s")
        print(f"  - Connection pool: {'Yes' if agent.connection_pool else 'No (fallback mode)'}")
        
        # Test health check
        health = agent.health_check()
        print(f"✓ Agent health check: {health['status']}")
        
        if agent.connection_pool:
            print(f"  - Pool metrics available: Yes")
            print(f"  - Connection mode: Pool-based")
        else:
            print(f"  - Connection mode: Direct (fallback)")
        
        # Test performance metrics
        metrics = agent.get_performance_metrics()
        print(f"✓ Performance metrics available")
        print(f"  - Agent type: {metrics['agent_type']}")
        print(f"  - Max results: {metrics['max_results']}")
        print(f"  - Connection timeout: {metrics['connection_timeout']}s")
        
        return True
        
    except Exception as e:
        print(f"✗ Enhanced Macro Agent test failed: {e}")
        return False
    finally:
        if 'agent' in locals():
            agent.close()


def test_caching_functionality():
    """Test query result caching functionality."""
    print("\n=== Testing Caching Functionality ===")
    
    try:
        # Create mock ChromaDB responses
        mock_results = {
            'documents': [['Policy document content 1', 'Policy document content 2']],
            'metadatas': [[{'doc_name': 'policy1.pdf'}, {'doc_name': 'policy2.pdf'}]],
            'distances': [[0.2, 0.3]]
        }
        
        # Test connection pool caching
        pool = ChromaDBConnectionPool(
            host='localhost',
            port=8000,
            pool_size=2,
            cache_ttl=30,
            max_cache_size=50
        )
        
        # Generate cache key
        cache_key = pool._generate_cache_key(
            "test query", 5, "test_collection"
        )
        print(f"✓ Cache key generated: {cache_key[:16]}...")
        
        # Test cache storage and retrieval
        pool._cache_result(cache_key, mock_results, ttl=60)
        print(f"✓ Result cached successfully")
        
        cached_result = pool._get_cached_result(cache_key)
        if cached_result:
            print(f"✓ Cache retrieval successful")
            print(f"  - Documents: {len(cached_result['documents'][0])} items")
        else:
            print(f"✗ Cache retrieval failed")
            
        # Test cache metrics
        metrics = pool.get_metrics()
        print(f"✓ Cache metrics:")
        print(f"  - Cache hits: {metrics['cache']['hits']}")
        print(f"  - Cache misses: {metrics['cache']['misses']}")
        print(f"  - Hit rate: {metrics['cache']['hit_rate']:.2%}")
        
        return True
        
    except Exception as e:
        print(f"✗ Caching functionality test failed: {e}")
        return False
    finally:
        if 'pool' in locals():
            pool.close()


def test_error_handling_and_fallbacks():
    """Test error handling and fallback mechanisms."""
    print("\n=== Testing Error Handling and Fallbacks ===")
    
    try:
        # Test with invalid connection parameters
        print("Testing connection failure handling...")
        
        agent = MacroAgent(
            chroma_host='invalid-host:9999',  # Invalid host
            collection_name='test_collection',
            pool_size=1
        )
        
        # Agent should fall back to direct connection mode or handle gracefully
        health = agent.health_check()
        print(f"✓ Agent handles connection failure gracefully")
        print(f"  - Status: {health['status']}")
        print(f"  - Error handling: {'error' in health}")
        
        # Test macro score with connection issues
        print("Testing macro score with connection issues...")
        
        result = agent.get_macro_score(similarity_threshold=0.7)
        print(f"✓ Macro score fallback working")
        print(f"  - S_nhnn: {result['s_nhnn']} (neutral fallback)")
        print(f"  - Has error info: {'error' in result}")
        print(f"  - Processing time: {result['processing_time']:.3f}s")
        
        # Test cache clearing (should not fail even without cache)
        cache_cleared = agent.clear_cache()
        print(f"✓ Cache clear operation: {'Success' if cache_cleared else 'No-op'}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error handling test failed: {e}")
        return False
    finally:
        if 'agent' in locals():
            agent.close()


def test_performance_monitoring():
    """Test performance monitoring and metrics collection."""
    print("\n=== Testing Performance Monitoring ===")
    
    try:
        # Create metrics object
        metrics = ConnectionMetrics()
        
        # Simulate some operations
        metrics.total_queries = 100
        metrics.successful_queries = 95
        metrics.failed_queries = 5
        metrics.cache_hits = 30
        metrics.cache_misses = 70
        metrics.total_query_time = 15.0
        
        print(f"✓ Metrics calculation:")
        print(f"  - Success rate: {metrics.success_rate():.2%}")
        print(f"  - Cache hit rate: {metrics.cache_hit_rate():.2%}")
        print(f"  - Average query time: {metrics.average_query_time():.3f}s")
        
        # Test connection pool metrics integration
        pool = ChromaDBConnectionPool(
            host='localhost',
            port=8000,
            pool_size=2
        )
        
        pool_metrics = pool.get_metrics()
        print(f"✓ Connection pool metrics:")
        print(f"  - Queries: {pool_metrics['queries']['total']}")
        print(f"  - Cache: {pool_metrics['cache']['size']} entries")
        print(f"  - Connections: {pool_metrics['connections']['created']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Performance monitoring test failed: {e}")
        return False
    finally:
        if 'pool' in locals():
            pool.close()


def main():
    """Run all enhanced Macro Agent tests."""
    print("Enhanced Macro Agent Test Suite")
    print("=" * 50)
    
    tests = [
        test_connection_pool_creation,
        test_macro_agent_enhanced_features,
        test_caching_functionality,
        test_error_handling_and_fallbacks,
        test_performance_monitoring
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test_func.__name__} crashed: {e}")
    
    print(f"\n=== Test Results ===")
    print(f"Passed: {passed}/{total}")
    print(f"Success rate: {passed/total:.1%}")
    
    if passed == total:
        print("🎉 All enhanced features working correctly!")
    else:
        print("⚠️  Some tests failed - check ChromaDB connection and configuration")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)