#!/usr/bin/env python3
"""
Simple test for MacroAgent implementation.
Tests only the MacroAgent functionality without importing other modules.
"""

import os
import sys
import traceback
from pathlib import Path

# Add the project root to Python path for proper module resolution
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import required modules with absolute imports
sys.path.insert(0, str(project_root / 'agents'))
sys.path.insert(0, str(project_root / 'utils'))

# Direct imports to avoid relative import issues
from macro_agent import MacroAgent, PolicyDocument

def test_macro_agent():
    """Test MacroAgent functionality."""
    print("Testing MacroAgent Implementation")
    print("=" * 40)
    
    try:
        # Test initialization
        print("\n1. Testing initialization...")
        agent = MacroAgent()
        print("✅ MacroAgent initialized successfully")
        
        # Test health check  
        print("\n2. Testing health check...")
        health = agent.health_check()
        print(f"Health status: {health.get('status', 'unknown')}")
        
        if health.get('status') == 'healthy':
            print("✅ ChromaDB connection is healthy")
            
            # Test policy query
            print("\n3. Testing policy query...")
            try:
                policies = agent.query_nhnn_policies(similarity_threshold=0.5)
                print(f"✅ Retrieved {len(policies)} policies")
                
                # Show sample policy if available
                if policies:
                    sample = policies[0]
                    print(f"Sample policy similarity: {sample.similarity_score:.3f}")
                    print(f"Content preview: {sample.content[:100]}...")
                
                # Test sentiment analysis
                print("\n4. Testing sentiment analysis...")
                s_nhnn, summary, confidence = agent.analyze_policy_sentiment(policies)
                print(f"✅ S_nhnn: {s_nhnn}, Confidence: {confidence:.2f}")
                print(f"Summary: {summary[:100]}...")
                
                # Test complete macro score
                print("\n5. Testing macro score generation...")
                result = agent.get_macro_score()
                print(f"✅ Macro score result: {result}")
                
                # Validate requirements
                print("\n6. Validating requirements...")
                
                # Requirement 3.1: ChromaDB connection to 'macro_policies'
                if 'macro_policies' in agent.collection_name:
                    print("✅ Requirement 3.1: Connected to 'macro_policies' collection")
                
                # Requirement 3.2: S_nhnn score validation
                if s_nhnn in [-1, 0, 1]:
                    print("✅ Requirement 3.2: S_nhnn score is valid (-1, 0, 1)")
                
                # Requirement 3.4: Neutral score when no policies
                empty_score, _, _ = agent.analyze_policy_sentiment([])
                if empty_score == 0:
                    print("✅ Requirement 3.4: Returns 0 when no policies found")
                
                # Requirement 3.5: Semantic similarity search
                if hasattr(agent, 'query_nhnn_policies'):
                    print("✅ Requirement 3.5: Semantic similarity search implemented")
                
                # Requirement 7.1: ChromaDB integration 
                if agent._connection_healthy:
                    print("✅ Requirement 7.1: ChromaDB integration working")
                
                # Requirement 7.2: Configurable similarity threshold
                try:
                    agent.query_nhnn_policies(similarity_threshold=0.8)
                    print("✅ Requirement 7.2: Configurable similarity threshold works")
                except:
                    print("❌ Requirement 7.2: Similarity threshold configuration failed")
                    
            except Exception as e:
                print(f"❌ Policy operations failed: {e}")
                
        else:
            print(f"❌ ChromaDB connection unhealthy: {health}")
            print("Note: Ensure ChromaDB is running and 'macro_policies' collection exists")
            
    except Exception as e:
        print(f"❌ MacroAgent test failed: {e}")
        print(f"Traceback: {traceback.format_exc()}")
    
    print("\n" + "=" * 40)
    print("MacroAgent test completed")

if __name__ == "__main__":
    test_macro_agent()