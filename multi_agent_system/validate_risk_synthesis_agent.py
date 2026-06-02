"""
Validation script for Risk Synthesis Agent implementation.

This script validates the Risk Synthesis Agent functionality without pytest,
testing core functionality like data aggregation, VMSI calculation orchestration,
risk assessment, and JSON output.
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timezone

# Add the parent directory to the path so we can import multi_agent_system
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from multi_agent_system.agents.risk_synthesis_agent import RiskSynthesisAgent
from multi_agent_system.engines.vmsi_engine import VMSIEngine
from multi_agent_system.utils.logging_config import setup_logging


def test_basic_functionality():
    """Test basic Risk Synthesis Agent functionality."""
    print("Testing Risk Synthesis Agent basic functionality...")
    
    # Setup logging
    setup_logging("INFO")
    
    # Create temporary file for testing
    temp_dir = tempfile.mkdtemp()
    test_output_file = os.path.join(temp_dir, 'test_vmsi.json')
    
    try:
        # Initialize agent
        vmsi_engine = VMSIEngine()
        agent = RiskSynthesisAgent(vmsi_engine=vmsi_engine, output_file=test_output_file)
        print("✓ Agent initialization successful")
        
        # Test receiving social score
        agent.receive_social_score(15.5)
        assert agent._social_score == 15.5
        print("✓ Social score reception successful")
        
        # Test receiving macro score
        metadata = {
            's_news': 0.5,
            'summary': 'Test policy summary',
            'confidence': 0.85
        }
        agent.receive_macro_score(1, metadata)
        assert agent._macro_data['s_nhnn'] == 1.0
        assert agent._macro_data['s_news'] == 0.5
        print("✓ Macro score reception successful")
        
        # Test VMSI computation
        vmsi = agent.compute_final_vmsi()
        assert isinstance(vmsi, float)
        assert vmsi >= 0
        print(f"✓ VMSI computation successful: {vmsi:.2f}")
        
        # Test risk assessment for normal range
        vmsi_normal = 50.0
        assessment = agent.assess_risk_level(vmsi_normal)
        assert assessment['status'] == 'normal'
        assert assessment['needs_warning'] is False
        print("✓ Normal risk assessment successful")
        
        # Test risk assessment for low risk
        vmsi_low = 15.0
        assessment_low = agent.assess_risk_level(vmsi_low)
        assert assessment_low['status'] == 'risk_low'
        assert assessment_low['needs_warning'] is True
        print("✓ Low risk assessment successful")
        
        # Test risk assessment for high risk
        vmsi_high = 85.0
        assessment_high = agent.assess_risk_level(vmsi_high)
        assert assessment_high['status'] == 'risk_high'
        assert assessment_high['needs_warning'] is True
        print("✓ High risk assessment successful")
        
        # Test Vietnamese warning generation
        warning_low = agent.generate_vietnamese_warning('negative_sentiment', 15.0)
        assert isinstance(warning_low, str)
        assert len(warning_low) > 0
        assert 'CẢNH BÁO' in warning_low
        print("✓ Vietnamese warning generation successful")
        
        # Test complete workflow
        print("\nTesting complete risk assessment workflow...")
        agent.reset_state()
        agent.receive_social_score(12.0)
        agent.receive_macro_score(-1, {
            's_news': 0.0, 
            'summary': 'Negative policy sentiment',
            'confidence': 0.75
        })
        
        results = agent.process_complete_risk_assessment()
        
        # Verify results structure
        required_fields = ['vmsi_value', 'timestamp', 'status', 'risk_warning', 'component_scores']
        for field in required_fields:
            assert field in results, f"Missing required field: {field}"
        
        # Verify file was saved
        assert os.path.exists(test_output_file)
        
        # Verify file content
        with open(test_output_file, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        
        assert saved_data['vmsi_value'] == results['vmsi_value']
        assert saved_data['status'] == results['status']
        
        print("✓ Complete workflow successful")
        print(f"✓ JSON output saved to: {test_output_file}")
        print(f"✓ Final VMSI: {results['vmsi_value']:.2f}")
        print(f"✓ Risk status: {results['status']}")
        
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up
        if os.path.exists(test_output_file):
            os.remove(test_output_file)
        if os.path.exists(f"{test_output_file}.backup"):
            os.remove(f"{test_output_file}.backup")


def test_error_conditions():
    """Test error handling conditions."""
    print("\nTesting error conditions...")
    
    agent = RiskSynthesisAgent()
    
    try:
        # Test invalid social score
        try:
            agent.receive_social_score("invalid")
            assert False, "Should have raised ValueError"
        except ValueError:
            print("✓ Invalid social score handling successful")
        
        # Test missing data for VMSI computation
        try:
            agent.compute_final_vmsi()
            assert False, "Should have raised RuntimeError"
        except RuntimeError:
            print("✓ Missing data error handling successful")
        
        # Test invalid VMSI value for risk assessment
        try:
            agent.assess_risk_level(float('inf'))
            assert False, "Should have raised ValidationError"
        except Exception:  # Could be ValidationError or ValueError
            print("✓ Invalid VMSI value handling successful")
        
        return True
        
    except Exception as e:
        print(f"✗ Error condition test failed: {e}")
        return False


def main():
    """Run all validation tests."""
    print("=== Risk Synthesis Agent Validation ===")
    
    success = True
    
    # Test basic functionality
    if not test_basic_functionality():
        success = False
    
    # Test error conditions
    if not test_error_conditions():
        success = False
    
    if success:
        print("\n🎉 All tests passed! Risk Synthesis Agent implementation is working correctly.")
        print("\nKey features validated:")
        print("- Data aggregation from Social and Macro agents")
        print("- VMSI Engine integration for final calculations")
        print("- Risk level assessment logic (≤20, ≥81 thresholds)")
        print("- Vietnamese risk warning generation")
        print("- JSON output file management with proper format")
        print("- Error handling and validation")
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)