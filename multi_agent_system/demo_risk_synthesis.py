"""
Demonstration script showing Risk Synthesis Agent functionality.

This script demonstrates the complete workflow of the Risk Synthesis Agent,
including data aggregation, VMSI calculation, risk assessment, and output generation.
"""

import sys
import os
import json

# Add the parent directory to the path so we can import multi_agent_system
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from multi_agent_system.agents.risk_synthesis_agent import RiskSynthesisAgent
from multi_agent_system.engines.vmsi_engine import VMSIEngine
from multi_agent_system.utils.logging_config import setup_logging


def demo_normal_scenario():
    """Demonstrate normal risk scenario."""
    print("=== Normal Risk Scenario Demo ===")
    
    # Initialize agent
    agent = RiskSynthesisAgent(output_file='demo_normal_vmsi.json')
    
    # Simulate receiving data from Social Agent (moderate positive sentiment)
    agent.receive_social_score(8.5)
    print("📱 Received social score: 8.5 (moderate positive sentiment)")
    
    # Simulate receiving data from Macro Agent (neutral policy)
    macro_metadata = {
        's_news': 0.2,
        'summary': 'Chính sách tiền tệ ổn định, không có thay đổi lãi suất',
        'confidence': 0.78
    }
    agent.receive_macro_score(0, macro_metadata)
    print("🏛️ Received macro score: s_nhnn=0 (neutral policy)")
    
    # Process complete assessment
    results = agent.process_complete_risk_assessment()
    
    print(f"📊 Final VMSI: {results['vmsi_value']:.2f}")
    print(f"⚠️ Risk Status: {results['status']}")
    print(f"💬 Warning: {results['risk_warning']}")
    
    return results


def demo_high_risk_scenario():
    """Demonstrate high risk scenario."""
    print("\n=== High Risk Scenario Demo ===")
    
    # Initialize new agent
    agent = RiskSynthesisAgent(output_file='demo_high_risk_vmsi.json')
    
    # Simulate receiving very negative social sentiment
    agent.receive_social_score(-15.2)
    print("📱 Received social score: -15.2 (very negative sentiment)")
    
    # Simulate receiving negative policy sentiment
    macro_metadata = {
        's_news': -0.8,
        'summary': 'Chính sách thắt chặt tiền tệ, tăng lãi suất mạnh',
        'confidence': 0.92
    }
    agent.receive_macro_score(-1, macro_metadata)
    print("🏛️ Received macro score: s_nhnn=-1 (restrictive policy)")
    
    # Process complete assessment
    results = agent.process_complete_risk_assessment()
    
    print(f"📊 Final VMSI: {results['vmsi_value']:.2f}")
    print(f"⚠️ Risk Status: {results['status']}")
    print(f"💬 Warning: {results['risk_warning']}")
    
    return results


def demo_excessive_optimism_scenario():
    """Demonstrate excessive optimism scenario."""
    print("\n=== Excessive Optimism Scenario Demo ===")
    
    # Initialize new agent
    agent = RiskSynthesisAgent(output_file='demo_optimism_vmsi.json')
    
    # Simulate receiving very positive social sentiment
    agent.receive_social_score(25.8)
    print("📱 Received social score: 25.8 (very positive sentiment)")
    
    # Simulate receiving positive policy sentiment
    macro_metadata = {
        's_news': 0.9,
        'summary': 'Chính sách kích thích kinh tế mạnh mẽ, giảm lãi suất',
        'confidence': 0.85
    }
    agent.receive_macro_score(1, macro_metadata)
    print("🏛️ Received macro score: s_nhnn=1 (expansionary policy)")
    
    # Process complete assessment
    results = agent.process_complete_risk_assessment()
    
    print(f"📊 Final VMSI: {results['vmsi_value']:.2f}")
    print(f"⚠️ Risk Status: {results['status']}")
    print(f"💬 Warning: {results['risk_warning']}")
    
    return results


def show_json_output(filename):
    """Show the JSON output from the file."""
    if os.path.exists(filename):
        print(f"\n📄 JSON Output ({filename}):")
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        # Clean up demo file
        os.remove(filename)


def main():
    """Run demonstration scenarios."""
    # Setup logging
    setup_logging("WARNING")  # Reduce log verbosity for demo
    
    print("🎯 Risk Synthesis Agent Demonstration")
    print("=" * 50)
    
    # Demo normal scenario
    normal_results = demo_normal_scenario()
    show_json_output('demo_normal_vmsi.json')
    
    # Demo high risk scenario  
    high_risk_results = demo_high_risk_scenario()
    show_json_output('demo_high_risk_vmsi.json')
    
    # Demo excessive optimism scenario
    optimism_results = demo_excessive_optimism_scenario()
    show_json_output('demo_optimism_vmsi.json')
    
    print("\n📈 Summary of Scenarios:")
    print(f"Normal Risk: VMSI {normal_results['vmsi_value']:.1f} → {normal_results['status']}")
    print(f"High Risk: VMSI {high_risk_results['vmsi_value']:.1f} → {high_risk_results['status']}")
    print(f"Optimism Risk: VMSI {optimism_results['vmsi_value']:.1f} → {optimism_results['status']}")
    
    print("\n✅ Demonstration completed!")
    print("\nKey Features Demonstrated:")
    print("- Data aggregation from Social and Macro agents")
    print("- VMSI Engine integration with proper calculations")
    print("- Risk threshold detection (≤20 low, ≥81 high)")
    print("- Vietnamese risk warning generation")
    print("- JSON output with complete metadata")
    print("- EMA smoothing for temporal stability")


if __name__ == "__main__":
    main()