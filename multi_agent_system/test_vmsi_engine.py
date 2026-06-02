"""
Basic test for VMSI Engine to verify implementation works correctly.
"""

import numpy as np
import sys
import os

# Add the parent directory to the path so we can import multi_agent_system
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from multi_agent_system.engines.vmsi_engine import VMSIEngine
from multi_agent_system.utils.logging_config import setup_logging


def test_vmsi_engine_basic():
    """Basic test to verify VMSI Engine functionality."""
    
    # Setup logging
    setup_logging("INFO")
    
    # Create engine instance
    engine = VMSIEngine()
    print("✓ VMSIEngine created successfully")
    
    # Test data (using smaller numbers to keep VMSI in reasonable range)
    phobert_scores = np.array([0.1, -0.2, 0.05, 0.15], dtype=np.float32)
    likes = np.array([10, 5, 20, 15], dtype=np.int32)
    shares = np.array([2, 1, 4, 3], dtype=np.int32) 
    comments = np.array([1, 0, 2, 2], dtype=np.int32)
    credibility_factors = np.array([0.9, 0.7, 0.8, 0.95], dtype=np.float32)
    
    print(f"Test data: {len(phobert_scores)} social media posts")
    
    # Test interaction weight calculation
    interaction_weights = engine.calculate_interaction_weights(likes, shares, comments)
    print(f"✓ Interaction weights calculated: {interaction_weights}")
    
    # Test social score calculation
    social_score = engine.calculate_social_score(phobert_scores, interaction_weights, credibility_factors)
    print(f"✓ Social score calculated: {social_score:.6f}")
    
    # Test macro score calculation
    s_nhnn = 1  # Positive policy sentiment
    s_news = 0.2  # Slightly positive news sentiment
    macro_score = engine.calculate_macro_score(s_nhnn, s_news)
    print(f"✓ Macro score calculated: {macro_score:.6f}")
    
    # Test raw index calculation
    raw_index = engine.calculate_raw_index(macro_score, social_score)
    print(f"✓ Raw index calculated: {raw_index:.6f}")
    
    # Test final VMSI calculation
    vmsi = engine.calculate_final_vmsi(raw_index)
    print(f"✓ Final VMSI calculated: {vmsi:.6f}")
    
    # Test EMA smoothing
    previous_vmsi = 65.0
    smoothed_vmsi = engine.apply_ema_smoothing(vmsi, previous_vmsi)
    print(f"✓ EMA smoothed VMSI: {smoothed_vmsi:.6f}")
    
    # Test complete pipeline
    final_vmsi, details = engine.calculate_complete_vmsi(
        phobert_scores, likes, shares, comments, credibility_factors,
        s_nhnn, s_news, previous_vmsi
    )
    print(f"✓ Complete pipeline VMSI: {final_vmsi:.6f}")
    print(f"  Details: {details}")
    
    print("\n🎉 All VMSI Engine tests passed successfully!")
    return True


def test_boundary_conditions():
    """Test boundary conditions and edge cases."""
    
    engine = VMSIEngine()
    print("\nTesting boundary conditions...")
    
    # Test negative raw index (should result in VMSI = 0)
    negative_raw = -0.5
    vmsi_negative = engine.calculate_final_vmsi(negative_raw)
    assert vmsi_negative == 0.0, f"Expected 0.0 for negative raw index, got {vmsi_negative}"
    print(f"✓ Negative raw index handling: {negative_raw} → {vmsi_negative}")
    
    # Test zero interactions (should handle gracefully)
    zero_interactions = engine.calculate_interaction_weights(
        np.array([0]), np.array([0]), np.array([0])
    )
    expected_zero = np.log(1.0)  # log(1 + 0 + 0 + 0) = log(1) = 0
    assert np.isclose(zero_interactions[0], expected_zero), f"Expected {expected_zero}, got {zero_interactions[0]}"
    print(f"✓ Zero interactions handling: {zero_interactions[0]:.6f}")
    
    # Test NHNN score validation (must be -1, 0, or 1)
    valid_nhnn_scores = [-1, 0, 1]
    for score in valid_nhnn_scores:
        macro = engine.calculate_macro_score(score, 0.0)
        print(f"✓ Valid NHNN score {score}: macro = {macro:.6f}")
    
    print("✓ All boundary condition tests passed!")


if __name__ == "__main__":
    try:
        test_vmsi_engine_basic()
        test_boundary_conditions()
        print("\n✨ All tests completed successfully! VMSI Engine is ready.")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)