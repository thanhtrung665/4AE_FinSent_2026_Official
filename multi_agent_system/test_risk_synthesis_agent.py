"""
Test module for Risk Synthesis Agent functionality.

This module contains unit tests for the Risk Synthesis Agent implementation,
verifying data aggregation, VMSI calculation orchestration, risk assessment,
and JSON output functionality.
"""

import json
import os
import sys
import tempfile
import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.risk_synthesis_agent import RiskSynthesisAgent
from engines.vmsi_engine import VMSIEngine
from utils.exceptions import VMSICalculationError, FileOperationError


class TestRiskSynthesisAgent:
    """Test cases for Risk Synthesis Agent."""
    
    def setup_method(self):
        """Set up test fixtures before each test."""
        # Create temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        self.test_output_file = os.path.join(self.temp_dir, 'test_vmsi.json')
        
        # Initialize agent with test configuration
        self.vmsi_engine = VMSIEngine()
        self.agent = RiskSynthesisAgent(
            vmsi_engine=self.vmsi_engine,
            output_file=self.test_output_file
        )
    
    def teardown_method(self):
        """Clean up after each test."""
        # Remove test files
        if os.path.exists(self.test_output_file):
            os.remove(self.test_output_file)
        if os.path.exists(f"{self.test_output_file}.backup"):
            os.remove(f"{self.test_output_file}.backup")
    
    def test_initialization(self):
        """Test agent initialization."""
        agent = RiskSynthesisAgent()
        assert agent.vmsi_engine is not None
        assert agent.output_file == 'live_vmsi.json'
        assert agent.RISK_LOW_THRESHOLD == 20.0
        assert agent.RISK_HIGH_THRESHOLD == 81.0
        assert agent._social_score is None
        assert agent._macro_data is None
    
    def test_receive_social_score_valid(self):
        """Test receiving valid social score."""
        social_score = 15.5
        self.agent.receive_social_score(social_score)
        assert self.agent._social_score == social_score
    
    def test_receive_social_score_invalid(self):
        """Test receiving invalid social score."""
        with pytest.raises(ValueError):
            self.agent.receive_social_score("invalid")
        
        with pytest.raises(ValueError):
            self.agent.receive_social_score(float('inf'))
    
    def test_receive_macro_score_valid(self):
        """Test receiving valid macro score and metadata."""
        s_nhnn = 1
        metadata = {
            's_news': 0.5,
            'summary': 'Test policy summary',
            'confidence': 0.85
        }
        
        self.agent.receive_macro_score(s_nhnn, metadata)
        
        assert self.agent._macro_data['s_nhnn'] == 1.0
        assert self.agent._macro_data['s_news'] == 0.5
        assert self.agent._macro_data['summary'] == 'Test policy summary'
        assert self.agent._macro_data['confidence'] == 0.85
    
    def test_receive_macro_score_invalid(self):
        """Test receiving invalid macro score."""
        with pytest.raises(ValueError):
            self.agent.receive_macro_score("invalid", {})
        
        with pytest.raises(ValueError):
            self.agent.receive_macro_score(1, "invalid_metadata")
    
    def test_compute_final_vmsi_success(self):
        """Test successful VMSI computation."""
        # Set up agent data
        self.agent.receive_social_score(10.0)
        self.agent.receive_macro_score(1, {'s_news': 0.0, 'confidence': 0.8})
        
        # Compute VMSI
        vmsi = self.agent.compute_final_vmsi()
        
        # Verify result is valid
        assert isinstance(vmsi, float)
        assert vmsi >= 0
        assert vmsi <= 100
    
    def test_compute_final_vmsi_missing_data(self):
        """Test VMSI computation with missing data."""
        # Missing social score
        with pytest.raises(RuntimeError):
            self.agent.compute_final_vmsi()
        
        # Missing macro score
        self.agent.receive_social_score(10.0)
        with pytest.raises(RuntimeError):
            self.agent.compute_final_vmsi()
    
    def test_assess_risk_level_normal(self):
        """Test risk assessment for normal VMSI value."""
        vmsi = 50.0
        assessment = self.agent.assess_risk_level(vmsi)
        
        assert assessment['status'] == 'normal'
        assert assessment['needs_warning'] is False
        assert assessment['risk_type'] == 'none'
        assert assessment['vmsi_value'] == vmsi
    
    def test_assess_risk_level_low_risk(self):
        """Test risk assessment for low VMSI value."""
        vmsi = 15.0
        assessment = self.agent.assess_risk_level(vmsi)
        
        assert assessment['status'] == 'risk_low'
        assert assessment['needs_warning'] is True
        assert assessment['risk_type'] == 'negative_sentiment'
    
    def test_assess_risk_level_high_risk(self):
        """Test risk assessment for high VMSI value."""
        vmsi = 85.0
        assessment = self.agent.assess_risk_level(vmsi)
        
        assert assessment['status'] == 'risk_high'
        assert assessment['needs_warning'] is True
        assert assessment['risk_type'] == 'excessive_optimism'
    
    def test_generate_vietnamese_warning_low_risk(self):
        """Test Vietnamese warning generation for low risk."""
        warning = self.agent.generate_vietnamese_warning('negative_sentiment', 15.0)
        
        assert isinstance(warning, str)
        assert len(warning) > 0
        assert 'CẢNH BÁO' in warning
        assert '15.0' in warning
        assert 'tiêu cực' in warning
    
    def test_generate_vietnamese_warning_high_risk(self):
        """Test Vietnamese warning generation for high risk."""
        warning = self.agent.generate_vietnamese_warning('excessive_optimism', 85.0)
        
        assert isinstance(warning, str)
        assert len(warning) > 0
        assert 'CẢNH BÁO' in warning
        assert '85.0' in warning
        assert 'lạc quan' in warning
    
    def test_generate_vietnamese_warning_no_risk(self):
        """Test Vietnamese warning generation for no risk."""
        warning = self.agent.generate_vietnamese_warning('none', 50.0)
        assert warning == ""
    
    def test_save_output_json_success(self):
        """Test successful JSON output saving."""
        results = {
            'vmsi_value': 50.0,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'status': 'normal',
            'risk_warning': '',
            'component_scores': {
                's_social': 10.0,
                's_macro': 0.7,
                's_nhnn': 1,
                'confidence': 0.8
            }
        }
        
        success = self.agent.save_output_json(results)
        assert success is True
        assert os.path.exists(self.test_output_file)
        
        # Verify file content
        with open(self.test_output_file, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        
        assert saved_data['vmsi_value'] == 50.0
        assert saved_data['status'] == 'normal'
    
    def test_save_output_json_backup_creation(self):
        """Test backup creation when overwriting existing file."""
        # Create initial file
        initial_data = {'test': 'data'}
        with open(self.test_output_file, 'w') as f:
            json.dump(initial_data, f)
        
        # Save new data
        new_results = {
            'vmsi_value': 60.0,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'status': 'normal',
            'risk_warning': '',
            'component_scores': {
                's_social': 10.0,
                's_macro': 0.7,
                's_nhnn': 1,
                'confidence': 0.8
            }
        }
        
        success = self.agent.save_output_json(new_results)
        assert success is True
        
        # Verify new content
        with open(self.test_output_file, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        assert saved_data['vmsi_value'] == 60.0
    
    def test_process_complete_risk_assessment(self):
        """Test complete risk assessment workflow."""
        # Set up agent data
        self.agent.receive_social_score(12.0)
        self.agent.receive_macro_score(-1, {
            's_news': 0.0, 
            'summary': 'Negative policy sentiment',
            'confidence': 0.75
        })
        
        # Process complete assessment
        results = self.agent.process_complete_risk_assessment()
        
        # Verify results structure
        assert 'vmsi_value' in results
        assert 'timestamp' in results
        assert 'status' in results
        assert 'risk_warning' in results
        assert 'component_scores' in results
        assert 'processing_metadata' in results
        
        # Verify VMSI calculation
        assert isinstance(results['vmsi_value'], float)
        assert results['vmsi_value'] >= 0
        
        # Verify component scores
        components = results['component_scores']
        assert components['s_social'] == 12.0
        assert components['s_nhnn'] == -1
        assert components['confidence'] == 0.75
        
        # Verify file was saved
        assert os.path.exists(self.test_output_file)
    
    def test_reset_state(self):
        """Test agent state reset."""
        # Set up some state
        self.agent.receive_social_score(25.0)
        self.agent.receive_macro_score(1, {'confidence': 0.9})
        
        # Reset state
        self.agent.reset_state()
        
        # Verify state is cleared
        assert self.agent._social_score is None
        assert self.agent._macro_data is None
    
    def test_ema_smoothing_integration(self):
        """Test EMA smoothing with multiple VMSI calculations."""
        # First calculation
        self.agent.receive_social_score(10.0)
        self.agent.receive_macro_score(1, {'s_news': 0.0, 'confidence': 0.8})
        
        vmsi1 = self.agent.compute_final_vmsi()
        
        # Reset for second calculation
        self.agent.reset_state()
        self.agent.receive_social_score(15.0)
        self.agent.receive_macro_score(0, {'s_news': 0.5, 'confidence': 0.7})
        
        vmsi2 = self.agent.compute_final_vmsi()
        
        # Second VMSI should be smoothed with first
        # EMA formula: 0.2 * current + 0.8 * previous
        # Since we have previous VMSI stored, smoothing should be applied
        assert vmsi2 != vmsi1  # Should be different due to different inputs
        assert isinstance(vmsi2, float)
        assert vmsi2 >= 0


if __name__ == "__main__":
    pytest.main([__file__])