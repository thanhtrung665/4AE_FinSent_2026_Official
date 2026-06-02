"""
Multi-Agent System for Vietnam Market Sentiment Index (VMSI) calculation.

This package implements a multi-agent controller system that processes social media 
sentiment and macroeconomic policy data to compute the VMSI in real-time.
"""

__version__ = "1.0.0"
__author__ = "FinSent-Agent Team"

from .engines.vmsi_engine import VMSIEngine
from .agents.social_agent import SocialAgent
from .agents.macro_agent import MacroAgent
from .agents.risk_synthesis_agent import RiskSynthesisAgent
from .controller.mac_system import MACSystem

__all__ = [
    'VMSIEngine',
    'SocialAgent', 
    'MacroAgent',
    'RiskSynthesisAgent',
    'MACSystem'
]