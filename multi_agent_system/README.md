# Multi-Agent System for VMSI Calculation

## Overview

This package implements a Multi-Agent Controller (MAC) system for calculating the Vietnam Market Sentiment Index (VMSI) in real-time. The system processes social media sentiment data and macroeconomic policies to produce financial market sentiment indicators with automated risk assessment.

## Project Structure

```
multi_agent_system/
├── __init__.py                 # Main package exports
├── engines/
│   ├── __init__.py            
│   └── vmsi_engine.py         # ✅ VMSI mathematical calculations
├── agents/
│   ├── __init__.py
│   ├── social_agent.py        # 🔄 Kafka consumer (Task 3.1)
│   ├── macro_agent.py         # 🔄 ChromaDB analyzer (Task 4.1)  
│   └── risk_synthesis_agent.py # 🔄 Risk assessment (Task 5.1)
├── controller/
│   ├── __init__.py
│   └── mac_system.py          # 🔄 LangChain orchestrator (Task 7.1)
├── utils/
│   ├── __init__.py
│   ├── exceptions.py          # ✅ Custom exceptions
│   ├── validators.py          # ✅ Input validation utilities
│   └── logging_config.py      # ✅ Structured logging setup
├── requirements.txt           # ✅ Project dependencies
├── test_vmsi_engine.py        # ✅ Basic functionality test
└── README.md                  # ✅ This file
```

**Legend**: ✅ Completed, 🔄 Placeholder (to be implemented in later tasks)

## Features Implemented (Task 1.1)

### ✅ Core Project Structure
- Modular directory organization with agents, engines, utils, and controller packages
- Proper Python packaging with `__init__.py` files and exports
- Clear separation of concerns following the design architecture

### ✅ VMSI Mathematical Engine
Complete implementation of the VMSI calculation engine with:

**Mathematical Functions:**
- `calculate_interaction_weights()` - Logarithmic interaction weighting: `log(1 + likes + shares + comments)`
- `calculate_social_score()` - Weighted sentiment sum: `Σ(PhoBERT_Score × Interaction_Weight × Credibility_Factor)`
- `calculate_macro_score()` - Weighted macro average: `0.7 × S_nhnn + 0.3 × S_news`
- `calculate_raw_index()` - Combined index: `0.6 × S_macro + 0.4 × S_social`
- `calculate_final_vmsi()` - Final transformation with boundary handling: `50 × (I_raw + 1)` or `0` if negative
- `apply_ema_smoothing()` - Temporal smoothing: `0.2 × VMSI(t) + 0.8 × VMSI_smoothed(t-1)`
- `calculate_complete_vmsi()` - End-to-end pipeline calculation

**Key Features:**
- **Numpy-based calculations** for performance and precision
- **Comprehensive input validation** for all mathematical operations  
- **Robust error handling** with custom exceptions
- **Structured logging** throughout the calculation pipeline
- **Boundary condition handling** (negative raw index → VMSI = 0)
- **Array operation safety** with finite value checking and type validation

### ✅ Input Validation System
Comprehensive validation utilities in `utils/validators.py`:
- **Numpy array validation** with type, shape, and value range checking
- **Social score input validation** ensuring PhoBERT scores [-1, 1], non-negative interactions, credibility [0.1, 1.0]
- **Macro score validation** enforcing S_nhnn ∈ {-1, 0, 1} constraint
- **VMSI input validation** with finite value checking
- **Array length consistency** validation across all vector inputs

### ✅ Exception Handling
Custom exception hierarchy for different error types:
- `VMSICalculationError` - Mathematical computation failures
- `ValidationError` - Input validation failures  
- `AgentError` - Base class for agent-related errors
- `KafkaConnectionError` - Kafka connectivity issues (for future tasks)
- `ChromaDBConnectionError` - ChromaDB connectivity issues (for future tasks)
- `ConfigurationError` - System configuration problems

### ✅ Structured Logging
Configurable logging system with:
- **Multiple log levels** (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- **Contextual information** including function names and line numbers
- **Namespace organization** under `multi_agent_system.*`
- **Calculation traceability** with detailed mathematical step logging

## Testing & Verification

### ✅ Basic Functionality Test
The `test_vmsi_engine.py` demonstrates:
- ✅ VMSI Engine initialization and configuration
- ✅ Interaction weight calculation with realistic social media data
- ✅ Social score computation with PhoBERT sentiment scores
- ✅ Macro score calculation with NHNN policy and news sentiment
- ✅ Raw index and final VMSI transformation
- ✅ EMA smoothing with temporal data
- ✅ Complete end-to-end pipeline execution
- ✅ Boundary condition handling (negative values, zero interactions)
- ✅ NHNN score validation for discrete values {-1, 0, 1}

**Test Results:**
```
✓ VMSIEngine created successfully
✓ Interaction weights calculated: [2.639, 1.946, 3.296, 3.045]
✓ Social score calculated: 0.530766
✓ Macro score calculated: 0.760000  
✓ Raw index calculated: 0.668306
✓ Final VMSI calculated: 83.415313
✓ EMA smoothed VMSI: 68.683063
✓ Complete pipeline VMSI: 68.683063
✓ All boundary condition tests passed!
```

## Usage Example

```python
from multi_agent_system.engines.vmsi_engine import VMSIEngine
from multi_agent_system.utils.logging_config import setup_logging
import numpy as np

# Setup logging
setup_logging("INFO")

# Create VMSI Engine
engine = VMSIEngine()

# Prepare social media data
phobert_scores = np.array([0.1, -0.2, 0.05, 0.15], dtype=np.float32)
likes = np.array([10, 5, 20, 15], dtype=np.int32)
shares = np.array([2, 1, 4, 3], dtype=np.int32)
comments = np.array([1, 0, 2, 2], dtype=np.int32)
credibility_factors = np.array([0.9, 0.7, 0.8, 0.95], dtype=np.float32)

# Macro economic data
s_nhnn = 1      # Positive NHNN policy sentiment
s_news = 0.2    # Slightly positive news sentiment

# Calculate complete VMSI
vmsi, details = engine.calculate_complete_vmsi(
    phobert_scores, likes, shares, comments, credibility_factors,
    s_nhnn, s_news, previous_vmsi=65.0
)

print(f"VMSI: {vmsi:.2f}")
print(f"Details: {details}")
```

## Requirements Met

This implementation satisfies the following requirements from the specification:

**✅ Requirement 1.1**: VMSI_Engine implements social score calculation using numpy arrays  
**✅ Requirement 1.2**: Validates non-negative interactions before logarithm application  
**✅ Requirement 1.3**: Uses exact formula `np.log(1 + likes + shares + comments)`  
**✅ Requirement 1.4**: Computes social score as weighted sum of sentiment factors  
**✅ Requirement 1.5**: Calculates macro score as `0.7 × S_nhnn + 0.3 × S_news`  
**✅ Requirement 1.6**: Computes raw index as `0.6 × S_macro + 0.4 × S_social`  
**✅ Requirement 1.7**: Calculates final VMSI as `50 × (I_raw + 1)`  
**✅ Requirement 1.8**: Returns VMSI = 0 when I_raw is negative  
**✅ Requirement 1.9**: Applies EMA smoothing with specified weights  
**✅ Requirement 1.10**: Validates data types and handles numpy arrays correctly  
**✅ Requirement 1.11**: Returns precise floating-point results with error handling  

## Dependencies

Core dependencies (see `requirements.txt` for complete list):
- `numpy>=1.24.0` - Mathematical operations and array handling
- `pytest>=7.0.0` - Testing framework (for property-based tests in later tasks)
- `hypothesis>=6.100.0` - Property-based testing (for later tasks)

## Next Steps

The following components are placeholders and will be implemented in subsequent tasks:

1. **Task 1.2** - Property-based testing for VMSI mathematical formulas
2. **Task 3.1** - Social Agent with Kafka integration  
3. **Task 4.1** - Macro Agent with ChromaDB integration
4. **Task 5.1** - Risk Synthesis Agent with LLM integration
5. **Task 7.1** - MAC System with LangChain orchestration

## Development Notes

- All mathematical constants are defined as class attributes for easy modification
- Extensive logging provides full calculation traceability
- Input validation prevents invalid data from corrupting calculations
- Numpy operations are vectorized for optimal performance
- Error handling ensures graceful degradation under failure conditions
- The design supports both individual function calls and complete pipeline execution