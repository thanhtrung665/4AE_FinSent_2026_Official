"""
Input validation utilities for the Multi-Agent System.
"""

import numpy as np
from typing import Union, Any
from .exceptions import ValidationError


def validate_numpy_input(data: Any, name: str, dtype: np.dtype = None, 
                        shape: tuple = None, min_value: float = None, 
                        max_value: float = None) -> np.ndarray:
    """
    Validate and convert input to numpy array with specified constraints.
    
    Args:
        data: Input data to validate
        name: Name of the parameter for error messages
        dtype: Expected numpy dtype (optional)
        shape: Expected shape tuple (optional)
        min_value: Minimum allowed value (optional)
        max_value: Maximum allowed value (optional)
        
    Returns:
        Validated numpy array
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        # Convert to numpy array
        if not isinstance(data, np.ndarray):
            data = np.array(data)
            
        # Check if array is empty
        if data.size == 0:
            raise ValidationError(f"{name} cannot be empty array")
            
        # Check data type
        if dtype is not None and data.dtype != dtype:
            try:
                data = data.astype(dtype)
            except (ValueError, TypeError) as e:
                raise ValidationError(f"{name} cannot be converted to {dtype}: {e}")
                
        # Check shape
        if shape is not None and data.shape != shape:
            raise ValidationError(
                f"{name} shape {data.shape} does not match expected {shape}"
            )
            
        # Check for non-finite values
        if np.issubdtype(data.dtype, np.floating):
            if not np.all(np.isfinite(data)):
                raise ValidationError(f"{name} contains non-finite values (NaN or infinity)")
                
        # Check value range
        if min_value is not None and np.any(data < min_value):
            raise ValidationError(f"{name} contains values below minimum {min_value}")
            
        if max_value is not None and np.any(data > max_value):
            raise ValidationError(f"{name} contains values above maximum {max_value}")
            
        return data
        
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Unexpected error validating {name}: {e}")


def validate_scores(phobert_scores: np.ndarray, interaction_weights: np.ndarray, 
                   credibility_factors: np.ndarray) -> None:
    """
    Validate arrays for social score calculation.
    
    Args:
        phobert_scores: PhoBERT sentiment scores
        interaction_weights: Logarithmic interaction weights
        credibility_factors: Source credibility factors
        
    Raises:
        ValidationError: If validation fails
    """
    # Validate PhoBERT scores
    validate_numpy_input(
        phobert_scores, "phobert_scores", 
        dtype=np.float32, min_value=-1.0, max_value=1.0
    )
    
    # Validate interaction weights (should be non-negative due to log(1 + positive))
    validate_numpy_input(
        interaction_weights, "interaction_weights",
        dtype=np.float32, min_value=0.0
    )
    
    # Validate credibility factors
    validate_numpy_input(
        credibility_factors, "credibility_factors",
        dtype=np.float32, min_value=0.1, max_value=1.0
    )
    
    # Check that all arrays have the same length
    if not (len(phobert_scores) == len(interaction_weights) == len(credibility_factors)):
        raise ValidationError(
            f"Array length mismatch: phobert_scores({len(phobert_scores)}), "
            f"interaction_weights({len(interaction_weights)}), "
            f"credibility_factors({len(credibility_factors)})"
        )


def validate_interaction_inputs(likes: Union[int, np.ndarray], 
                              shares: Union[int, np.ndarray], 
                              comments: Union[int, np.ndarray]) -> tuple:
    """
    Validate interaction data for weight calculation.
    
    Args:
        likes: Number of likes
        shares: Number of shares  
        comments: Number of comments
        
    Returns:
        Tuple of validated numpy arrays
        
    Raises:
        ValidationError: If validation fails
    """
    likes = validate_numpy_input(likes, "likes", dtype=np.int32, min_value=0)
    shares = validate_numpy_input(shares, "shares", dtype=np.int32, min_value=0)  
    comments = validate_numpy_input(comments, "comments", dtype=np.int32, min_value=0)
    
    # Check array length consistency
    if not (len(likes) == len(shares) == len(comments)):
        raise ValidationError(
            f"Interaction array length mismatch: likes({len(likes)}), "
            f"shares({len(shares)}), comments({len(comments)})"
        )
        
    return likes, shares, comments


def validate_macro_scores(s_nhnn: float, s_news: float) -> None:
    """
    Validate macro economic scores.
    
    Args:
        s_nhnn: NHNN policy score (should be -1, 0, or 1)
        s_news: News sentiment score
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(s_nhnn, (int, float)):
        raise ValidationError(f"s_nhnn must be numeric, got {type(s_nhnn)}")
        
    if not isinstance(s_news, (int, float)):
        raise ValidationError(f"s_news must be numeric, got {type(s_news)}")
        
    if s_nhnn not in [-1, 0, 1]:
        raise ValidationError(f"s_nhnn must be -1, 0, or 1, got {s_nhnn}")
        
    if not np.isfinite(s_news):
        raise ValidationError(f"s_news must be finite, got {s_news}")


def validate_vmsi_inputs(s_macro: float, s_social: float) -> None:
    """
    Validate inputs for VMSI calculation.
    
    Args:
        s_macro: Macro economic score
        s_social: Social sentiment score
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(s_macro, (int, float)):
        raise ValidationError(f"s_macro must be numeric, got {type(s_macro)}")
        
    if not isinstance(s_social, (int, float)):
        raise ValidationError(f"s_social must be numeric, got {type(s_social)}")
        
    if not np.isfinite(s_macro):
        raise ValidationError(f"s_macro must be finite, got {s_macro}")
        
    if not np.isfinite(s_social):
        raise ValidationError(f"s_social must be finite, got {s_social}")


def validate_ema_inputs(current_vmsi: float, previous_vmsi: float) -> None:
    """
    Validate inputs for EMA smoothing.
    
    Args:
        current_vmsi: Current VMSI value
        previous_vmsi: Previous smoothed VMSI value
        
    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(current_vmsi, (int, float)):
        raise ValidationError(f"current_vmsi must be numeric, got {type(current_vmsi)}")
        
    if not isinstance(previous_vmsi, (int, float)):
        raise ValidationError(f"previous_vmsi must be numeric, got {type(previous_vmsi)}")
        
    if not np.isfinite(current_vmsi):
        raise ValidationError(f"current_vmsi must be finite, got {current_vmsi}")
        
    if not np.isfinite(previous_vmsi):
        raise ValidationError(f"previous_vmsi must be finite, got {previous_vmsi}")
        
    if current_vmsi < 0:
        raise ValidationError(f"current_vmsi must be non-negative, got {current_vmsi}")
        
    if previous_vmsi < 0:
        raise ValidationError(f"previous_vmsi must be non-negative, got {previous_vmsi}")


def validate_vmsi_value(vmsi: float) -> None:
    """
    Validate VMSI value for risk assessment.
    
    Args:
        vmsi: VMSI value to validate
        
    Raises:
        ValidationError: If VMSI value is invalid
    """
    if not isinstance(vmsi, (int, float)):
        raise ValidationError(f"VMSI must be numeric, got {type(vmsi)}")
        
    if not np.isfinite(vmsi):
        raise ValidationError(f"VMSI must be finite, got {vmsi}")
        
    if vmsi < 0:
        raise ValidationError(f"VMSI must be non-negative, got {vmsi}")


def validate_json_schema(results: dict) -> None:
    """
    Validate JSON output schema for live_vmsi.json.
    
    Args:
        results: Results dictionary to validate
        
    Raises:
        ValidationError: If schema validation fails
    """
    if not isinstance(results, dict):
        raise ValidationError(f"Results must be dict, got {type(results)}")
    
    # Required fields
    required_fields = ['vmsi_value', 'timestamp', 'status', 'risk_warning', 'component_scores']
    
    for field in required_fields:
        if field not in results:
            raise ValidationError(f"Missing required field: {field}")
    
    # Validate vmsi_value
    validate_vmsi_value(results['vmsi_value'])
    
    # Validate status
    valid_statuses = ['normal', 'risk_low', 'risk_high']
    if results['status'] not in valid_statuses:
        raise ValidationError(f"Invalid status '{results['status']}', must be one of {valid_statuses}")
    
    # Validate risk_warning is string
    if not isinstance(results['risk_warning'], str):
        raise ValidationError(f"risk_warning must be string, got {type(results['risk_warning'])}")
    
    # Validate component_scores structure
    if not isinstance(results['component_scores'], dict):
        raise ValidationError(f"component_scores must be dict, got {type(results['component_scores'])}")
    
    component_required = ['s_social', 's_macro', 's_nhnn', 'confidence']
    for field in component_required:
        if field not in results['component_scores']:
            raise ValidationError(f"Missing component_scores field: {field}")
        
        value = results['component_scores'][field]
        if not isinstance(value, (int, float)):
            raise ValidationError(f"component_scores.{field} must be numeric, got {type(value)}")
        
        if not np.isfinite(value):
            raise ValidationError(f"component_scores.{field} must be finite, got {value}")
    
    # Validate confidence is in valid range [0, 1]
    confidence = results['component_scores']['confidence']
    if not (0.0 <= confidence <= 1.0):
        raise ValidationError(f"Confidence must be in range [0, 1], got {confidence}")
    
    # Validate processing_metadata if present (Requirement 8.7)
    if 'processing_metadata' in results:
        metadata = results['processing_metadata']
        if not isinstance(metadata, dict):
            raise ValidationError(f"processing_metadata must be dict, got {type(metadata)}")
        
        # Validate processing_time
        if 'processing_time' in metadata:
            proc_time = metadata['processing_time']
            if proc_time is not None and (not isinstance(proc_time, (int, float)) or not np.isfinite(proc_time)):
                raise ValidationError(f"processing_time must be numeric or None, got {proc_time}")
        
        # Validate agent_versions
        if 'agent_versions' in metadata:
            versions = metadata['agent_versions']
            if not isinstance(versions, dict):
                raise ValidationError(f"agent_versions must be dict, got {type(versions)}")
            
            for agent, version in versions.items():
                if not isinstance(version, str):
                    raise ValidationError(f"agent_versions.{agent} must be string, got {type(version)}")
        
        # Validate data_sources
        if 'data_sources' in metadata:
            sources = metadata['data_sources']
            if not isinstance(sources, dict):
                raise ValidationError(f"data_sources must be dict, got {type(sources)}")
    
    # Validate timestamp format (ISO 8601, Requirement 8.8)
    timestamp = results['timestamp']
    if not isinstance(timestamp, str):
        raise ValidationError(f"timestamp must be string, got {type(timestamp)}")
    
    # Basic ISO 8601 format check (simplified)
    try:
        from datetime import datetime
        datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    except ValueError as e:
        raise ValidationError(f"timestamp must be in ISO 8601 format: {e}")