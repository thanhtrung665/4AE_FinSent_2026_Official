"""
VMSI (Vietnam Market Sentiment Index) Mathematical Engine.

This module implements the core mathematical calculations for the VMSI index
using numpy operations for performance and precision.
"""

import numpy as np
from typing import Union, Tuple
from ..utils.exceptions import VMSICalculationError, ValidationError
from ..utils.validators import (
    validate_scores, validate_interaction_inputs, validate_macro_scores,
    validate_vmsi_inputs, validate_ema_inputs, validate_numpy_input
)
from ..utils.logging_config import get_logger

logger = get_logger('vmsi_engine')


class VMSIEngine:
    """
    Mathematical engine for VMSI (Vietnam Market Sentiment Index) calculations.
    
    This class implements all mathematical formulas specified in the requirements:
    - Social score calculation from social media sentiment data
    - Macro score calculation from policy and news sentiment
    - Raw index combination using weighted averages
    - Final VMSI transformation and boundary handling
    - EMA (Exponential Moving Average) smoothing for temporal stability
    
    All calculations use numpy operations for performance and precision.
    """
    
    def __init__(self):
        """Initialize the VMSI Engine."""
        logger.info("Initializing VMSI Engine")
        
        # Mathematical constants from requirements
        self.MACRO_NHNN_WEIGHT = 0.7
        self.MACRO_NEWS_WEIGHT = 0.3
        self.RAW_MACRO_WEIGHT = 0.6
        self.RAW_SOCIAL_WEIGHT = 0.4
        self.VMSI_SCALE_FACTOR = 50.0
        self.VMSI_OFFSET = 1.0
        self.EMA_CURRENT_WEIGHT = 0.2
        self.EMA_PREVIOUS_WEIGHT = 0.8
        
        logger.debug(f"Engine constants: macro_weights=({self.MACRO_NHNN_WEIGHT}, {self.MACRO_NEWS_WEIGHT}), "
                    f"raw_weights=({self.RAW_MACRO_WEIGHT}, {self.RAW_SOCIAL_WEIGHT}), "
                    f"ema_weights=({self.EMA_CURRENT_WEIGHT}, {self.EMA_PREVIOUS_WEIGHT})")
    
    def calculate_interaction_weights(self, likes: Union[int, np.ndarray], 
                                   shares: Union[int, np.ndarray], 
                                   comments: Union[int, np.ndarray]) -> np.ndarray:
        """
        Calculate interaction weights using logarithmic formula.
        
        Formula: np.log(1 + likes + shares + comments)
        
        Args:
            likes: Number of likes (must be non-negative)
            shares: Number of shares (must be non-negative)
            comments: Number of comments (must be non-negative)
            
        Returns:
            Numpy array of interaction weights
            
        Raises:
            ValidationError: If inputs are invalid
            VMSICalculationError: If calculation fails
        """
        try:
            # Validate inputs
            likes, shares, comments = validate_interaction_inputs(likes, shares, comments)
            
            logger.debug(f"Calculating interaction weights for {len(likes)} posts")
            
            # Apply logarithmic formula: log(1 + total_interactions)
            total_interactions = likes + shares + comments
            interaction_weights = np.log(1.0 + total_interactions.astype(np.float32))
            
            logger.debug(f"Interaction weights: min={np.min(interaction_weights):.4f}, "
                        f"max={np.max(interaction_weights):.4f}, "
                        f"mean={np.mean(interaction_weights):.4f}")
            
            return interaction_weights
            
        except ValidationError:
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to calculate interaction weights: {e}")
    
    def calculate_social_score(self, phobert_scores: np.ndarray,
                             interaction_weights: np.ndarray,
                             credibility_factors: np.ndarray) -> float:
        """
        Calculate social score using weighted sum formula.
        
        Formula: S_social(t) = Σ(PhoBERT_Score × Interaction_Weight × Credibility_Factor)
        
        Args:
            phobert_scores: PhoBERT sentiment scores [-1, 1]
            interaction_weights: Logarithmic interaction weights [0, ∞)
            credibility_factors: Source credibility factors [0.1, 1.0]
            
        Returns:
            Social score as float
            
        Raises:
            ValidationError: If inputs are invalid
            VMSICalculationError: If calculation fails
        """
        try:
            # Validate all input arrays
            validate_scores(phobert_scores, interaction_weights, credibility_factors)
            
            logger.debug(f"Calculating social score for {len(phobert_scores)} posts")
            
            # Calculate weighted sum using vectorized operations
            weighted_scores = phobert_scores * interaction_weights * credibility_factors
            social_score = float(np.sum(weighted_scores))
            
            # Validate result
            if not np.isfinite(social_score):
                raise VMSICalculationError(f"Social score calculation produced non-finite result: {social_score}")
            
            logger.info(f"Social score calculated: {social_score:.6f}")
            return social_score
            
        except ValidationError:
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to calculate social score: {e}")
    
    def calculate_macro_score(self, s_nhnn: float, s_news: float) -> float:
        """
        Calculate macro economic score using weighted average.
        
        Formula: S_macro(t) = 0.7 × S_nhnn + 0.3 × S_news
        
        Args:
            s_nhnn: NHNN policy sentiment score (-1, 0, or 1)
            s_news: News sentiment score
            
        Returns:
            Macro score as float
            
        Raises:
            ValidationError: If inputs are invalid
            VMSICalculationError: If calculation fails
        """
        try:
            # Validate inputs
            validate_macro_scores(s_nhnn, s_news)
            
            logger.debug(f"Calculating macro score: s_nhnn={s_nhnn}, s_news={s_news}")
            
            # Apply weighted average formula
            macro_score = (self.MACRO_NHNN_WEIGHT * s_nhnn + 
                          self.MACRO_NEWS_WEIGHT * s_news)
            
            # Validate result
            if not np.isfinite(macro_score):
                raise VMSICalculationError(f"Macro score calculation produced non-finite result: {macro_score}")
            
            logger.info(f"Macro score calculated: {macro_score:.6f}")
            return macro_score
            
        except ValidationError:
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to calculate macro score: {e}")
    
    def calculate_raw_index(self, s_macro: float, s_social: float) -> float:
        """
        Calculate raw VMSI index using weighted combination.
        
        Formula: I_raw(t) = 0.6 × S_macro(t) + 0.4 × S_social(t)
        
        Args:
            s_macro: Macro economic score
            s_social: Social sentiment score
            
        Returns:
            Raw index as float
            
        Raises:
            ValidationError: If inputs are invalid
            VMSICalculationError: If calculation fails
        """
        try:
            # Validate inputs
            validate_vmsi_inputs(s_macro, s_social)
            
            logger.debug(f"Calculating raw index: s_macro={s_macro}, s_social={s_social}")
            
            # Apply weighted combination formula
            raw_index = (self.RAW_MACRO_WEIGHT * s_macro + 
                        self.RAW_SOCIAL_WEIGHT * s_social)
            
            # Validate result
            if not np.isfinite(raw_index):
                raise VMSICalculationError(f"Raw index calculation produced non-finite result: {raw_index}")
            
            logger.info(f"Raw index calculated: {raw_index:.6f}")
            return raw_index
            
        except ValidationError:
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to calculate raw index: {e}")
    
    def calculate_final_vmsi(self, i_raw: float) -> float:
        """
        Calculate final VMSI with boundary handling.
        
        Formula: 
        - If I_raw(t) < 0: VMSI = 0
        - Else: VMSI = 50 × (I_raw(t) + 1)
        
        Args:
            i_raw: Raw index value
            
        Returns:
            Final VMSI value [0, 100]
            
        Raises:
            ValidationError: If input is invalid
            VMSICalculationError: If calculation fails
        """
        try:
            # Validate input
            if not isinstance(i_raw, (int, float)):
                raise ValidationError(f"i_raw must be numeric, got {type(i_raw)}")
            
            if not np.isfinite(i_raw):
                raise ValidationError(f"i_raw must be finite, got {i_raw}")
            
            logger.debug(f"Calculating final VMSI: i_raw={i_raw}")
            
            # Apply boundary handling and transformation
            if i_raw < 0:
                vmsi = 0.0
                logger.debug("Applied negative boundary condition: VMSI = 0")
            else:
                vmsi = self.VMSI_SCALE_FACTOR * (i_raw + self.VMSI_OFFSET)
                logger.debug(f"Applied transformation: VMSI = 50 × ({i_raw} + 1) = {vmsi}")
            
            # Validate result is in expected range
            if vmsi < 0 or vmsi > 100:
                logger.warning(f"VMSI value {vmsi} is outside normal range [0, 100]")
            
            logger.info(f"Final VMSI calculated: {vmsi:.6f}")
            return vmsi
            
        except ValidationError:
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to calculate final VMSI: {e}")
    
    def apply_ema_smoothing(self, current_vmsi: float, previous_vmsi: float) -> float:
        """
        Apply exponential moving average smoothing.
        
        Formula: VMSI_smoothed(t) = 0.2 × VMSI(t) + 0.8 × VMSI_smoothed(t-1)
        
        Args:
            current_vmsi: Current VMSI value [0, 100]
            previous_vmsi: Previous smoothed VMSI value [0, 100]
            
        Returns:
            Smoothed VMSI value
            
        Raises:
            ValidationError: If inputs are invalid
            VMSICalculationError: If calculation fails
        """
        try:
            # Validate inputs
            validate_ema_inputs(current_vmsi, previous_vmsi)
            
            logger.debug(f"Applying EMA smoothing: current={current_vmsi}, previous={previous_vmsi}")
            
            # Apply EMA formula
            smoothed_vmsi = (self.EMA_CURRENT_WEIGHT * current_vmsi + 
                           self.EMA_PREVIOUS_WEIGHT * previous_vmsi)
            
            # Validate result
            if not np.isfinite(smoothed_vmsi):
                raise VMSICalculationError(f"EMA smoothing produced non-finite result: {smoothed_vmsi}")
            
            logger.info(f"EMA smoothed VMSI: {smoothed_vmsi:.6f}")
            return smoothed_vmsi
            
        except ValidationError:
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to apply EMA smoothing: {e}")
    
    def calculate_complete_vmsi(self, 
                              phobert_scores: np.ndarray,
                              likes: np.ndarray,
                              shares: np.ndarray, 
                              comments: np.ndarray,
                              credibility_factors: np.ndarray,
                              s_nhnn: float,
                              s_news: float,
                              previous_vmsi: float = None) -> Tuple[float, dict]:
        """
        Calculate complete VMSI pipeline in one call.
        
        Args:
            phobert_scores: PhoBERT sentiment scores
            likes: Number of likes per post
            shares: Number of shares per post
            comments: Number of comments per post
            credibility_factors: Source credibility factors
            s_nhnn: NHNN policy sentiment score
            s_news: News sentiment score
            previous_vmsi: Previous VMSI for smoothing (optional)
            
        Returns:
            Tuple of (final_vmsi, calculation_details)
            
        Raises:
            ValidationError: If inputs are invalid
            VMSICalculationError: If calculation fails
        """
        try:
            logger.info("Starting complete VMSI calculation pipeline")
            
            # Step 1: Calculate interaction weights
            interaction_weights = self.calculate_interaction_weights(likes, shares, comments)
            
            # Step 2: Calculate social score
            s_social = self.calculate_social_score(phobert_scores, interaction_weights, credibility_factors)
            
            # Step 3: Calculate macro score
            s_macro = self.calculate_macro_score(s_nhnn, s_news)
            
            # Step 4: Calculate raw index
            i_raw = self.calculate_raw_index(s_macro, s_social)
            
            # Step 5: Calculate final VMSI
            vmsi_current = self.calculate_final_vmsi(i_raw)
            
            # Step 6: Apply EMA smoothing (if previous value provided)
            if previous_vmsi is not None:
                vmsi_final = self.apply_ema_smoothing(vmsi_current, previous_vmsi)
            else:
                vmsi_final = vmsi_current
                logger.info("No previous VMSI provided, skipping EMA smoothing")
            
            # Prepare calculation details
            calculation_details = {
                'num_posts': len(phobert_scores),
                's_social': s_social,
                's_macro': s_macro, 
                's_nhnn': s_nhnn,
                's_news': s_news,
                'i_raw': i_raw,
                'vmsi_current': vmsi_current,
                'vmsi_smoothed': vmsi_final,
                'ema_applied': previous_vmsi is not None
            }
            
            logger.info(f"Complete VMSI calculation finished: {vmsi_final:.6f}")
            return vmsi_final, calculation_details
            
        except (ValidationError, VMSICalculationError):
            raise
        except Exception as e:
            raise VMSICalculationError(f"Failed to calculate complete VMSI: {e}")