"""
Custom exceptions for the Multi-Agent System.
"""


class VMSICalculationError(Exception):
    """Raised when VMSI mathematical calculations fail."""
    pass


class ValidationError(Exception):
    """Raised when input validation fails."""
    pass


class AgentError(Exception):
    """Base exception for agent-related errors."""
    pass


class KafkaConnectionError(AgentError):
    """Raised when Kafka connection fails."""
    pass


class ChromaDBConnectionError(AgentError):
    """Raised when ChromaDB connection fails."""
    pass


class ConfigurationError(Exception):
    """Raised when system configuration is invalid."""
    pass


class FileOperationError(Exception):
    """Raised when file operations fail."""
    pass