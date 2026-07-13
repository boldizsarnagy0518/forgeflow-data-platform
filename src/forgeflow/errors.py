"""Domain-specific ForgeFlow exceptions."""


class ForgeFlowError(Exception):
    """Base class for expected platform failures."""


class ConfigurationError(ForgeFlowError):
    """Raised when required configuration is invalid."""


class ObjectStoreError(ForgeFlowError):
    """Raised when raw landing storage cannot complete an operation."""


class WarehouseError(ForgeFlowError):
    """Raised when warehouse metadata or data operations fail."""


class ContractError(ForgeFlowError):
    """Raised when a source cannot be evaluated against its contract."""


class DbtExecutionError(ForgeFlowError):
    """Raised after dbt artifacts have been captured for a failed command."""


class WritesDisabledError(ForgeFlowError):
    """Raised when a mutation is attempted while writes are disabled."""
