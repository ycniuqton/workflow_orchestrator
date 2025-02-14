class WorkflowError(Exception):
    """Base exception for all workflow-related errors"""
    pass

class WorkflowNotFoundError(WorkflowError):
    """Raised when a workflow with the specified ID cannot be found"""
    def __init__(self, workflow_id: str):
        self.workflow_id = workflow_id
        super().__init__(f"Workflow '{workflow_id}' not found")

class EventNotFoundError(WorkflowError):
    """Raised when an event with the specified ID cannot be found in a workflow"""
    def __init__(self, event_id: str, workflow_id: str):
        self.event_id = event_id
        self.workflow_id = workflow_id
        super().__init__(f"Event '{event_id}' not found in workflow '{workflow_id}'")

class HandlerNotFoundError(WorkflowError):
    """Raised when a handler with the specified name is not registered"""
    def __init__(self, handler_name: str):
        self.handler_name = handler_name
        super().__init__(f"Handler '{handler_name}' not found")

class EventValidationError(WorkflowError):
    """Raised when event data validation fails"""
    def __init__(self, event_id: str, missing_fields: list):
        self.event_id = event_id
        self.missing_fields = missing_fields
        fields_str = ", ".join(missing_fields)
        super().__init__(f"Event '{event_id}' validation failed. Missing required fields: {fields_str}")

class EventExecutionError(WorkflowError):
    """Raised when an event execution fails"""
    def __init__(self, event_id: str, error_message: str):
        self.event_id = event_id
        self.error_message = error_message
        super().__init__(f"Event '{event_id}' execution failed: {error_message}")

class WorkflowConfigurationError(WorkflowError):
    """Raised when there is an error in the workflow configuration"""
    def __init__(self, workflow_id: str, error_message: str):
        self.workflow_id = workflow_id
        self.error_message = error_message
        super().__init__(f"Invalid workflow configuration for '{workflow_id}': {error_message}")

class MaxRetriesExceededError(WorkflowError):
    """Raised when an event has exceeded its maximum retry attempts"""
    def __init__(self, event_id: str, max_retries: int):
        self.event_id = event_id
        self.max_retries = max_retries
        super().__init__(f"Event '{event_id}' has exceeded maximum retry attempts ({max_retries})")

class EventTimeoutError(WorkflowError):
    """Raised when an event execution exceeds its timeout"""
    def __init__(self, event_id: str, timeout_ms: int):
        self.event_id = event_id
        self.timeout_ms = timeout_ms
        super().__init__(f"Event '{event_id}' timed out after {timeout_ms}ms")
