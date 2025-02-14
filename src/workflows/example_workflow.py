from ..models.workflow import Workflow, EventNode

def create_transaction_workflow() -> Workflow:
    """Create an example transaction processing workflow"""
    
    # Define the events
    validation_event = EventNode(
        event_id="validate_transaction",
        topic="transaction_validation",
        handler="ValidationHandler",
        next_on_success="process_transaction",
        next_on_failure="handle_error",
        required_fields=['user_id', 'amount']
    )
    
    processing_event = EventNode(
        event_id="process_transaction",
        topic="transaction_processing",
        handler="ProcessingHandler",
        next_on_success="send_notification",
        next_on_failure="handle_error"
    )
    
    notification_event = EventNode(
        event_id="send_notification",
        topic="transaction_notification",
        handler="NotificationHandler",
        next_on_success=None,  # End of workflow
        next_on_failure="handle_error"
    )
    
    error_event = EventNode(
        event_id="handle_error",
        topic="error_handling",
        handler="ErrorHandler",
        next_on_success=None,  # End of workflow
        next_on_failure=None
    )
    
    # Create the workflow
    workflow = Workflow(
        workflow_id="transaction_processing",
        name="Transaction Processing Workflow",
        description="Process a financial transaction with validation and notification",
        start_event="validate_transaction",
        events={
            "validate_transaction": validation_event,
            "process_transaction": processing_event,
            "send_notification": notification_event,
            "handle_error": error_event
        },
        metadata={
            "version": "1.0",
            "owner": "finance_team"
        }
    )
    
    return workflow
