from typing import Dict, Any, Tuple
from .base import BaseEventHandler
from ..models.workflow import EventContext

class ValidationHandler(BaseEventHandler):
    async def handle(self, context: EventContext = None) -> Tuple[bool, Dict[str, Any]]:
        if not context:
            context = self.context
        """Validate input data"""
        data = context.data
        required_fields = ['user_id', 'amount']
        
        if not self.validate_required_fields(required_fields, data):
            return False, {
                "error": "Missing required fields",
                "required_fields": required_fields
            }
        
        if data['amount'] <= 0:
            return False, {
                "error": "Amount must be positive"
            }
        
        return True, {
            "validated_data": data
        }

class ProcessingHandler(BaseEventHandler):
    async def handle(self, context: EventContext = None) -> Tuple[bool, Dict[str, Any]]:
        if not context:
            context = self.context
        """Process the validated data"""
        previous_result = self.get_previous_result(context)
        if not previous_result or 'validated_data' not in previous_result:
            return False, {"error": "No validated data found"}
        
        data = previous_result['validated_data']
        # Simulate processing
        processed_amount = data['amount'] * 1.1  # Add 10% fee
        
        return True, {
            "processed_amount": processed_amount,
            "user_id": data['user_id']
        }

class NotificationHandler(BaseEventHandler):
    async def handle(self, context: EventContext = None) -> Tuple[bool, Dict[str, Any]]:
        if not context:
            context = self.context
        """Send notification about the processed transaction"""
        previous_result = self.get_previous_result(context)
        if not previous_result:
            return False, {"error": "No processing result found"}
        
        # Simulate sending notification
        notification = {
            "user_id": previous_result['user_id'],
            "message": f"Transaction processed. Final amount: {previous_result['processed_amount']}"
        }
        
        return True, {
            "notification_sent": True,
            "notification": notification
        }

class ErrorHandler(BaseEventHandler):
    async def handle(self, context: EventContext = None) -> Tuple[bool, Dict[str, Any]]:
        if not context:
            context = self.context
        """Handle errors from previous events"""
        previous_result = self.get_previous_result(context)
        error_message = previous_result.get('error', 'Unknown error') if previous_result else 'Unknown error'
        
        # Simulate error handling
        error_notification = {
            "workflow_id": context.workflow_id,
            "event_id": context.event_id,
            "error": error_message
        }
        
        return True, {
            "error_handled": True,
            "error_notification": error_notification
        }
