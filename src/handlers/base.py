from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
from ..models.workflow import EventContext, EventStatus

class BaseEventHandler(ABC):
    """Base class for all event handlers"""
    
    def __init__(self):
        self.context: Optional[EventContext] = None
    
    @abstractmethod
    async def handle(self, context: EventContext) -> Tuple[bool, Dict[str, Any]]:
        """
        Handle the event
        
        Args:
            context: Event context containing workflow and event information
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (success, result_data)
            - success: True if event handled successfully, False otherwise
            - result_data: Data to be passed to the next event
        """
        pass
    
    def validate_required_fields(self, required_fields: list, data: Dict[str, Any]) -> bool:
        """Validate that all required fields are present in the data"""
        if not required_fields:
            return True
        return all(field in data for field in required_fields)
    
    def get_previous_result(self) -> Optional[Dict[str, Any]]:
        """Get the result from the previous event if it exists"""
        if self.context and self.context.previous_result:
            return self.context.previous_result
        return None
