import asyncio
import logging
from typing import Dict, Any, Optional, Type, Tuple
from src.models.workflow import Workflow, EventNode, EventContext, EventStatus
from src.handlers.base import BaseEventHandler
from src.exceptions.workflow_exceptions import (
    WorkflowNotFoundError,
    EventNotFoundError,
    HandlerNotFoundError
)

logger = logging.getLogger(__name__)

class WorkflowEngine:
    def __init__(self):
        self._workflows: Dict[str, Workflow] = {}
        self._handlers: Dict[str, Type[BaseEventHandler]] = {}
        
    def register_workflow(self, workflow: Workflow) -> None:
        """Register a new workflow"""
        self._workflows[workflow.workflow_id] = workflow
        
    def register_handler(self, handler_name: str, handler_class: Type[BaseEventHandler]) -> None:
        """Register an event handler"""
        self._handlers[handler_name] = handler_class
    
    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """Get workflow by ID"""
        return self._workflows.get(workflow_id)
    
    async def execute_event(self, context: EventContext) -> Tuple[bool, Dict[str, Any]]:
        """Execute a single event in a workflow"""
        workflow = self._workflows.get(context.workflow_id)
        if not workflow:
            raise WorkflowNotFoundError(f"Workflow {context.workflow_id} not found")
            
        event = workflow.events.get(context.event_id)
        if not event:
            raise EventNotFoundError(f"Event {context.event_id} not found in workflow {context.workflow_id}")
            
        handler_class = self._handlers.get(event.handler)
        if not handler_class:
            raise HandlerNotFoundError(f"Handler {event.handler} not found")
            
        handler = handler_class(context)
        event.status = EventStatus.RUNNING
        
        try:
            success, result = await handler.handle()
            event.status = EventStatus.SUCCESS if success else EventStatus.FAILED
            return success, result
        except Exception as e:
            logger.exception(f"Error executing event {context.event_id}")
            event.status = EventStatus.FAILED
            return False, {"error": str(e)}
    
    async def execute_workflow(self, workflow_id: str, initial_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a workflow from start to finish"""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise WorkflowNotFoundError(f"Workflow {workflow_id} not found")
        
        workflow.status = EventStatus.RUNNING
        current_event_id = workflow.start_event
        previous_result = None
        
        while current_event_id:
            event = workflow.events[current_event_id]
            context = EventContext(
                workflow_id=workflow_id,
                event_id=current_event_id,
                data=initial_data,
                metadata=workflow.metadata,
                previous_result=previous_result
            )
            
            success, result = await self.execute_event(context)
            previous_result = result
            
            # Determine next event based on success/failure
            current_event_id = event.next_on_success if success else event.next_on_failure
        
        workflow.status = EventStatus.SUCCESS
        return previous_result  # Return the result of the last executed event
    
    def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get the current status of a workflow"""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise WorkflowNotFoundError(f"Workflow {workflow_id} not found")
            
        return {
            "workflow_id": workflow.workflow_id,
            "status": workflow.status.value,
            "current_event": workflow.current_event,
            "events": {
                event_id: {
                    "status": event.status.value,
                    "retry_count": event.retry_count
                }
                for event_id, event in workflow.events.items()
            }
        }
