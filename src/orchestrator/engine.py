import asyncio
from typing import Dict, Any, Optional, Tuple, Type
from ..models.workflow import Workflow, EventStatus, EventContext, EventNode
from ..handlers.base import BaseEventHandler
from ..exceptions.workflow_exceptions import WorkflowNotFoundError, EventNotFoundError, HandlerNotFoundError
from libs.kafka_flow import KafkaMessage, KafkaProducer
from config import config
import logging

logger = logging.getLogger('orchestrator.engine')

class WorkflowEngine:
    """Engine for executing workflows"""
    
    def __init__(self):
        self._workflows: Dict[str, Workflow] = {}
        self._handlers: Dict[str, Type[BaseEventHandler]] = {}
        # Create Kafka producer for publishing event completion
        self._producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_CONFIG.KAFKA_SERVER,
            client_id=f"workflow-engine-{config.APP_CONFIG.APP_ID}"
        )
        self._topic = config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC
    
    def register_workflow(self, workflow: Workflow) -> None:
        """Register a workflow"""
        self._workflows[workflow.workflow_id] = workflow
    
    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """Get a workflow by ID"""
        return self._workflows.get(workflow_id)
    
    def register_handler(self, name: str, handler_class: Type[BaseEventHandler]) -> None:
        """Register an event handler"""
        self._handlers[name] = handler_class
    
    async def execute_event(self, context: EventContext) -> Tuple[bool, Dict[str, Any]]:
        """Execute a single event"""
        workflow = self._workflows.get(context.workflow_id)
        if not workflow:
            raise WorkflowNotFoundError(context.workflow_id)
        
        event = workflow.events.get(context.event_id)
        if not event:
            raise EventNotFoundError(context.workflow_id, context.event_id)
        
        handler_class = self._handlers.get(event.handler)
        if not handler_class:
            raise HandlerNotFoundError(f"Handler {event.handler} not found")
        
        handler = handler_class(context)
        event.status = EventStatus.RUNNING
        
        try:
            success, result = await handler.handle()
            event.status = EventStatus.SUCCESS if success else EventStatus.FAILED
            
            # Publish EVENT_COMPLETED message
            self._producer.publish(KafkaMessage(
                topic=self._topic,
                value={
                    'type': 'EVENT_COMPLETED',
                    'context': context.to_dict(),
                    'result': result,
                    'success': success
                }
            ))
            
            logger.info(f"Event {context.event_id} completed with status: {'success' if success else 'failed'}")
            logger.debug(f"Event result: {result}")
            
            return success, result
        except Exception as e:
            event.status = EventStatus.FAILED
            error_result = {"error": str(e)}
            
            # Publish EVENT_COMPLETED message with error
            self._producer.publish(KafkaMessage(
                topic=self._topic,
                value={
                    'type': 'EVENT_COMPLETED',
                    'context': context.to_dict(),
                    'result': error_result,
                    'success': False
                }
            ))
            
            logger.error(f"Error executing event {context.event_id}: {str(e)}", exc_info=True)
            raise
    
    async def execute_workflow(self, workflow_id: str, initial_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a workflow"""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise WorkflowNotFoundError(workflow_id)
        
        logger.info(f"Starting workflow: {workflow_id}")
        logger.debug(f"Initial data: {initial_data}")
        
        # Reset workflow state
        for event in workflow.events.values():
            event.status = EventStatus.PENDING
        
        # Start with the first event
        current_event = workflow.get_first_event()
        if not current_event:
            raise EventNotFoundError(workflow_id, "No first event found")
        
        context = EventContext(
            workflow_id=workflow_id,
            event_id=current_event.event_id,
            data=initial_data,
            metadata=workflow.metadata
        )
        
        success, result = await self.execute_event(context)
        
        if not success and current_event.next_on_failure:
            # Handle failure path
            error_context = EventContext(
                workflow_id=workflow_id,
                event_id=current_event.next_on_failure,
                data=initial_data,
                metadata=workflow.metadata,
                previous_event=current_event.event_id,
                previous_result=result
            )
            success, result = await self.execute_event(error_context)
        
        logger.info(f"Workflow {workflow_id} completed")
        logger.debug(f"Final result: {result}")
        
        return result
