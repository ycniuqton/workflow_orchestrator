import json
import asyncio
from typing import Dict, Any, Optional
from libs.kafka_factory import KafkaPublisher, KafkaListener, HandlerFactory, BaseHandler
from .engine import WorkflowEngine
from ..models.workflow import EventContext, Workflow
from ..exceptions.workflow_exceptions import WorkflowError
from config import config

class WorkflowMessageHandler(BaseHandler):
    def __init__(self, workflow_engine: WorkflowEngine):
        super().__init__()
        self.workflow_engine = workflow_engine
    
    async def _handle(self, payload: Dict[str, Any]) -> None:
        """Handle incoming workflow messages"""
        try:
            msg_type = payload.get('type')
            
            if msg_type == 'START_WORKFLOW':
                # Start a new workflow
                workflow_id = payload['workflow_id']
                initial_data = payload.get('data', {})
                result = await self.workflow_engine.execute_workflow(workflow_id, initial_data)
                
                # Publish workflow completion event
                await self.publish_event(
                    config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC,
                    {
                        'type': 'WORKFLOW_COMPLETED',
                        'workflow_id': workflow_id,
                        'result': result
                    }
                )
            
            elif msg_type == 'EVENT_COMPLETED':
                # Handle event completion and trigger next event if needed
                context = EventContext.from_dict(payload['context'])
                workflow = self.workflow_engine.get_workflow(context.workflow_id)
                
                if workflow:
                    event = workflow.events.get(context.event_id)
                    if event:
                        next_event_id = event.next_on_success
                        if next_event_id:
                            next_context = EventContext(
                                workflow_id=context.workflow_id,
                                event_id=next_event_id,
                                data=context.data,
                                metadata=workflow.metadata,
                                previous_event=context.event_id,
                                previous_result=payload.get('result')
                            )
                            await self.workflow_engine.execute_event(next_context)
        
        except WorkflowError as e:
            # Handle workflow-specific errors
            print(f"Workflow error: {e}")
            # Publish error event if needed
        except Exception as e:
            # Handle unexpected errors
            print(f"Unexpected error: {e}")

class KafkaOrchestrator:
    def __init__(self, workflow_engine: WorkflowEngine):
        self.workflow_engine = workflow_engine
        
        # Create message handler
        handlers = {
            'WorkflowMessageHandler': WorkflowMessageHandler(workflow_engine)
        }
        self.handler_factory = HandlerFactory(handlers=handlers)
        
        # Create Kafka listener
        self.listener = KafkaListener(
            settings=config.KAFKA_CONFIG,
            handler_factory=self.handler_factory,
            topic=config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC
        )
        
        # Create Kafka publisher for events
        self.publisher = KafkaPublisher(
            servers=config.KAFKA_CONFIG.BOOTSTRAP_SERVERS,
            topic=config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC
        )
    
    async def publish_event(self, topic: str, value: Dict[str, Any]):
        """Publish an event to a Kafka topic"""
        try:
            await self.publisher.publish(value)
        except Exception as e:
            print(f"Error publishing message: {e}")
            raise
    
    async def start(self):
        """Start the Kafka orchestrator"""
        await self.listener.listen()
    
    def stop(self):
        """Stop the Kafka orchestrator"""
        self.listener.stopped = True
