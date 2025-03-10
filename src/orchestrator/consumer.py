import json
import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

from marshmallow import Schema, fields

from libs.kafka_flow import KafkaProducer, KafkaConsumer, KafkaMessage, MessageHandler
from src.orchestrator.engine import WorkflowEngine
from src.models.workflow import EventContext, Workflow
from src.exceptions.workflow_exceptions import WorkflowError
from config import config

logger = logging.getLogger('orchestrator')

def generate_trace_id() -> str:
    """Generate a unique trace ID for workflow execution"""
    return f"wf_{uuid.uuid4().hex[:16]}_{int(datetime.now().timestamp())}"

class WorkflowMessageHandler(MessageHandler):
    def __init__(self, workflow_engine: WorkflowEngine, topic: str):
        self.workflow_engine = workflow_engine
        self._topic = topic
        self.logger = logging.getLogger('orchestrator.handler')

    def get_topics(self) -> list[str]:
        return [self._topic]

    async def handle(self, message: KafkaMessage) -> None:
        """Handle incoming workflow messages"""
        try:
            payload = message.value
            msg_type = payload.get('type')
            
            self.logger.info(f"Received message type: {msg_type}")
            self.logger.debug(f"Message payload: {payload}")

            if msg_type == 'START_WORKFLOW':        
                workflow_id = payload['workflow_id']
                initial_data = payload.get('data', {})
                trace_id = payload.get('trace_id')
                
                workflow = self.workflow_engine.get_workflow(workflow_id)
                if not workflow:
                    self.logger.error(f"Workflow {workflow_id} not found")
                    return

                # If no trace_id provided, generate one
                if not trace_id:
                    trace_id = generate_trace_id()
                    self.logger.info(f"No trace_id provided, generated new one: {trace_id}")
                else:
                    self.logger.info(f"Using provided trace_id: {trace_id}")

                # Get the first event
                first_event = workflow.get_first_event()
                if not first_event:
                    self.logger.error(f"No first event found for workflow {workflow_id}")
                    return

                # Create initial context with trace_id
                context = EventContext(
                    workflow_id=workflow_id,
                    event_id=first_event.event_id,
                    data=initial_data,
                    metadata=workflow.metadata or {},
                    trace_id=trace_id
                )
                
                await self.workflow_engine.execute_event(context)

            elif msg_type == 'EVENT_COMPLETED':
                context = EventContext.from_dict(payload.get('context', {}))
                workflow = self.workflow_engine.get_workflow(context.workflow_id)
                
                if not context.trace_id:
                    self.logger.warning(f"No trace_id in EVENT_COMPLETED for workflow {context.workflow_id}, event {context.event_id}")
                    context.trace_id = generate_trace_id()
                    self.logger.info(f"Generated new trace_id: {context.trace_id}")

                self.logger.info(f"Event completed: {context.event_id} in workflow {context.workflow_id} with trace_id: {context.trace_id}")
                self.logger.debug(f"Event context: {context}")

                if workflow:
                    event = workflow.events.get(context.event_id)
                    if event:
                        if payload.get('success', False):
                            next_event_id = event.next_on_success
                        else:
                            next_event_id = event.next_on_failure
                            
                        if next_event_id:
                            self.logger.info(f"Triggering next event: {next_event_id}")
                            
                            next_context = EventContext(
                                workflow_id=context.workflow_id,
                                event_id=next_event_id,
                                data=context.data,
                                metadata=workflow.metadata,
                                previous_event=context.event_id,
                                previous_result=payload.get('result'),
                                trace_id=context.trace_id  # Forward the trace_id to next event
                            )
                            await self.workflow_engine.execute_event(next_context)
                        else:
                            # This was the last event in the workflow
                            self.logger.info(f"Workflow {context.workflow_id} completed successfully - all events processed")
                            self.logger.debug(f"Final event result: {payload.get('result')}")
                            
                            # Publish workflow completion event with same trace_id
                            self.publisher.publish(KafkaMessage(
                                topic=self._topic,
                                value={
                                    'type': 'WORKFLOW_COMPLETED',
                                    'workflow_id': context.workflow_id,
                                    'final_event': context.event_id,
                                    'result': payload.get('result'),
                                    'trace_id': context.trace_id,
                                    'metadata': {
                                        'completed_at': datetime.now().isoformat(),
                                        'total_events': len(workflow.events)
                                    }
                                }
                            ))
                    else:
                        self.logger.warning(f"Event {context.event_id} not found in workflow {context.workflow_id}")
                else:
                    self.logger.warning(f"Workflow {context.workflow_id} not found")
            elif msg_type == 'WORKFLOW_COMPLETED':
                workflow_id = payload.get('workflow_id')
                result = payload.get('result')
                self.logger.info(f"Received workflow completion notification for workflow: {workflow_id}")
                self.logger.debug(f"Workflow result: {result}")
                # We don't need to take any action here, just log it
                
            else:
                self.logger.warning(f"Unknown message type: {msg_type}")

        except WorkflowError as e:
            self.logger.error(f"Workflow error: {str(e)}", exc_info=True)
        except Exception as e:      
            self.logger.error(f"Unexpected error: {str(e)}", exc_info=True)


class KafkaOrchestrator:
    def __init__(self, workflow_engine: WorkflowEngine):
        self.workflow_engine = workflow_engine
        self.topic = config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC
        self.logger = logging.getLogger('orchestrator.kafka')

        self.logger.info("Initializing Kafka Orchestrator")
        self.logger.debug(f"Using topic: {self.topic}")
        
        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=config.KAFKA_CONFIG.KAFKA_SERVER,
            group_id=config.ORCHESTRATOR_CONFIG.GROUP_ID,
            auto_offset_reset=config.ORCHESTRATOR_CONFIG.AUTO_OFFSET_RESET,
            enable_auto_commit=config.ORCHESTRATOR_CONFIG.ENABLE_AUTO_COMMIT
        )
        self.logger.info(f"Created Kafka consumer with group ID: {config.ORCHESTRATOR_CONFIG.GROUP_ID}")

        # Create message handler
        self.handler = WorkflowMessageHandler(workflow_engine, self.topic)
        self.consumer.add_handler(self.handler)
        self.logger.info("Registered workflow message handler")

        # Create Kafka publisher for events
        self.publisher = KafkaProducer(
            bootstrap_servers=config.KAFKA_CONFIG.KAFKA_SERVER,
            client_id=f"workflow-orchestrator-{config.APP_CONFIG.APP_ID}"
        )
        self.logger.info(f"Created Kafka producer with client ID: workflow-orchestrator-{config.APP_CONFIG.APP_ID}")
        
        # Add publisher to handler for completion events
        self.handler.publisher = self.publisher

    def publish_event(self, topic: str, value: Dict[str, Any]) -> None:
        """Publish an event to a Kafka topic"""
        try:
            self.logger.info(f"Publishing event to topic: {topic}")
            self.logger.debug(f"Event data: {value}")
            
            message = KafkaMessage(topic=topic, value=value)
            self.publisher.publish(message)
            
            self.logger.info("Event published successfully")
        except Exception as e:
            self.logger.error(f"Error publishing message: {str(e)}", exc_info=True)
            raise

    async def start(self) -> None:
        """Start the Kafka orchestrator"""
        self.logger.info("Starting Kafka orchestrator")
        try:
            await self.consumer.start()
        except Exception as e:
            self.logger.error(f"Error starting orchestrator: {str(e)}", exc_info=True)
            raise

    def stop(self) -> None:
        """Stop the Kafka orchestrator"""
        self.logger.info("Stopping Kafka orchestrator")
        try:
            self.consumer.stop()
            self.publisher.close()
            self.logger.info("Kafka orchestrator stopped successfully")
        except Exception as e:
            self.logger.error(f"Error stopping orchestrator: {str(e)}", exc_info=True)
