import json
import asyncio
from typing import Dict, Any, Optional

from marshmallow import Schema, fields

from libs.kafka_flow import KafkaProducer, KafkaConsumer, KafkaMessage, MessageHandler
from src.orchestrator.engine import WorkflowEngine
from src.models.workflow import EventContext, Workflow
from src.exceptions.workflow_exceptions import WorkflowError
from config import config


class WorkflowMessageHandler(MessageHandler):
    def __init__(self, workflow_engine: WorkflowEngine, topic: str):
        self.workflow_engine = workflow_engine
        self._topic = topic

    def get_topics(self) -> list[str]:
        return [self._topic]

    async def handle(self, message: KafkaMessage) -> None:
        """Handle incoming workflow messages"""
        try:
            payload = message.value
            msg_type = payload.get('type')

            if msg_type == 'START_WORKFLOW':
                # Start a new workflow
                workflow_id = payload['workflow_id']
                initial_data = payload.get('data', {})
                result = await self.workflow_engine.execute_workflow(workflow_id, initial_data)

                # Publish workflow completion event
                self.publisher.publish(KafkaMessage(
                    topic=self._topic,
                    value={
                        'type': 'WORKFLOW_COMPLETED',
                        'workflow_id': workflow_id,
                        'result': result
                    }
                ))

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
        except Exception as e:
            # Handle unexpected errors
            print(f"Unexpected error: {e}")


class KafkaOrchestrator:
    def __init__(self, workflow_engine: WorkflowEngine):
        self.workflow_engine = workflow_engine
        self.topic = config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC

        # Create Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=config.KAFKA_CONFIG.KAFKA_SERVER,
            group_id=config.ORCHESTRATOR_CONFIG.GROUP_ID,
            auto_offset_reset=config.ORCHESTRATOR_CONFIG.AUTO_OFFSET_RESET,
            enable_auto_commit=config.ORCHESTRATOR_CONFIG.ENABLE_AUTO_COMMIT
        )

        # Create message handler
        self.handler = WorkflowMessageHandler(workflow_engine, self.topic)
        self.consumer.add_handler(self.handler)

        # Create Kafka publisher for events
        self.publisher = KafkaProducer(
            bootstrap_servers=config.KAFKA_CONFIG.KAFKA_SERVER,
            client_id=f"workflow-orchestrator-{config.APP_CONFIG.APP_ID}"
        )
        # Add publisher to handler for completion events
        self.handler.publisher = self.publisher

    def publish_event(self, topic: str, value: Dict[str, Any]) -> None:
        """Publish an event to a Kafka topic"""
        try:
            message = KafkaMessage(topic=topic, value=value)
            self.publisher.publish(message)
        except Exception as e:
            print(f"Error publishing message: {e}")
            raise

    async def start(self) -> None:
        """Start the Kafka orchestrator"""
        await self.consumer.start()

    def stop(self) -> None:
        """Stop the Kafka orchestrator"""
        self.consumer.stop()
        self.publisher.close()
