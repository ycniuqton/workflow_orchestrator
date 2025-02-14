import json
import asyncio
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, Producer, KafkaError
from .engine import WorkflowEngine
from ..models.workflow import EventContext, Workflow
from ..exceptions.workflow_exceptions import WorkflowError

class KafkaOrchestrator:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        workflow_engine: WorkflowEngine,
        orchestrator_topic: str = "workflow_orchestrator"
    ):
        self.workflow_engine = workflow_engine
        self.orchestrator_topic = orchestrator_topic
        
        # Configure Kafka consumer
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        
        # Configure Kafka producer for publishing events
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })
        
        self._running = False
    
    def delivery_report(self, err, msg):
        """Callback for producer delivery reports"""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    async def publish_event(self, topic: str, key: str, value: Dict[str, Any]):
        """Publish an event to a Kafka topic"""
        try:
            self.producer.produce(
                topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)  # Trigger delivery reports
        except Exception as e:
            print(f"Error publishing message: {e}")
            raise
    
    async def handle_workflow_message(self, message: Dict[str, Any]):
        """Handle incoming workflow messages"""
        try:
            msg_type = message.get('type')
            
            if msg_type == 'START_WORKFLOW':
                # Start a new workflow
                workflow_id = message['workflow_id']
                initial_data = message.get('data', {})
                result = await self.workflow_engine.execute_workflow(workflow_id, initial_data)
                
                # Publish workflow completion event
                await self.publish_event(
                    self.orchestrator_topic,
                    f"workflow_{workflow_id}",
                    {
                        'type': 'WORKFLOW_COMPLETED',
                        'workflow_id': workflow_id,
                        'result': result
                    }
                )
            
            elif msg_type == 'EVENT_COMPLETED':
                # Handle event completion and trigger next event if needed
                context = EventContext.from_dict(message['context'])
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
                                previous_result=message.get('result')
                            )
                            await self.workflow_engine.execute_event(next_context)
        
        except WorkflowError as e:
            # Handle workflow-specific errors
            print(f"Workflow error: {e}")
            # Publish error event if needed
        except Exception as e:
            # Handle unexpected errors
            print(f"Unexpected error: {e}")
    
    async def start(self):
        """Start the Kafka orchestrator"""
        self._running = True
        self.consumer.subscribe([self.orchestrator_topic])
        
        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    await self.handle_workflow_message(value)
                except json.JSONDecodeError:
                    print(f"Error decoding message: {msg.value()}")
                except Exception as e:
                    print(f"Error processing message: {e}")
        
        finally:
            self.consumer.close()
    
    def stop(self):
        """Stop the Kafka orchestrator"""
        self._running = False
