import os
import sys
import asyncio
import click
import json
import logging
import uuid
from datetime import datetime

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.orchestrator.engine import WorkflowEngine
from src.orchestrator.consumer import KafkaOrchestrator
from src.handlers.example_handlers import ValidationHandler, ProcessingHandler, NotificationHandler, ErrorHandler
from src.workflows.example_workflow import create_transaction_workflow
from src.utils.logging import setup_logging
from libs.kafka_flow import KafkaProducer, KafkaMessage
from config import config


def setup_orchestrator() -> KafkaOrchestrator:
    """Setup and configure the workflow orchestrator"""
    # Create workflow engine
    workflow_engine = WorkflowEngine()
    
    # Register handlers
    workflow_engine.register_handler("ValidationHandler", ValidationHandler)
    workflow_engine.register_handler("ProcessingHandler", ProcessingHandler)
    workflow_engine.register_handler("NotificationHandler", NotificationHandler)
    workflow_engine.register_handler("ErrorHandler", ErrorHandler)
    
    # Create and register workflow
    workflow = create_transaction_workflow()
    workflow_engine.register_workflow(workflow)
    
    # Create Kafka orchestrator
    return KafkaOrchestrator(workflow_engine=workflow_engine)


@click.group()
def cli():
    """Kafka Workflow Orchestrator CLI"""
    pass


@cli.command()
@click.option('--debug', is_flag=True, help='Enable debug logging')
def run_consumer(debug):
    """Run the Kafka consumer to process workflow events"""
    # Setup logging
    log_level = logging.DEBUG if debug else logging.INFO
    logger = setup_logging(log_level)
    
    logger.info("Starting Kafka consumer...")
    
    async def run():
        orchestrator = setup_orchestrator()
        try:
            await orchestrator.start()
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
            orchestrator.stop()
    
    asyncio.run(run())


@cli.command()
@click.option('--workflow-id', '-w', default='transaction_processing', help='ID of the workflow to execute')
@click.option('--data', '-d', help='JSON string of workflow data')
@click.option('--debug', is_flag=True, help='Enable debug logging')
def start_workflow(workflow_id: str, data: str, debug: bool):
    """Start a new workflow with the given ID and data"""
    # Setup logging
    log_level = logging.DEBUG if debug else logging.INFO
    logger = setup_logging(log_level)
    
    try:
        # Parse the JSON data or use empty dict if not provided
        workflow_data = json.loads(data) if data else {}
        
        # Generate trace_id at workflow start
        trace_id = f"wf_{uuid.uuid4().hex[:16]}_{int(datetime.now().timestamp())}"
        logger.info(f"Starting workflow {workflow_id} with trace_id: {trace_id}")
        logger.debug(f"Workflow data: {workflow_data}")
        
        async def execute():
            # Create Kafka producer just for publishing
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_CONFIG.KAFKA_SERVER,
                client_id=f"workflow-starter-{config.APP_CONFIG.APP_ID}"
            )
            
            # Publish START_WORKFLOW event
            producer.publish(KafkaMessage(
                topic=config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC,
                value={
                    'type': 'START_WORKFLOW',
                    'workflow_id': workflow_id,
                    'data': workflow_data,
                    'trace_id': trace_id
                }
            ))
            
            # Give some time for the message to be published
            await asyncio.sleep(1)
            logger.info("Workflow start message published successfully")
        
        asyncio.run(execute())
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON data format")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error starting workflow: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
