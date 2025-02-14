import asyncio
import os
import sys
import click
import json
import logging

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.orchestrator.engine import WorkflowEngine
from src.orchestrator.consumer import KafkaOrchestrator
from src.handlers.example_handlers import ValidationHandler, ProcessingHandler, NotificationHandler, ErrorHandler
from src.workflows.example_workflow import create_transaction_workflow
from src.utils.logging import setup_logging
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
        
        logger.info(f"Starting workflow: {workflow_id}")
        logger.debug(f"Workflow data: {workflow_data}")
        
        async def execute():
            orchestrator = setup_orchestrator()
            orchestrator.publish_event(
                config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC,
                {
                    'type': 'START_WORKFLOW',
                    'workflow_id': workflow_id,
                    'data': workflow_data
                }
            )
            # Give some time for the message to be published
            await asyncio.sleep(1)
            orchestrator.stop()
        
        asyncio.run(execute())
        logger.info(f"Workflow {workflow_id} started successfully!")
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON data format")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error starting workflow: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
