import asyncio
import os
import sys
import click
import json

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.orchestrator.engine import WorkflowEngine
from src.orchestrator.consumer import KafkaOrchestrator
from src.handlers.example_handlers import ValidationHandler, ProcessingHandler, NotificationHandler, ErrorHandler
from src.workflows.example_workflow import create_transaction_workflow
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
def run_consumer():
    """Run the Kafka consumer to process workflow events"""
    click.echo("Starting Kafka consumer...")
    
    async def run():
        orchestrator = setup_orchestrator()
        try:
            await orchestrator.start()
        except KeyboardInterrupt:
            click.echo("\nShutting down consumer...")
            orchestrator.stop()
    
    asyncio.run(run())


@cli.command()
@click.option('--workflow-id', '-w', default='transaction_processing', help='ID of the workflow to execute')
@click.option('--data', '-d', help='JSON string of workflow data')
def start_workflow(workflow_id: str, data: str):
    """Start a new workflow with the given ID and data"""
    try:
        # Parse the JSON data or use empty dict if not provided
        workflow_data = json.loads(data) if data else {}
        
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
        click.echo(f"Workflow {workflow_id} started successfully!")
        
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON data format", err=True)
    except Exception as e:
        click.echo(f"Error starting workflow: {e}", err=True)


if __name__ == "__main__":
    cli()
