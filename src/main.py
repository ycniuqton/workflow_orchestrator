import asyncio
from .orchestrator.engine import WorkflowEngine
from .orchestrator.consumer import KafkaOrchestrator
from .handlers.example_handlers import ValidationHandler, ProcessingHandler, NotificationHandler, ErrorHandler
from .workflows.example_workflow import create_transaction_workflow
from config import config

async def main():
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
    
    # Create Kafka orchestrator using configuration
    orchestrator = KafkaOrchestrator(workflow_engine=workflow_engine)
    
    # Example: Trigger a workflow
    await orchestrator.publish_event(
        config.ORCHESTRATOR_CONFIG.ORCHESTRATOR_TOPIC,
        {
            'type': 'START_WORKFLOW',
            'workflow_id': 'transaction_processing',
            'data': {
                'user_id': '12345',
                'amount': 100.00
            }
        }
    )
    
    # Start the orchestrator
    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        orchestrator.stop()

if __name__ == "__main__":
    asyncio.run(main())
