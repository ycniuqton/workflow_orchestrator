import asyncio
from typing import Dict, Any, Optional, List
from confluent_kafka import Consumer as ConfluentConsumer, KafkaError
from .message import KafkaMessage, MessageHandler
import logging

logger = logging.getLogger(__name__)

class KafkaConsumer:
    """An async Kafka consumer that supports message handlers"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True,
        **kwargs
    ):
        config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
            **kwargs
        }
        self._consumer = ConfluentConsumer(config)
        self._handlers: Dict[str, List[MessageHandler]] = {}
        self._running = False
        self._poll_timeout = 1.0  # seconds
    
    def add_handler(self, handler: MessageHandler) -> None:
        """
        Add a message handler for specific topics
        
        Args:
            handler: MessageHandler instance
        """
        for topic in handler.get_topics():
            if topic not in self._handlers:
                self._handlers[topic] = []
            self._handlers[topic].append(handler)
    
    def remove_handler(self, handler: MessageHandler) -> None:
        """
        Remove a message handler
        
        Args:
            handler: MessageHandler instance to remove
        """
        for handlers in self._handlers.values():
            if handler in handlers:
                handlers.remove(handler)
    
    async def _process_message(self, message: KafkaMessage) -> None:
        """Process a message using registered handlers"""
        topic = message.topic
        if topic in self._handlers:
            for handler in self._handlers[topic]:
                try:
                    await handler.handle(message)
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")
    
    async def start(self) -> None:
        """Start consuming messages"""
        if not self._handlers:
            raise ValueError("No message handlers registered")
        
        # Subscribe to all topics from handlers
        topics = list(self._handlers.keys())
        self._consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")
        
        self._running = True
        
        while self._running:
            try:
                msg = self._consumer.poll(self._poll_timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Create KafkaMessage from raw message
                message = KafkaMessage.deserialize(
                    topic=msg.topic(),
                    key=msg.key(),
                    value=msg.value(),
                    headers=msg.headers()
                )
                
                # Process message with handlers
                await self._process_message(message)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    
    def stop(self) -> None:
        """Stop consuming messages"""
        self._running = False
        self._consumer.close()
        logger.info("Consumer stopped")
