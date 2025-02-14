from typing import Dict, Any, Optional
from confluent_kafka import Producer as ConfluentProducer
from .message import KafkaMessage
import logging

logger = logging.getLogger(__name__)

class KafkaProducer:
    """A Kafka producer that supports both sync and async message publishing"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: Optional[str] = None,
        **kwargs
    ):
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': client_id or 'kafka-flow-producer',
            **kwargs
        }
        self._producer = ConfluentProducer(config)
        
    def _delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def publish(self, message: KafkaMessage) -> None:
        """
        Publish a message to Kafka synchronously
        
        Args:
            message: KafkaMessage instance to publish
        """
        try:
            key_bytes, value_bytes, headers = message.serialize()
            
            self._producer.produce(
                topic=message.topic,
                key=key_bytes,
                value=value_bytes,
                headers=headers,
                callback=self._delivery_report
            )
            # Trigger any available delivery report callbacks
            self._producer.poll(0)
            
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            raise
    
    def flush(self, timeout: float = None) -> None:
        """
        Wait for all messages in the Producer queue to be delivered.
        
        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.
        """
        self._producer.flush(timeout=timeout if timeout else -1)
    
    def close(self) -> None:
        """Close the producer and wait for any outstanding messages to be delivered"""
        self.flush()
        self._producer = None
