from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass
import json
import logging

logger = logging.getLogger(__name__)

@dataclass
class KafkaMessage:
    """Represents a Kafka message with key, value, and metadata"""
    topic: str
    value: Dict[str, Any]
    key: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary format"""
        return {
            'topic': self.topic,
            'value': self.value,
            'key': self.key,
            'headers': self.headers
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'KafkaMessage':
        """Create message from dictionary format"""
        return cls(**data)
    
    def serialize(self) -> tuple:
        """Serialize message for Kafka"""
        key_bytes = self.key.encode('utf-8') if self.key else None
        value_bytes = json.dumps(self.value).encode('utf-8')
        headers = [(k, v.encode('utf-8')) for k, v in (self.headers or {}).items()]
        return key_bytes, value_bytes, headers

    @classmethod
    def deserialize(cls, topic: str, key: Optional[bytes], value: bytes, headers=None) -> 'KafkaMessage':
        """Deserialize message from Kafka"""
        try:
            key_str = key.decode('utf-8') if key else None
            value_dict = json.loads(value.decode('utf-8'))
            headers_dict = {
                k: v.decode('utf-8') 
                for k, v in (headers or [])
            } if headers else None
            
            return cls(
                topic=topic,
                key=key_str,
                value=value_dict,
                headers=headers_dict
            )
        except Exception as e:
            logger.error(f"Error deserializing message: {e}")
            raise

class MessageHandler(ABC):
    """Base class for message handlers"""
    
    @abstractmethod
    async def handle(self, message: KafkaMessage) -> None:
        """Handle a Kafka message"""
        pass
    
    @abstractmethod
    def get_topics(self) -> list[str]:
        """Get list of topics this handler is interested in"""
        pass
