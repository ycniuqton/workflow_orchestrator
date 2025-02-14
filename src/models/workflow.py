from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from enum import Enum

class EventStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

@dataclass
class EventNode:
    event_id: str
    topic: str
    handler: str  # Handler class name
    next_on_success: Optional[str] = None  # Next event_id on success
    next_on_failure: Optional[str] = None  # Next event_id on failure
    required_fields: List[str] = None  # Required fields in the event data
    retry_count: int = 0
    max_retries: int = 3
    timeout_ms: int = 30000  # Timeout in milliseconds
    status: EventStatus = EventStatus.PENDING
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "topic": self.topic,
            "handler": self.handler,
            "next_on_success": self.next_on_success,
            "next_on_failure": self.next_on_failure,
            "required_fields": self.required_fields,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "timeout_ms": self.timeout_ms,
            "status": self.status.value
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EventNode':
        data = data.copy()
        data['status'] = EventStatus(data.get('status', 'PENDING'))
        return cls(**data)

@dataclass
class Workflow:
    workflow_id: str
    name: str
    description: str
    start_event: str  # event_id of the first event
    events: Dict[str, EventNode]  # Map of event_id to EventNode
    metadata: Dict[str, Any] = None
    status: EventStatus = EventStatus.PENDING
    current_event: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "description": self.description,
            "start_event": self.start_event,
            "events": {
                event_id: event.to_dict()
                for event_id, event in self.events.items()
            },
            "metadata": self.metadata or {},
            "status": self.status.value,
            "current_event": self.current_event
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Workflow':
        data = data.copy()
        data['events'] = {
            event_id: EventNode.from_dict(event_data)
            for event_id, event_data in data.get('events', {}).items()
        }
        data['status'] = EventStatus(data.get('status', 'PENDING'))
        return cls(**data)
    
    def get_first_event(self) -> Optional[EventNode]:
        """Get the first event in the workflow"""
        return self.events.get(self.start_event)

@dataclass
class EventContext:
    workflow_id: str
    event_id: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    trace_id: str  # Added trace_id for tracking workflow execution
    previous_event: Optional[str] = None
    previous_result: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "event_id": self.event_id,
            "data": self.data,
            "metadata": self.metadata,
            "trace_id": self.trace_id,
            "previous_event": self.previous_event,
            "previous_result": self.previous_result
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EventContext':
        return cls(**data)
