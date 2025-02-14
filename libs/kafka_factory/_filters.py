import logging
from typing import Any, List
from ._exceptions import UnexpectedEvent

logger = logging.getLogger(__name__)


class Filterable:
    def filter(self, event: Any) -> bool:
        raise NotImplementedError()


class ValidEventTypeFilter(Filterable):
    def __init__(self, supported_event_types: List[str]) -> None:
        self._supported_event_types = supported_event_types
        logger.info("SUPPORTED EVENT TYPES: %s", supported_event_types)

    def filter(self, event: Any) -> bool:
        event_type = event.get("event_type")
        if not event_type:
            raise UnexpectedEvent("EVENT DOES NOT HAVE EVENT TYPE")
        if event_type not in self._supported_event_types:
            raise UnexpectedEvent("EVENT IS NOT SUPPORTED")

        return True
