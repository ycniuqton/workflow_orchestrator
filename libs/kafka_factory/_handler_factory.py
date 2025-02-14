class HandlerFactory:
    def __init__(self, handlers):
        self._handlers = handlers

    def create(self, event_type: str):
        try:
            handler = self._handlers[event_type]
            return handler
        except KeyError as e:
            raise Exception(f"'{event_type}' is invalid event type") from e


class SingleHandler:
    def __init__(self, handler):
        self._handler = handler

    def create(self, event_type: str):
        return self._handler
