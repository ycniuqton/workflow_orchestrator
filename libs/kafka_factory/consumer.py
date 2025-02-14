from ._base import KafkaListener, HandlerFactory, BaseHandler
from config import BillingKafkaConfig
from marshmallow import Schema, fields
from datetime import datetime
from typing import Dict, Any, Optional


class CustomeHandler(BaseHandler):
    def __init__(self) -> None:
        super().__init__()

    def _get_schema(self) -> Schema:
        class MySchema(Schema):
            tenant_id = fields.String(required=True)
            executed_at = fields.DateTime(missing=datetime.utcnow)
            amount = fields.Float(required=False)
            balance_type_id = fields.String(required=False)
            payment_id = fields.String(required=False)

        return MySchema()

    def _handle(self, payload: Dict[str, Any]) -> None:
        print(payload)


if __name__ == "__main__":
    handlers = {
        'CustomeHandler': CustomeHandler()
    }
    handler_factory = HandlerFactory(handlers=handlers)
    listener = KafkaListener(
        BillingKafkaConfig,
        handler_factory=handler_factory,
    )
    listener.listen()
