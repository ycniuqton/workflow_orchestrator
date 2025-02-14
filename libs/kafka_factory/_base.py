import json
import time
from datetime import date, datetime, timezone
from marshmallow import Schema, fields, validate, ValidationError
import logging
import os
import signal as sn
import sys
import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata
from json import JSONDecodeError

from ._constants import *
from ._exceptions import KafkaPublishMessageFailed

import logging_context

from ._handler_factory import HandlerFactory

from ._filters import Filterable
from ._exceptions import *


logger = logging.getLogger(__name__)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class KafkaPublisher:
    def __init__(self, servers, topic, ssl_config: Optional[Dict[str, Any]] = None) -> None:
        """
        :param servers: list of kafka servers
        :param topic: str
        :param ssl_config: Optional[Dict[str, Any]]
        example: ssl_config = {
            "security_protocol": self.settings.PRODUCER_SECURITY_PROTOCOL,
            "ssl_check_hostname": self.settings.PRODUCER_SSL_CHECK_HOSTNAME,
            "ssl_cafile": self.settings.PRODUCER_SSL_CA_FILE,
            "sasl_mechanism": self.settings.PRODUCER_SASL_MECHANISM,
            "sasl_plain_username": self.settings.PRODUCER_SASL_PLAIN_USERNAME,
            "sasl_plain_password": self.settings.PRODUCER_SASL_PLAIN_PASSWORD,
        }
        """
        self.__topic = topic
        config = {
            "bootstrap_servers": servers,
            "acks": ACK,
            "retries": RETRIES,
            "max_in_flight_requests_per_connection": MAX_IN_FLIGHT_PER_CONNECTION,
        }
        if ssl_config:
            config.update(ssl_config)

        self.__producer = KafkaProducer(**config)
        self.__max_retry = MAX_RETRY
        self.__delay = RETRY_DELAY

    def publish(
            self, event_type: str = None, payload: Dict[str, Any] = {}, key: Optional[str] = None,
            issued_at: Optional[datetime] = None
    ):
        retry_count = 0
        if not issued_at:
            issued_at = datetime.now(tz=timezone.utc)

        while True:
            if event_type:
                message = dict(
                    event_type=event_type or "",
                    payload=payload or {},
                    issued_at=issued_at,
                )
            else:
                message = payload
            if not key:
                key_formated = key
            else:
                key_formated = key.encode("utf-8")
            try:
                params = dict(
                    value=json.dumps(message, default=json_serial).encode("utf-8"), key=key_formated, topic=self.__topic
                )
                self.__producer.send(**params).get(MESSAGE_TIMEOUT)
            except (KafkaTimeoutError, ConnectionResetError) as e:
                if retry_count >= self.__max_retry:
                    logger.exception("Send message failed - %s - %s", message, e)
                    raise KafkaPublishMessageFailed(e) from e
                logger.warning("Send message failed - %s - %s", message, e)
                retry_count += 1
                logger.warning(
                    "Retry %s/%s in %s seconds on message - %s", retry_count, self.__max_retry, self.__delay, message
                )
                time.sleep(self.__delay)

            except Exception as e:
                logger.exception(f"Send message failed - %s - %s", message, e)
                raise KafkaPublishMessageFailed(e) from e
            else:
                break


class Listener(ABC):
    def __init__(
        self,
        handler_factory: HandlerFactory,
        filters: Optional[List[Filterable]] = None,
        before_handle_callback: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
        after_handle_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.handler_factory = handler_factory
        self._filters = filters if filters else []
        self._before_handle_callback = before_handle_callback
        self._after_handle_callback = after_handle_callback

        # graceful shutdown settings
        self.stopped = False
        sn.signal(sn.SIGINT, self._stop)
        sn.signal(sn.SIGTERM, self._stop)

    def _stop(self, signal, frame):  # pylint: disable=W0613
        if self.stopped:
            sys.exit(0)
        self.stopped = True
        logger.warning(
            "This worker will stop after completing the current job. Press Ctrl+C or send kill -9 %s to force stop",
            os.getpid(),
        )

    def _filter_event(self, event: Dict[str, Any]):
        for event_filter in self._filters:
            event_filter.filter(event)

    @abstractmethod
    def _get_new_event(self) -> Dict[str, Any]:
        return {}

    @abstractmethod
    def _commit_event(self, event: Dict[str, Any]) -> None:
        pass

    def _handle_event(self, event_data: Dict[str, Any]) -> None:
        prepared_data = None
        if self._before_handle_callback:
            prepared_data = self._before_handle_callback(event_data)

        data = {"event": event_data, "prepared_data": prepared_data}
        event_type = event_data.get("event_type")
        if not event_type:
            logger.warning("EVENT %s DOES NOT HAVE EVENT TYPE")
            return

        handler = self.handler_factory.create(event_type=event_type)
        result = handler.handle(data)

        if self._after_handle_callback:
            self._after_handle_callback(result)

    def listen(self):
        logger.info("LISTENING EVENT...")
        context = logging_context.get_logging_context()
        while not self.stopped:
            event = self._get_new_event()
            if not event:
                continue
            audit_id = uuid.uuid4().hex
            context.set_value("AUDIT_ID", audit_id)
            logger.info("RETRIEVED EVENT: %s", event)
            try:
                event_data = event.get("event", {})
                logger.info("FILTERING EVENT")
                self._filter_event(event_data)
                logger.info("HANDLING EVENT")
                self._handle_event(event_data)
            except SkippableException as se:
                logger.info("EVENT SKIPPED BECAUSE %s", se)
                self._commit_event(event)
                continue
            except InvalidEventPayload as ex:
                logger.info(f"INVALID PAYLOAD {str(ex)}")
                self._commit_event(event)
                continue
            else:
                logger.info("EVENT HANDLED")
                self._commit_event(event)
            finally:
                logger.info("FINISHED EVENT")
                context.delete_value("AUDIT_ID")


class KafkaListener(Listener):
    def __init__(
        self,
        settings,
        handler_factory: HandlerFactory,
        filters: Optional[List[Filterable]] = None,
        before_handle_callback: Optional[
            Callable[[Dict[str, Any]], Dict[str, Any]]
        ] = None,
        after_handle_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        kafka_consumer: KafkaConsumer = None,
        topic: str = None,
    ) -> None:
        super().__init__(
            handler_factory=handler_factory,
            filters=filters,
            before_handle_callback=before_handle_callback,
            after_handle_callback=after_handle_callback,
        )
        self.settings = settings
        self.__topic = topic or self.settings.TOPIC
        config = {
            "bootstrap_servers": self.settings.KAFKA_SERVER,
            "group_id": self.settings.GROUP_ID,
            "enable_auto_commit": AUTO_COMMIT,
            "auto_offset_reset": AUTO_OFFSET_RESET,
        }
        if self.settings.ENABLE_KAFKA_SSL:
            config = {
                "security_protocol": self.settings.KAFKA_SECURITY_PROTOCOL,
                "ssl_check_hostname": self.settings.KAFKA_SSL_CHECK_HOSTNAME,
                "ssl_cafile": self.settings.KAFKA_SSL_CA_FILE,
                "sasl_mechanism": self.settings.KAFKA_SASL_MECHANISM,
                "sasl_plain_username": self.settings.KAFKA_SASL_PLAIN_USERNAME,
                "sasl_plain_password": self.settings.KAFKA_SASL_PLAIN_PASSWORD,
                **config,
            }
        self.__consumer = kafka_consumer or KafkaConsumer(self.__topic, **config)

    def _get_new_event(self) -> Dict[str, Any]:
        message_batch = self.__consumer.poll(TIMEOUT_LISTENER, MAX_RECORD)
        if not message_batch:
            return {}
        for topic_partition, message_record in message_batch.items():
            record = message_record[0]
            try:
                message_decode = record.value.decode("utf-8")
                message = json.loads(message_decode)
                if not record.key:
                    key = record.key
                else:
                    key = record.key.decode("utf-8")
                offset = record.offset

            except (UnicodeDecodeError, JSONDecodeError) as e:
                logger.exception("DECODING ERROR: %s - EXCEPTIONS: %s", record.value, e)
                return {}
            return {
                "event": message,
                "meta_data": {
                    "offset": offset,
                    "key": key,
                    "topic_partition": topic_partition,
                },
            }
        else:
            return {}

    def _commit_event(self, event: Dict[str, Any]) -> None:
        event_metadata = event.get("meta_data", {})
        topic_partition = event_metadata.get("topic_partition")
        offset = event_metadata.get("offset")
        if topic_partition is not None and offset is not None:
            meta = self.__consumer.partitions_for_topic(topic_partition.topic)
            self.__consumer.commit(
                {topic_partition: OffsetAndMetadata(offset + 1, metadata=meta)}
            )
            logger.info("EVENT COMMITTED")
        else:
            logger.error(
                "CANNOT COMMIT EVENT %s. MISSING FIELD TOPIC_PARTITION OR OFFSET", event
            )


class BaseHandler(ABC):
    def __make_connection(self):
        pass

    @abstractmethod
    def _get_schema(self) -> Schema:
        raise NotImplementedError()

    @abstractmethod
    def _handle(self, payload: Dict[str, Any]) -> None:
        raise NotImplementedError()

    def _validate_payload(self, payload: Any) -> Any:
        schema = self._get_schema()
        try:
            validated_payload = schema.load(payload)
            return validated_payload
        except ValidationError as e:
            raise InvalidEventPayload(str(payload))

    def handle(self, data: Any) -> Any:
        self.__make_connection()
        payload = data.get("event", {}).get("payload")
        payload = self._validate_payload(payload)
        result = None
        try:
            result = self._handle(payload)
        except Exception as exc:
            logger.error(f"Unexpected error: {str(exc)}")
            raise CriticalException

        return result
