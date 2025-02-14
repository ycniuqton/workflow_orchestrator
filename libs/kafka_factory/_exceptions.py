class KafkaPublishMessageFailed(Exception):
    pass


class CriticalException(Exception):
    pass


class NoticeableException(Exception):
    pass


class SkippableException(Exception):
    pass


class PostPaidAccount(SkippableException):
    pass


class InvalidEventPayload(NoticeableException):
    pass


class InvalidEventTypeError(CriticalException):
    pass


class KafkaPublishMessageFailed(CriticalException):
    pass


class UnexpectedEvent(SkippableException):
    pass


class V4EventMissingField(CriticalException):
    pass


class V4EventEmpty(CriticalException):
    pass


class InvalidUsageCalculator(CriticalException):
    pass


class ResourceNotFound(CriticalException):
    pass


class DuplicateResource(CriticalException):
    pass


class InvalidPlan(CriticalException):
    pass


class LeavedAccount(CriticalException):
    pass

