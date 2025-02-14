# Constant for kafka consumer
TIMEOUT_LISTENER = 300
MAX_RECORD = 1
AUTO_COMMIT = False
AUTO_OFFSET_RESET = "earliest"

# Constant for kafka producer
MESSAGE_TIMEOUT = 30
RETRY_DELAY = 1
MAX_RETRY = 2
MAX_IN_FLIGHT_PER_CONNECTION = 1
ACK = "all"
RETRIES = 2
