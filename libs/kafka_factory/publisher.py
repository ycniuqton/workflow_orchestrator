from ._base import KafkaPublisher


def make_kafka_publisher(kafka_config) -> KafkaPublisher:
    ssl_config = {}
    if kafka_config.ENABLE_KAFKA_SSL:
        ssl_config = {
            "security_protocol": kafka_config.KAFKA_SECURITY_PROTOCOL,
            "ssl_check_hostname": kafka_config.KAFKA_SSL_CHECK_HOSTNAME,
            "ssl_cafile": kafka_config.KAFKA_SSL_CA_FILE,
            "sasl_mechanism": kafka_config.KAFKA_SASL_MECHANISM,
            "sasl_plain_username": kafka_config.KAFKA_SASL_PLAIN_USERNAME,
            "sasl_plain_password": kafka_config.KAFKA_SASL_PLAIN_PASSWORD,
        }
    publisher = KafkaPublisher(
        servers=kafka_config.KAFKA_SERVER,
        topic=kafka_config.TOPIC,
        ssl_config=ssl_config,
    )
    return publisher
