from config import config

def main():
    # Print APP_NAME from APP_CONFIG
    print(f"Current APP_NAME: {config.APP_CONFIG.APP_NAME}")
    print(f"Current APP_ID: {config.APP_CONFIG.APP_ID}")
    
    # You can also access other config sections
    print(f"\nKafka Configuration:")
    print(f"Bootstrap Servers: {config.KAFKA_CONFIG.BOOTSTRAP_SERVERS}")
    print(f"Topic: {config.KAFKA_CONFIG.TOPIC}")

if __name__ == "__main__":
    main()
