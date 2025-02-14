import os
import yaml
from typing import Dict, Any, List

class BaseConfig:
    def __init__(self, config_data: Dict[str, Any]):
        self._config_data = config_data

    def __getattr__(self, name: str) -> Any:
        if name in self._config_data:
            return self._config_data[name]
        raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")

class AppConfig(BaseConfig):
    @property
    def APP_NAME(self) -> str:
        return self._config_data.get('APP_NAME', '')
    
    @property
    def APP_ID(self) -> str:
        return self._config_data.get('APP_ID', '')

class KafkaConfig(BaseConfig):
    @property
    def BOOTSTRAP_SERVERS(self) -> str:
        return self._config_data.get('BOOTSTRAP_SERVERS', 'localhost:9092')
    
    @property
    def TOPIC(self) -> str:
        return self._config_data.get('TOPIC', '')
    
    @property
    def GROUP_ID(self) -> str:
        return self._config_data.get('GROUP_ID', '')

class TelegramConfig(BaseConfig):
    @property
    def BOT_TOKEN(self) -> str:
        return self._config_data.get('BOT_TOKEN', '')
    
    @property
    def ALLOWED_USERS(self) -> List[int]:
        return self._config_data.get('ALLOWED_USERS', [])
    
    @property
    def ALLOWED_GROUPS(self) -> List[int]:
        return self._config_data.get('ALLOWED_GROUPS', [])

class OrchestratorConfig(BaseConfig):
    @property
    def ORCHESTRATOR_TOPIC(self) -> str:
        return self._config_data.get('ORCHESTRATOR_TOPIC', 'workflow_orchestrator')
    
    @property
    def GROUP_ID(self) -> str:
        return self._config_data.get('GROUP_ID', 'workflow_orchestrator_group')
    
    @property
    def AUTO_OFFSET_RESET(self) -> str:
        return self._config_data.get('AUTO_OFFSET_RESET', 'earliest')
    
    @property
    def ENABLE_AUTO_COMMIT(self) -> bool:
        return self._config_data.get('ENABLE_AUTO_COMMIT', True)

class Config:
    def __init__(self, yaml_path: str = None):
        if yaml_path is None:
            yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'env.yaml')
        
        with open(yaml_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        self.APP_CONFIG = AppConfig(config_data.get('APP_CONFIG', {}))
        self.KAFKA_CONFIG = KafkaConfig(config_data.get('KAFKA_CONFIG', {}))
        self.TELEGRAM_CONFIG = TelegramConfig(config_data.get('TELEGRAM_CONFIG', {}))
        self.ORCHESTRATOR_CONFIG = OrchestratorConfig(config_data.get('ORCHESTRATOR_CONFIG', {}))

# Create a global config instance
config = Config()
