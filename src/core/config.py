from pydantic_settings import BaseSettings
from typing import Optional
from urllib.parse import quote_plus


class Settings(BaseSettings):
    populate_by_name: bool = True

    # Standardized MongoDB (источник стандартизированных товаров)
    standardized_mongo_host: str = "localhost"
    standardized_mongo_port: int = 27017
    standardized_mongo_user: Optional[str] = None
    standardized_mongo_pass: Optional[str] = None
    standardized_mongo_authsource: Optional[str] = None
    standardized_mongo_authmechanism: str = "SCRAM-SHA-256"
    standardized_mongo_direct_connection: bool = False
    standardized_mongodb_database: str = "standardized_products"
    standardized_collection_name: str = "standardized_products"

    # Source MongoDB (для получения данных о поставщиках)
    source_mongo_host: str = "localhost"
    source_mongo_port: int = 27017
    source_mongo_user: Optional[str] = None
    source_mongo_pass: Optional[str] = None
    source_mongo_authsource: Optional[str] = None
    source_mongo_authmechanism: str = "SCRAM-SHA-256"
    source_mongo_direct_connection: bool = False
    source_mongodb_database: str = "source_products"

    # Unique Products MongoDB (наша БД)
    unique_mongo_host: str = "localhost"
    unique_mongo_port: int = 27017
    unique_mongo_user: Optional[str] = None
    unique_mongo_pass: Optional[str] = None
    unique_mongo_authsource: Optional[str] = None
    unique_mongo_authmechanism: str = "SCRAM-SHA-256"
    unique_mongo_direct_connection: bool = False
    unique_mongodb_database: str = "unique_products"
    unique_collection_name: str = "unique_products"

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Processing
    deduplication_batch_size: int = 1000
    worker_delay: int = 5

    # API
    api_key: str
    service_name: str = "deduplication_service"
    service_port: int = 8001

    @property
    def standardized_mongodb_connection_string(self) -> str:
        """Строка подключения для Standardized MongoDB"""
        if self.standardized_mongo_user and self.standardized_mongo_pass:
            connection_string = (
                f"mongodb://{self.standardized_mongo_user}:{quote_plus(self.standardized_mongo_pass)}@"
                f"{self.standardized_mongo_host}:{self.standardized_mongo_port}"
            )

            if self.standardized_mongo_authsource:
                connection_string += f"/{self.standardized_mongo_authsource}"
                connection_string += f"?authMechanism={self.standardized_mongo_authmechanism}"
            else:
                connection_string += f"/?authMechanism={self.standardized_mongo_authmechanism}"
        else:
            connection_string = f"mongodb://{self.standardized_mongo_host}:{self.standardized_mongo_port}"

        return connection_string

    @property
    def source_mongodb_connection_string(self) -> str:
        """Строка подключения для Source MongoDB"""
        if self.source_mongo_user and self.source_mongo_pass:
            connection_string = (
                f"mongodb://{self.source_mongo_user}:{quote_plus(self.source_mongo_pass)}@"
                f"{self.source_mongo_host}:{self.source_mongo_port}"
            )

            if self.source_mongo_authsource:
                connection_string += f"/{self.source_mongo_authsource}"
                connection_string += f"?authMechanism={self.source_mongo_authmechanism}"
            else:
                connection_string += f"/?authMechanism={self.source_mongo_authmechanism}"
        else:
            connection_string = f"mongodb://{self.source_mongo_host}:{self.source_mongo_port}"

        return connection_string

    @property
    def unique_mongodb_connection_string(self) -> str:
        """Строка подключения для Unique Products MongoDB"""
        if self.unique_mongo_user and self.unique_mongo_pass:
            connection_string = (
                f"mongodb://{self.unique_mongo_user}:{quote_plus(self.unique_mongo_pass)}@"
                f"{self.unique_mongo_host}:{self.unique_mongo_port}"
            )

            if self.unique_mongo_authsource:
                connection_string += f"/{self.unique_mongo_authsource}"
                connection_string += f"?authMechanism={self.unique_mongo_authmechanism}"
            else:
                connection_string += f"/?authMechanism={self.unique_mongo_authmechanism}"
        else:
            connection_string = f"mongodb://{self.unique_mongo_host}:{self.unique_mongo_port}"

        return connection_string

    class Config:
        env_file = ".env"


settings = Settings()