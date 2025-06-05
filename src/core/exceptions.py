class DeduplicationException(Exception):
    """Базовое исключение для сервиса дедупликации"""
    pass


class DatabaseConnectionException(DeduplicationException):
    """Исключение при подключении к БД"""
    pass


class ProductFetchException(DeduplicationException):
    """Исключение при получении данных товара"""
    pass


class DeduplicationProcessException(DeduplicationException):
    """Исключение в процессе дедупликации"""
    pass


class StorageException(DeduplicationException):
    """Исключение при работе с хранилищем"""
    pass