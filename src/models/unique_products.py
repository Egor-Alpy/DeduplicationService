from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum


class SupplierOffer(BaseModel):
    """Предложение от поставщика"""
    qnt: int
    discount: float
    price: float


class UniqueSupplier(BaseModel):
    """Уникальный поставщик с его уникальными предложениями"""
    supplier_key: str = Field(..., description="Уникальный ключ поставщика (имя + телефон + домен)")
    supplier_name: str
    supplier_tel: Optional[str] = None
    supplier_address: Optional[str] = None
    supplier_description: Optional[str] = None

    # Уникальные предложения от этого поставщика (после дедупликации)
    supplier_offers: List[Dict[str, Any]] = Field(...,
                                                  description="Уникальные предложения от поставщика (дедуплицированные)")

    # URL для обратной совместимости
    purchase_url: Optional[str] = Field(None,
                                        description="URL из первого предложения")

    last_updated: datetime = Field(..., description="Дата последнего обновления")

    # Для обратной совместимости - последний источник
    source_product_id: str = Field(...,
                                   description="ID последнего исходного товара")
    collection_name: str = Field(...,
                                 description="Коллекция последнего исходного товара")

    # Информация о всех источниках этого поставщика
    source_products_info: Optional[List[Dict[str, str]]] = Field(default_factory=list,
                                                                 description="Список всех исходных товаров где встречался этот поставщик")


class SourceProduct(BaseModel):
    """Исходный товар из которого взята информация"""
    standardized_mongo_id: str = Field(..., description="ID в коллекции стандартизированных товаров")
    original_mongo_id: str = Field(..., description="ID в исходной коллекции")
    collection_name: str = Field(..., description="Имя исходной коллекции")
    created_at: datetime = Field(..., description="Дата создания/парсинга")


class UniqueProduct(BaseModel):
    """
    Уникальный товар - результат объединения товаров с идентичными характеристиками

    Система работает так:
    1. Парсятся товары из разных источников
    2. Товары с одинаковыми характеристиками (OKPD2 + атрибуты) объединяются
    3. Собираются все поставщики этих товаров
    4. Дедуплицируются поставщики и их предложения
    """

    # Уникальный идентификатор товара
    product_hash: str = Field(...,
                              description="SHA256 хеш от OKPD2 кода и стандартизированных атрибутов")

    # ОКПД2 информация
    okpd2_code: str = Field(..., description="Код ОКПД2")
    okpd2_name: str = Field(..., description="Название по ОКПД2")

    # Стандартизированные атрибуты (определяют уникальность товара)
    standardized_attributes: List[Dict[str, Any]] = Field(...,
                                                          description="Стандартизированные атрибуты товара (определяют его уникальность)")

    # Связи с исходными товарами
    source_products: List[SourceProduct] = Field(...,
                                                 description="Все исходные товары с идентичными характеристиками")

    # Уникальные поставщики после дедупликации
    unique_suppliers: List[UniqueSupplier] = Field(...,
                                                   description="Уникальные поставщики с дедуплицированными предложениями")

    # Статистика
    total_sources: int = Field(...,
                               description="Количество исходных товаров (из скольких источников собран)")
    unique_suppliers_count: int = Field(...,
                                        description="Количество уникальных поставщиков")
    total_offers_count: Optional[int] = Field(None,
                                              description="Общее количество уникальных предложений от всех поставщиков")

    # Метаданные
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Пример информации (из первого товара для отображения)
    sample_title: Optional[str] = Field(None, description="Пример названия товара")
    sample_brand: Optional[str] = Field(None, description="Пример бренда")
    sample_article: Optional[str] = Field(None, description="Пример артикула")

    class Config:
        use_enum_values = True