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
    """Уникальный поставщик"""
    supplier_key: str = Field(..., description="Уникальный ключ поставщика")
    supplier_name: str
    supplier_tel: Optional[str] = None
    supplier_address: Optional[str] = None
    supplier_description: Optional[str] = None
    supplier_offers: List[Dict[str, Any]]
    purchase_url: Optional[str] = None
    last_updated: datetime = Field(..., description="Дата последнего обновления")
    source_product_id: str = Field(..., description="ID исходного товара")
    collection_name: str = Field(..., description="Коллекция исходного товара")


class SourceProduct(BaseModel):
    """Исходный товар для связи"""
    standardized_mongo_id: str
    original_mongo_id: str
    collection_name: str
    created_at: datetime


class UniqueProduct(BaseModel):
    """Уникальный товар с объединенными поставщиками"""
    # Уникальный идентификатор товара
    product_hash: str = Field(..., description="SHA256 хеш уникальных характеристик")

    # ОКПД2 информация
    okpd2_code: str
    okpd2_name: str

    # Стандартизированные атрибуты (определяют уникальность)
    standardized_attributes: List[Dict[str, Any]] = Field(...,
                                                          description="Отсортированные стандартизированные атрибуты")

    # Связи с исходными товарами
    source_products: List[SourceProduct] = Field(..., description="Все исходные товары с такими же характеристиками")

    # Уникальные поставщики
    unique_suppliers: List[UniqueSupplier] = Field(..., description="Дедуплицированные поставщики")

    # Метаданные
    total_sources: int = Field(..., description="Количество исходных товаров")
    unique_suppliers_count: int = Field(..., description="Количество уникальных поставщиков")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # Дополнительная информация из первого товара (для отображения)
    sample_title: Optional[str] = None
    sample_brand: Optional[str] = None
    sample_article: Optional[str] = None

    class Config:
        use_enum_values = True