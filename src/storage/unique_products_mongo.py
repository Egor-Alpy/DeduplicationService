from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from src.core.config import settings
from src.models.unique_products import UniqueProduct, SourceProduct, UniqueSupplier

logger = logging.getLogger(__name__)


class UniqueProductsMongoStore:
    """Работа с MongoDB уникальных товаров"""

    def __init__(self, database_name: str, collection_name: str = "unique_products"):
        self.client = AsyncIOMotorClient(
            settings.unique_mongodb_connection_string,
            directConnection=settings.unique_mongo_direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.collection = self.db[collection_name]

    async def initialize(self):
        """Инициализация хранилища и создание индексов"""
        connected = await self.test_connection()
        if not connected:
            raise Exception("Failed to connect to unique products MongoDB")

        await self._setup_indexes()

    async def _setup_indexes(self):
        """Создать необходимые индексы"""
        try:
            # Уникальный индекс по хешу
            await self.collection.create_index(
                "product_hash",
                unique=True
            )

            # Индексы для поиска
            await self.collection.create_index("okpd2_code")
            await self.collection.create_index("unique_suppliers_count")
            await self.collection.create_index("total_sources")
            await self.collection.create_index("created_at")
            await self.collection.create_index("updated_at")

            # Индекс для поиска по исходным товарам
            await self.collection.create_index("source_products.original_mongo_id")

            # Текстовый индекс для поиска
            await self.collection.create_index([
                ("sample_title", "text"),
                ("sample_brand", "text")
            ])

            logger.info("Unique products indexes created successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes (may already exist): {e}")

    async def insert_unique_product(self, product: UniqueProduct) -> bool:
        """Вставить уникальный товар"""
        try:
            document = product.dict()
            result = await self.collection.insert_one(document)
            logger.info(f"Inserted unique product: {product.product_hash[:8]}...")
            return True
        except Exception as e:
            if "duplicate key error" in str(e):
                logger.warning(f"Product already exists: {product.product_hash[:8]}...")
                return False
            else:
                logger.error(f"Error inserting unique product: {e}")
                return False

    async def find_by_hash(self, product_hash: str) -> Optional[Dict[str, Any]]:
        """Найти товар по хешу"""
        product = await self.collection.find_one({"product_hash": product_hash})
        if product:
            product["_id"] = str(product["_id"])
        return product

    async def update_unique_product(
            self,
            product_hash: str,
            new_sources: List[SourceProduct],
            unique_suppliers: List[UniqueSupplier]
    ) -> bool:
        """Обновить уникальный товар новыми источниками и поставщиками"""
        try:
            # Получаем текущий документ
            current = await self.collection.find_one({"product_hash": product_hash})
            if not current:
                logger.error(f"Product not found: {product_hash}")
                return False

            # Добавляем новые источники
            current_sources = current.get("source_products", [])
            for new_source in new_sources:
                current_sources.append(new_source.dict())

            # Обновляем документ
            update_data = {
                "source_products": current_sources,
                "unique_suppliers": [s.dict() for s in unique_suppliers],
                "total_sources": len(current_sources),
                "unique_suppliers_count": len(unique_suppliers),
                "updated_at": datetime.utcnow()
            }

            result = await self.collection.update_one(
                {"product_hash": product_hash},
                {"$set": update_data}
            )

            if result.modified_count > 0:
                logger.info(f"Updated unique product: {product_hash[:8]}...")
                return True
            return False

        except Exception as e:
            logger.error(f"Error updating unique product: {e}")
            return False

    async def find_products(
            self,
            filters: Dict[str, Any] = None,
            limit: int = 100,
            skip: int = 0,
            sort_by: str = "unique_suppliers_count",
            sort_order: int = -1
    ) -> List[Dict[str, Any]]:
        """Поиск уникальных товаров"""
        query = filters or {}

        cursor = self.collection.find(query)
        cursor = cursor.sort(sort_by, sort_order)
        cursor = cursor.skip(skip).limit(limit)

        products = await cursor.to_list(length=limit)

        # Преобразуем ObjectId в строки
        for product in products:
            product["_id"] = str(product["_id"])

        return products

    async def find_by_original_product(self, original_mongo_id: str) -> Optional[Dict[str, Any]]:
        """Найти уникальный товар по ID исходного товара"""
        product = await self.collection.find_one({
            "source_products.original_mongo_id": original_mongo_id
        })

        if product:
            product["_id"] = str(product["_id"])

        return product

    async def search_products(
            self,
            search_text: str,
            limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Текстовый поиск товаров"""
        cursor = self.collection.find(
            {"$text": {"$search": search_text}},
            {"score": {"$meta": "textScore"}}
        )
        cursor = cursor.sort([("score", {"$meta": "textScore"})])
        cursor = cursor.limit(limit)

        products = await cursor.to_list(length=limit)

        for product in products:
            product["_id"] = str(product["_id"])

        return products

    async def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику по уникальным товарам"""
        pipeline = [
            {"$facet": {
                "total": [{"$count": "count"}],
                "by_okpd_class": [
                    {"$group": {
                        "_id": {"$substr": ["$okpd2_code", 0, 2]},
                        "count": {"$sum": 1},
                        "total_suppliers": {"$sum": "$unique_suppliers_count"}
                    }},
                    {"$sort": {"count": -1}}
                ],
                "by_suppliers_count": [
                    {"$bucket": {
                        "groupBy": "$unique_suppliers_count",
                        "boundaries": [0, 1, 2, 3, 5, 10, 20, 50, 100, 1000],
                        "default": "1000+",
                        "output": {
                            "count": {"$sum": 1}
                        }
                    }}
                ],
                "by_sources_count": [
                    {"$bucket": {
                        "groupBy": "$total_sources",
                        "boundaries": [0, 1, 2, 3, 5, 10, 20, 50, 100],
                        "default": "100+",
                        "output": {
                            "count": {"$sum": 1}
                        }
                    }}
                ],
                "top_duplicated": [
                    {"$match": {"total_sources": {"$gt": 1}}},
                    {"$sort": {"total_sources": -1}},
                    {"$limit": 10},
                    {"$project": {
                        "product_hash": 1,
                        "okpd2_code": 1,
                        "sample_title": 1,
                        "total_sources": 1,
                        "unique_suppliers_count": 1
                    }}
                ]
            }}
        ]

        cursor = self.collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)

        if not result:
            return {"total": 0}

        facets = result[0]
        stats = {
            "total_unique_products": facets["total"][0]["count"] if facets["total"] else 0,
            "by_okpd_class": {
                item["_id"]: {
                    "products": item["count"],
                    "suppliers": item["total_suppliers"]
                }
                for item in facets["by_okpd_class"]
            },
            "by_suppliers_count": {
                str(item["_id"]): item["count"]
                for item in facets["by_suppliers_count"]
            },
            "by_sources_count": {
                str(item["_id"]): item["count"]
                for item in facets["by_sources_count"]
            },
            "top_duplicated_products": facets["top_duplicated"]
        }

        # Добавляем общую статистику
        total_with_duplicates = sum(
            bucket["count"] * (int(bucket["_id"]) if bucket["_id"] != "100+" else 100)
            for bucket in facets["by_sources_count"]
            if bucket["_id"] != 0
        )

        stats["deduplication_rate"] = round(
            (1 - stats["total_unique_products"] / total_with_duplicates) * 100, 2
        ) if total_with_duplicates > 0 else 0

        return stats

    async def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            await self.client.admin.command('ping')
            logger.info("Successfully connected to unique products MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to unique products MongoDB: {e}")
            return False

    async def close(self):
        """Закрыть соединение"""
        self.client.close()