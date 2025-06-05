from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from bson import ObjectId
from pymongo import UpdateOne

from src.core.config import settings

logger = logging.getLogger(__name__)


class StandardizedMongoStore:
    """Чтение стандартизированных товаров (read-only с возможностью пометки)"""

    def __init__(self, database_name: str, collection_name: str = "standardized_products"):
        self.client = AsyncIOMotorClient(
            settings.standardized_mongodb_connection_string,
            directConnection=settings.standardized_mongo_direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.collection = self.db[collection_name]

    async def find_products(
            self,
            filters: Dict[str, Any] = None,
            limit: int = 1000,
            skip: int = 0
    ) -> List[Dict[str, Any]]:
        """Найти стандартизированные товары"""
        query = filters or {}

        cursor = self.collection.find(query).skip(skip).limit(limit)
        products = await cursor.to_list(length=limit)

        # Преобразуем ObjectId в строки
        for product in products:
            product["_id"] = str(product["_id"])

        return products

    async def count_products(self, filters: Dict[str, Any] = None) -> int:
        """Подсчитать количество товаров"""
        query = filters or {}
        return await self.collection.count_documents(query)

    async def mark_as_grouped(self, product_ids: List[str]) -> int:
        """Пометить товары как сгруппированные"""
        if not product_ids:
            return 0

        bulk_operations = []
        for product_id in product_ids:
            operation = UpdateOne(
                {"_id": ObjectId(product_id)},
                {"$set": {"grouped": True, "grouped_at": datetime.utcnow()}}
            )
            bulk_operations.append(operation)

        if bulk_operations:
            result = await self.collection.bulk_write(bulk_operations, ordered=False)
            logger.info(f"Marked {result.modified_count} products as grouped")
            return result.modified_count

        return 0

    async def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику по стандартизированным товарам"""
        pipeline = [
            {"$facet": {
                "total": [{"$count": "count"}],
                "grouped": [
                    {"$match": {"grouped": True}},
                    {"$count": "count"}
                ],
                "ungrouped": [
                    {"$match": {"$or": [
                        {"grouped": {"$ne": True}},
                        {"grouped": {"$exists": False}}
                    ]}},
                    {"$count": "count"}
                ]
            }}
        ]

        cursor = self.collection.aggregate(pipeline)
        result = await cursor.to_list(length=1)

        if not result:
            return {"total": 0, "grouped": 0, "ungrouped": 0}

        facets = result[0]
        stats = {
            "total": facets["total"][0]["count"] if facets["total"] else 0,
            "grouped": facets["grouped"][0]["count"] if facets["grouped"] else 0,
            "ungrouped": facets["ungrouped"][0]["count"] if facets["ungrouped"] else 0
        }

        return stats

    async def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            await self.client.admin.command('ping')
            logger.info("Successfully connected to standardized MongoDB")

            # Проверяем наличие коллекции
            collections = await self.db.list_collection_names()
            if self.collection.name not in collections:
                logger.warning(f"Collection {self.collection.name} not found")
                return False

            # Проверяем количество документов
            count = await self.collection.count_documents({})
            logger.info(f"Found {count} documents in {self.collection.name}")

            return True
        except Exception as e:
            logger.error(f"Failed to connect to standardized MongoDB: {e}")
            return False

    async def close(self):
        """Закрыть соединение"""
        self.client.close()