# deduplication_service/src/storage/source_mongo.py

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from bson import ObjectId
import logging

from src.core.config import settings

logger = logging.getLogger(__name__)


class SourceMongoStore:
    """Работа с исходной MongoDB (только чтение)"""

    def __init__(self, database_name: str):
        self.client = AsyncIOMotorClient(
            settings.source_mongodb_connection_string,
            directConnection=settings.source_mongo_direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self._collections_cache = {}

    def _get_collection(self, collection_name: str):
        """Получить коллекцию с кэшированием"""
        if collection_name not in self._collections_cache:
            self._collections_cache[collection_name] = self.db[collection_name]
        return self._collections_cache[collection_name]

    async def fetch_product(
            self,
            product_id: str,
            collection_name: str
    ) -> Optional[Dict[str, Any]]:
        """Получить товар по ID"""
        try:
            collection = self._get_collection(collection_name)

            # Пробуем найти по ObjectId
            try:
                object_id = ObjectId(product_id)
                product = await collection.find_one({"_id": object_id})
            except:
                # Если не ObjectId, ищем как строку
                product = await collection.find_one({"_id": product_id})

            if product:
                # Преобразуем ObjectId в строку
                if "_id" in product and isinstance(product["_id"], ObjectId):
                    product["_id"] = str(product["_id"])

                return product

            return None

        except Exception as e:
            logger.error(f"Error fetching product {product_id}: {e}")
            return None

    async def fetch_multiple_products(
            self,
            product_ids: List[tuple]  # List of (product_id, collection_name)
    ) -> Dict[str, Dict[str, Any]]:
        """Получить несколько товаров за раз"""
        results = {}

        # Группируем по коллекциям для оптимизации
        by_collection = {}
        for product_id, coll_name in product_ids:
            if coll_name not in by_collection:
                by_collection[coll_name] = []
            by_collection[coll_name].append(product_id)

        # Получаем товары из каждой коллекции
        for collection_name, ids in by_collection.items():
            try:
                collection = self._get_collection(collection_name)

                # Преобразуем ID в ObjectId где возможно
                query_ids = []
                for id_str in ids:
                    try:
                        query_ids.append(ObjectId(id_str))
                    except:
                        query_ids.append(id_str)

                # Получаем все товары одним запросом
                cursor = collection.find({"_id": {"$in": query_ids}})
                products = await cursor.to_list(length=len(ids))

                # Сохраняем результаты
                for product in products:
                    if isinstance(product["_id"], ObjectId):
                        product_id = str(product["_id"])
                        product["_id"] = product_id
                    else:
                        product_id = product["_id"]

                    results[product_id] = product

                logger.info(f"Fetched {len(products)} products from collection {collection_name}")

            except Exception as e:
                logger.error(f"Error fetching products from {collection_name}: {e}")
                continue

        return results

    async def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            await self.client.admin.command('ping')
            logger.info("Successfully connected to source MongoDB")

            # Проверяем существование БД
            db_list = await self.client.list_database_names()
            if settings.source_mongodb_database not in db_list:
                logger.warning(f"Database {settings.source_mongodb_database} not found")
                return False

            # Получаем список коллекций
            collections = await self.db.list_collection_names()
            logger.info(f"Found {len(collections)} collections in source database")

            return True
        except Exception as e:
            logger.error(f"Failed to connect to source MongoDB: {e}")
            return False

    async def close(self):
        """Закрыть соединение"""
        self.client.close()