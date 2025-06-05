import logging
from typing import Dict, Any, Optional, List
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from src.core.config import settings

logger = logging.getLogger(__name__)


class SupplierFetcher:
    """Сервис для получения данных о поставщиках из исходной БД"""

    def __init__(self):
        self.client = AsyncIOMotorClient(
            settings.source_mongodb_connection_string,
            directConnection=settings.source_mongo_direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        self.db: AsyncIOMotorDatabase = self.client[settings.source_mongodb_database]

        # Кэш коллекций для быстрого доступа
        self._collections_cache = {}

    def _get_collection(self, collection_name: str):
        """Получить коллекцию с кэшированием"""
        if collection_name not in self._collections_cache:
            self._collections_cache[collection_name] = self.db[collection_name]
        return self._collections_cache[collection_name]

    async def fetch_product_details(
            self,
            old_mongo_id: str,
            collection_name: str
    ) -> Optional[Dict[str, Any]]:
        """Получить полные данные товара из исходной БД"""
        try:
            collection = self._get_collection(collection_name)

            # Пробуем найти по ObjectId
            try:
                object_id = ObjectId(old_mongo_id)
                product = await collection.find_one({"_id": object_id})
            except:
                # Если не ObjectId, ищем как строку
                product = await collection.find_one({"_id": old_mongo_id})

            if product:
                # Преобразуем ObjectId в строку
                if "_id" in product and isinstance(product["_id"], ObjectId):
                    product["_id"] = str(product["_id"])

                logger.debug(f"Found product {old_mongo_id} in collection {collection_name}")
                return product
            else:
                logger.warning(f"Product {old_mongo_id} not found in collection {collection_name}")
                return None

        except Exception as e:
            logger.error(f"Error fetching product {old_mongo_id}: {e}")
            return None

    async def fetch_multiple_products(
            self,
            product_ids: List[tuple]  # List of (old_mongo_id, collection_name)
    ) -> Dict[str, Dict[str, Any]]:
        """Получить несколько товаров за раз"""
        results = {}

        # Группируем по коллекциям для оптимизации
        by_collection = {}
        for old_id, coll_name in product_ids:
            if coll_name not in by_collection:
                by_collection[coll_name] = []
            by_collection[coll_name].append(old_id)

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
        """Проверить подключение к исходной БД"""
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