import hashlib
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse

from src.models.unique_products import UniqueProduct, UniqueSupplier, SourceProduct
from src.storage.standardized_mongo import StandardizedMongoStore
from src.storage.unique_products_mongo import UniqueProductsMongoStore
from src.services.supplier_fetcher import SupplierFetcher

logger = logging.getLogger(__name__)


class ProductDeduplicationService:
    """Сервис для дедупликации товаров и объединения поставщиков"""

    def __init__(
            self,
            standardized_store: StandardizedMongoStore,
            unique_products_store: UniqueProductsMongoStore,
            supplier_fetcher: SupplierFetcher
    ):
        self.standardized_store = standardized_store
        self.unique_products_store = unique_products_store
        self.supplier_fetcher = supplier_fetcher

    def _create_product_hash(self, okpd2_code: str, standardized_attributes: List[Dict[str, Any]]) -> str:
        """Создать уникальный хеш товара на основе OKPD2 и атрибутов"""
        # Сортируем атрибуты по standard_name для консистентности
        sorted_attrs = sorted(
            standardized_attributes,
            key=lambda x: x.get("standard_name", "")
        )

        # Создаем строку для хеширования
        hash_parts = [okpd2_code]

        for attr in sorted_attrs:
            # Формат: "name:value:unit"
            attr_str = f"{attr.get('standard_name', '')}:{attr.get('standard_value', '')}:{attr.get('unit', '')}"
            hash_parts.append(attr_str)

        # Объединяем и хешируем
        hash_string = "|".join(hash_parts)
        product_hash = hashlib.sha256(hash_string.encode()).hexdigest()

        logger.debug(f"Created hash {product_hash[:8]}... for {okpd2_code} with {len(sorted_attrs)} attributes")

        return product_hash

    def _normalize_phone(self, phone: Optional[str]) -> str:
        """Нормализовать телефонный номер"""
        if not phone or phone == "Нет данных":
            return ""

        # Убираем все не-цифры
        normalized = ''.join(filter(str.isdigit, phone))

        # Если начинается с 8, заменяем на 7
        if normalized.startswith('8') and len(normalized) == 11:
            normalized = '7' + normalized[1:]

        return normalized

    def _extract_domain(self, url: Optional[str]) -> str:
        """Извлечь домен из URL"""
        if not url or url == "Нет данных":
            return ""

        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            # Убираем www.
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception:
            return ""

    def _create_supplier_key(self, supplier: Dict[str, Any]) -> str:
        """Создать уникальный ключ поставщика"""
        name = supplier.get("supplier_name", "").lower().strip()
        phone = self._normalize_phone(supplier.get("supplier_tel"))

        # Извлекаем домен из первого URL в offers
        domain = ""
        offers = supplier.get("supplier_offers", [])
        if offers and isinstance(offers[0], dict):
            url = offers[0].get("purchase_url")
            domain = self._extract_domain(url)

        # Формируем ключ
        key_parts = [name]
        if phone:
            key_parts.append(phone)
        if domain:
            key_parts.append(domain)

        supplier_key = "|".join(key_parts)
        return supplier_key

    async def process_batch(self, limit: int = 1000) -> Dict[str, Any]:
        """Обработать батч стандартизированных товаров"""
        logger.info(f"Starting product deduplication batch, limit={limit}")

        # 1. Получаем стандартизированные товары
        filters = {
            "standardization_status": "standardized",
            "$or": [
                {"grouped": {"$ne": True}},
                {"grouped": {"$exists": False}}
            ]
        }

        standardized_products = await self.standardized_store.find_products(
            filters=filters,
            limit=limit
        )

        if not standardized_products:
            logger.info("No products to deduplicate")
            return {"processed": 0, "groups_created": 0, "groups_updated": 0}

        logger.info(f"Found {len(standardized_products)} products to deduplicate")

        # 2. Группируем товары по хешу
        product_groups = {}

        for product in standardized_products:
            # Создаем хеш
            product_hash = self._create_product_hash(
                product["okpd2_code"],
                product.get("standardized_attributes", [])
            )

            if product_hash not in product_groups:
                product_groups[product_hash] = {
                    "okpd2_code": product["okpd2_code"],
                    "okpd2_name": product.get("okpd2_name", ""),
                    "standardized_attributes": product.get("standardized_attributes", []),
                    "products": []
                }

            product_groups[product_hash]["products"].append(product)

        logger.info(f"Created {len(product_groups)} unique product groups")

        # 3. Обрабатываем каждую группу
        groups_created = 0
        groups_updated = 0

        for product_hash, group_data in product_groups.items():
            try:
                # Проверяем, существует ли уже такая группа
                existing_group = await self.unique_products_store.find_by_hash(product_hash)

                if existing_group:
                    # Обновляем существующую группу
                    result = await self._update_product_group(
                        existing_group,
                        group_data["products"]
                    )
                    if result:
                        groups_updated += 1
                else:
                    # Создаем новую группу
                    result = await self._create_product_group(
                        product_hash,
                        group_data
                    )
                    if result:
                        groups_created += 1

            except Exception as e:
                logger.error(f"Error processing group {product_hash}: {e}")
                continue

        # 4. Помечаем обработанные товары
        product_ids = [p["_id"] for p in standardized_products]
        await self.standardized_store.mark_as_grouped(product_ids)

        return {
            "processed": len(standardized_products),
            "groups_created": groups_created,
            "groups_updated": groups_updated,
            "total_groups": len(product_groups)
        }

    async def _create_product_group(
            self,
            product_hash: str,
            group_data: Dict[str, Any]
    ) -> bool:
        """Создать новую группу товаров"""
        products = group_data["products"]

        # Собираем исходные товары
        source_products = []
        all_suppliers = []

        # Получаем данные из исходной БД для каждого товара
        for product in products:
            # Добавляем в source_products
            source_products.append(SourceProduct(
                standardized_mongo_id=str(product["_id"]),
                original_mongo_id=product["old_mongo_id"],
                collection_name=product["collection_name"],
                created_at=product.get("standardization_completed_at", datetime.utcnow())
            ))

            # Получаем полные данные из исходной БД
            original_product = await self.supplier_fetcher.fetch_product_details(
                product["old_mongo_id"],
                product["collection_name"]
            )

            if original_product:
                # Извлекаем поставщиков
                for supplier in original_product.get("suppliers", []):
                    supplier_with_meta = supplier.copy()
                    supplier_with_meta["source_product_id"] = product["old_mongo_id"]
                    supplier_with_meta["collection_name"] = product["collection_name"]
                    supplier_with_meta["created_at"] = original_product.get("created_at", "")
                    all_suppliers.append(supplier_with_meta)

        # Дедуплицируем поставщиков
        unique_suppliers = self._deduplicate_suppliers(all_suppliers)

        # Получаем дополнительную информацию из первого товара
        sample_info = {}
        if products and all_suppliers:
            first_original = await self.supplier_fetcher.fetch_product_details(
                products[0]["old_mongo_id"],
                products[0]["collection_name"]
            )
            if first_original:
                sample_info = {
                    "sample_title": first_original.get("title"),
                    "sample_brand": first_original.get("brand"),
                    "sample_article": first_original.get("article")
                }

        # Создаем объект UniqueProduct
        unique_product = UniqueProduct(
            product_hash=product_hash,
            okpd2_code=group_data["okpd2_code"],
            okpd2_name=group_data["okpd2_name"],
            standardized_attributes=group_data["standardized_attributes"],
            source_products=source_products,
            unique_suppliers=unique_suppliers,
            total_sources=len(source_products),
            unique_suppliers_count=len(unique_suppliers),
            **sample_info
        )

        # Сохраняем в БД
        return await self.unique_products_store.insert_unique_product(unique_product)

    async def _update_product_group(
            self,
            existing_group: Dict[str, Any],
            new_products: List[Dict[str, Any]]
    ) -> bool:
        """Обновить существующую группу новыми товарами"""
        # Получаем текущие исходные товары
        existing_sources = {
            sp["original_mongo_id"]: sp
            for sp in existing_group.get("source_products", [])
        }

        # Добавляем новые товары
        new_sources = []
        new_suppliers = []

        for product in new_products:
            # Проверяем, не добавлен ли уже этот товар
            if product["old_mongo_id"] not in existing_sources:
                new_sources.append(SourceProduct(
                    standardized_mongo_id=str(product["_id"]),
                    original_mongo_id=product["old_mongo_id"],
                    collection_name=product["collection_name"],
                    created_at=product.get("standardization_completed_at", datetime.utcnow())
                ))

                # Получаем поставщиков
                original_product = await self.supplier_fetcher.fetch_product_details(
                    product["old_mongo_id"],
                    product["collection_name"]
                )

                if original_product:
                    for supplier in original_product.get("suppliers", []):
                        supplier_with_meta = supplier.copy()
                        supplier_with_meta["source_product_id"] = product["old_mongo_id"]
                        supplier_with_meta["collection_name"] = product["collection_name"]
                        supplier_with_meta["created_at"] = original_product.get("created_at", "")
                        new_suppliers.append(supplier_with_meta)

        if not new_sources:
            logger.debug("No new sources to add")
            return False

        # Объединяем поставщиков
        all_suppliers = []

        # Существующие поставщики
        for supplier in existing_group.get("unique_suppliers", []):
            all_suppliers.append(supplier)

        # Новые поставщики
        all_suppliers.extend(new_suppliers)

        # Дедуплицируем
        unique_suppliers = self._deduplicate_suppliers(all_suppliers)

        # Обновляем группу
        return await self.unique_products_store.update_unique_product(
            product_hash=existing_group["product_hash"],
            new_sources=new_sources,
            unique_suppliers=unique_suppliers
        )

    def _deduplicate_suppliers(self, suppliers: List[Dict[str, Any]]) -> List[UniqueSupplier]:
        """Дедуплицировать поставщиков"""
        supplier_map = {}

        for supplier in suppliers:
            key = self._create_supplier_key(supplier)

            if key not in supplier_map:
                # Первое вхождение поставщика
                supplier_map[key] = supplier
            else:
                # Сравниваем даты и оставляем более свежее предложение
                existing_date = supplier_map[key].get("created_at", "")
                new_date = supplier.get("created_at", "")

                # Простое сравнение строк дат
                if new_date > existing_date:
                    supplier_map[key] = supplier

        # Преобразуем в модели UniqueSupplier
        unique_suppliers = []

        for key, supplier_data in supplier_map.items():
            try:
                unique_supplier = UniqueSupplier(
                    supplier_key=key,
                    supplier_name=supplier_data.get("supplier_name", ""),
                    supplier_tel=supplier_data.get("supplier_tel"),
                    supplier_address=supplier_data.get("supplier_address"),
                    supplier_description=supplier_data.get("supplier_description"),
                    supplier_offers=supplier_data.get("supplier_offers", []),
                    purchase_url=supplier_data.get("supplier_offers", [{}])[0].get("purchase_url") if supplier_data.get(
                        "supplier_offers") else None,
                    last_updated=datetime.utcnow(),
                    source_product_id=supplier_data.get("source_product_id", ""),
                    collection_name=supplier_data.get("collection_name", "")
                )
                unique_suppliers.append(unique_supplier)
            except Exception as e:
                logger.warning(f"Error creating UniqueSupplier: {e}")
                continue

        logger.info(f"Deduplicated {len(suppliers)} suppliers to {len(unique_suppliers)} unique")

        return unique_suppliers

    async def get_statistics(self) -> Dict[str, Any]:
        """Получить статистику дедупликации"""
        unique_stats = await self.unique_products_store.get_statistics()
        standardized_stats = await self.standardized_store.get_statistics()

        return {
            **unique_stats,
            "total_standardized": standardized_stats.get("total", 0),
            "ungrouped_products": standardized_stats.get("ungrouped", 0)
        }