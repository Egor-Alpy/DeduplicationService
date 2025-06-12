from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict, Any

from src.api.dependencies import verify_api_key
from src.storage.unique_products_mongo import UniqueProductsMongoStore
from src.core.config import settings

router = APIRouter()


async def get_unique_products_store() -> UniqueProductsMongoStore:
    """Получить экземпляр UniqueProductsMongoStore"""
    store = UniqueProductsMongoStore(
        settings.unique_mongodb_database,
        settings.unique_collection_name
    )
    await store.initialize()
    return store


@router.get("/stats")
async def get_unique_products_statistics(
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику по уникальным товарам"""
    stats = await unique_products_store.get_statistics()

    return {
        "total_unique_products": stats.get("total_unique_products", 0),
        "deduplication_rate": stats.get("deduplication_rate", 0),
        "by_okpd_class": stats.get("by_okpd_class", {}),
        "by_suppliers_count": stats.get("by_suppliers_count", {}),
        "by_sources_count": stats.get("by_sources_count", {}),
        "top_duplicated_products": stats.get("top_duplicated_products", [])
    }


@router.get("/products")
async def get_unique_products(
        okpd_code: Optional[str] = Query(None, description="Filter by OKPD2 code prefix"),
        min_suppliers: Optional[int] = Query(None, description="Minimum number of suppliers"),
        min_sources: Optional[int] = Query(None, description="Minimum number of source products"),
        search: Optional[str] = Query(None, description="Search in title and brand"),
        limit: int = Query(50, ge=1, le=500),
        skip: int = Query(0, ge=0),
        sort_by: str = Query("unique_suppliers_count", description="Sort field"),
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить список уникальных товаров"""

    # Если есть поисковый запрос
    if search:
        products = await unique_products_store.search_products(
            search_text=search,
            limit=limit
        )
    else:
        # Обычный поиск с фильтрами
        filters = {}

        if okpd_code:
            filters["okpd2_code"] = {"$regex": f"^{okpd_code}"}

        if min_suppliers:
            filters["unique_suppliers_count"] = {"$gte": min_suppliers}

        if min_sources:
            filters["total_sources"] = {"$gte": min_sources}

        products = await unique_products_store.find_products(
            filters=filters,
            limit=limit,
            skip=skip,
            sort_by=sort_by
        )

    return {
        "products": products,
        "count": len(products),
        "limit": limit,
        "skip": skip
    }


@router.get("/products/{product_hash}")
async def get_unique_product_details(
        product_hash: str,
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить детальную информацию о уникальном товаре"""
    product = await unique_products_store.find_by_hash(product_hash)

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    return product


@router.get("/products/by-original/{original_id}")
async def get_unique_product_by_original(
        original_id: str,
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """Найти уникальный товар по ID исходного товара"""
    product = await unique_products_store.find_by_original_product(original_id)

    if not product:
        raise HTTPException(
            status_code=404,
            detail="No unique product found for this original ID"
        )

    return product


@router.get("/duplicates")
async def get_duplicate_products(
        min_duplicates: int = Query(2, description="Minimum number of duplicates"),
        okpd_code: Optional[str] = Query(None, description="Filter by OKPD2 code"),
        limit: int = Query(50, ge=1, le=200),
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """
    Получить товары, найденные в наибольшем количестве источников

    Показывает товары с идентичными характеристиками, которые были
    найдены в нескольких источниках и успешно объединены
    """
    filters = {
        "total_sources": {"$gte": min_duplicates}
    }

    if okpd_code:
        filters["okpd2_code"] = {"$regex": f"^{okpd_code}"}

    products = await unique_products_store.find_products(
        filters=filters,
        limit=limit,
        sort_by="total_sources",
        sort_order=-1
    )

    # Форматируем для удобного просмотра
    formatted_products = []
    for product in products:
        formatted_products.append({
            "product_hash": product["product_hash"],
            "okpd2_code": product["okpd2_code"],
            "sample_title": product.get("sample_title"),
            "sample_brand": product.get("sample_brand"),
            "total_duplicates": product["total_sources"],
            "unique_suppliers": product["unique_suppliers_count"],
            "attributes_count": len(product.get("standardized_attributes", [])),
            "standardized_attributes": [
                f"{attr['standard_name']}: {attr['standard_value']}"
                for attr in product.get("standardized_attributes", [])[:5]  # Первые 5
            ]
        })

    return {
        "products": formatted_products,
        "count": len(formatted_products)
    }


@router.get("/suppliers/analysis")
async def analyze_suppliers(
        okpd_code: Optional[str] = Query(None, description="Filter by OKPD2 code"),
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """Анализ поставщиков по уникальным товарам"""
    # Агрегация для анализа поставщиков
    pipeline = [
        {"$unwind": "$unique_suppliers"},
        {"$group": {
            "_id": "$unique_suppliers.supplier_name",
            "products_count": {"$sum": 1},
            "avg_price": {
                "$avg": {
                    "$arrayElemAt": [
                        "$unique_suppliers.supplier_offers.price.price",
                        0
                    ]
                }
            }
        }},
        {"$sort": {"products_count": -1}},
        {"$limit": 20}
    ]

    if okpd_code:
        pipeline.insert(0, {"$match": {"okpd2_code": {"$regex": f"^{okpd_code}"}}})

    cursor = unique_products_store.collection.aggregate(pipeline)
    results = await cursor.to_list(length=None)

    return {
        "top_suppliers": results,
        "okpd_filter": okpd_code
    }


@router.get("/products/{product_hash}/offers-analysis")
async def get_product_offers_analysis(
        product_hash: str,
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """
    Получить анализ всех предложений по уникальному товару

    Показывает все уникальные предложения от всех поставщиков
    для товара с идентичными характеристиками из разных источников
    """
    product = await unique_products_store.find_by_hash(product_hash)

    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Анализируем все предложения
    all_offers = []
    suppliers_summary = []

    for supplier in product.get("unique_suppliers", []):
        supplier_offers = supplier.get("supplier_offers", [])

        # Собираем все предложения
        for offer in supplier_offers:
            # Обрабатываем цену (может быть как число, так и объект)
            price_value = offer.get("price", 0)
            if isinstance(price_value, dict):
                price_value = price_value.get("price", 0)

            all_offers.append({
                "supplier_name": supplier["supplier_name"],
                "supplier_key": supplier["supplier_key"],
                "price": price_value,
                "quantity": offer.get("qnt", 0),
                "discount": offer.get("discount", 0),
                "created_at": offer.get("created_at", ""),
                "source_product_id": offer.get("source_product_id", ""),
                "collection_name": offer.get("collection_name", ""),
                "purchase_url": offer.get("purchase_url", "")
            })

        # Статистика по поставщику
        prices = []
        for offer in supplier_offers:
            price_value = offer.get("price", 0)
            if isinstance(price_value, dict):
                price_value = price_value.get("price", 0)
            if price_value > 0:
                prices.append(price_value)

        suppliers_summary.append({
            "supplier_name": supplier["supplier_name"],
            "supplier_key": supplier["supplier_key"],
            "offers_count": len(supplier_offers),
            "sources_count": len(supplier.get("source_products_info", [])),
            "min_price": min(prices) if prices else 0,
            "max_price": max(prices) if prices else 0,
            "avg_price": sum(prices) / len(prices) if prices else 0,
            "last_updated": supplier["last_updated"]
        })

    # Общая статистика
    all_prices = [o["price"] for o in all_offers if o["price"] > 0]

    return {
        "product_hash": product_hash,
        "total_offers": len(all_offers),
        "unique_suppliers": len(product.get("unique_suppliers", [])),
        "total_sources": product.get("total_sources", 0),
        "price_statistics": {
            "min_price": min(all_prices) if all_prices else 0,
            "max_price": max(all_prices) if all_prices else 0,
            "avg_price": sum(all_prices) / len(all_prices) if all_prices else 0,
            "price_range": max(all_prices) - min(all_prices) if all_prices else 0,
            "offers_with_price": len(all_prices),
            "offers_without_price": len(all_offers) - len(all_prices)
        },
        "suppliers_summary": sorted(suppliers_summary, key=lambda x: x["offers_count"], reverse=True),
        "all_offers": sorted(all_offers, key=lambda x: (x["price"], x["created_at"])),
        "sample_info": {
            "title": product.get("sample_title"),
            "brand": product.get("sample_brand"),
            "article": product.get("sample_article"),
            "okpd2_code": product.get("okpd2_code"),
            "okpd2_name": product.get("okpd2_name")
        },
        "attributes": product.get("standardized_attributes", [])
    }


@router.get("/suppliers/{supplier_key}/products")
async def get_supplier_products(
        supplier_key: str,
        okpd_code: Optional[str] = Query(None, description="Filter by OKPD2 code"),
        limit: int = Query(100, ge=1, le=500),
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """
    Получить все товары и предложения конкретного поставщика

    Показывает все уникальные товары, которые предлагает данный поставщик,
    и все его предложения по этим товарам
    """
    # Поиск всех товаров с этим поставщиком
    filters = {
        "unique_suppliers.supplier_key": supplier_key
    }

    if okpd_code:
        filters["okpd2_code"] = {"$regex": f"^{okpd_code}"}

    products = await unique_products_store.find_products(
        filters=filters,
        limit=limit
    )

    if not products:
        raise HTTPException(
            status_code=404,
            detail=f"No products found for supplier key: {supplier_key}"
        )

    # Собираем все предложения этого поставщика
    supplier_offers = []
    supplier_info = None
    products_summary = []

    for product in products:
        for supplier in product.get("unique_suppliers", []):
            if supplier.get("supplier_key") == supplier_key:
                # Сохраняем информацию о поставщике
                if not supplier_info:
                    supplier_info = {
                        "supplier_name": supplier["supplier_name"],
                        "supplier_tel": supplier.get("supplier_tel"),
                        "supplier_address": supplier.get("supplier_address")
                    }

                # Информация о товаре
                product_info = {
                    "product_hash": product["product_hash"],
                    "sample_title": product.get("sample_title"),
                    "sample_brand": product.get("sample_brand"),
                    "okpd2_code": product["okpd2_code"],
                    "okpd2_name": product.get("okpd2_name"),
                    "offers_count": len(supplier.get("supplier_offers", [])),
                    "sources_count": len(supplier.get("source_products_info", []))
                }
                products_summary.append(product_info)

                # Все предложения по этому товару
                for offer in supplier.get("supplier_offers", []):
                    # Обрабатываем цену
                    price_value = offer.get("price", 0)
                    if isinstance(price_value, dict):
                        price_value = price_value.get("price", 0)

                    supplier_offers.append({
                        "product_hash": product["product_hash"],
                        "sample_title": product.get("sample_title"),
                        "price": price_value,
                        "quantity": offer.get("qnt", 0),
                        "discount": offer.get("discount", 0),
                        "source_product_id": offer.get("source_product_id", ""),
                        "collection_name": offer.get("collection_name", ""),
                        "purchase_url": offer.get("purchase_url", "")
                    })

    # Статистика
    prices = [o["price"] for o in supplier_offers if o["price"] > 0]

    return {
        "supplier_key": supplier_key,
        "supplier_info": supplier_info,
        "total_products": len(products),
        "total_offers": len(supplier_offers),
        "products_summary": products_summary,
        "price_statistics": {
            "min_price": min(prices) if prices else 0,
            "max_price": max(prices) if prices else 0,
            "avg_price": sum(prices) / len(prices) if prices else 0
        },
        "okpd_filter": okpd_code,
        "all_offers": supplier_offers
    }


@router.get("/offers/summary")
async def get_offers_summary(
        min_offers: int = Query(10, description="Minimum offers per product"),
        okpd_code: Optional[str] = Query(None, description="Filter by OKPD2 code"),
        limit: int = Query(20, ge=1, le=100),
        unique_products_store=Depends(get_unique_products_store),
        api_key: str = Depends(verify_api_key)
):
    """
    Получить сводку по товарам с наибольшим количеством предложений

    Показывает товары, которые предлагает больше всего поставщиков
    или у которых больше всего уникальных предложений
    """
    filters = {
        "total_offers_count": {"$gte": min_offers}
    }

    if okpd_code:
        filters["okpd2_code"] = {"$regex": f"^{okpd_code}"}

    products = await unique_products_store.find_products(
        filters=filters,
        limit=limit,
        sort_by="total_offers_count",
        sort_order=-1
    )

    summary = []
    for product in products:
        # Собираем все цены
        all_prices = []
        for supplier in product.get("unique_suppliers", []):
            for offer in supplier.get("supplier_offers", []):
                price_value = offer.get("price", 0)
                if isinstance(price_value, dict):
                    price_value = price_value.get("price", 0)
                if price_value > 0:
                    all_prices.append(price_value)

        summary.append({
            "product_hash": product["product_hash"],
            "sample_title": product.get("sample_title"),
            "sample_brand": product.get("sample_brand"),
            "okpd2_code": product["okpd2_code"],
            "total_offers": product.get("total_offers_count", 0),
            "unique_suppliers": product["unique_suppliers_count"],
            "total_sources": product["total_sources"],
            "price_range": {
                "min": min(all_prices) if all_prices else 0,
                "max": max(all_prices) if all_prices else 0,
                "avg": sum(all_prices) / len(all_prices) if all_prices else 0
            }
        })

    return {
        "products": summary,
        "count": len(summary),
        "filters": {
            "min_offers": min_offers,
            "okpd_code": okpd_code
        }
    }