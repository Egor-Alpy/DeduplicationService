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
    """Получить товары с наибольшим количеством дубликатов"""
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