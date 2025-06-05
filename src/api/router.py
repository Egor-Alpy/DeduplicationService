from fastapi import APIRouter
from src.api.endpoints import unique_products

router = APIRouter()

router.include_router(
    unique_products.router,
    prefix="/unique-products",
    tags=["unique_products"]
)