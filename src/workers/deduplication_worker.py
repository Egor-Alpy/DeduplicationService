import asyncio
import logging
import sys
from typing import Optional

from src.services.product_deduplicator import ProductDeduplicationService
from src.services.supplier_fetcher import SupplierFetcher
from src.storage.standardized_mongo import StandardizedMongoStore
from src.storage.unique_products_mongo import UniqueProductsMongoStore
from src.core.config import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class DeduplicationWorker:
    """Воркер для дедупликации товаров"""

    def __init__(self, worker_id: str = "dedup_worker_1"):
        self.worker_id = worker_id
        self.standardized_store = None
        self.unique_products_store = None
        self.supplier_fetcher = None
        self.deduplication_service = None
        self.running = False
        logger.info(f"Initializing deduplication worker: {self.worker_id}")

    async def start(self):
        """Запустить воркер"""
        logger.info(f"Starting deduplication worker {self.worker_id}...")

        try:
            # Инициализируем хранилища
            logger.info("Connecting to standardized MongoDB...")
            self.standardized_store = StandardizedMongoStore(
                settings.standardized_mongodb_database,
                settings.standardized_collection_name
            )

            # Проверяем подключение
            if not await self.standardized_store.test_connection():
                logger.error("Failed to connect to standardized MongoDB!")
                return

            # Проверяем наличие стандартизированных товаров
            stats = await self.standardized_store.get_statistics()
            total_standardized = stats.get("total", 0)
            logger.info(f"Found {total_standardized} standardized products")

            if total_standardized == 0:
                logger.warning("No standardized products found!")
                logger.info("Please run standardization service first.")
                return

            # Статистика по группировке
            ungrouped = stats.get("ungrouped", 0)
            logger.info(f"Products pending deduplication: {ungrouped}")

            if ungrouped == 0:
                logger.warning("No products pending deduplication!")
                return

            logger.info("Connecting to unique products MongoDB...")
            self.unique_products_store = UniqueProductsMongoStore(
                settings.unique_mongodb_database,
                settings.unique_collection_name
            )

            # Инициализируем хранилище (создание индексов)
            await self.unique_products_store.initialize()

            logger.info("Connecting to source MongoDB for supplier details...")
            self.supplier_fetcher = SupplierFetcher()

            # Проверяем подключение к исходной БД
            if not await self.supplier_fetcher.test_connection():
                logger.error("Failed to connect to source MongoDB!")
                return

            logger.info("Creating deduplication service...")
            logger.info(f"Batch size: {settings.deduplication_batch_size}")
            logger.info(f"Worker delay: {settings.worker_delay}s")

            self.deduplication_service = ProductDeduplicationService(
                standardized_store=self.standardized_store,
                unique_products_store=self.unique_products_store,
                supplier_fetcher=self.supplier_fetcher
            )

            self.running = True
            logger.info(f"Worker {self.worker_id} initialized successfully. Starting continuous deduplication...")

            # Запускаем непрерывную дедупликацию
            await self.run_continuous_deduplication()

        except KeyboardInterrupt:
            logger.info(f"Worker {self.worker_id} interrupted by user")
            raise
        except Exception as e:
            logger.error(f"Deduplication worker {self.worker_id} error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def run_continuous_deduplication(self):
        """Запустить непрерывную дедупликацию"""
        batch_size = settings.deduplication_batch_size
        delay_between_batches = settings.worker_delay

        while self.running:
            try:
                # Получаем статистику
                stats = await self.deduplication_service.get_statistics()
                ungrouped = stats.get("ungrouped_products", 0)

                if ungrouped == 0:
                    logger.info("No ungrouped products found, waiting...")
                    await asyncio.sleep(30)
                    continue

                logger.info(f"Found {ungrouped} ungrouped products")

                # Обрабатываем батч
                result = await self.deduplication_service.process_batch(limit=batch_size)

                logger.info(
                    f"Batch processed: {result['processed']} products, "
                    f"{result['groups_created']} new groups, "
                    f"{result['groups_updated']} updated groups"
                )

                # Показываем текущую статистику
                if result['processed'] > 0:
                    current_stats = await self.unique_products_store.get_statistics()
                    logger.info(
                        f"Total unique products: {current_stats['total_unique_products']}, "
                        f"Deduplication rate: {current_stats['deduplication_rate']}%"
                    )

                # Задержка между батчами
                await asyncio.sleep(delay_between_batches)

            except Exception as e:
                logger.error(f"Error in continuous deduplication: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def stop(self):
        """Остановить воркер"""
        logger.info(f"Stopping deduplication worker {self.worker_id}...")
        self.running = False

        if self.standardized_store:
            await self.standardized_store.close()

        if self.unique_products_store:
            await self.unique_products_store.close()

        if self.supplier_fetcher:
            await self.supplier_fetcher.close()

        logger.info(f"Worker {self.worker_id} stopped successfully")


async def main():
    """Запуск воркера из командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='Product deduplication worker')
    parser.add_argument('--worker-id', default='dedup_worker_1', help='Worker ID')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')
    args = parser.parse_args()

    # Настройка уровня логирования
    log_level = getattr(logging, args.log_level.upper())
    logging.getLogger().setLevel(log_level)
    logging.getLogger('src').setLevel(log_level)

    logger.info("=" * 60)
    logger.info("Product Deduplication Worker Starting")
    logger.info("=" * 60)
    logger.info(f"Worker ID: {args.worker_id}")
    logger.info(f"Log Level: {args.log_level}")
    logger.info(f"Service: {settings.service_name}")
    logger.info(f"Batch Size: {settings.deduplication_batch_size}")
    logger.info(f"Worker Delay: {settings.worker_delay}s")
    logger.info("=" * 60)

    try:
        worker = DeduplicationWorker(args.worker_id)
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())