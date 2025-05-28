import asyncio
import asyncpg
import logging
from typing import Optional, AsyncGenerator

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._pool: Optional[asyncpg.Pool] = None

    async def initialize(self, max_retries: int = 10, retry_delay: float = 2.0):
        """Initialize database connection with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to database (attempt {attempt + 1}/{max_retries})")
                
                self._pool = await asyncpg.create_pool(
                    self.database_url,
                    min_size=1,
                    max_size=10,
                    command_timeout=60,
                )
                
                if not self._pool:
                    raise RuntimeError("Failed to create connection pool")
                
                # Test the connection
                async with self._pool.acquire() as connection:
                    await connection.execute("SELECT 1")
                
                logger.info("Database connection established successfully")
                await self._create_tables()
                return
                
            except Exception as e:
                logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to database.")
                    raise

    async def _create_tables(self):
        """Create tables if they don't exist"""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        try:
            async with self._pool.acquire() as connection:
                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            logger.info("Database tables created/verified successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get database connection from pool."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self._pool.acquire() as conn:
            yield conn

    async def close(self):
        """Close database connection pool"""
        if self._pool:
            await self._pool.close()
            logger.info("Database connection closed")

    def get_pool(self) -> asyncpg.Pool:
        """Get the database connection pool"""
        if not self._pool:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._pool