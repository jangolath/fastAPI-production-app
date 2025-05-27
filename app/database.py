import asyncio
import logging
from typing import AsyncGenerator

import asyncpg
from asyncpg import Pool

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class DatabaseManager:
    """Database connection manager."""
    
    def __init__(self):
        self._pool: Optional[Pool] = None
    
    async def initialize(self):
        """Initialize database connection pool."""
        try:
            self._pool = await asyncpg.create_pool(
                settings.database_url,
                min_size=settings.database_pool_min,
                max_size=settings.database_pool_max,
                command_timeout=60
            )
            logger.info("Database pool initialized")
            
            # Create tables if they don't exist
            await self._create_tables()
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def _create_tables(self):
        """Create database tables."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Database tables created/verified")
    
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get database connection from pool."""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self._pool.acquire() as conn:
            yield conn
    
    async def close(self):
        """Close database connection pool."""
        if self._pool:
            await self._pool.close()
            logger.info("Database pool closed")