import os
from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # App settings
    app_name: str = "FastAPI Production App"
    debug: bool = Field(default=False, env="DEBUG")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    allowed_hosts: List[str] = Field(default=["*"], env="ALLOWED_HOSTS")
    
    # Database settings
    database_url: str = Field(env="DATABASE_URL", default="postgresql://postgres:cu+eP@nda12@localhost/dbname")
    database_pool_min: int = Field(default=10, env="DB_POOL_MIN")
    database_pool_max: int = Field(default=20, env="DB_POOL_MAX")
    
    # Redis settings (for caching/sessions)
    redis_url: str = Field(default="redis://localhost:6379", env="REDIS_URL")
    
    # Security settings
    secret_key: str = Field(env="SECRET_KEY", default="your-secret-key-here")
    
    model_config = ConfigDict(env_file=".env")


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
