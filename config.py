import os
from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""
    
    # App settings
    app_name: str = "FastAPI Production App"
    debug: bool = Field(default=False, description="Debug mode")
    log_level: str = Field(default="INFO", description="Logging level")
    allowed_hosts: List[str] = Field(default=["*"], description="Allowed hosts for CORS")
    
    # Database settings
    database_url: str = Field(
        default="postgresql://postgres:password@localhost/dbname",
        description="Database connection URL"
    )
    database_pool_min: int = Field(default=10, description="Minimum database pool size")
    database_pool_max: int = Field(default=20, description="Maximum database pool size")
    
    # Redis settings (for caching/sessions)
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL"
    )
    
    # Security settings
    secret_key: str = Field(
        default="your-secret-key-here-change-in-production",
        description="Secret key for security operations"
    )
    
    # Configuration for BaseSettings
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # Ignore extra environment variables
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()