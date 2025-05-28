import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Request, Response, Query  # Added Query import
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer
from pydantic import BaseModel, Field, ConfigDict
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from config import get_settings
from database import DatabaseManager
from exceptions import AppException, ValidationError, NotFoundError, DatabaseError
from middleware import RequestLoggingMiddleware, ErrorHandlingMiddleware
from models import User, UserCreate, UserUpdate, UserResponse
from services import UserService
from utils import setup_logging

# Initialize settings and logging
settings = get_settings()
setup_logging()
logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Database manager instance
db_manager = DatabaseManager(database_url=settings.database_url)

# Security
security = HTTPBearer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    logger.info("Starting up application...")
    await db_manager.initialize()
    
    # Background task example
    background_task = asyncio.create_task(background_worker())
    
    try:
        yield
    finally:
        # Shutdown
        logger.info("Shutting down application...")
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass
        await db_manager.close()


async def background_worker():
    """Example background worker using asyncio."""
    while True:
        try:
            logger.info("Background worker running...")
            # Simulate background work
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            logger.info("Background worker cancelled")
            break
        except Exception as e:
            logger.error(f"Background worker error: {e}")
            await asyncio.sleep(5)


# Initialize FastAPI app
app = FastAPI(
    title="Production FastAPI Application",
    description="A production-ready FastAPI app with error handling, rate limiting, and async operations",
    version="1.0.0",
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
    lifespan=lifespan
)

# Add rate limiting
app.state.limiter = limiter

def rate_limit_exceeded_handler(request: Request, exc: Exception) -> Response:
    return _rate_limit_exceeded_handler(request, exc)  # type: ignore

app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# Add middleware (order matters!)
app.add_middleware(SlowAPIMiddleware)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ErrorHandlingMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_hosts,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.allowed_hosts
)


# Dependency for database connection
async def get_db():
    """Database dependency."""
    async for conn in db_manager.get_connection():
        yield conn
        break


# Dependency for user service
async def get_user_service(db=Depends(get_db)) -> UserService:
    """User service dependency."""
    return UserService(db)


# Health check endpoints
@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": time.time()}


@app.get("/health/db", tags=["Health"])
async def database_health_check(db=Depends(get_db)):
    """Database health check."""
    try:
        await db.fetchval("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database unavailable")


# User endpoints with rate limiting and async operations
@app.post("/users/", response_model=UserResponse, tags=["Users"])
@limiter.limit("10/minute")
async def create_user(
    request: Request,
    user_data: UserCreate,
    user_service: UserService = Depends(get_user_service)
):
    """Create a new user with rate limiting."""
    try:
        user = await user_service.create_user(user_data)
        if not user:
            raise DatabaseError("Failed to create user")
        logger.info(f"User created: {user.id}")
        return UserResponse.model_validate(user)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except DatabaseError as e:
        raise HTTPException(status_code=500, detail="Database error occurred")


@app.get("/users/{user_id}", response_model=UserResponse, tags=["Users"])
@limiter.limit("30/minute")
async def get_user(
    request: Request,
    user_id: int,
    user_service: UserService = Depends(get_user_service)
):
    """Get user by ID with rate limiting."""
    try:
        user = await user_service.get_user(user_id)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        return UserResponse.model_validate(user)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/users/", response_model=List[UserResponse], tags=["Users"])
@limiter.limit("20/minute")
async def list_users(
    request: Request,
    skip: int = 0,
    limit: int = Query(default=10, le=100),  # Fixed: Changed Field to Query
    user_service: UserService = Depends(get_user_service)
):
    """List users with pagination and rate limiting."""
    users = await user_service.list_users(skip=skip, limit=limit)
    return [UserResponse.model_validate(user) for user in users]


@app.put("/users/{user_id}", response_model=UserResponse, tags=["Users"])
@limiter.limit("5/minute")
async def update_user(
    request: Request,
    user_id: int,
    user_data: UserUpdate,
    user_service: UserService = Depends(get_user_service)
):
    """Update user with rate limiting."""
    try:
        user = await user_service.update_user(user_id, user_data)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        logger.info(f"User updated: {user.id}")
        return UserResponse.model_validate(user)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/users/{user_id}", tags=["Users"])
@limiter.limit("3/minute")
async def delete_user(
    request: Request,
    user_id: int,
    user_service: UserService = Depends(get_user_service)
):
    """Delete user with rate limiting."""
    try:
        success = await user_service.delete_user(user_id)
        if not success:
            raise NotFoundError(f"User with ID {user_id} not found")
        logger.info(f"User deleted: {user_id}")
        return {"message": "User deleted successfully"}
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# Async operations endpoint
@app.post("/users/batch", response_model=List[UserResponse], tags=["Users"])
@limiter.limit("2/minute")
async def create_users_batch(
    request: Request,
    users_data: List[UserCreate],
    user_service: UserService = Depends(get_user_service)
):
    """Create multiple users concurrently."""
    if len(users_data) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 users per batch")
    
    try:
        # Use asyncio.gather for concurrent creation
        tasks = [user_service.create_user(user_data) for user_data in users_data]
        users = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions in the results
        successful_users = []
        errors = []
        
        for i, result in enumerate(users):
            if isinstance(result, Exception):
                errors.append(f"User {i}: {str(result)}")
            else:
                successful_users.append(UserResponse.model_validate(result))
        
        if errors:
            logger.warning(f"Batch creation partially failed: {errors}")
        
        logger.info(f"Batch created {len(successful_users)} users")
        return successful_users
        
    except Exception as e:
        logger.error(f"Batch creation failed: {e}")
        raise HTTPException(status_code=500, detail="Batch creation failed")


# Example of long-running async operation
@app.post("/users/{user_id}/process", tags=["Users"])
@limiter.limit("1/minute")
async def process_user_data(
    request: Request,
    user_id: int,
    user_service: UserService = Depends(get_user_service)
):
    """Simulate long-running async operation."""
    try:
        user = await user_service.get_user(user_id)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        
        # Simulate async processing
        await asyncio.sleep(2)  # Simulate work
        
        # Update user with processed flag
        processed_data = UserUpdate(name=user.name, email=user.email, is_active=True)
        updated_user = await user_service.update_user(user_id, processed_data)
        
        logger.info(f"User data processed: {user_id}")
        return {"message": "User data processed successfully", "user_id": user_id}
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Processing failed for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Processing failed")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info" if not settings.debug else "debug",
        access_log=True
    )