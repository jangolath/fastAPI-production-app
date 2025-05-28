import asyncio
import logging
import time
import json
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Dict, List, Optional, Any, cast

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Request, Response, Query, Body
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
from redis_service import RedisService, RedisKeys

# Initialize settings and logging
settings = get_settings()
setup_logging()
logger = logging.getLogger(__name__)

# Initialize rate limiter
limiter = Limiter(key_func=get_remote_address)

# Database and Redis manager instances
db_manager = DatabaseManager(database_url=settings.database_url)
redis_service = RedisService(redis_url=settings.redis_url)

# Security
security = HTTPBearer()


# Additional Pydantic models for Redis demonstrations
class UserPreferences(BaseModel):
    """User preferences model."""
    theme: str = "light"
    language: str = "en"
    notifications: bool = True
    timezone: str = "UTC"


class UserActivity(BaseModel):
    """User activity model."""
    action: str
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None


class ApiStats(BaseModel):
    """API statistics model."""
    endpoint: str
    call_count: int
    last_called: Optional[datetime] = None


class LeaderboardEntry(BaseModel):
    """Leaderboard entry model."""
    user_id: int
    user_name: str
    score: float
    rank: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    logger.info("Starting up application...")
    await db_manager.initialize()
    await redis_service.initialize()
    
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
        await redis_service.close()


async def background_worker():
    """Example background worker using asyncio."""
    while True:
        try:
            logger.info("Background worker running...")
            
            # Example: Update daily statistics in Redis
            today = date.today().isoformat()
            await redis_service.increment_counter(f"stats:daily:{today}:background_tasks")
            
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
    title="Production FastAPI Application with Redis",
    description="A production-ready FastAPI app with Redis caching, counters, and real-time features",
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


# Dependencies
async def get_db():
    """Database dependency."""
    async for conn in db_manager.get_connection():
        yield conn
        break


async def get_redis() -> RedisService:
    """Redis dependency."""
    return redis_service


async def get_user_service(db=Depends(get_db)) -> UserService:
    """User service dependency."""
    return UserService(db)


# Middleware for API call counting
@app.middleware("http")
async def count_api_calls(request: Request, call_next):
    """Count API calls per endpoint."""
    start_time = time.time()
    
    # Count the API call
    endpoint = f"{request.method}:{request.url.path}"
    await redis_service.increment_counter(RedisKeys.api_counter(endpoint))
    
    response = await call_next(request)
    
    # Track response time
    process_time = time.time() - start_time
    await redis_service.set_hash_field(
        f"api:performance:{endpoint}",
        "last_response_time",
        process_time
    )
    
    return response


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


@app.get("/health/redis", tags=["Health"])
async def redis_health_check(redis: RedisService = Depends(get_redis)):
    """Redis health check."""
    try:
        await redis.client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        raise HTTPException(status_code=503, detail="Redis unavailable")


# ============== USER ENDPOINTS WITH REDIS CACHING ==============

@app.post("/users/", response_model=UserResponse, tags=["Users"])
@limiter.limit("10/minute")
async def create_user(
    request: Request,
    user_data: UserCreate,
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """Create a new user with Redis integration."""
    try:
        user = await user_service.create_user(user_data)
        if not user:
            raise DatabaseError("Failed to create user")
        
        # Cache the new user
        cache_key = RedisKeys.user_cache(user.id)
        user_dict = user.model_dump()
        await redis.set_cache(cache_key, user_dict, ttl=300)  # Cache for 5 minutes
        
        # Add to active users set
        await redis.add_to_set(RedisKeys.active_users(), user.id)
        
        # Track user creation activity
        activity = {
            "action": "user_created",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {"user_id": user.id, "email": user.email}
        }
        await redis.add_to_list(RedisKeys.user_activity(user.id), activity, max_length=50)
        
        # Add to leaderboard with initial score
        await redis.add_to_sorted_set(RedisKeys.user_leaderboard(), 0, {
            "user_id": user.id,
            "name": user.name,
            "email": user.email
        })
        
        logger.info(f"User created and cached: {user.id}")
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
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """Get user by ID with Redis caching."""
    try:
        # Try to get from cache first
        cache_key = RedisKeys.user_cache(user_id)
        cached_user = await redis.get_cache(cache_key)
        
        if cached_user:
            logger.info(f"User {user_id} retrieved from cache")
            # Track cache hit
            await redis.increment_counter("cache:hits:users")
            return UserResponse.model_validate(cached_user)
        
        # If not in cache, get from database
        user = await user_service.get_user(user_id)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        
        # Cache the user
        user_dict = user.model_dump()
        await redis.set_cache(cache_key, user_dict, ttl=300)
        
        # Track cache miss
        await redis.increment_counter("cache:misses:users")
        
        # Track user access activity
        activity = {
            "action": "user_accessed",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {"accessed_by": "api"}
        }
        await redis.add_to_list(RedisKeys.user_activity(user_id), activity, max_length=50)
        
        logger.info(f"User {user_id} retrieved from database and cached")
        return UserResponse.model_validate(user)
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/users/", response_model=List[UserResponse], tags=["Users"])
@limiter.limit("20/minute")
async def list_users(
    request: Request,
    skip: int = 0,
    limit: int = Query(default=10, le=100),
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """List users with pagination and Redis caching."""
    # Cache key for paginated results
    cache_key = f"users:list:{skip}:{limit}"
    cached_users = await redis.get_cache(cache_key)
    
    if cached_users:
        logger.info(f"User list retrieved from cache (skip={skip}, limit={limit})")
        await redis.increment_counter("cache:hits:user_lists")
        return [UserResponse.model_validate(user) for user in cached_users]
    
    # Get from database
    users = await user_service.list_users(skip=skip, limit=limit)
    
    # Cache the results
    users_dict = [user.model_dump() for user in users]
    await redis.set_cache(cache_key, users_dict, ttl=120)  # Cache for 2 minutes
    
    await redis.increment_counter("cache:misses:user_lists")
    
    return [UserResponse.model_validate(user) for user in users]


@app.put("/users/{user_id}", response_model=UserResponse, tags=["Users"])
@limiter.limit("5/minute")
async def update_user(
    request: Request,
    user_id: int,
    user_data: UserUpdate,
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """Update user with cache invalidation."""
    try:
        user = await user_service.update_user(user_id, user_data)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        
        # Invalidate cache
        cache_key = RedisKeys.user_cache(user_id)
        await redis.delete_cache(cache_key)
        
        # Invalidate list caches (simple approach - delete all list caches)
        await redis.delete_keys_pattern("users:list:*")
        
        # Track user update activity
        activity = {
            "action": "user_updated",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {"updated_fields": list(user_data.model_dump(exclude_unset=True).keys())}
        }
        await redis.add_to_list(RedisKeys.user_activity(user_id), activity, max_length=50)
        
        # Update leaderboard entry
        await redis.add_to_sorted_set(RedisKeys.user_leaderboard(), 
                                     await redis.get_counter(f"user:score:{user_id}"), 
                                     {
                                         "user_id": user.id,
                                         "name": user.name,
                                         "email": user.email
                                     })
        
        logger.info(f"User updated and cache invalidated: {user.id}")
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
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """Delete user with Redis cleanup."""
    try:
        success = await user_service.delete_user(user_id)
        if not success:
            raise NotFoundError(f"User with ID {user_id} not found")
        
        # Clean up Redis data
        await redis.delete_cache(RedisKeys.user_cache(user_id))
        await redis.delete_keys_pattern(f"user:*:{user_id}")
        await redis.delete_keys_pattern("users:list:*")
        
        # Remove from active users set using the proper method
        await redis.remove_from_set(RedisKeys.active_users(), user_id)
        
        logger.info(f"User deleted and Redis data cleaned: {user_id}")
        return {"message": "User deleted successfully"}
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ============== REDIS DEMONSTRATION ENDPOINTS ==============

@app.get("/redis/cache/stats", tags=["Redis Demo"])
async def get_cache_stats(redis: RedisService = Depends(get_redis)):
    """Get cache statistics."""
    hits = await redis.get_counter("cache:hits:users") + await redis.get_counter("cache:hits:user_lists")
    misses = await redis.get_counter("cache:misses:users") + await redis.get_counter("cache:misses:user_lists")
    total = hits + misses
    hit_rate = (hits / total * 100) if total > 0 else 0
    
    return {
        "cache_hits": hits,
        "cache_misses": misses,
        "total_requests": total,
        "hit_rate_percentage": round(hit_rate, 2)
    }


@app.get("/redis/api/stats", tags=["Redis Demo"])
async def get_api_stats(redis: RedisService = Depends(get_redis)):
    """Get API call statistics."""
    # Get all API counter keys
    counter_keys = await redis.get_keys_pattern("api:counter:*")
    
    stats = []
    for key in counter_keys:
        endpoint = key.replace("api:counter:", "").replace(":", "/")
        count = await redis.get_counter(key)
        
        # Get performance data if available
        perf_key = f"api:performance:{key.replace('api:counter:', '')}"
        last_response_time = await redis.get_hash_field(perf_key, "last_response_time")
        
        stats.append({
            "endpoint": endpoint,
            "call_count": count,
            "last_response_time_seconds": last_response_time
        })
    
    # Sort by call count
    stats.sort(key=lambda x: x["call_count"], reverse=True)
    
    return {"api_statistics": stats}


@app.get("/users/{user_id}/preferences", response_model=UserPreferences, tags=["Redis Demo"])
async def get_user_preferences(
    user_id: int,
    redis: RedisService = Depends(get_redis)
):
    """Get user preferences from Redis."""
    prefs_key = RedisKeys.user_preferences(user_id)
    preferences = await redis.get_hash_all(prefs_key)
    
    if not preferences:
        # Return default preferences
        default_prefs = UserPreferences()
        return default_prefs
    
    return UserPreferences(**preferences)


@app.put("/users/{user_id}/preferences", tags=["Redis Demo"])
async def update_user_preferences(
    user_id: int,
    preferences: UserPreferences,
    redis: RedisService = Depends(get_redis)
):
    """Update user preferences in Redis."""
    prefs_key = RedisKeys.user_preferences(user_id)
    
    # Set each preference field
    prefs_dict = preferences.model_dump()
    for field, value in prefs_dict.items():
        await redis.set_hash_field(prefs_key, field, value)
    
    # Set expiration (preferences expire after 7 days of inactivity)
    await redis.expire_key(prefs_key, 7 * 24 * 60 * 60)
    
    # Track activity
    activity = {
        "action": "preferences_updated",
        "timestamp": datetime.utcnow().isoformat(),
        "details": {"updated_preferences": list(prefs_dict.keys())}
    }
    await redis.add_to_list(RedisKeys.user_activity(user_id), activity, max_length=50)
    
    return {"message": "Preferences updated successfully", "preferences": preferences}


@app.get("/users/{user_id}/activity", tags=["Redis Demo"])
async def get_user_activity(
    user_id: int,
    limit: int = Query(default=10, le=50),
    redis: RedisService = Depends(get_redis)
):
    """Get user activity history from Redis."""
    activities = await redis.get_list(RedisKeys.user_activity(user_id), 0, limit - 1)
    
    return {
        "user_id": user_id,
        "activities": activities,
        "total_activities": await redis.get_list_length(RedisKeys.user_activity(user_id))
    }


@app.post("/users/{user_id}/score", tags=["Redis Demo"])
async def update_user_score(
    user_id: int,
    score_change: int = Body(..., description="Score change (positive or negative)"),
    redis: RedisService = Depends(get_redis),
    user_service: UserService = Depends(get_user_service)
):
    """Update user score in Redis leaderboard."""
    try:
        # Check if user exists
        user = await user_service.get_user(user_id)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        
        # Update score
        new_score = await redis.increment_counter(f"user:score:{user_id}", score_change)
        
        # Update leaderboard
        await redis.add_to_sorted_set(RedisKeys.user_leaderboard(), new_score, {
            "user_id": user.id,
            "name": user.name,
            "email": user.email
        })
        
        # Track activity
        activity = {
            "action": "score_updated",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {"score_change": score_change, "new_score": new_score}
        }
        await redis.add_to_list(RedisKeys.user_activity(user_id), activity, max_length=50)
        
        # Get current rank
        rank = await redis.get_sorted_set_rank(RedisKeys.user_leaderboard(), {
            "user_id": user.id,
            "name": user.name,
            "email": user.email
        })
        
        return {
            "user_id": user_id,
            "score_change": score_change,
            "new_score": new_score,
            "current_rank": rank
        }
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/leaderboard", response_model=List[LeaderboardEntry], tags=["Redis Demo"])
async def get_leaderboard(
    limit: int = Query(default=10, le=100),
    redis: RedisService = Depends(get_redis)
):
    """Get user leaderboard from Redis."""
    leaderboard_data = await redis.get_sorted_set_top(RedisKeys.user_leaderboard(), limit)
    
    leaderboard = []
    for rank, entry in enumerate(leaderboard_data, 1):
        member_data = entry["member"]
        leaderboard.append(LeaderboardEntry(
            user_id=member_data["user_id"],
            user_name=member_data["name"],
            score=entry["score"],
            rank=rank
        ))
    
    return leaderboard


@app.get("/users/active", tags=["Redis Demo"])
async def get_active_users(redis: RedisService = Depends(get_redis)):
    """Get currently active users from Redis set."""
    active_user_ids = await redis.get_set(RedisKeys.active_users())
    
    return {
        "active_users": active_user_ids,
        "total_active": len(active_user_ids)
    }


@app.post("/redis/demo/data", tags=["Redis Demo"])
async def create_demo_data(
    redis: RedisService = Depends(get_redis),
    user_service: UserService = Depends(get_user_service)
):
    """Create demo data to showcase Redis features."""
    try:
        # Create some demo users if they don't exist
        demo_users = []
        for i in range(3):
            try:
                user_data = UserCreate(
                    email=f"demo{i+1}@example.com",
                    name=f"Demo User {i+1}",
                    is_active=True
                )
                user = await user_service.create_user(user_data)
                demo_users.append(user)
            except ValidationError:
                # User already exists, fetch it
                existing_users = await user_service.list_users(skip=0, limit=10)
                demo_user = next((u for u in existing_users if f"demo{i+1}@example.com" in u.email), None)
                if demo_user:
                    demo_users.append(demo_user)
        
        # Add some demo activities and scores
        activities = [
            "logged_in", "viewed_dashboard", "updated_profile", 
            "completed_task", "shared_content", "left_feedback"
        ]
        
        for user in demo_users:
            # Add random activities
            for activity in activities[:3]:  # Add 3 activities per user
                activity_data = {
                    "action": activity,
                    "timestamp": datetime.utcnow().isoformat(),
                    "details": {"demo": True}
                }
                await redis.add_to_list(RedisKeys.user_activity(user.id), activity_data)
            
            # Add random scores
            import random
            score = random.randint(50, 500)
            await redis.increment_counter(f"user:score:{user.id}", score)
            await redis.add_to_sorted_set(RedisKeys.user_leaderboard(), score, {
                "user_id": user.id,
                "name": user.name,
                "email": user.email
            })
            
            # Add demo preferences
            demo_prefs = UserPreferences(
                theme=random.choice(["light", "dark"]),
                language=random.choice(["en", "es", "fr"]),
                notifications=random.choice([True, False]),
                timezone=random.choice(["UTC", "EST", "PST"])
            )
            prefs_key = RedisKeys.user_preferences(user.id)
            for field, value in demo_prefs.model_dump().items():
                await redis.set_hash_field(prefs_key, field, value)
        
        return {
            "message": "Demo data created successfully",
            "created_users": len(demo_users),
            "tip": "Try the following endpoints to see Redis in action:",
            "endpoints": [
                "GET /redis/cache/stats - View cache statistics",
                "GET /redis/api/stats - View API call statistics", 
                "GET /leaderboard - View user leaderboard",
                "GET /users/active - View active users",
                f"GET /users/{demo_users[0].id if demo_users else 1}/activity - View user activity",
                f"GET /users/{demo_users[0].id if demo_users else 1}/preferences - View user preferences"
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to create demo data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create demo data: {str(e)}")

@app.post("/users/batch", response_model=List[UserResponse], tags=["Users"])
@limiter.limit("2/minute")
async def create_users_batch(
    request: Request,
    users_data: List[UserCreate],
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """Create multiple users concurrently with Redis integration."""
    if len(users_data) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 users per batch")
    
    try:
        tasks = [user_service.create_user(user_data) for user_data in users_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful_users = []
        errors = []
        
        for i, result in enumerate(results):
            try:
                # Handle exceptions
                if isinstance(result, Exception):
                    errors.append(f"User {i}: {str(result)}")
                    continue
                    
                # Handle None results
                if result is None:
                    errors.append(f"User {i}: Creation returned None")
                    continue
                
                # Process successful user creation
                # Instead of accessing attributes directly, use the validated response
                user_response = UserResponse.model_validate(result)
                successful_users.append(user_response)
                
                # For Redis operations, use the validated response data
                user_id = user_response.id
                cache_key = RedisKeys.user_cache(user_id)
                
                # Convert the user_response to dict for caching (avoiding the type issue)
                user_dict = user_response.model_dump()
                await redis.set_cache(cache_key, user_dict, ttl=300)
                await redis.add_to_set(RedisKeys.active_users(), user_id)
                
            except Exception as e:
                errors.append(f"User {i}: Failed to process result: {str(e)}")
        
        if errors:
            logger.warning(f"Batch creation partially failed: {errors}")
        
        logger.info(f"Batch created {len(successful_users)} users")
        return successful_users
        
    except Exception as e:
        logger.error(f"Batch creation failed: {e}")
        raise HTTPException(status_code=500, detail="Batch creation failed")

@app.post("/users/{user_id}/process", tags=["Users"])
@limiter.limit("1/minute")
async def process_user_data(
    request: Request,
    user_id: int,
    user_service: UserService = Depends(get_user_service),
    redis: RedisService = Depends(get_redis)
):
    """Simulate long-running async operation with Redis tracking."""
    try:
        user = await user_service.get_user(user_id)
        if not user:
            raise NotFoundError(f"User with ID {user_id} not found")
        
        # Track processing start
        activity = {
            "action": "processing_started",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {"operation": "data_processing"}
        }
        await redis.add_to_list(RedisKeys.user_activity(user_id), activity, max_length=50)
        
        # Simulate async processing
        await asyncio.sleep(2)
        
        # Update user with processed flag
        processed_data = UserUpdate(name=user.name, email=user.email, is_active=True)
        updated_user = await user_service.update_user(user_id, processed_data)
        
        # Track processing completion
        activity = {
            "action": "processing_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {"operation": "data_processing", "duration_seconds": 2}
        }
        await redis.add_to_list(RedisKeys.user_activity(user_id), activity, max_length=50)
        
        # Award points for processing
        await redis.increment_counter(f"user:score:{user_id}", 10)
        
        # Invalidate cache
        await redis.delete_cache(RedisKeys.user_cache(user_id))
        
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