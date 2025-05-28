import logging
from datetime import datetime
from typing import List, Optional

import asyncpg

from exceptions import ValidationError, DatabaseError
from models import User, UserCreate, UserUpdate

logger = logging.getLogger(__name__)


class UserService:
    """User business logic service with optional Redis integration."""
    
    def __init__(self, db: asyncpg.Connection, redis_service=None):
        self.db = db
        self.redis = redis_service
    
    async def create_user(self, user_data: UserCreate) -> Optional[User]:
        """Create a new user."""
        try:
            # Check if user already exists
            existing = await self.db.fetchrow(
                "SELECT id FROM users WHERE email = $1", user_data.email
            )
            if existing:
                raise ValidationError(f"User with email {user_data.email} already exists")
            
            # Create user
            row = await self.db.fetchrow(
                """
                INSERT INTO users (email, name, is_active)
                VALUES ($1, $2, $3)
                RETURNING id, email, name, is_active, created_at, updated_at
                """,
                user_data.email, user_data.name, user_data.is_active
            )
            
            user = User(**dict(row)) if row else None
            
            # If Redis is available, cache the user and track creation
            if user and self.redis:
                try:
                    # Cache the user
                    cache_key = f"user:cache:{user.id}"
                    user_dict = user.model_dump()
                    await self.redis.set_cache(cache_key, user_dict, ttl=300)
                    
                    # Add to active users set
                    await self.redis.add_to_set("users:active", user.id)
                    
                    # Track creation activity
                    activity = {
                        "action": "user_created",
                        "timestamp": datetime.utcnow().isoformat(),
                        "details": {"service": "UserService"}
                    }
                    await self.redis.add_to_list(f"user:activity:{user.id}", activity)
                    
                    # Initialize user score
                    await self.redis.set_cache(f"user:score:{user.id}", 0, ttl=86400)  # 24 hours
                    
                except Exception as redis_error:
                    logger.warning(f"Redis operation failed during user creation: {redis_error}")
                    # Don't fail the entire operation if Redis fails
            
            return user
            
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            raise DatabaseError(f"Failed to create user: {str(e)}")
    
    async def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID with Redis caching."""
        try:
            # Try Redis cache first if available
            if self.redis:
                try:
                    cache_key = f"user:cache:{user_id}"
                    cached_user = await self.redis.get_cache(cache_key)
                    if cached_user:
                        logger.debug(f"User {user_id} found in cache")
                        await self.redis.increment_counter("service:cache:hits")
                        return User(**cached_user)
                    
                    await self.redis.increment_counter("service:cache:misses")
                except Exception as redis_error:
                    logger.warning(f"Redis cache lookup failed: {redis_error}")
            
            # Get from database
            row = await self.db.fetchrow(
                "SELECT * FROM users WHERE id = $1", user_id
            )
            
            user = User(**dict(row)) if row else None
            
            # Cache the result if Redis is available and user exists
            if user and self.redis:
                try:
                    cache_key = f"user:cache:{user_id}"
                    user_dict = user.model_dump()
                    await self.redis.set_cache(cache_key, user_dict, ttl=300)
                    logger.debug(f"User {user_id} cached")
                except Exception as redis_error:
                    logger.warning(f"Redis caching failed: {redis_error}")
            
            return user
            
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            raise DatabaseError(f"Failed to get user: {str(e)}")
    
    async def list_users(self, skip: int = 0, limit: int = 10) -> List[User]:
        """List users with pagination and optional Redis caching."""
        try:
            # Try cache first if Redis is available
            if self.redis:
                try:
                    cache_key = f"users:list:{skip}:{limit}"
                    cached_users = await self.redis.get_cache(cache_key)
                    if cached_users:
                        logger.debug(f"User list found in cache (skip={skip}, limit={limit})")
                        await self.redis.increment_counter("service:cache:list_hits")
                        return [User(**user_data) for user_data in cached_users]
                    
                    await self.redis.increment_counter("service:cache:list_misses")
                except Exception as redis_error:
                    logger.warning(f"Redis list cache lookup failed: {redis_error}")
            
            # Get from database
            rows = await self.db.fetch(
                "SELECT * FROM users ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                limit, skip
            )
            
            users = [User(**dict(row)) for row in rows]
            
            # Cache the results if Redis is available
            if users and self.redis:
                try:
                    cache_key = f"users:list:{skip}:{limit}"
                    users_dict = [user.model_dump() for user in users]
                    await self.redis.set_cache(cache_key, users_dict, ttl=120)  # 2 minutes
                    logger.debug(f"User list cached (skip={skip}, limit={limit})")
                except Exception as redis_error:
                    logger.warning(f"Redis list caching failed: {redis_error}")
            
            return users
            
        except Exception as e:
            logger.error(f"Failed to list users: {e}")
            raise DatabaseError(f"Failed to list users: {str(e)}")
    
    async def update_user(self, user_id: int, user_data: UserUpdate) -> Optional[User]:
        """Update user with cache invalidation."""
        try:
            # Build update query dynamically
            update_fields = []
            values = []
            param_count = 1
            
            if user_data.email is not None:
                update_fields.append(f"email = ${param_count}")
                values.append(user_data.email)
                param_count += 1
            
            if user_data.name is not None:
                update_fields.append(f"name = ${param_count}")
                values.append(user_data.name)
                param_count += 1
            
            if user_data.is_active is not None:
                update_fields.append(f"is_active = ${param_count}")
                values.append(user_data.is_active)
                param_count += 1
            
            if not update_fields:
                # No fields to update, return current user
                return await self.get_user(user_id)
            
            update_fields.append(f"updated_at = ${param_count}")
            values.append(datetime.utcnow())
            values.append(user_id)
            
            query = f"""
            UPDATE users 
            SET {', '.join(update_fields)}
            WHERE id = ${param_count + 1}
            RETURNING *
            """
            
            row = await self.db.fetchrow(query, *values)
            user = User(**dict(row)) if row else None
            
            # Invalidate cache if Redis is available
            if user and self.redis:
                try:
                    # Remove user from cache
                    cache_key = f"user:cache:{user_id}"
                    await self.redis.delete_cache(cache_key)
                    
                    # Invalidate list caches
                    await self.redis.delete_keys_pattern("users:list:*")
                    
                    # Track update activity
                    activity = {
                        "action": "user_updated",
                        "timestamp": datetime.utcnow().isoformat(),
                        "details": {
                            "service": "UserService",
                            "updated_fields": list(user_data.model_dump(exclude_unset=True).keys())
                        }
                    }
                    await self.redis.add_to_list(f"user:activity:{user_id}", activity)
                    
                    logger.debug(f"Cache invalidated for user {user_id}")
                except Exception as redis_error:
                    logger.warning(f"Redis cache invalidation failed: {redis_error}")
            
            return user
            
        except Exception as e:
            logger.error(f"Failed to update user {user_id}: {e}")
            raise DatabaseError(f"Failed to update user: {str(e)}")
    
    async def delete_user(self, user_id: int) -> bool:
        """Delete user with Redis cleanup."""
        try:
            result = await self.db.execute(
                "DELETE FROM users WHERE id = $1", user_id
            )
            
            success = result == "DELETE 1"
            
            # Clean up Redis if available and deletion was successful
            if success and self.redis:
                try:
                    # Remove all user-related data from Redis
                    await self.redis.delete_cache(f"user:cache:{user_id}")
                    await self.redis.delete_keys_pattern(f"user:*:{user_id}")
                    await self.redis.delete_keys_pattern("users:list:*")
                    
                    # Remove from active users set
                    await self.redis.client.srem("users:active", user_id)
                    
                    # Track deletion activity (before cleanup)
                    activity = {
                        "action": "user_deleted",
                        "timestamp": datetime.utcnow().isoformat(),
                        "details": {"service": "UserService"}
                    }
                    await self.redis.add_to_list(f"user:activity:{user_id}", activity)
                    
                    logger.debug(f"Redis cleanup completed for user {user_id}")
                except Exception as redis_error:
                    logger.warning(f"Redis cleanup failed for user {user_id}: {redis_error}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to delete user {user_id}: {e}")
            raise DatabaseError(f"Failed to delete user: {str(e)}")
    
    async def get_user_statistics(self, user_id: int) -> dict:
        """Get user statistics from Redis if available."""
        if not self.redis:
            return {"error": "Redis not available"}
        
        try:
            stats = {}
            
            # Get user score
            score = await self.redis.get_counter(f"user:score:{user_id}")
            stats["score"] = score
            
            # Get activity count
            activity_count = await self.redis.get_list_length(f"user:activity:{user_id}")
            stats["total_activities"] = activity_count
            
            # Get leaderboard rank
            # This requires getting user data for the leaderboard lookup
            user = await self.get_user(user_id)
            if user:
                rank = await self.redis.get_sorted_set_rank("leaderboard:users", {
                    "user_id": user.id,
                    "name": user.name,
                    "email": user.email
                })
                stats["leaderboard_rank"] = rank
            
            # Check if user is active
            is_active = await self.redis.is_in_set("users:active", user_id)
            stats["is_active_user"] = is_active
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get user statistics for {user_id}: {e}")
            return {"error": str(e)}
    
    async def get_service_statistics(self) -> dict:
        """Get service-level statistics from Redis."""
        if not self.redis:
            return {"error": "Redis not available"}
        
        try:
            stats = {}
            
            # Cache statistics
            cache_hits = await self.redis.get_counter("service:cache:hits")
            cache_misses = await self.redis.get_counter("service:cache:misses")
            list_hits = await self.redis.get_counter("service:cache:list_hits")
            list_misses = await self.redis.get_counter("service:cache:list_misses")
            
            total_requests = cache_hits + cache_misses
            total_list_requests = list_hits + list_misses
            
            stats["cache"] = {
                "single_user_hits": cache_hits,
                "single_user_misses": cache_misses,
                "single_user_hit_rate": (cache_hits / total_requests * 100) if total_requests > 0 else 0,
                "list_hits": list_hits,
                "list_misses": list_misses,
                "list_hit_rate": (list_hits / total_list_requests * 100) if total_list_requests > 0 else 0
            }
            
            # Active users count
            active_users_set = await self.redis.get_set("users:active")
            stats["active_users_count"] = len(active_users_set)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get service statistics: {e}")
            return {"error": str(e)}