import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
import redis.asyncio as redis
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RedisService:
    """Redis service for caching, counters, and temporary data storage."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._client: Optional[Redis] = None
    
    async def initialize(self):
        """Initialize Redis connection."""
        try:
            self._client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            
            # Test connection
            await self._client.ping()
            logger.info("Redis connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def close(self):
        """Close Redis connection."""
        if self._client:
            await self._client.close()
            logger.info("Redis connection closed")
    
    @property
    def client(self) -> Redis:
        """Get Redis client."""
        if not self._client:
            raise RuntimeError("Redis not initialized. Call initialize() first.")
        return self._client
    
    # ============== CACHING METHODS ==============
    
    async def set_cache(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Set cache with TTL (Time To Live) in seconds."""
        try:
            serialized_value = json.dumps(value, default=str)
            result = await self.client.setex(key, ttl, serialized_value)
            return result is True
        except Exception as e:
            logger.error(f"Failed to set cache for key {key}: {e}")
            return False
    
    async def get_cache(self, key: str) -> Optional[Any]:
        """Get cached value."""
        try:
            value = await self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Failed to get cache for key {key}: {e}")
            return None
    
    async def delete_cache(self, key: str) -> bool:
        """Delete cached value."""
        try:
            result = await self.client.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to delete cache for key {key}: {e}")
            return False
    
    async def get_cache_ttl(self, key: str) -> int:
        """Get remaining TTL for a key."""
        try:
            ttl_result = await self.client.ttl(key)
            return int(ttl_result)
        except Exception as e:
            logger.error(f"Failed to get TTL for key {key}: {e}")
            return -1
    
    # ============== COUNTER METHODS ==============
    
    async def increment_counter(self, key: str, amount: int = 1) -> int:
        """Increment counter and return new value."""
        try:
            result = await self.client.incrby(key, amount)
            return int(result)
        except Exception as e:
            logger.error(f"Failed to increment counter {key}: {e}")
            return 0
    
    async def get_counter(self, key: str) -> int:
        """Get counter value."""
        try:
            value = await self.client.get(key)
            return int(value) if value else 0
        except Exception as e:
            logger.error(f"Failed to get counter {key}: {e}")
            return 0
    
    async def reset_counter(self, key: str) -> bool:
        """Reset counter to zero."""
        try:
            result = await self.client.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to reset counter {key}: {e}")
            return False
    
    # ============== LIST METHODS ==============
    
    async def add_to_list(self, key: str, value: Any, max_length: int = 100) -> int:
        """Add item to list (FIFO) with max length."""
        try:
            serialized_value = json.dumps(value, default=str)
            
            # Add to the beginning of the list
            length_result = await self.client.lpush(key, serialized_value)
            
            # Trim list to max length
            await self.client.ltrim(key, 0, max_length - 1)
            
            return int(length_result)
        except Exception as e:
            logger.error(f"Failed to add to list {key}: {e}")
            return 0
    
    async def get_list(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get list items."""
        try:
            items_result = await self.client.lrange(key, start, end)
            if items_result:
                return [json.loads(item) for item in items_result]
            return []
        except Exception as e:
            logger.error(f"Failed to get list {key}: {e}")
            return []
    
    async def get_list_length(self, key: str) -> int:
        """Get list length."""
        try:
            length_result = await self.client.llen(key)
            return int(length_result)
        except Exception as e:
            logger.error(f"Failed to get list length {key}: {e}")
            return 0
    
    # ============== SET METHODS ==============
    
    async def add_to_set(self, key: str, *values: Any) -> int:
        """Add items to set."""
        try:
            serialized_values = [json.dumps(value, default=str) for value in values]
            result = await self.client.sadd(key, *serialized_values)
            return int(result)
        except Exception as e:
            logger.error(f"Failed to add to set {key}: {e}")
            return 0
    
    async def get_set(self, key: str) -> List[Any]:
        """Get all set members."""
        try:
            items_result = await self.client.smembers(key)
            if items_result:
                return [json.loads(item) for item in items_result]
            return []
        except Exception as e:
            logger.error(f"Failed to get set {key}: {e}")
            return []
    
    async def is_in_set(self, key: str, value: Any) -> bool:
        """Check if value is in set."""
        try:
            serialized_value = json.dumps(value, default=str)
            result = await self.client.sismember(key, serialized_value)
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to check set membership {key}: {e}")
            return False
    
    async def remove_from_set(self, key: str, *values: Any) -> int:
        """Remove items from set."""
        try:
            serialized_values = [json.dumps(value, default=str) for value in values]
            result = await self.client.srem(key, *serialized_values)
            return int(result)
        except Exception as e:
            logger.error(f"Failed to remove from set {key}: {e}")
            return 0
    
    # ============== SORTED SET METHODS ==============
    
    async def add_to_sorted_set(self, key: str, score: float, member: Any) -> int:
        """Add member to sorted set with score."""
        try:
            serialized_member = json.dumps(member, default=str)
            result = await self.client.zadd(key, {serialized_member: score})
            return int(result)
        except Exception as e:
            logger.error(f"Failed to add to sorted set {key}: {e}")
            return 0
    
    async def get_sorted_set_top(self, key: str, count: int = 10, reverse: bool = True) -> List[Dict[str, Any]]:
        """Get top members from sorted set."""
        try:
            if reverse:
                # Get highest scores first
                items_result = await self.client.zrevrange(key, 0, count - 1, withscores=True)
            else:
                # Get lowest scores first  
                items_result = await self.client.zrange(key, 0, count - 1, withscores=True)
            
            if not items_result:
                return []
                
            return [
                {
                    "member": json.loads(member),
                    "score": float(score)
                }
                for member, score in items_result
            ]
        except Exception as e:
            logger.error(f"Failed to get sorted set top {key}: {e}")
            return []
    
    async def get_sorted_set_rank(self, key: str, member: Any) -> Optional[int]:
        """Get member rank in sorted set."""
        try:
            serialized_member = json.dumps(member, default=str)
            rank_result = await self.client.zrevrank(key, serialized_member)
            return rank_result + 1 if rank_result is not None else None  # Convert to 1-based ranking
        except Exception as e:
            logger.error(f"Failed to get rank in sorted set {key}: {e}")
            return None
    
    # ============== HASH METHODS ==============
    
    async def set_hash_field(self, key: str, field: str, value: Any) -> bool:
        """Set field in hash."""
        try:
            serialized_value = json.dumps(value, default=str)
            result = await self.client.hset(key, field, serialized_value)
            return result >= 0
        except Exception as e:
            logger.error(f"Failed to set hash field {key}.{field}: {e}")
            return False
    
    async def get_hash_field(self, key: str, field: str) -> Optional[Any]:
        """Get field from hash."""
        try:
            value_result = await self.client.hget(key, field)
            if value_result:
                return json.loads(value_result)
            return None
        except Exception as e:
            logger.error(f"Failed to get hash field {key}.{field}: {e}")
            return None
    
    async def get_hash_all(self, key: str) -> Dict[str, Any]:
        """Get all fields from hash."""
        try:
            hash_result = await self.client.hgetall(key)
            if not hash_result:
                return {}
            return {
                field: json.loads(value)
                for field, value in hash_result.items()
            }
        except Exception as e:
            logger.error(f"Failed to get hash {key}: {e}")
            return {}
    
    # ============== UTILITY METHODS ==============
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        try:
            result = await self.client.exists(key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to check existence of key {key}: {e}")
            return False
    
    async def expire_key(self, key: str, seconds: int) -> bool:
        """Set expiration for key."""
        try:
            result = await self.client.expire(key, seconds)
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to set expiration for key {key}: {e}")
            return False
    
    async def get_keys_pattern(self, pattern: str) -> List[str]:
        """Get keys matching pattern."""
        try:
            keys_result = await self.client.keys(pattern)
            return list(keys_result) if keys_result else []
        except Exception as e:
            logger.error(f"Failed to get keys with pattern {pattern}: {e}")
            return []
    
    async def delete_keys_pattern(self, pattern: str) -> int:
        """Delete keys matching pattern."""
        try:
            keys_result = await self.client.keys(pattern)
            if keys_result:
                delete_result = await self.client.delete(*keys_result)
                return int(delete_result)
            return 0
        except Exception as e:
            logger.error(f"Failed to delete keys with pattern {pattern}: {e}")
            return 0


# Helper functions for common Redis operations
class RedisKeys:
    """Redis key patterns and helpers."""
    
    @staticmethod
    def user_cache(user_id: int) -> str:
        return f"user:cache:{user_id}"
    
    @staticmethod
    def user_activity(user_id: int) -> str:
        return f"user:activity:{user_id}"
    
    @staticmethod
    def api_counter(endpoint: str) -> str:
        return f"api:counter:{endpoint.replace('/', ':')}"
    
    @staticmethod
    def user_preferences(user_id: int) -> str:
        return f"user:preferences:{user_id}"
    
    @staticmethod
    def user_leaderboard() -> str:
        return "leaderboard:users"
    
    @staticmethod
    def active_users() -> str:
        return "users:active"
    
    @staticmethod
    def daily_stats(date: str) -> str:
        return f"stats:daily:{date}"