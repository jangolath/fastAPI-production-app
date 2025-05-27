import logging
from datetime import datetime
from typing import List, Optional

import asyncpg

from exceptions import ValidationError, DatabaseError
from models import User, UserCreate, UserUpdate

logger = logging.getLogger(__name__)


class UserService:
    """User business logic service."""
    
    def __init__(self, db: asyncpg.Connection):
        self.db = db
    
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
            
            return User(**dict(row)) if row else None
            
        except ValidationError:
            raise
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            raise DatabaseError(f"Failed to create user: {str(e)}")
    
    async def get_user(self, user_id: int) -> Optional[User]:
        """Get user by ID."""
        try:
            row = await self.db.fetchrow(
                "SELECT * FROM users WHERE id = $1", user_id
            )
            return User(**dict(row)) if row else None
            
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            raise DatabaseError(f"Failed to get user: {str(e)}")
    
    async def list_users(self, skip: int = 0, limit: int = 10) -> List[User]:
        """List users with pagination."""
        try:
            rows = await self.db.fetch(
                "SELECT * FROM users ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                limit, skip
            )
            return [User(**dict(row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Failed to list users: {e}")
            raise DatabaseError(f"Failed to list users: {str(e)}")
    
    async def update_user(self, user_id: int, user_data: UserUpdate) -> Optional[User]:
        """Update user."""
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
            return User(**dict(row)) if row else None
            
        except Exception as e:
            logger.error(f"Failed to update user {user_id}: {e}")
            raise DatabaseError(f"Failed to update user: {str(e)}")
    
    async def delete_user(self, user_id: int) -> bool:
        """Delete user."""
        try:
            result = await self.db.execute(
                "DELETE FROM users WHERE id = $1", user_id
            )
            return result == "DELETE 1"
            
        except Exception as e:
            logger.error(f"Failed to delete user {user_id}: {e}")
            raise DatabaseError(f"Failed to delete user: {str(e)}")