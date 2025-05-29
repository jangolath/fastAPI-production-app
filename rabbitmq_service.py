import asyncio
import json
import logging
import aio_pika

from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from aio_pika import ExchangeType, DeliveryMode, Message
from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractQueue, AbstractExchange

logger = logging.getLogger(__name__)


class QueueNames(Enum):
    """Queue name constants."""
    USER_NOTIFICATIONS = "user.notifications"
    DATA_PROCESSING = "data.processing"
    EMAIL_QUEUE = "email.queue"
    REPORT_GENERATION = "report.generation"
    USER_EVENTS = "user.events"
    BATCH_OPERATIONS = "batch.operations"


class ExchangeNames(Enum):
    """Exchange name constants."""
    USER_EVENTS = "user_events"
    NOTIFICATIONS = "notifications"
    PROCESSING = "processing"


class MessagePriority(Enum):
    """Message priority levels."""
    LOW = 1
    NORMAL = 5
    HIGH = 8
    URGENT = 10


class RabbitMQService:
    """RabbitMQ service for async message processing."""
    
    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url
        self._connection: Optional[AbstractConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._exchanges: Dict[str, AbstractExchange] = {}
        self._queues: Dict[str, AbstractQueue] = {}
        self._is_initialized = False
    
    async def initialize(self, max_retries: int = 10, retry_delay: float = 2.0):
        """Initialize RabbitMQ connection with retry logic."""
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{max_retries})")
                
                # Create connection
                self._connection = await aio_pika.connect_robust(
                    self.rabbitmq_url,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                
                # Create channel
                self._channel = await self._connection.channel()
                await self._channel.set_qos(prefetch_count=10)  # Process 10 messages at once
                
                # Setup exchanges and queues
                await self._setup_infrastructure()
                
                self._is_initialized = True
                logger.info("RabbitMQ connection established successfully")
                return
                
            except Exception as e:
                logger.error(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to RabbitMQ.")
                    raise
    
    async def _setup_infrastructure(self):
        """Setup exchanges, queues, and bindings."""
        if not self._channel:
            raise RuntimeError("Channel not initialized")
        
        # Declare exchanges
        self._exchanges[ExchangeNames.USER_EVENTS.value] = await self._channel.declare_exchange(
            ExchangeNames.USER_EVENTS.value,
            ExchangeType.TOPIC,
            durable=True
        )
        
        self._exchanges[ExchangeNames.NOTIFICATIONS.value] = await self._channel.declare_exchange(
            ExchangeNames.NOTIFICATIONS.value,
            ExchangeType.DIRECT,
            durable=True
        )
        
        self._exchanges[ExchangeNames.PROCESSING.value] = await self._channel.declare_exchange(
            ExchangeNames.PROCESSING.value,
            ExchangeType.DIRECT,
            durable=True
        )
        
        # Declare queues with dead letter exchange for failed messages
        dlx_exchange = await self._channel.declare_exchange(
            "dlx", ExchangeType.DIRECT, durable=True
        )
        
        for queue_name in QueueNames:
            # Main queue
            queue = await self._channel.declare_queue(
                queue_name.value,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "dlx",
                    "x-dead-letter-routing-key": f"dlx.{queue_name.value}",
                    "x-max-priority": 10,  # Enable priority queues
                }
            )
            self._queues[queue_name.value] = queue
            
            # Dead letter queue
            dlq = await self._channel.declare_queue(
                f"dlx.{queue_name.value}",
                durable=True
            )
            await dlq.bind(dlx_exchange, f"dlx.{queue_name.value}")
        
        # Bind queues to exchanges
        await self._queues[QueueNames.USER_NOTIFICATIONS.value].bind(
            self._exchanges[ExchangeNames.NOTIFICATIONS.value], "user.notification"
        )
        
        await self._queues[QueueNames.DATA_PROCESSING.value].bind(
            self._exchanges[ExchangeNames.PROCESSING.value], "data.process"
        )
        
        await self._queues[QueueNames.EMAIL_QUEUE.value].bind(
            self._exchanges[ExchangeNames.NOTIFICATIONS.value], "email.send"
        )
        
        await self._queues[QueueNames.USER_EVENTS.value].bind(
            self._exchanges[ExchangeNames.USER_EVENTS.value], "user.*"
        )
        
        await self._queues[QueueNames.REPORT_GENERATION.value].bind(
            self._exchanges[ExchangeNames.PROCESSING.value], "report.generate"
        )
        
        await self._queues[QueueNames.BATCH_OPERATIONS.value].bind(
            self._exchanges[ExchangeNames.PROCESSING.value], "batch.operation"
        )
        
        logger.info("RabbitMQ infrastructure setup completed")
    
    async def close(self):
        """Close RabbitMQ connection."""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("RabbitMQ connection closed")
    
    def _check_initialized(self):
        """Check if service is initialized."""
        if not self._is_initialized:
            raise RuntimeError("RabbitMQ service not initialized. Call initialize() first.")
    
    # ============== MESSAGE PUBLISHING ==============
    
    async def publish_message(
        self,
        exchange_name: str,
        routing_key: str,
        message_body: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL,
        delay_seconds: int = 0,
        correlation_id: Optional[str] = None
    ) -> bool:
        """Publish a message to an exchange."""
        self._check_initialized()
        
        try:
            message_data = {
                "id": correlation_id or f"msg_{datetime.utcnow().timestamp()}",
                "timestamp": datetime.utcnow().isoformat(),
                "body": message_body,
                "routing_key": routing_key,
                "priority": priority.value
            }
            
            message_properties = {
                "delivery_mode": DeliveryMode.PERSISTENT,
                "priority": priority.value,
                "correlation_id": message_data["id"],
                "timestamp": datetime.utcnow(),
                "content_type": "application/json",
                "headers": {
                    "published_at": datetime.utcnow().isoformat(),
                    "publisher": "fastapi-app"
                }
            }
            
            # Add delay if specified (using x-delay)
            if delay_seconds > 0:
                message_properties["headers"]["x-delay"] = delay_seconds * 1000  # milliseconds
            
            message = Message(
                json.dumps(message_data, default=str).encode(),
                **message_properties
            )
            
            exchange = self._exchanges.get(exchange_name)
            if not exchange:
                raise ValueError(f"Exchange {exchange_name} not found")
            
            await exchange.publish(message, routing_key=routing_key)
            
            logger.info(f"Message published to {exchange_name}/{routing_key}: {message_data['id']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message to {exchange_name}/{routing_key}: {e}")
            return False
    
    async def publish_user_event(
        self,
        event_type: str,
        user_id: int,
        event_data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Publish user event."""
        return await self.publish_message(
            ExchangeNames.USER_EVENTS.value,
            f"user.{event_type}",
            {
                "user_id": user_id,
                "event_type": event_type,
                "data": event_data
            },
            priority=priority
        )
    
    async def publish_notification(
        self,
        notification_type: str,
        recipient_id: int,
        content: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Publish notification message."""
        return await self.publish_message(
            ExchangeNames.NOTIFICATIONS.value,
            f"{notification_type}.notification",
            {
                "recipient_id": recipient_id,
                "type": notification_type,
                "content": content
            },
            priority=priority
        )
    
    async def publish_processing_job(
        self,
        job_type: str,
        job_data: Dict[str, Any],
        priority: MessagePriority = MessagePriority.NORMAL,
        delay_seconds: int = 0
    ) -> bool:
        """Publish processing job."""
        return await self.publish_message(
            ExchangeNames.PROCESSING.value,
            f"{job_type}.process",
            {
                "job_type": job_type,
                "data": job_data
            },
            priority=priority,
            delay_seconds=delay_seconds
        )
    
    # ============== MESSAGE CONSUMPTION ==============
    
    async def consume_messages(
        self,
        queue_name: str,
        callback: Callable,
        auto_ack: bool = False,
        prefetch_count: int = 1
    ):
        """Start consuming messages from a queue."""
        self._check_initialized()
        
        try:
            queue = self._queues.get(queue_name)
            if not queue:
                raise ValueError(f"Queue {queue_name} not found")
            
            await self._channel.set_qos(prefetch_count=prefetch_count)
            
            async def message_processor(message):
                async with message.process(ignore_processed=True):
                    try:
                        # Decode message
                        message_data = json.loads(message.body.decode())
                        
                        # Add message metadata
                        message_data["_metadata"] = {
                            "queue": queue_name,
                            "routing_key": message.routing_key,
                            "delivery_tag": message.delivery_tag,
                            "priority": message.priority,
                            "correlation_id": message.correlation_id,
                            "received_at": datetime.utcnow().isoformat()
                        }
                        
                        # Process message
                        result = await callback(message_data)
                        
                        if result is False:
                            # Reject message and send to DLX
                            logger.warning(f"Message processing failed, rejecting: {message.correlation_id}")
                            message.reject(requeue=False)
                        else:
                            # Acknowledge message
                            if not auto_ack:
                                message.ack()
                            logger.info(f"Message processed successfully: {message.correlation_id}")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message JSON: {e}")
                        message.reject(requeue=False)
                    
                    except Exception as e:
                        logger.error(f"Error processing message {message.correlation_id}: {e}")
                        message.reject(requeue=False)
            
            await queue.consume(message_processor, no_ack=auto_ack)
            logger.info(f"Started consuming messages from queue: {queue_name}")
            
        except Exception as e:
            logger.error(f"Failed to start consuming from queue {queue_name}: {e}")
            raise
    
    # ============== QUEUE MANAGEMENT ==============
    
    async def get_queue_info(self, queue_name: str) -> Dict[str, Any]:
        """Get queue information."""
        self._check_initialized()
        
        try:
            queue = self._queues.get(queue_name)
            if not queue:
                return {"error": f"Queue {queue_name} not found"}
            
            # This is a simplified version - in production you'd use RabbitMQ Management API
            return {
                "name": queue_name,
                "durable": queue.durable,
                "status": "active" if self._connection and not self._connection.is_closed else "inactive",
            }
        except Exception as e:
            logger.error(f"Failed to get queue info for {queue_name}: {e}")
            return {"error": str(e)}
    
    async def purge_queue(self, queue_name: str) -> bool:
        """Purge all messages from a queue."""
        self._check_initialized()
        
        try:
            queue = self._queues.get(queue_name)
            if not queue:
                raise ValueError(f"Queue {queue_name} not found")
            
            await queue.purge()
            logger.info(f"Queue purged: {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to purge queue {queue_name}: {e}")
            return False
    
    async def get_all_queues_info(self) -> Dict[str, Any]:
        """Get information about all queues."""
        queues_info = {}
        
        for queue_name in self._queues.keys():
            queues_info[queue_name] = await self.get_queue_info(queue_name)
        
        return {
            "queues": queues_info,
            "connection_status": "connected" if self._connection and not self._connection.is_closed else "disconnected",
            "total_queues": len(self._queues)
        }


# Helper functions for common message types
class MessageHelpers:
    """Helper functions for creating common message types."""
    
    @staticmethod
    def create_user_welcome_message(user_id: int, user_email: str, user_name: str) -> Dict[str, Any]:
        """Create welcome message for new user."""
        return {
            "user_id": user_id,
            "email": user_email,
            "name": user_name,
            "template": "welcome_email",
            "subject": f"Welcome {user_name}!",
            "priority": "normal"
        }
    
    @staticmethod
    def create_user_processing_job(user_id: int, processing_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create user data processing job."""
        return {
            "user_id": user_id,
            "processing_type": processing_type,
            "data": data,
            "created_at": datetime.utcnow().isoformat()
        }
    
    @staticmethod
    def create_batch_operation_job(operation: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create batch operation job."""
        return {
            "operation": operation,
            "items": items,
            "total_items": len(items),
            "created_at": datetime.utcnow().isoformat()
        }
    
    @staticmethod
    def create_report_generation_job(report_type: str, parameters: Dict[str, Any], user_id: int) -> Dict[str, Any]:
        """Create report generation job."""
        return {
            "report_type": report_type,
            "parameters": parameters,
            "requested_by": user_id,
            "created_at": datetime.utcnow().isoformat()
        }