#!/usr/bin/env python3
"""
Background worker for processing RabbitMQ messages.
Usage: python worker.py [worker_type]
Worker types: notifications, processing, all
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, Any

from config import get_settings
from database import DatabaseManager
from redis_service import RedisService
from rabbitmq_service import RabbitMQService, QueueNames
from utils import setup_logging

# Initialize settings and logging
settings = get_settings()
setup_logging()
logger = logging.getLogger(__name__)

# Global instances
db_manager = DatabaseManager(database_url=settings.database_url)
redis_service = RedisService(redis_url=settings.redis_url)
rabbitmq_service = RabbitMQService(rabbitmq_url=getattr(settings, 'rabbitmq_url', 'amqp://user:password@localhost:5672/'))

# Worker shutdown flag
shutdown_event = asyncio.Event()


class MessageProcessor:
    """Message processing handlers."""
    
    def __init__(self, db_manager, redis_service):
        self.db = db_manager
        self.redis = redis_service
    
    async def process_user_notification(self, message_data: Dict[str, Any]) -> bool:
        """Process user notification messages."""
        try:
            logger.info(f"Processing user notification: {message_data.get('id')}")
            
            body = message_data.get('body', {})
            recipient_id = body.get('recipient_id')
            notification_type = body.get('type')
            content = body.get('content', {})
            
            # Simulate email sending delay
            await asyncio.sleep(2)
            
            # Track notification in Redis
            await self.redis.increment_counter(f"notifications:sent:{notification_type}")
            await self.redis.add_to_list(
                f"user:notifications:{recipient_id}",
                {
                    "type": notification_type,
                    "content": content,
                    "sent_at": datetime.utcnow().isoformat(),
                    "message_id": message_data.get('id')
                },
                max_length=100
            )
            
            # Log successful processing
            logger.info(f"Notification sent to user {recipient_id}: {notification_type}")
            
            # Track processing stats
            await self.redis.increment_counter("worker:notifications:processed")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process notification: {e}")
            await self.redis.increment_counter("worker:notifications:failed")
            return False
    
    async def process_email_message(self, message_data: Dict[str, Any]) -> bool:
        """Process email sending messages."""
        try:
            logger.info(f"Processing email message: {message_data.get('id')}")
            
            body = message_data.get('body', {})
            user_id = body.get('user_id')
            email = body.get('email')
            template = body.get('template')
            subject = body.get('subject')
            
            # Simulate email sending
            logger.info(f"Sending email to {email} with template {template}")
            await asyncio.sleep(3)  # Simulate email API call
            
            # Track email in Redis
            await self.redis.increment_counter("emails:sent:total")
            await self.redis.increment_counter(f"emails:sent:{template}")
            
            # Store sent email record
            email_record = {
                "user_id": user_id,
                "email": email,
                "template": template,
                "subject": subject,
                "sent_at": datetime.utcnow().isoformat(),
                "message_id": message_data.get('id')
            }
            
            await self.redis.add_to_list("emails:sent:history", email_record, max_length=1000)
            
            # Update user activity
            if user_id:
                await self.redis.add_to_list(
                    f"user:activity:{user_id}",
                    {
                        "action": "email_sent",
                        "timestamp": datetime.utcnow().isoformat(),
                        "details": {"template": template, "subject": subject}
                    },
                    max_length=50
                )
            
            logger.info(f"Email sent successfully to {email}")
            await self.redis.increment_counter("worker:emails:processed")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process email: {e}")
            await self.redis.increment_counter("worker:emails:failed")
            return False
    
    async def process_data_processing_job(self, message_data: Dict[str, Any]) -> bool:
        """Process data processing jobs."""
        try:
            logger.info(f"Processing data job: {message_data.get('id')}")
            
            body = message_data.get('body', {})
            job_type = body.get('job_type')
            job_data = body.get('data', {})
            
            # Simulate different types of processing
            if job_type == "user_analytics":
                await self._process_user_analytics(job_data)
            elif job_type == "data_export":
                await self._process_data_export(job_data)
            elif job_type == "cleanup":
                await self._process_cleanup_job(job_data)
            else:
                logger.warning(f"Unknown job type: {job_type}")
                return False
            
            # Track job completion
            await self.redis.increment_counter(f"jobs:completed:{job_type}")
            await self.redis.increment_counter("worker:processing:completed")
            
            # Store job result
            job_result = {
                "job_type": job_type,
                "data": job_data,
                "completed_at": datetime.utcnow().isoformat(),
                "message_id": message_data.get('id')
            }
            
            await self.redis.add_to_list("jobs:completed:history", job_result, max_length=1000)
            
            logger.info(f"Data processing job completed: {job_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process data job: {e}")
            await self.redis.increment_counter("worker:processing:failed")
            return False
    
    async def process_batch_operation(self, message_data: Dict[str, Any]) -> bool:
        """Process batch operations."""
        try:
            logger.info(f"Processing batch operation: {message_data.get('id')}")
            
            body = message_data.get('body', {})
            operation = body.get('operation')
            items = body.get('items', [])
            
            # Process items in batches
            batch_size = 10
            processed_count = 0
            failed_count = 0
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                
                # Simulate batch processing
                await asyncio.sleep(1)
                
                for item in batch:
                    try:
                        # Simulate item processing
                        await self._process_batch_item(operation, item)
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Failed to process batch item: {e}")
                        failed_count += 1
                
                # Update progress
                progress = (i + len(batch)) / len(items) * 100
                await self.redis.set_cache(
                    f"batch:progress:{message_data.get('id')}",
                    {
                        "progress": progress,
                        "processed": processed_count,
                        "failed": failed_count,
                        "total": len(items)
                    },
                    ttl=3600
                )
            
            # Track batch completion
            await self.redis.increment_counter("worker:batch:completed")
            
            logger.info(f"Batch operation completed: {operation} - {processed_count}/{len(items)} items processed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process batch operation: {e}")
            await self.redis.increment_counter("worker:batch:failed")
            return False
    
    async def process_report_generation(self, message_data: Dict[str, Any]) -> bool:
        """Process report generation jobs."""
        try:
            logger.info(f"Processing report generation: {message_data.get('id')}")
            
            body = message_data.get('body', {})
            report_type = body.get('report_type')
            parameters = body.get('parameters', {})
            requested_by = body.get('requested_by')
            
            # Simulate report generation
            logger.info(f"Generating {report_type} report with parameters: {parameters}")
            
            # Simulate long-running report generation
            total_steps = 5
            for step in range(1, total_steps + 1):
                await asyncio.sleep(2)  # Simulate work
                
                progress = (step / total_steps) * 100
                await self.redis.set_cache(
                    f"report:progress:{message_data.get('id')}",
                    {
                        "report_type": report_type,
                        "progress": progress,
                        "step": step,
                        "total_steps": total_steps,
                        "current_task": f"Processing step {step} of {total_steps}"
                    },
                    ttl=3600
                )
            
            # Store completed report info
            report_info = {
                "report_type": report_type,
                "parameters": parameters,
                "requested_by": requested_by,
                "generated_at": datetime.utcnow().isoformat(),
                "file_path": f"/reports/{report_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pdf",
                "message_id": message_data.get('id')
            }
            
            await self.redis.add_to_list("reports:generated", report_info, max_length=1000)
            await self.redis.increment_counter("worker:reports:generated")
            
            logger.info(f"Report generated successfully: {report_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            await self.redis.increment_counter("worker:reports:failed")
            return False
    
    async def process_user_event(self, message_data: Dict[str, Any]) -> bool:
        """Process user events."""
        try:
            logger.info(f"Processing user event: {message_data.get('id')}")
            
            body = message_data.get('body', {})
            user_id = body.get('user_id')
            event_type = body.get('event_type')
            event_data = body.get('data', {})
            
            # Track event in Redis
            await self.redis.increment_counter(f"events:processed:{event_type}")
            
            # Store event for analytics
            event_record = {
                "user_id": user_id,
                "event_type": event_type,
                "data": event_data,
                "processed_at": datetime.utcnow().isoformat(),
                "message_id": message_data.get('id')
            }
            
            await self.redis.add_to_list("events:processed:history", event_record, max_length=10000)
            
            # Update user-specific event tracking
            await self.redis.add_to_list(
                f"user:events:{user_id}",
                event_record,
                max_length=100
            )
            
            # Trigger additional actions based on event type
            if event_type == "user_created":
                # Send welcome email
                await rabbitmq_service.publish_message(
                    "notifications",
                    "email.send",
                    {
                        "user_id": user_id,
                        "email": event_data.get("email"),
                        "name": event_data.get("name"),
                        "template": "welcome_email",
                        "subject": f"Welcome {event_data.get('name')}!"
                    }
                )
            
            await self.redis.increment_counter("worker:events:processed")
            
            logger.info(f"User event processed: {event_type} for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process user event: {e}")
            await self.redis.increment_counter("worker:events:failed")
            return False
    
    # Helper methods for specific processing tasks
    async def _process_user_analytics(self, data: Dict[str, Any]):
        """Process user analytics job."""
        user_id = data.get('user_id')
        await asyncio.sleep(3)  # Simulate analytics processing
        logger.info(f"User analytics processed for user {user_id}")
    
    async def _process_data_export(self, data: Dict[str, Any]):
        """Process data export job."""
        export_type = data.get('export_type', 'csv')
        await asyncio.sleep(5)  # Simulate export generation
        logger.info(f"Data export completed: {export_type}")
    
    async def _process_cleanup_job(self, data: Dict[str, Any]):
        """Process cleanup job."""
        cleanup_type = data.get('cleanup_type')
        await asyncio.sleep(2)  # Simulate cleanup
        logger.info(f"Cleanup completed: {cleanup_type}")
    
    async def _process_batch_item(self, operation: str, item: Dict[str, Any]):
        """Process individual batch item."""
        await asyncio.sleep(0.1)  # Simulate item processing
        logger.debug(f"Processed batch item: {operation} - {item}")


async def setup_notification_worker():
    """Setup notification worker."""
    processor = MessageProcessor(db_manager, redis_service)
    
    # Consume user notifications
    await rabbitmq_service.consume_messages(
        QueueNames.USER_NOTIFICATIONS.value,
        processor.process_user_notification,
        prefetch_count=5
    )
    
    # Consume email messages
    await rabbitmq_service.consume_messages(
        QueueNames.EMAIL_QUEUE.value,
        processor.process_email_message,
        prefetch_count=3
    )
    
    logger.info("Notification worker started")


async def setup_processing_worker():
    """Setup processing worker."""
    processor = MessageProcessor(db_manager, redis_service)
    
    # Consume data processing jobs
    await rabbitmq_service.consume_messages(
        QueueNames.DATA_PROCESSING.value,
        processor.process_data_processing_job,
        prefetch_count=2
    )
    
    # Consume batch operations
    await rabbitmq_service.consume_messages(
        QueueNames.BATCH_OPERATIONS.value,
        processor.process_batch_operation,
        prefetch_count=1
    )
    
    # Consume report generation jobs
    await rabbitmq_service.consume_messages(
        QueueNames.REPORT_GENERATION.value,
        processor.process_report_generation,
        prefetch_count=1
    )
    
    # Consume user events
    await rabbitmq_service.consume_messages(
        QueueNames.USER_EVENTS.value,
        processor.process_user_event,
        prefetch_count=10
    )
    
    logger.info("Processing worker started")


async def setup_all_workers():
    """Setup all workers."""
    await setup_notification_worker()
    await setup_processing_worker()
    logger.info("All workers started")


async def initialize_services():
    """Initialize all services."""
    await db_manager.initialize()
    await redis_service.initialize()
    await rabbitmq_service.initialize()
    logger.info("All services initialized")


async def cleanup_services():
    """Cleanup all services."""
    await rabbitmq_service.close()
    await redis_service.close()
    await db_manager.close()
    logger.info("All services closed")


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, shutting down...")
    shutdown_event.set()


async def main():
    """Main worker function."""
    worker_type = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info(f"Starting worker: {worker_type}")
        
        # Initialize services
        await initialize_services()
        
        # Start appropriate workers
        if worker_type == "notifications":
            await setup_notification_worker()
        elif worker_type == "processing":
            await setup_processing_worker()
        elif worker_type == "all":
            await setup_all_workers()
        else:
            logger.error(f"Unknown worker type: {worker_type}")
            return
        
        # Wait for shutdown signal
        await shutdown_event.wait()
        
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        raise
    finally:
        await cleanup_services()


if __name__ == "__main__":
    asyncio.run(main())