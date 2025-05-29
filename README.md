# 🚀 FastAPI Production Application

A **production-ready FastAPI application** showcasing modern backend architecture with **PostgreSQL**, **Redis**, **RabbitMQ**, and comprehensive **async message processing**.

## 🏗 **Architecture Overview**

This application demonstrates enterprise-grade patterns including:

- **FastAPI** with async request handling and comprehensive API documentation
- **PostgreSQL** with connection pooling and async database operations  
- **Redis** for caching, session management, and real-time statistics
- **RabbitMQ** for robust message queuing and background processing
- **Background Workers** for async task processing
- **Docker Compose** for complete development environment
- **Rate Limiting** and security best practices
- **Health Checks** and monitoring endpoints

---

## 🛠 **Tech Stack**

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Web Framework** | FastAPI 0.115+ | High-performance async API |
| **Database** | PostgreSQL 17 | Primary data storage |
| **Cache/Session** | Redis 7 | Caching & session management |
| **Message Queue** | RabbitMQ 3.13 | Async message processing |
| **Database Admin** | pgAdmin 4 | Database management UI |
| **Containerization** | Docker Compose | Development environment |
| **Background Workers** | Python asyncio | Message processing |

---

## 🚀 **Quick Start**

### **Prerequisites**
- Docker & Docker Compose
- Git

### **1. Clone & Setup**
```bash
git clone <your-repo-url>
cd fastapi-production-app

# Copy environment template
cp .env.example .env

# Edit .env with your settings (optional for development)
nano .env
```

### **2. Start All Services**
```bash
# Start complete stack with RabbitMQ and workers
docker-compose up -d

# View logs from all services
docker-compose logs -f

# Check service status
docker-compose ps
```

### **3. Verify Everything Works**
```bash
# Health check all services
curl http://localhost:8000/health

# Check RabbitMQ connection
curl http://localhost:8000/health/rabbitmq

# Create demo data to test message queues
curl -X POST http://localhost:8000/demo/rabbitmq
```

---

## 🌐 **Service Access Points**

| Service | URL | Credentials | Description |
|---------|-----|-------------|-------------|
| **FastAPI API** | http://localhost:8000 | - | Main application API |
| **API Documentation** | http://localhost:8000/docs | - | Interactive API docs (Swagger) |
| **Alternative API Docs** | http://localhost:8000/redoc | - | ReDoc documentation |
| **RabbitMQ Management** | http://localhost:15672 | user / password | Message queue monitoring |
| **pgAdmin** | http://localhost:8080 | admin@example.com / admin123 | Database management |
| **PostgreSQL** | localhost:5432 | postgres / mypassword123 | Direct database access |
| **Redis** | localhost:6379 | - | Cache server |

---

## 🐰 **RabbitMQ Message Queue System**

### **Overview**
Comprehensive async message processing system with background workers handling:

- **Notifications & Emails**: User notifications, email delivery, SMS alerts
- **Background Processing**: Data analytics, exports, cleanup jobs
- **Batch Operations**: Bulk database operations and mass updates
- **Report Generation**: Complex report creation without blocking API
- **Event-Driven Actions**: User events triggering cascading processes
- **Dead Letter Queues**: Failed message handling and retry mechanisms

### **Message Queue Architecture**

| Queue Name | Purpose | Workers | Priority Support |
|------------|---------|---------|------------------|
| `user.notifications` | In-app notifications | Notification Worker | ✅ High/Normal/Low |
| `email.queue` | Email delivery | Notification Worker | ✅ Urgent/Normal |
| `data.processing` | Background data jobs | Processing Worker | ✅ All levels |
| `batch.operations` | Bulk operations | Processing Worker | ✅ Normal/Low |
| `report.generation` | Report creation | Processing Worker | ✅ High/Normal |
| `user.events` | User activity events | Processing Worker | ✅ Real-time/Batch |

### **Background Workers**
- **Notification Worker**: Processes notifications and emails with delivery tracking
- **Processing Worker**: Handles data processing, batch operations, and report generation
- **Automatic Scaling**: Add more workers by scaling Docker containers

---

## 📨 **API Endpoints**

### **Core Application**
```bash
# Health checks
GET  /health                    # Overall application health
GET  /health/rabbitmq          # RabbitMQ connection status
GET  /health/redis             # Redis connection status

# User management
POST /users/                   # Create new user
GET  /users/{user_id}          # Get user details
PUT  /users/{user_id}          # Update user
DELETE /users/{user_id}        # Delete user

# Data endpoints
GET  /data/                    # Get cached data
POST /data/                    # Create data entry
```

### **RabbitMQ Message Processing**

#### **Notifications System**
```bash
# Send user notification
POST /notifications/send
{
  "user_id": 1,
  "type": "user",           # user, email, sms
  "title": "Welcome!",
  "message": "Account created successfully",
  "priority": "high"        # low, normal, high, urgent
}

# Get user notifications
GET /users/{user_id}/notifications?limit=20
```

#### **Background Processing Jobs**
```bash
# Queue data processing job
POST /jobs/processing
{
  "job_type": "user_analytics",    # user_analytics, data_export, cleanup
  "user_id": 1,
  "parameters": {"date_range": "last_30_days"},
  "delay_seconds": 0,              # Optional delay
  "priority": "normal"
}

# Queue batch operation
POST /jobs/batch
{
  "operation": "bulk_update",      # bulk_update, bulk_delete, bulk_export
  "items": [
    {"id": 1, "action": "update", "data": {"status": "active"}},
    {"id": 2, "action": "update", "data": {"status": "inactive"}}
  ],
  "priority": "normal"
}

# Generate report
POST /reports/generate
{
  "report_type": "user_activity",   # user_activity, system_stats, custom
  "parameters": {"date_range": "last_week"},
  "format": "pdf",                 # pdf, excel, csv
  "priority": "normal"
}
```

#### **Monitoring & Analytics**
```bash
# Worker processing statistics
GET /workers/stats

# Queue status and message counts
GET /queues/status

# Job progress tracking
GET /jobs/progress/{job_id}

# Message processing history
GET /messages/history?message_type=emails&limit=50
```

---

## 🎯 **Quick Demo & Testing**

### **1. Create Demo Data**
```bash
# Generate comprehensive demo messages
curl -X POST http://localhost:8000/demo/rabbitmq

# This creates:
# - 3 different notification types
# - 3 background processing jobs  
# - 1 batch operation (5 items)
# - 1 report generation job
# - 3 user activity events
```

### **2. Watch Real-Time Processing**
```bash
# Watch workers process messages
docker-compose logs -f worker-notifications worker-processing

# Monitor queue statistics
curl http://localhost:8000/workers/stats

# Check RabbitMQ Management UI
open http://localhost:15672
```

### **3. Test Individual Features**

**Send High-Priority Notification:**
```bash
curl -X POST http://localhost:8000/notifications/send \
-H "Content-Type: application/json" \
-d '{
  "user_id": 1,
  "type": "email",
  "title": "Account Security Alert",
  "message": "New login detected from unknown device",
  "priority": "urgent"
}'
```

**Queue Long-Running Job:**
```bash
curl -X POST http://localhost:8000/jobs/processing \
-H "Content-Type: application/json" \
-d '{
  "job_type": "data_export",
  "parameters": {"format": "excel", "table": "users", "include_analytics": true},
  "priority": "low"
}'
```

**Batch Update Operation:**
```bash
curl -X POST http://localhost:8000/jobs/batch \
-H "Content-Type: application/json" \
-d '{
  "operation": "bulk_update",
  "items": [
    {"id": 1, "field": "status", "value": "premium"},
    {"id": 2, "field": "status", "value": "basic"},
    {"id": 3, "field": "last_login", "value": "2024-01-15"}
  ]
}'
```

---

## 📊 **Monitoring & Management**

### **RabbitMQ Management UI**
Access comprehensive queue monitoring at **http://localhost:15672**:

- **Queue Overview**: Message counts, processing rates, consumer status
- **Exchange Monitoring**: Message routing and binding visualization
- **Connection Management**: Active connections and channel monitoring
- **Performance Metrics**: Throughput, latency, memory usage
- **Message Tracing**: Debug message flow through the system

**Default Credentials**: `user` / `password`

### **Application Monitoring**
```bash
# Overall system health
curl http://localhost:8000/health

# Detailed worker statistics
curl http://localhost:8000/workers/stats

# Queue status and processing rates
curl http://localhost:8000/queues/status

# Redis cache statistics  
curl http://localhost:8000/health/redis
```

### **Database Management**
Access pgAdmin at **http://localhost:8080** with:
- **Email**: admin@example.com
- **Password**: admin123

---

## 🏗 **Development & Production**

### **Environment Configuration**
```bash
# Development (default)
DEBUG=true
LOG_LEVEL=DEBUG

# Production settings
DEBUG=false
LOG_LEVEL=INFO
SECRET_KEY=your-secure-secret-key
DATABASE_URL=postgresql://user:pass@host:5432/db
REDIS_URL=redis://host:6379
RABBITMQ_URL=amqp://user:pass@host:5672/
```

### **Scaling Background Workers**
```bash
# Scale notification workers
docker-compose up -d --scale worker-notifications=3

# Scale processing workers  
docker-compose up -d --scale worker-processing=2

# Monitor worker distribution
docker-compose ps
```

### **Database Migrations**
```bash
# Run database migrations
docker-compose exec app python -c "
from database import DatabaseManager
import asyncio
async def migrate():
    db = DatabaseManager('your-db-url')
    await db.initialize()
    # Add migration logic here
asyncio.run(migrate())
"
```

---

## 🔧 **Production Deployment**

### **Docker Image Build**
```bash
# Build production image
docker build -t your-app:latest .

# Multi-stage build for optimization
docker build --target production -t your-app:prod .
```

### **Environment Variables**
Required for production:
```bash
DATABASE_URL=postgresql://user:password@db-host:5432/database
REDIS_URL=redis://redis-host:6379
RABBITMQ_URL=amqp://user:password@rabbitmq-host:5672/
SECRET_KEY=your-super-secure-secret-key
DEBUG=false
LOG_LEVEL=INFO
```

### **Health Checks**
```bash
# Application health endpoint
curl http://your-domain/health

# Individual service health
curl http://your-domain/health/rabbitmq
curl http://your-domain/health/redis
```

---

## 🚨 **Troubleshooting**

### **Common Issues**

**RabbitMQ Connection Problems:**
```bash
# Check RabbitMQ container
docker-compose ps rabbitmq
docker-compose logs rabbitmq

# Restart RabbitMQ
docker-compose restart rabbitmq

# Verify connection
curl http://localhost:8000/health/rabbitmq
```

**Worker Not Processing Messages:**
```bash
# Check worker status
docker-compose ps worker-notifications worker-processing

# View worker logs
docker-compose logs -f worker-notifications

# Restart workers
docker-compose restart worker-notifications worker-processing
```

**Database Connection Issues:**
```bash
# Check database status
docker-compose ps db
docker-compose logs db

# Test database connection
docker-compose exec app python -c "
from database import DatabaseManager
import asyncio
asyncio.run(DatabaseManager('$DATABASE_URL').test_connection())
"
```

**High Memory Usage:**
```bash
# Monitor container resources
docker stats

# Check RabbitMQ memory usage in Management UI
open http://localhost:15672/#/

# Purge queues if needed (development only)
curl -X POST http://localhost:8000/admin/queues/purge
```

---

## 📚 **Key Features Demonstrated**

### **🔄 Async Processing Patterns**
- **Priority Queues**: Critical messages processed first
- **Dead Letter Queues**: Failed message handling and analysis
- **Message Persistence**: Survive server restarts
- **Retry Logic**: Automatic retry with exponential backoff
- **Progress Tracking**: Real-time job progress monitoring

### **📈 Scalability Features**
- **Horizontal Worker Scaling**: Add more processing capacity
- **Connection Pooling**: Efficient resource utilization
- **Message Batching**: Optimal throughput configuration
- **Load Distribution**: Round-robin message processing

### **🛡 Production-Ready Elements**
- **Health Monitoring**: Comprehensive service health checks
- **Error Handling**: Graceful failure management
- **Logging & Metrics**: Detailed processing statistics
- **Security**: Rate limiting and input validation
- **Documentation**: Complete API documentation

### **🎯 Real-World Use Cases**
- **User Onboarding**: Registration → Welcome email → Profile setup
- **E-commerce**: Order processing → Payment → Shipping notifications
- **Analytics**: Event collection → Processing → Report generation
- **System Maintenance**: Scheduled tasks → Cleanup → Notifications

---

## 🤝 **Contributing**

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

---

## 📄 **License**

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🎉 **Getting Started Summary**

1. **Clone & Start**: `docker-compose up -d`
2. **Create Demo Data**: `curl -X POST http://localhost:8000/demo/rabbitmq`
3. **Watch Processing**: `docker-compose logs -f worker-notifications worker-processing`
4. **Monitor Queues**: Open http://localhost:15672 (user/password)
5. **Test APIs**: Visit http://localhost:8000/docs

**You now have a complete production-ready FastAPI application with async message processing!** 🚀