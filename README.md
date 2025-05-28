# Production FastAPI Application with Redis Integration

## 🎯 Project Overview

This is a **production-ready FastAPI application** designed to demonstrate modern Python web development capabilities and rapid language acquisition skills. Built as a showcase for **Canopy's technical evaluation**, this project highlights proficiency in contemporary web technologies, async programming, and cloud-native architecture patterns.

### 🚀 What This Demonstrates

**For Canopy's Technical Team**: This project showcases the ability to rapidly adapt and deliver production-quality solutions using modern Python technologies, demonstrating how quickly new languages and frameworks can be mastered while maintaining high development standards.

**Key Capabilities Highlighted**:
- **Modern Async Python**: Leveraging `async/await` for high-performance concurrent operations
- **Production Architecture**: Rate limiting, caching, monitoring, and error handling
- **Database Design**: PostgreSQL with connection pooling and migration handling
- **Redis Integration**: Caching, counters, leaderboards, and real-time features
- **API Design**: RESTful endpoints with comprehensive validation and documentation
- **DevOps Ready**: Complete Docker containerization and environment management
- **Scalable Patterns**: Microservice-ready architecture with proper separation of concerns

---

## 🛠 Prerequisites Installation

### Python 3.11.9 Installation

#### Windows (Recommended Method)
1. **Download Python 3.11.9**:
   - Visit: https://www.python.org/downloads/release/python-3119/
   - Download "Windows installer (64-bit)" or "Windows installer (ARM64)" for Apple Silicon
   - **Important**: Check "Add Python to PATH" during installation

2. **Verify Installation**:
   ```cmd
   python --version
   # Should output: Python 3.11.9
   
   pip --version
   # Should show pip version
   ```

#### macOS
1. **Using Homebrew** (Recommended):
   ```bash
   # Install Homebrew if not already installed
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   
   # Install Python 3.11
   brew install python@3.11
   
   # Create symlink (if needed)
   brew link python@3.11
   ```

2. **Alternative - Direct Download**:
   - Visit: https://www.python.org/downloads/release/python-3119/
   - Download "macOS 64-bit universal2 installer"
   - Run the installer

3. **Verify Installation**:
   ```bash
   python3.11 --version  # or python --version
   pip3.11 --version     # or pip --version
   ```

### Docker Desktop Installation

#### Windows
1. **Download Docker Desktop**:
   - Visit: https://docs.docker.com/desktop/install/windows-install/
   - Download "Docker Desktop for Windows"
   - **Requirements**: Windows 10/11 with WSL 2 enabled

2. **Installation Steps**:
   - Run the installer as Administrator
   - Enable WSL 2 integration when prompted
   - Restart your computer when installation completes

3. **Verify Installation**:
   ```cmd
   docker --version
   docker-compose --version
   ```

#### macOS
1. **Download Docker Desktop**:
   - Visit: https://docs.docker.com/desktop/install/mac-install/
   - Choose appropriate version:
     - **Intel Macs**: "Docker Desktop for Mac with Intel chip"
     - **Apple Silicon (M1/M2)**: "Docker Desktop for Mac with Apple chip"

2. **Installation**:
   - Open the downloaded `.dmg` file
   - Drag Docker to Applications folder
   - Launch Docker Desktop from Applications

3. **Verify Installation**:
   ```bash
   docker --version
   docker-compose --version
   ```

---

## 📦 Installation & Setup

### 1. Clone and Setup Project
```bash
# Clone the repository
git clone <repository-url>
cd fastapi-production-app

# Create Python virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Environment Configuration
```bash
# Copy example environment file
cp .env.example .env

# Edit .env file with your preferred settings
# The default settings work for local development
```

### 3. Start Services with Docker
```bash
# Start all services (PostgreSQL, Redis, PgAdmin, FastAPI)
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

---

## 🚀 Running the Application

### Option 1: Docker Compose (Recommended)
```bash
# Start all services
docker-compose up -d

# The application will be available at:
# - FastAPI: http://localhost:8000
# - API Documentation: http://localhost:8000/docs
# - ReDoc Documentation: http://localhost:8000/redoc
# - PgAdmin: http://localhost:8080 (admin@example.com / admin123)
```

### Option 2: Local Development
```bash
# Ensure virtual environment is activated
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Start only database services
docker-compose up -d db redis

# Run FastAPI development server
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

---

## 🌐 Service Addresses & Access

| Service | Address | Credentials |
|---------|---------|-------------|
| **FastAPI API** | http://localhost:8000 | - |
| **API Documentation** | http://localhost:8000/docs | - |
| **ReDoc Documentation** | http://localhost:8000/redoc | - |
| **PostgreSQL Database** | localhost:5432 | postgres / mypassword123 |
| **Redis Cache** | localhost:6379 | - |
| **PgAdmin Web Interface** | http://localhost:8080 | admin@example.com / admin123 |

### Quick API Testing
```bash
# Health check
curl http://localhost:8000/health

# Create demo data to explore features
curl -X POST http://localhost:8000/redis/demo/data

# View API statistics
curl http://localhost:8000/redis/api/stats

# Check user leaderboard
curl http://localhost:8000/leaderboard
```

---

## 🔧 Key Technologies & Libraries

### Core Framework & Server
- **FastAPI 0.115.12**: Modern, fast web framework for building APIs with Python 3.7+
- **Uvicorn 0.34.2**: Lightning-fast ASGI server implementation
- **Pydantic 2.11.5**: Data validation using Python type annotations

### Database & Caching
- **asyncpg 0.29.0**: Fast PostgreSQL database interface library for Python
- **Redis 5.2.1**: In-memory data structure store for caching and real-time features
- **PostgreSQL 17**: Robust relational database for persistent data storage

### Production Features
- **SlowAPI 0.1.9**: Rate limiting middleware for FastAPI applications
- **CORS Middleware**: Cross-Origin Resource Sharing support
- **Request Logging**: Comprehensive request/response tracking
- **Error Handling**: Centralized exception management

### Development & Deployment
- **Docker & Docker Compose**: Containerization for consistent environments
- **PgAdmin 4**: Web-based PostgreSQL administration tool

---

## 🎨 Architecture Highlights

### Async/Await Programming
```python
# Demonstrates modern Python concurrency
async def create_user(user_data: UserCreate):
    async with db_manager.get_connection() as conn:
        user = await user_service.create_user(user_data)
        await redis_service.cache_user(user)
    return user
```

### Rate Limiting Implementation
```python
# Production-ready API protection
@app.post("/users/")
@limiter.limit("10/minute")  # Prevent API abuse
async def create_user(request: Request, user_data: UserCreate):
    # Rate-limited endpoint logic
```

### Redis Integration Patterns
```python
# Caching, counters, leaderboards, and activity tracking
await redis.set_cache(f"user:{user_id}", user_data, ttl=300)
await redis.increment_counter("api:calls:users")
await redis.add_to_sorted_set("leaderboard", score, user_data)
```

### Database Connection Management
```python
# Production-ready connection pooling
async def initialize_database():
    self._pool = await asyncpg.create_pool(
        database_url, min_size=1, max_size=10, command_timeout=60
    )
```

---

## 📋 Available API Endpoints

### User Management
- `POST /users/` - Create new user (Rate limited: 10/minute)
- `GET /users/{user_id}` - Get user by ID (with Redis caching)
- `GET /users/` - List users with pagination
- `PUT /users/{user_id}` - Update user (with cache invalidation)
- `DELETE /users/{user_id}` - Delete user (with Redis cleanup)
- `POST /users/batch` - Batch user creation (Rate limited: 2/minute)

### Redis Demonstration Features
- `GET /redis/cache/stats` - Cache hit/miss statistics
- `GET /redis/api/stats` - API call analytics
- `GET /users/{user_id}/preferences` - User preferences (Redis hash)
- `PUT /users/{user_id}/preferences` - Update preferences
- `GET /users/{user_id}/activity` - User activity timeline (Redis list)
- `POST /users/{user_id}/score` - Update user score
- `GET /leaderboard` - User leaderboard (Redis sorted set)
- `GET /users/active` - Active users (Redis set)

### Health & Monitoring
- `GET /health` - Basic health check
- `GET /health/db` - Database connectivity check
- `GET /health/redis` - Redis connectivity check

---

## 💡 What This Demonstrates for Canopy

### Technical Versatility & Rapid Language Adoption
This project demonstrates the ability to rapidly deliver production-quality solutions in **modern Python ecosystems**, showcasing how Python represents an exciting opportunity to expand my offerings as a Senior Developer. The speed of implementation highlights the ability to work effectively in new languages and frameworks quickly while maintaining high code quality standards.

### Production-Ready Patterns
- **Rate Limiting**: Protects against API abuse (critical for financial applications)
- **Caching Strategies**: Redis integration for performance optimization
- **Database Design**: Proper connection pooling and async operations
- **Error Handling**: Comprehensive exception management
- **Monitoring**: Built-in health checks and performance metrics

### Modern Development Practices
- **Async Programming**: High-performance concurrent operations
- **API Documentation**: Auto-generated OpenAPI/Swagger documentation
- **Containerization**: Docker-ready for cloud deployment
- **Environment Management**: Production/development configuration separation

### Scalability Considerations
- **Microservice Architecture**: Loosely coupled service design
- **Connection Pooling**: Database performance optimization
- **Caching Layer**: Redis for reduced database load
- **Rate Limiting**: API protection and resource management

---

## 🔧 Development Commands

```bash
# View application logs
docker-compose logs -f app

# Access PostgreSQL directly
docker-compose exec db psql -U postgres -d myapp

# Access Redis CLI
docker-compose exec redis redis-cli

# Restart specific service
docker-compose restart app

# View service status
docker-compose ps

# Clean up everything
docker-compose down -v  # Warning: This removes all data
```

---

## 📈 Performance Features

- **Async Database Operations**: Non-blocking PostgreSQL queries
- **Redis Caching**: Sub-millisecond data retrieval
- **Connection Pooling**: Efficient database resource utilization
- **Rate Limiting**: API protection without performance degradation
- **Background Tasks**: Async job processing for non-blocking operations

---

## 🎯 Next Steps for Production

This application is designed with production deployment in mind:

1. **Environment Variables**: All sensitive data configurable via environment
2. **Health Checks**: Ready for load balancer integration
3. **Logging**: Structured logging for monitoring systems
4. **Security**: Rate limiting, CORS, and input validation
5. **Scalability**: Async architecture supports high concurrency

**For Canopy's Evaluation**: This project demonstrates the ability to rapidly prototype and deliver production-quality solutions in new technology stacks while maintaining code quality and following industry best practices.

---

*Built to showcase modern Python development capabilities and rapid language acquisition skills. Demonstrates adaptability, quick learning, and production-ready development practices across technology stacks.*