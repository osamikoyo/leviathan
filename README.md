# Leviathan

[![Go Report Card](https://goreportcard.com/badge/github.com/osamikoyo/leviathan)](https://goreportcard.com/report/github.com/osamikoyo/leviathan)
[![Go Version](https://img.shields.io/badge/go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Leviathan is a distributed database system built on top of SQLite, designed for high availability and horizontal scaling. It uses a heart-node architecture where the heart service acts as a coordinator and load balancer, while node services handle the actual database operations.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   Client App    │───▶│ Heart Service │───▶│   RabbitMQ      │
│                 │    │ (Coordinator) │    │ (Write Queue)   │
└─────────────────┘    └──────────────┘    └─────────────────┘
                               │                       │
                               ▼                       ▼
                    ┌─────────────────┐    ┌─────────────────┐
                    │   Node 1        │    │   Consumer      │
                    │   (SQLite)      │    │   Services      │
                    └─────────────────┘    └─────────────────┘
                    ┌─────────────────┐
                    │   Node 2        │
                    │   (SQLite)      │
                    └─────────────────┘
                    ┌─────────────────┐
                    │   Node 3        │
                    │   (SQLite)      │
                    └─────────────────┘
```

## ✨ Features

- **High Availability**: Multiple node instances with health checking
- **Load Balancing**: Intelligent request routing based on node load
- **Async Writes**: Write operations are queued through RabbitMQ for consistency
- **Sync Reads**: Read operations are load-balanced across healthy nodes
- **gRPC Communication**: High-performance inter-service communication
- **Prometheus Metrics**: Built-in monitoring and observability
- **Graceful Shutdown**: Proper resource cleanup and connection handling
- **Configuration Management**: YAML-based configuration with validation

## 🚀 Quick Start

### Prerequisites

- Go 1.25+
- [Task](https://taskfile.dev/) (task runner)
- RabbitMQ
- Protocol Buffers compiler (`protoc`)
- Docker (optional, for containerized deployment)

### Installation

```bash
git clone https://github.com/osamikoyo/leviathan.git
cd leviathan
task setup
```

### Configuration

Copy and modify the example configurations:

```bash
cp configs/examples/heart.yaml heart_config.yaml
cp configs/examples/node1.yaml node_config.yaml
```

### Running Services

1. **Start RabbitMQ**:
   ```bash
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
   ```

2. **Start Heart Service**:
   ```bash
   task run-heart
   ```

3. **Start Node Services**:
   ```bash
   task run-node
   ```

## 🛠️ Development

### Available Task Commands

```bash
task                     # Show all available tasks
task build              # Build both services
task test               # Run tests
task test-coverage      # Run tests with coverage
task lint               # Run linter
task fmt                # Format code
task proto              # Generate protobuf code
task clean              # Clean build artifacts
task ci                 # Full CI pipeline
task all                # Complete development pipeline

# Infrastructure tasks
task start-rabbitmq     # Start RabbitMQ container
task stop-rabbitmq      # Stop RabbitMQ container
```

### Project Structure

```
.
├── cmd/                 # Application entry points
│   ├── heart/          # Heart service main
│   └── node/           # Node service main
├── config/             # Configuration handling
├── configs/            # Configuration examples
├── errors/             # Common error definitions
├── health/             # Health checking system
├── heart/              # Heart service implementation
├── heartcore/          # Heart business logic
├── heartserver/        # Heart gRPC server
├── logger/             # Logging utilities
├── metrics/            # Prometheus metrics
├── models/             # Data models
├── producer/           # RabbitMQ producer
├── proto/              # Protocol buffer definitions
├── retrier/            # Retry logic utilities
└── taskfile.yml       # Task runner configuration
```

## 📊 Monitoring

Leviathan exposes Prometheus metrics on the following endpoints:

- `leviathan_requests_total` - Total number of requests
- `leviathan_request_duration_seconds` - Request processing time
- `leviathan_active_connections` - Current active connections
- `leviathan_request_errors_total` - Total number of errors
- `leviathan_node_health_status` - Node health status (1=healthy, 0=unhealthy)

## 🔧 Configuration

### Heart Service Configuration

```yaml
# Heart service configuration
addr: "localhost:8080"                    # Listen address
rabbitmq_url: "amqp://guest:guest@localhost:5672/"
exchange: "leviathan_writes"               # RabbitMQ exchange
queue_name: "write_requests"              # Queue name

# Node addresses for load balancing
nodes:
  - "localhost:9001"
  - "localhost:9002"
  - "localhost:9003"

# Health checking configuration
health_check_interval: "30s"             # Health check frequency
health_check_timeout: "5s"               # Health check timeout

# Request handling configuration
max_retries: 3                           # Maximum retry attempts
request_timeout: "30s"                   # Request timeout
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Standards

- Follow Go conventions and best practices
- Write tests for new functionality
- Run `task ci` before submitting PRs
- Keep functions under 100 lines
- Use meaningful variable and function names

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- SQLite team for the awesome embedded database
- gRPC team for high-performance RPC framework
- RabbitMQ team for reliable messaging
- Prometheus team for monitoring solution
