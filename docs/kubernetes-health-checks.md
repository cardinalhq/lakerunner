# Kubernetes Health Checks for Query Worker

The query worker supports both HTTP and gRPC health checks for Kubernetes deployments, following [official Kubernetes probe guidelines](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/).

## Probe Types Overview

- **Startup Probe**: Ensures the application has time to initialize (especially for slow-starting containers)
- **Liveness Probe**: Detects and restarts unresponsive containers
- **Readiness Probe**: Controls when a pod receives traffic from services

## HTTP Health Check

Lightweight HTTP health check endpoint:
- **Endpoint**: `GET /healthz`
- **Port**: 8080 (configurable via `QUERY_WORKER_PORT`)
- **Response**: JSON with service status

```yaml
# Startup probe - gives container time to initialize
startupProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 12  # Allow up to 60s for startup (12 * 5s)

# Liveness probe - restart if unresponsive
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3

# Readiness probe - control traffic routing
readinessProbe:
  httpGet:
    path: /healthz
    port: 8080
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 1
```

## gRPC Health Check

Native gRPC health check support using Kubernetes built-in functionality:

```yaml
# Startup probe
startupProbe:
  grpc:
    port: 9090
  initialDelaySeconds: 10
  periodSeconds: 5
  failureThreshold: 12

# Liveness probe  
livenessProbe:
  grpc:
    port: 9090
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3

# Readiness probe
readinessProbe:
  grpc:
    port: 9090
    service: queryworker.QueryWorker  # Service-specific health check
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 1
```

## Configuration

Environment variables:
- `QUERY_WORKER_PORT=8080` - HTTP health check port  
- `QUERY_WORKER_GRPC_PORT=9090` - gRPC service and health check port

## Health Status Management

The service automatically manages health status following Kubernetes best practices:

- **Startup**: Sets status to `SERVING` when fully initialized
- **Shutdown**: Sets status to `NOT_SERVING` during graceful shutdown
- **Runtime**: Can be updated programmatically via `UpdateHealthStatus()` method

### Service-Specific Health Checks

The gRPC health service supports checking specific services:

- **Overall health**: Default service (empty service name)
- **QueryWorker service**: `queryworker.QueryWorker` - specific to query processing functionality

## Best Practices

1. **Use Startup Probes**: Especially important for query workers that may need time to:
   - Initialize cache connections
   - Load storage profiles
   - Establish AWS/S3 connections

2. **Conservative Liveness Settings**: Query workers should use higher `failureThreshold` to avoid unnecessary restarts during:
   - Heavy query processing
   - Large file downloads from S3
   - Cache warmup operations

3. **Responsive Readiness**: Use shorter intervals for readiness probes to quickly remove unhealthy pods from load balancers

4. **Prefer gRPC**: Native gRPC health checks provide better integration with gRPC-based services

This configuration enables proper Kubernetes pod lifecycle management and zero-downtime deployments for the query worker service.