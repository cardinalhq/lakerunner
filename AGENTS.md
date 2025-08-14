# Agent and Worker Discovery System

This document describes the worker discovery system for distributed query processing in lakerunner.

## Overview

Lakerunner supports distributed query processing across multiple execution environments. Workers are discovered dynamically through environment-specific discovery mechanisms.

## Supported Execution Environments

### Kubernetes

The primary execution environment with full EndpointSlice-based discovery.

#### Required Environment Variables

**All three environment variables are mandatory:**

- `POD_NAMESPACE`: Kubernetes namespace containing worker pods
- `WORKER_POD_LABEL_SELECTOR`: Label selector to identify worker pods
- `QUERY_WORKER_PORT`: Port number for worker connections

#### Example Configuration

```bash
POD_NAMESPACE=cardinalhq-datalake
WORKER_POD_LABEL_SELECTOR=app.kubernetes.io/instance=lakerunner,app.kubernetes.io/component=query-worker
QUERY_WORKER_PORT=7101
```

#### Discovery Mechanism

1. **Local Development**: Uses `~/.kube/config` (kubeconfig)
2. **Production**: Uses in-cluster Kubernetes configuration
3. **Endpoint Discovery**: Queries EndpointSlices matching the label selector
4. **Health Filtering**: Only includes ready endpoints
5. **Network Filtering**: Only includes IPv4 addresses

### ECS (Future)

ECS support is planned but not yet implemented.

## Worker Discovery API

### Interface

```go
type WorkerDiscovery interface {
    GetWorkers(ctx context.Context) ([]Worker, error)
}

type Worker struct {
    IP   string
    Port int
}
```

### Kubernetes Implementation

```go
config := promql.KubernetesWorkerDiscoveryConfig{
    Namespace:           "my-namespace",
    WorkerLabelSelector: "app=worker,component=query",
    WorkerPort:          7101,
}

discovery, err := promql.NewKubernetesWorkerDiscovery(config)
if err != nil {
    return err
}

workers, err := discovery.GetWorkers(ctx)
```

## Debug Commands

### Discovery Testing

Use the debug command to test worker discovery:

```bash
./bin/lakerunner debug kubernetes-discovery \
  --pod-namespace="cardinalhq-datalake" \
  --worker-pod-label-selector="app.kubernetes.io/instance=lakerunner,app.kubernetes.io/component=query-worker" \
  --query-worker-port=7101
```

#### Expected Output

```sh
Debugging Kubernetes worker discovery...
POD_NAMESPACE: cardinalhq-datalake
WORKER_POD_LABEL_SELECTOR: app.kubernetes.io/instance=lakerunner,app.kubernetes.io/component=query-worker
QUERY_WORKER_PORT: 7101

Discovering workers...
Found 1 workers:
  [1] 10.244.5.158:7101
```

#### Troubleshooting

If no workers are found, check:

1. **Label Selector**: Ensure it matches your worker pods exactly
2. **Namespace**: Verify worker pods exist in the specified namespace
3. **Pod Readiness**: Worker pods must be in Ready state
4. **EndpointSlices**: Service EndpointSlices must exist for the selector
5. **Network**: Only IPv4 addresses are supported

### Common Issues

#### Missing Environment Variables

The system fails fast if any required environment variables are missing:

```bash
# Missing QUERY_WORKER_PORT
POD_NAMESPACE=default WORKER_POD_LABEL_SELECTOR=app=worker ./bin/lakerunner command
# Returns empty worker list
```

#### Invalid Port Configuration

```bash
# Invalid port number
QUERY_WORKER_PORT=invalid ./bin/lakerunner command
# Returns empty worker list
```

#### Kubernetes Access

```bash
# No kubeconfig and not in cluster
Error: failed to create kubernetes config (tried kubeconfig and in-cluster)
```

## Implementation Details

### Configuration Priority

1. **Local Development**: `~/.kube/config` is tried first
2. **Production Fallback**: In-cluster config is used if kubeconfig fails
3. **Error Handling**: Clear errors when neither method works

### Discovery Process

1. Parse label selector into Kubernetes labels.Selector
2. Query EndpointSlices in target namespace with label selector
3. Filter endpoints by Ready condition
4. Extract IPv4 addresses only
5. Return Worker structs with IP and configured port

### Environment Variable Validation

All environment variables are validated at runtime:

- `POD_NAMESPACE`: Must be non-empty string
- `WORKER_POD_LABEL_SELECTOR`: Must be valid Kubernetes label selector
- `QUERY_WORKER_PORT`: Must be valid integer

Missing or invalid values cause the discovery to return empty results rather than error, allowing graceful degradation.
