# External Scaler

A KEDA external scaler service that provides autoscaling metrics for Lakerunner components in Kubernetes.

## Overview

This package implements the [KEDA External Scaler](https://keda.sh/docs/concepts/external-scalers/) protocol, allowing Kubernetes deployments to automatically scale based on custom metrics from Lakerunner's work queues and processing pipelines.

The external scaler runs as a standalone service that:

- Exposes a gRPC API implementing the KEDA external scaler protocol
- Provides HTTP health check endpoints for Kubernetes probes
- Connects to the Lakerunner database to query work queue depths
- Returns scaling metrics for different service types

## Metrics

### Kafka consumer lag

| Service               | Topic                               | Consumer Group |
| --------------------- | ----------------------------------- | -------------- |
| boxer-compact-logs    | lakerunner.boxer.logs.compact       | lakerunner.boxer.logs.compact |
| boxer-compact-metrics | lakerunner.boxer.metrics.compact    | lakerunner.boxer.metrics.compact |
| boxer-rollup-metrics  | lakerunner.boxer.metrics.rollup.    | lakerunner.boxer.metrics.rollup |
| boxer-compact-traces  | lakerunner.boxer.traces.compact     | lakerunner.boxer.traces.compact |
| compact-logs          | lakerunner.segments.logs.compact    | lakerunner.compact.logs |
| compact-metrics       | lakerunner.segments.metrics.compact | lakerunner.compact.metrics|
| compact-traces        | lakerunner.segments.traces.compact  | lakerunner.compact.traces |
| ingest-logs           | lakerunner.objstore.ingest.logs     | lakerunner.ingest.logs |
| ingest-metrics        | lakerunner.objstore.ingest.metrics  | lakerunner.ingest.metrics |
| ingest-traces         | lakerunner.objstore.ingest.traces   | lakerunner.ingest.traces |
| rollup-metrics        | lakerunner.segments.metrics.rollup  | lakerunner.rollup.metrics |

## Health Checks

HTTP endpoints for Kubernetes health monitoring:

- `/healthz` - General health check
- `/readyz` - Readiness probe
- `/livez` - Liveness probe

All endpoints return 200 for a positive indicator of health, readiness, or liveness.

## Usage in Kubernetes

Configure KEDA ScaledObject resources to use this external scaler:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: lakerunner-ingest-logs
spec:
  scaleTargetRef:
    name: ingest-logs-deployment
  triggers:
  - type: external
    metadata:
      scalerAddress: external-scaler.lakerunner:9090
      serviceType: ingest-logs
```
