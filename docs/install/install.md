# Installing Lakerunner

This guide shows how you can install Cardinal's Lakerunner to
implement your own private data lake for telemetry.

## Lakerunner

Lakerunner is the data storage that enables CardinalHQ's telemetry
storage and retrieval.  OpenTelemetry collectors write metrics, logs,
and traces to S3 in a Parquet format using CardinalHQ's S3 exporter,
and Lakerunner consumes these into an indexed format suitable for
querying.

## Requirements

* A Kubernetes cluster.  It is recommended to use a 1.30 or newer version.
* An S3 compatible bucket storage system, such as AWS's S3, Minio, Ceph, or others.  It must be capable of providing some sort of object create notification.
* A PostgreSQL v16 or newer database, such as AWS's RDS, or any other PostgreSQL provider.
* An OpenTelemetry collector running CardinalHQ's S3 exporter, and writing at least one signal type to S3.

## Credentials

### PostgreSQL

A single database named `metadata` is used.  This name can be changed in the deployment manifests if desired.
The manifests use a Kubernetes secret defined in the file `postgresql-credentials.yaml` to set these in plain
text, but in production a more secure method should be used.

### Object Storage

Currently, only AWS S3 or compatible APIs are supported.

Depending on which S3 provider you choose, you will need to provide the OpenTelemery collector
and Lakerunner with credentials that allow it to access those buckets, and for AWS S3's bucket
notification, some permissions on an SQS queue.

Notifications from the bucket storage are processed by the pubsub component of Lakerunner.  Currently,
either AWS SQS or a HTTP web hook are supported.

## Manifests

In the `manifests` directory, example files are provided.  Most of these will
not need to change.  It will use the namespace `cardinalhq-datalake` by default,
but it can be changed either by using the `kustomization.yaml` file, or if not
using Kustomize, during the apply step.

## Storage Profiles

In a self-hosted Lakerunner, you need to provide information about how to
access buckets.  These are storage profiles, and they define how to
associate an incoming bucket notification and the files stored there
with a specifc collector.

When the pubsub component receives a bucket notification, an associated
storage profile is found to know what collector created this file.

```yaml
- organization_id: b932c6f0-b968-4ff9-ae8f-365873c552f0
  instance_num: 1
  collector_name: "kubepi"
  cloud_provider: "ceph"
  region: "ceph-objectstore"
  bucket: "datalake-4f7ca22b-9726-4c41-8658-e11bdcbb502b"
  endpoint: "http://rook-ceph-rgw-ceph-objectstore.rook-ceph.svc.cluster.local:80"
  insecure_tls: false
  use_path_style: true
```

In this example, there is one collector defined named `kubepi`, and a single organization.  When writing to S3, it will use the bucket named `datalake-4f7ca22b-9726-4c41-8658-e11bdcbb502b`, and it will write to object IDs such as `otel-raw/b932c6f0-b968-4ff9-ae8f-365873c552f0/kubepi/year=2025/month=5/day=19/hour=9/minute=45/logs_245123123_1231251.parquet`.

When processing these input files, Lakerunner will write to the same bucket, but with a prefix beginning with `db/`

Description of these fields:

* `organization_id`:  This field either comes from the CardinalHQ UI if you are using a hybrid hosted/SaaS UI model, or can be any random UUID if fully self hosted.  It is used to segregate data into permission bins.  One organazation cannot query data for another.  For most self-hosted installations, there will just be one organizaiton ID and all profiles will use the same value.
* `instance_num`:  For each `collector_name`, choose a unique value for this.  It is a number between 1 and 65,535 that indicates the collector partition, and must be unique within an `organization_id`.
* `collector_name`: This is the name of the collector used when writing the S3 files, and is part of the object path.
* `cloud_provider`: This currently is either `gcp` for Google Cloud object storage, or anything else which uses S3 APIs.
* `region`: The region the bucket is in.
* `bucket`: The name of the bucket.
* `endpoint`: When using non-AWS S3, this defines the endpoint to send S3 requests to.  If using AWS S3, leave this blank.  If `region` is set and `use_path_style` is false, this endpoint will be altered to include the region in the request.  Generally, if you are using a non-AWS S3 object storage you will need to set `endpoint` and also set `use_path_style` to `true`.
* `insecure_tls`:  If for some reason your endpoint uses TLS (https) but you want to ignore certificate validation, set this to `true`.
* `use_path_style`: This alters how the `endpoint` is handled.  If using AWS S3, this should be `false` or unset, and `endpoint` should be left empty.  If using any sort of self hosted S3 API, set this to `true` and set `endpoint` to the URL of your service.

For AWS, the profiles are a little more simple:

```yaml
- organization_id: b932c6f0-b968-4ff9-ae8f-365873c552f0
  instance_num: 1
  collector_name: "kubepi"
  cloud_provider: "aws"
  region: "us-east-2"
  bucket: "my-bucket-12345"
```

### Auto-Scaling

Each service that can scale has an associated Horizontal Pod Autoscaler manifest.  These should
be adjusted to support your load.

## Components

Each component runs as its own Kubernetes Deployment or StatefulSet.  There are two Kubernetes service accounts
used:  "lakerunner" and "query-api-sa".  These can be granted permissions using your Kubernetes cloud provider methods
to map cloud native roles to these service accounts, such as EKS's [method](https://docs.aws.amazon.com/eks/latest/userguide/associate-service-account-role.html) to map AWS roles to Kubernetes service accounts.

The "lakerunner" role needs access to any S3 buckets and if using SQS, access to that queue.

The "query-api-sa" role needs access to the S3 buckets.

### PubSub

The pubsub component handles bucket notifications and writes them to an ingest queue in PostgreSQL.  These events
are later processed by an ingest worker depending on the signal type.  SQS and HTTP web hooks are supported.

#### SQS

... write how to configure this

#### HTTP Web Hook

... write how to configure this

### Ingest

Two types of ingest workers run and scale independently.  They take a raw input file and convert it into an indexed
format suitable to run queries against.  The output is a Parquet format file with per-signal type column definitions.
They also write index information into the PostgreSQL database to keep track of the time range, signal type,
and other data about the file's contents.

The index is used by the query engine as well as by rollup and compact workers to know what data is available.

### Metric Rollups and Compaction

Rollups are straight forward.  Data is coming in usually at a 10s granularity for each metric.
Rolling this up to various time ranges (currently 1m, 5m, 20m, and 1h) makes it more efficient to query
for larger time ranges.  These are not currently configurable.

The 10s data is rolled up into the 1m data, 1m into 5m, all the way up.  Each interval is rolled up when it is less likely
that more data will affect the rollup to avoid re-processing data, but if some data does arrive late, it will be
handled.

Compaction occurs mostly at the 10s granularity that collectors submit data.  The main goal of compaction is to
ensure that files are as close to an optional size for query performance.

### Log Compaction

Similarly to the metric compaction, logs are compacted to make querying faster.

### Sweeper

The sweeper is the cleaner of the system.  It will clean up abandoned work as well as deleting S3 files
in a delayed manner.  This delay is necessary because, while the query worker will have a fresh index when
asking PostgreSQL, those files may then be rolled up, and thus may no longer exist when the query worker
attempts to fetch them.  This delay allows for consistent query behavior.

### Query API

The query-api service is the gateway to the data.  It provides an API that allows fetching characteristics
about the data for a given time range, such as the values for some attribute.  It also provides the actual
data for log or metric queries.

The query-api service is a planner, and sends work to the query-workers.

This service requires some Kubernetes RBAC permissions (see the manifests) to allow it to scale
query-workers.

### Query Worker

The query-worker service is one or more pods that does the heavy lifting of query.  It takes S3 object IDs
provided by the query-api service and fetches the data to satisfy the query.  Workers use a cache to
avoid fetching the same S3 file multiple times for repeated queries on the same time ranges.

This service is scaled by query-api configuration.
