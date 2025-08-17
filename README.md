<p align="center">
  <img src="./assets/lakerunner-chip.png" alt="LakeRunner Logo" height="120">
</p>

<h1 align="center">LakeRunner</h1>
<h3 align="center">Real-Time Telemetry, Straight From Your S3 Bucket</h3>

<p align="center">
<strong>LakeRunner</strong> is an event-driven ingestion engine that turns your S3-compatible object store into a blazing-fast observability backend. It watches for structured telemetry (CSV, Parquet, JSON.gz), transforms it into optimized Apache Parquet, then handles indexing, aggregation, and compaction, all in real time. Paired with our Grafana plugin, your bucket becomes instantly queryable — no black boxes, no vendor lock-in.
</p>

---

## Features

- Automatic ingestion from S3 on object creation
- Event-driven Architecture enabled by S3 Object Notifications. 
- Native support for OpenTelemetry log/metric proto files
- Seamless ingestion for `OTEL Proto`, `CSV` or `.json.gz` formats.
- Works with DataDog, FluentBit, or your custom source!
- Get started locally in <5 mins with the install script
- `docker-compose` support <em> coming soon! </em>

---

## Table of Contents

- [End User Guide](#end-user-guide)
  - [Demo Setup](#demo)
  - [S3 Event Setup](#s3-event-setup)
  - [Next Steps - Get the CLI and Grafana Plugin!](#next-steps)
  - [Configuration](#configuration)
- [License](#license)


## End User Guide

### Demo setup

#### Prerequisites

1. [Docker Desktop](https://docs.docker.com/desktop/), [Rancher Desktop](https://docs.rancherdesktop.io/getting-started/installation/), or equivalent
2. [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), [minikube](https://minikube.sigs.k8s.io/docs/start/), or equivalent (`brew install kind` or `brew install minikube`)
3. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) (`brew install kubectl`)
4. [Helm](https://helm.sh/docs/intro/install/) 3.14+ (`brew install helm`)

#### Install LakeRunner

1. Create a local cluster using `kind` or `minikube`

```bash copy
kind create cluster
```

2. Ensure that your `kubectl` context is set to the local cluster.

```bash copy
kubectl config use-context kind-kind
```

3. Download and run the LakeRunner install script.

```bash copy
curl -sSL -o install.sh https://raw.githubusercontent.com/cardinalhq/lakerunner-cli/main/scripts/install.sh
chmod +x install.sh
./install.sh
```

4. Follow the on-screen prompts until the installation is complete.

_(We recommend using the default values for all prompts during a local install for the fastest and most seamless experience.)_

##### Explore Data in Grafana

The LakeRunner install script also installs a local [Grafana](https://grafana.com/oss/grafana/), bundled with a preconfigured Cardinal LakeRunner Datasource plugin.

Wait for ~5 minutes for the OpenTelemetry Demo apps to generate some sample telemetry data. Then, access Grafana at `http://localhost:3000` and login with the default credentials:

- Username: `admin`
- Password: `admin`

Navigate to the `Explore` tab, select the `Cardinal` datasource, and try running some queries to explore logs and metrics.

1. [Docker Desktop](https://docs.docker.com/desktop/), [Rancher Desktop](https://docs.rancherdesktop.io/getting-started/installation/), or equivalent
2. [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation), [minikube](https://minikube.sigs.k8s.io/docs/start/), or equivalent (`brew install kind` or `brew install minikube`)
3. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) (`brew install kubectl`)
4. [Helm](https://helm.sh/docs/intro/install/) 3.14+ (`brew install helm`)


We will discuss configuration options in the next section

> **Note:**  
> Need help? Join the [LakeRunner community on Slack](https://join.slack.com/t/birdhousebycardinalhq/shared_invite/zt-34ds8vt2t-YhGFtoG5NjJX238cMXdLGw) or email us at **support@cardinalhq.io**.  
>  
> **CardinalHQ offers managed LakeRunner deployments.**


### Next Steps

Once you have Lakerunner installed, you are ready to explore the data! You can use the grafana plugin that is bundled in the helm chart, and optionally setup the [Lakerunner CLI](https://github.com/cardinalhq/lakerunner-cli) from either the [releases page](https://github.com/cardinalhq/lakerunner-cli/releases), or with brew


```
brew tap cardinalhq/lakerunner-cli
brew install lakerunner-cli
```

You just need to set the `LAKERUNNER_QUERY_URL` and `LAKERUNNER_API_KEY` urls in the env, and you should be able to explore logs from the CLI!

For a local demo installation with default values, you can run:

```bash copy
export LAKERUNNER_QUERY_URL="http://localhost:7101"
export LAKERUNNER_API_KEY="test-key"
```


### Configuration

#### values-local.yaml

The default [values.yaml](https://github.com/cardinalhq/charts/blob/main/lakerunner/values.yaml) file has a bunch of configuration options you can tweak. Below is a breakdown of mandatory and optional configuration fields.

> Note: The settings in the [values.yaml](https://github.com/cardinalhq/charts/blob/main/lakerunner/values.yaml) file are suitable for a small to medium   installation, but for a very small or larger installation, you will need to make  adjustments.

##### ✅ Required fields

These fields are required for a basic LakeRunner deployment:

| Section            | Key(s)                              | Description                                           |
|--------------------|-----------------------------------|-------------------------------------------------------|
| `aws`              | `accessKey`, `secretKey`           | Your S3 or compatible credentials, inline or via secret |
| `postgres`         | `host`, `user`, `database`, `password` | PostgreSQL config (password via `values` or secret)    |
| `storageProfiles.yaml` | `organization_id`, `bucket`, etc. | Maps S3 notifications to collector + org             |
| `apiKeys.yaml`      | `organization_id`, `keys`           | Authorizes querying for specific organizations         |
| `token`       | secretName                                   | Token for authenticating query api to worker pods  |
| `pubsub.receiver`   | `sqs` or `http` for webhook notifications                  | Choose based on your cloud provider                    |

---


##### ⚙️ Optional fields

These settings are not required but allow customization of performance, scaling, and integrations. Refer the [values.yaml](https://github.com/cardinalhq/charts/blob/main/lakerunner/values.yaml) file for detailed description of fields

| Field                       | Description                                                             |
|-----------------------------|-------------------------------------------------------------------------|
| `.replicas`                 | Number of replicas to run for each component - ingest, compact, rollup and query api services                       |
| `queryApi.minWorkers` & `queryApi.maxWorkers` | Query API scales workers based on load - this defines limits for it |
| `resources`                 | Tune per service: CPU/memory requests and limits per component.                          |
| `sqs.enabled`               | Enable/disable SQS-based event ingestion (enabled by default for AWS). |
| `global.tolerations, affinity, nodeSelector`           |    Configure pod scheduling                    |
| `serviceAccount.create`     | Whether to create a new Kubernetes service account.                    |
| `global.env.ENABLE_OTLP_TELEMETRY`         | Export lakerunner telemetry to an otlp endpoint                               |

---

#### Storage Profiles

Storage Profiles tell LakeRunner how to handle files from your bucket — who they belong to and which collector wrote them. If you are indexing raw files, you do not care about the org id and collector name, but if you want Lakerunner to pick up files that are being actively written by a collector, the collectors need to write parquet files to the bucket in the following format:

```
 otel-raw/<org-id>/<collector_name>/year=<year>/month=<month>/hour=<hour>/minute=<minute>/
 ```

You need **at least one**, even if you're only using a single bucket and a single setup.
Storage profiles are defined in YAML, via the values file:

```
storageProfiles:
  yaml: []
    - organization_id: dddddddd-aaaa-4ff9-ae8f-365873c552f0
      instance_num: 1
      collector_name: "kubepi"
      cloud_provider: "aws"
      region: "us-east-2"
      bucket: "datalake-11ndajkhk"
      use_path_style: true
```

The `organization_id` is arbitrary, but must be a UUID. `instance_num` should be unique within the same organization, and will need to be different per collector name. `collector_name` is used as part of the path prefix, and needs to be set to the name of the CardinalHQ collector. `use_path_style` should be true.

If you are using a non-AWS S3 provider, there is also an endpoint option to list the URL for your S3 endpoint.


#### API Keys 

API Keys are used to provide access to the data stored in your data lake. These are used by the `query-api` and `query-worker` to authenticate and authorize access to the organization's data stored in the lake.

These are defined in YAML, via the values file:

```
apiKeys:
  secretName: "apikeys" # will have the format <release-name>-apikeys once deployed
  create: true
  yaml: []
    - organization_id: dddddddd-aaaa-4ff9-ae8f-365873c552f0
      keys:
        - my-api-key-1
        - my-api-key-2
```


The `organization_id` should match ones used in the storage profile.

Multiple keys can be used. If the Lakerunner query API is not used, these can be left blank.


#### Additional notes on cloud provider settings

For all S3 compatible systems, several environment variables must be set.  Items marked with a `*`
must be provided if they cannot be obtained in other AWS-standard ways.

| name | default | purpose |
|-|-|-|
| *`AWS_ACCESS_KEY` | | The AWS Access key for the service account used to access the bucket. |
| *`AWS_SECRET_KEY` | | The AWS Secret key associated with the service account used to access the bucket. |
| `AWS_ENDPOINT` | aws-default | The S3 endpoint to use. |
| `AWS_REGION` | `auto` | The AWS region to use, if needed. |
| *`S3_BUCKET` | | The bucket name used. |
| `S3_PROVIDER` | `aws` | The S3 compatible provider. This must be set to `gcp` if running on Google Cloud Storage. |

##### GCP/GKE Permissions

Wuen running in GKE, it will use a service account.  This service account must be granted permissions on the
bucket(s) and on the pubsub subscription.

##### Bucket Permissions

Replace `cardinalhq`, `859860501229`, `profound-ship-384422`, `lakerunner`, and `cardinal-lakerunner` as needed.

*Note:* you can probably simplify this by granting access to just the GCP level service account, which is what the pubsub seems to need anyway...

```sh
% gcloud storage buckets add-iam-policy-binding gs://cardinalhq \
    --role=roles/storage.objectUser \
    --member=principal://iam.googleapis.com/projects/859860501229/locations/global/workloadIdentityPools/profound-ship-384422.svc.id.goog/subject/ns/lakerunner/sa/cardinal-lakerunner \
    --condition=None
```

##### Subscription Permissions

First, grant the GKE service account impersonation on the GCP SA:

```sh
% gcloud iam service-accounts add-iam-policy-binding lakerunner@profound-ship-384422.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:profound-ship-384422.svc.id.goog[lakerunner/cardinal-lakerunner]"
```

Next, grant that Google Service Account access to the subscription:

```sh
% gcloud pubsub subscriptions add-iam-policy-binding projects/profound-ship-384422/subscriptions/mgraff-test \
    --role=roles/pubsub.subscriber \
    --member=serviceAccount:lakerunner@profound-ship-384422.iam.gserviceaccount.com
```

### S3 Event Setup 

To configure Lakerunner with S3 and SQS, you need the following:

1. A S3 bucket
2. SQS queue
3. An IAM user

For the permissions part, you need to configure the following:
1. The user should have permission to access the S3 bucket and SQS queue
2. The S3 bucket should allow the user to access it's contents, and should send event notifications to the queue
3. The SQS queue should allow the bucket to send events to it, and allow the user to access the queue.

For reference, the settings below should be a good reference to set these things up:

1. Create a policy to allow access to the S3 bucket and SQS queue. Attach this to a group, and add the IAM user you create to this group

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowS3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket_name>",
                "arn:aws:s3:::<bucket_name>/*"
            ]
        },
        {
            "Sid": "AllowSQSAccess",
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:ChangeMessageVisibility"
            ],
            "Resource": "<sqs_arn>"
        }
    ]
}
```

2. S3 bucket configuration

You need to create a bucket and add event notifications for the following prefixes to the SQS queue: `otel-raw`, `logs-raw`, and `metrics-raw`. The policy for the bucket should allow the IAM user to access it's contents

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowReadWriteAccessToLakerunner",
            "Effect": "Allow",
            "Principal": {
                "AWS": "<IAM user ARN>"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket_name>",
                "arn:aws:s3:::<bucket_name>/*"
            ]
        }
    ]
}
```

3. SQS configuration

The SQS queue that you create should two extra statements - one to let S3 send messages and the other to let the user access the queue:

```
    {
      "Sid": "AllowS3ToSendMessages",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:SendMessage",
      "Resource": "<queue ARN>",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:::<bucket_name>"
        }
      }
    },
    {
      "Sid": "AllowLakerunnerToReadMessages",
      "Effect": "Allow",
      "Principal": {
        "AWS": "<IAM user ARN>"
      },
      "Action": [
        "SQS:ReceiveMessage",
        "SQS:DeleteMessage",
        "SQS:ChangeMessageVisibility"
      ],
      "Resource": "<queue ARN>"
    }
```



## LICENSE

This project is licensed under the [GNU Affero General Public License v3.0](./LICENSE).