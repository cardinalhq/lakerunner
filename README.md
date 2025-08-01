<p align="center">
  <img src="./assets/lakerunner-chip.png" alt="LakeRunner Logo" height="120">
</p>

<h1 align="center">LakeRunner</h1>
<h3 align="center">Real-Time Telemetry, Straight From Your S3 Bucket</h3>

<p align="center">
<strong>LakeRunner</strong> is an event-driven ingestion engine that turns your S3-compatible object store into a blazing-fast observability backend. It watches for structured telemetry (CSV, Parquet, JSON.gz), transforms it into optimized Apache Parquet, then handles indexing, aggregation, and compaction, all in real time. Paired with our Grafana plugin, your bucket becomes instantly queryable — no black boxes, no vendor lock-in.
</p>

---

## 🚀 Features

- 📥 Automatic ingestion from S3 on object creation
- 🔗 Event-driven Architecture enabled by S3 Object Notifications. 
- 📊 Native support for OpenTelemetry log/metric proto files
- 📦 Seamless ingestion for `OTEL Proto`, `CSV` or `.json.gz` formats.
- Works with DataDog, FluentBit, or your custom source!
- 🎛️ Helm-deployable for local or cloud environments
- 👀 `docker-compose` support <em> coming soon! </em>

---

## 📚 Table of Contents

- [End User Guide](#end-user-guide)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [S3 Event Setup](#s3-event-setup)
  - [Next Steps - Get the CLI and Grafana Plugin!](#next-steps)
  - [Running the Demo](#running-the-demo)
- [Developer Guide](#developer-guide)
  - [Architecture Overview](#architecture-overview)
  - [Local Development](#local-development)
  - [Contributing](#contributing)
- [License](#license)


## End User Guide

### Installation

As of today, we provide a helm chart to deploy lakerunner - so deploying it should be as simple as defining a `values-local.yaml` file and running:

```
    helm install lakerunner oci://public.ecr.aws/cardinalhq.io/lakerunner \
        --version 0.2.27 \
        --values values-local.yaml \
        --namespace lakerunner
```

We will discuss configuration options in the next section

> **ℹ️ Note:**  
> We have packaged all LakeRunner components into a Helm chart to make it easy to deploy, maintain, and scale individual components.  
>  
> All components are available as public images on AWS ECR. You can browse the [Helm charts](https://github.com/cardinalhq/charts/tree/main/lakerunner) to build your own Docker Compose setup or deploy LakeRunner to any cloud service (like ECS/EKS/GKE/GCE etc...) That said, a `docker-compose` setup is coming soon!  
>  
> Need help? Join the [LakeRunner community on Slack](https://join.slack.com/t/birdhousebycardinalhq/shared_invite/zt-34ds8vt2t-YhGFtoG5NjJX238cMXdLGw) or email us at **support@cardinalhq.io**.  
>  
> 💼 **CardinalHQ also offers managed LakeRunner deployments.**



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

To configure Lakerunner with S3, you need to create a iam user and assign it permissions to read the bucket and it's content, which would look something like the following:

```
{
    "Version": "xxx",
    "Statement": [
        {
            "Sid": "AllowReadWriteAccessToLakerunner",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<ACCOUNT_ID>:user/<LAKERUNNER_USER>"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<BUCKET_NAME>",
                "arn:aws:s3:::<BUCKET_NAME>/*"
            ]
        }
    ]
}
```

You will also need to create an SQS queue that receives notifications from the bucket, and grant both S3 and LakeRunner permissions to send and read messages from it. Here's an example queue policy:

```
{
  "Version": "xxx",
  "Statement": [
    {
      "Sid": "AllowS3ToSendMessages",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:SendMessage",
      "Resource": "arn:aws:sqs:<REGION>:<ACCOUNT_ID>:<QUEUE_NAME>",
      "Condition": {
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:::<BUCKET_NAME>"
        }
      }
    },
    {
      "Sid": "AllowLakerunnerToReadMessages",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<ACCOUNT_ID>:user/<LAKERUNNER_USER>"
      },
      "Action": [
        "SQS:ReceiveMessage",
        "SQS:DeleteMessage",
        "SQS:ChangeMessageVisibility"
      ],
      "Resource": "arn:aws:sqs:<REGION>:<ACCOUNT_ID>:<QUEUE_NAME>"
    }
  ]
}

```

The final step would be to create an event notification from the S3 bucket to the SQS queue, with the prefix `otel-raw`


### Next Steps

Once you have Lakerunner installed, you are ready to explore the data! You can use the grafana plugin that is bundled in the helm chart, and optionally setup the [Lakerunner CLI](https://github.com/cardinalhq/lakerunner-cli) from either the [releases page](https://github.com/cardinalhq/lakerunner-cli/releases), or with brew


```
brew tap cardinalhq/lakerunner-cli
brew install lakerunner-cli
```

You just need to set the `LAKERUNNER_QUERY_URL` and `LAKERUNNER_API_KEY` urls in the env, and you should be able to explore logs from the CLI!


### Running the demo

If you want to install the lakerunner demo, just get the install script and follow the default values. It will install both the OTel Demo Apps and Lakerunner, and write OTel telemetry to a local minio bucket under the `logs-raw/` prefix, from which Lakerunner picks up logs/metrics files. More details can be found on our [Website](https://docs.cardinalhq.io/lakerunner)

```
curl -sSL -o install.sh https://raw.githubusercontent.com/cardinalhq/lakerunner-cli/main/scripts/install.sh
chmod +x install.sh
./install.sh
```


## Developer Guide

