# lakerunner

Welcome to CardinalHQ's Lakerunner!

Lakerunner is part of the back-end processing used to convert the telemetry stream
writen by our Open Telemetry Collector components into a format that can be
queried by CardinalHQ's query engine.

Lakerunner uses S3 (or compatible) systems to read and write data.  It also
implements a locking system to ensure that data remiains consistent.

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

## GCP/GKE Permissions

Wuen running in GKE, it will use a service account.  This service account must be granted permissions on the
bucket(s) and on the pubsub subscription.

### Bucket Permissions

Replace `cardinalhq`, `859860501229`, `profound-ship-384422`, `lakerunner`, and `cardinal-lakerunner` as needed.

*Note:* you can probably simplify this by granting access to just the GCP level service account, which is what the pubsub seems to need anyway...

```sh
% gcloud storage buckets add-iam-policy-binding gs://cardinalhq \
    --role=roles/storage.objectUser \
    --member=principal://iam.googleapis.com/projects/859860501229/locations/global/workloadIdentityPools/profound-ship-384422.svc.id.goog/subject/ns/lakerunner/sa/cardinal-lakerunner \
    --condition=None
```

### Subscription Permissions

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
