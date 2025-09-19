# S3 and SQS Manual Setup for Lakerunner

## Overview

Lakerunner requires:
1. **S3 Bucket** - For storing telemetry data (logs, metrics, traces) in raw and processed formats
2. **SQS Queue** - For receiving S3 object creation notifications to trigger ingestion
3. **S3 Event Notifications** - To notify SQS when new data files are uploaded
4. **IAM Permissions** - For Lakerunner services to access S3 and SQS resources

## 1. Create S3 Bucket

```bash
# Set variables
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)
BUCKET_NAME="lakerunner-data-${ACCOUNT_ID}-${REGION}"

# Create bucket
aws s3 mb s3://${BUCKET_NAME} --region ${REGION}

# Enable server-side encryption
aws s3api put-bucket-encryption --bucket ${BUCKET_NAME} --server-side-encryption-configuration '{
  "Rules": [
    {
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }
  ]
}'
```

## 2. Create SQS Queue

```bash
# Set queue name (must match what Lakerunner expects)
QUEUE_NAME="lakerunner-ingest-queue"

# Create SQS queue
aws sqs create-queue \
  --queue-name ${QUEUE_NAME} \
  --attributes MessageRetentionPeriod=345600 \
  --tags Name=${QUEUE_NAME},Purpose=s3-notifications

# Get queue URL and ARN for later use
QUEUE_URL=$(aws sqs get-queue-url --queue-name ${QUEUE_NAME} --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url ${QUEUE_URL} --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

echo "Queue URL: ${QUEUE_URL}"
echo "Queue ARN: ${QUEUE_ARN}"
```

## 3. Configure SQS Queue Policy for S3 Notifications

```bash
# Create queue policy to allow S3 to send messages
cat > queue-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": [
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl",
        "sqs:SendMessage"
      ],
      "Resource": "${QUEUE_ARN}",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "${ACCOUNT_ID}"
        }
      }
    }
  ]
}
EOF



# Apply the policy 


POLICY_DOC=$(jq -c . queue-policy.json)

aws sqs set-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attributes "Policy='$POLICY_DOC'"

```

## 4. Configure S3 Event Notifications

Based on the CloudFormation templates, Lakerunner expects notifications for these prefixes:

```bash
# Create S3 notification configuration
cat > notification-config.json << EOF
{
  "QueueConfigurations": [
    {
      "Id": "OtelRawNotification",
      "QueueArn": "${QUEUE_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "otel-raw/"
            }
          ]
        }
      }
    },
    {
      "Id": "LogsRawNotification",
      "QueueArn": "${QUEUE_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "logs-raw/"
            }
          ]
        }
      }
    },
    {
      "Id": "MetricsRawNotification",
      "QueueArn": "${QUEUE_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {
              "Name": "prefix",
              "Value": "metrics-raw/"
            }
          ]
        }
      }
    }
  ]
}
EOF

# Apply S3 notification configuration
aws s3api put-bucket-notification-configuration \
  --bucket ${BUCKET_NAME} \
  --notification-configuration file://notification-config.json
```

## 5. Create IAM Role for Lakerunner


```bash
# Create trust policy for ECS/EKS services
cat > trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ecs-tasks.amazonaws.com",
          "eks.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name LakerunnerStorageRole \
  --assume-role-policy-document file://trust-policy.json \
  --tags Key=Name,Value=LakerunnerStorageRole

# Create policy for S3 and SQS access based on actual Lakerunner operations
cat > lakerunner-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjects",
        "s3:ListObjectsV2"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage"
      ],
      "Resource": "${QUEUE_ARN}"
    }
  ]
}
EOF

# Create and attach policy
aws iam create-policy \
  --policy-name LakerunnerStoragePolicy \
  --policy-document file://lakerunner-policy.json

aws iam attach-role-policy \
  --role-name LakerunnerStorageRole \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/LakerunnerStoragePolicy
```

### Alternative: Create Access Keys (for non-IAM role scenarios)

If you prefer to use access keys instead of IAM roles:

```bash
# Create IAM user for Lakerunner
aws iam create-user --user-name lakerunner-storage-user

# Attach the policy to the user instead of role
aws iam attach-user-policy \
  --user-name lakerunner-storage-user \
  --policy-arn arn:aws:iam::${ACCOUNT_ID}:policy/LakerunnerStoragePolicy

# Create access keys
aws iam create-access-key --user-name lakerunner-storage-user

# Save the output - you'll need AccessKeyId and SecretAccessKey for Lakerunner configuration
```

**Note**: Access keys provide the same permissions but are less secure than IAM roles. Use IAM roles with EKS IRSA or ECS task roles when possible.

## 6. Expected S3 Directory Structure

Lakerunner will create and use this directory structure:

```
your-bucket/
├── otel-raw/          # Raw OpenTelemetry data (triggers processing)
├── logs-raw/          # Raw log files (triggers processing)
├── metrics-raw/       # Raw metrics files (triggers processing)
└── db/                # Processed data in Parquet format
    └── {org-uuid}/    # Organization ID
        └── {collector}/   # Collector name (e.g., "default", "chq-saas")
            └── {YYYYMMDD}/    # Date partition
                ├── events/    # Log events
                │   └── {HH}/      # Hour partition
                │       └── tbl_{segment_id}.parquet
                ├── metrics/   # Metrics data
                │   └── {HH}/      # Hour partition
                │       └── tbl_{segment_id}.parquet
                └── traces/    # Trace data (if applicable)
                    └── {HH}/      # Hour partition
                        └── tbl_{segment_id}.parquet
```

**Example processed file path:**
`db/123e4567-e89b-12d3-a456-426614174000/default/20240607/metrics/15/tbl_42.parquet`

## 7. Configuration for Lakerunner Deployment

### For Helm Chart values.yaml:

```yaml
# Storage configuration
storageProfiles:
  source: "config"
  create: true
  yaml:
    - organization_id: "your-org-id"
      instance_num: 1
      collector_name: "lakerunner"
      cloud_provider: "aws"
      region: "${REGION}"
      bucket: "${BUCKET_NAME}"

# SQS configuration
pubsub:
  SQS:
    enabled: true
    region: "${REGION}"
    queueURL: "${QUEUE_URL}"
    roleARN: "arn:aws:iam::${ACCOUNT_ID}:role/LakerunnerStorageRole"

# Cloud provider configuration
cloudProvider:
  provider: "aws"
  aws:
    region: "${REGION}"
    # Either use IAM role (recommended) or access keys
    inject: false  # Set to true if using access keys instead of IAM roles
```

### For ECS/EKS with IAM Roles:

If using EKS with IRSA or ECS with task roles, configure:

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: "arn:aws:iam::${ACCOUNT_ID}:role/LakerunnerStorageRole"
```

## 8. Test the Setup

### Test S3 Access:
```bash
# Upload test file to trigger notification
echo '{"timestamp":"2024-01-01T00:00:00Z","message":"test"}' | aws s3 cp - s3://${BUCKET_NAME}/otel-raw/test-$(date +%s).json

# Verify file was uploaded
aws s3 ls s3://${BUCKET_NAME}/otel-raw/
```

### Test SQS Notification:
```bash
# Check for message in SQS (should appear within seconds)
aws sqs receive-message --queue-url ${QUEUE_URL} --max-number-of-messages 1

# If you see a message, the integration is working
```

### Clean up test:
```bash
aws s3 rm s3://${BUCKET_NAME}/otel-raw/test-*.json
```

## 9. Troubleshooting

**S3 notifications not working:**
- Verify queue policy allows S3 service access
- Check notification configuration is applied correctly
- Ensure S3 bucket and SQS queue are in same region

**SQS permission errors:**
- Confirm queue policy has correct account ID
- Verify SQS queue ARN in notification configuration

**Lakerunner can't access S3:**
- Check IAM role has correct S3 permissions
- Verify bucket name in storage profiles configuration
- Ensure AWS region is correct in all configurations

## Summary

This setup provides:
- S3 bucket for storing raw and processed telemetry data
- SQS queue for receiving S3 object creation events
- Event-driven pipeline: S3 upload → SQS notification → Lakerunner processing
- IAM role with minimal required permissions
- Proper security configurations following AWS best practices

The infrastructure is now ready for Lakerunner to start processing telemetry data automatically when files are uploaded to the configured S3 prefixes.
