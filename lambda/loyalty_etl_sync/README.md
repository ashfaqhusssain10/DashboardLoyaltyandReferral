# Loyalty ETL Sync Lambda Deployment Guide

## Overview
This Lambda function runs the daily ETL sync from DynamoDB to S3 (Redshift-ready CSVs).

## Lambda Configuration

| Setting | Value |
|---------|-------|
| **Runtime** | Python 3.11 |
| **Handler** | handler.lambda_handler |
| **Memory** | 1024 MB (recommended) |
| **Timeout** | 15 minutes (900 seconds) |
| **Architecture** | x86_64 |

## Environment Variables

| Variable | Value |
|----------|-------|
| `S3_ETL_BUCKET` | etl-bucket-05-01-2026 |
| `AWS_DEFAULT_REGION` | ap-south-1 |
| `REDSHIFT_IAM_ROLE` | arn:aws:iam::553915279041:role/RedshiftS3AccessRole |

## IAM Role Permissions

The Lambda execution role needs:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:Scan",
                "dynamodb:GetItem"
            ],
            "Resource": [
                "arn:aws:dynamodb:ap-south-1:553915279041:table/UserTable",
                "arn:aws:dynamodb:ap-south-1:553915279041:table/WalletTable",
                "arn:aws:dynamodb:ap-south-1:553915279041:table/WalletTransactionTable",
                "arn:aws:dynamodb:ap-south-1:553915279041:table/TierReferralTable",
                "arn:aws:dynamodb:ap-south-1:553915279041:table/TierDetailsTable",
                "arn:aws:dynamodb:ap-south-1:553915279041:table/LeadTable",
                "arn:aws:dynamodb:ap-south-1:553915279041:table/WithdrawnTable"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::etl-bucket-05-01-2026/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

## EventBridge Scheduler Setup

### Step 1: Create Schedule

1. Go to **Amazon EventBridge** → **Scheduler** → **Schedules**
2. Click **Create schedule**

### Step 2: Configure Schedule

| Field | Value |
|-------|-------|
| **Name** | `loyalty-etl-daily-sync` |
| **Schedule type** | Recurring schedule |
| **Schedule expression** | `cron(30 20 * * ? *)` (2 AM IST = 8:30 PM UTC) |
| **Timezone** | Asia/Kolkata (optional) |

### Step 3: Select Target

| Field | Value |
|-------|-------|
| **Target** | AWS Lambda |
| **Function** | loyalty_etl_sync |
| **Payload** | `{"source": "scheduler", "action": "full_sync"}` |

### Step 4: Configure Retry Policy

| Field | Value |
|-------|-------|
| **Retry attempts** | 2 |
| **Maximum age of event** | 1 hour |

## Deployment Steps

### Option 1: AWS Console (Manual)

1. **Create Lambda Function:**
   - Go to Lambda → Create function
   - Name: `loyalty_etl_sync`
   - Runtime: Python 3.11
   - Upload `handler.py` as a .zip file

2. **Configure:**
   - Set timeout to 15 minutes
   - Set memory to 1024 MB
   - Add environment variables
   - Attach IAM role with permissions above

3. **Create EventBridge Schedule:**
   - Follow steps above

### Option 2: AWS CLI

```bash
# Package the Lambda
cd lambda/loyalty_etl_sync
zip -r function.zip handler.py

# Create Lambda function
aws lambda create-function \
    --function-name loyalty_etl_sync \
    --runtime python3.11 \
    --handler handler.lambda_handler \
    --role arn:aws:iam::553915279041:role/LambdaETLRole \
    --zip-file fileb://function.zip \
    --timeout 900 \
    --memory-size 1024 \
    --environment "Variables={S3_ETL_BUCKET=etl-bucket-05-01-2026,REDSHIFT_IAM_ROLE=arn:aws:iam::553915279041:role/RedshiftS3AccessRole}"

# Create EventBridge schedule
aws scheduler create-schedule \
    --name loyalty-etl-daily-sync \
    --schedule-expression "cron(30 20 * * ? *)" \
    --target '{"Arn":"arn:aws:lambda:ap-south-1:553915279041:function:loyalty_etl_sync","RoleArn":"arn:aws:iam::553915279041:role/EventBridgeSchedulerRole"}' \
    --flexible-time-window '{"Mode":"OFF"}'
```

## Testing

### Test in Console
1. Go to Lambda → Test
2. Use empty event: `{}`
3. Check CloudWatch logs for output

### Monitor
- CloudWatch Logs: `/aws/lambda/loyalty_etl_sync`
- S3: Check for new files in `s3://etl-bucket-05-01-2026/processed/unified/loyalty/`

## After Lambda Runs

The Lambda uploads CSVs to S3 but does NOT load into Redshift directly.

**To complete the load**, run the generated COPY commands:
- Location: `s3://etl-bucket-05-01-2026/metadata/runs/loyalty/year=YYYY/month=MM/day=DD/copy_commands.sql`

**Or automate Redshift load** by:
1. Adding Redshift Data API calls to Lambda (requires additional IAM permissions)
2. Using Step Functions to chain Lambda + Redshift query
