#!/bin/sh
# Buckets, queues, and the S3 notifications that drive celld-cdc.
#   celld      celld's fleet bucket: cells/**.ltx and log/**.ltxb ring the doorbell
#   celld-cdc  celld-cdc's bucket: changes/**.parquet feed the materializer
set -eu
REGION=us-east-1
ACCOUNT=000000000000

awslocal s3 mb s3://celld
awslocal s3 mb s3://celld-cdc
for q in cdc-doorbell cdc-changes; do
  awslocal sqs create-queue --queue-name "$q" \
    --attributes VisibilityTimeout=120,MessageRetentionPeriod=1209600 >/dev/null
done

arn() { echo "arn:aws:sqs:$REGION:$ACCOUNT:$1"; }
rule() { # queue prefix suffix
  printf '{"QueueArn":"%s","Events":["s3:ObjectCreated:*"],"Filter":{"Key":{"FilterRules":[{"Name":"prefix","Value":"%s"},{"Name":"suffix","Value":"%s"}]}}}' \
    "$(arn "$1")" "$2" "$3"
}

awslocal s3api put-bucket-notification-configuration --bucket celld \
  --notification-configuration "{\"QueueConfigurations\":[$(rule cdc-doorbell cells/ .ltx),$(rule cdc-doorbell log/ .ltxb)]}"
awslocal s3api put-bucket-notification-configuration --bucket celld-cdc \
  --notification-configuration "{\"QueueConfigurations\":[$(rule cdc-changes changes/ .parquet)]}"

# S3 sends a test event when a notification is configured; drop them.
sleep 1
awslocal sqs purge-queue --queue-url "http://localhost:4566/$ACCOUNT/cdc-doorbell"
awslocal sqs purge-queue --queue-url "http://localhost:4566/$ACCOUNT/cdc-changes"
touch /tmp/celld-cdc-ready
echo "celld-cdc: buckets, queues, and notifications ready"
