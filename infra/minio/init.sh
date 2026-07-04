#!/bin/sh
set -e

# Wait for MinIO to be ready
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" 2>/dev/null; do
  echo "Waiting for MinIO..."
  sleep 2
done

# Create bucket
mc mb --ignore-existing local/${MINIO_BUCKET}

# Write engineer policy
cat > /tmp/engineer-policy.json << 'POLICY'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListAllMyBuckets", "s3:CreateBucket"],
      "Resource": ["arn:aws:s3:::*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:HeadBucket"],
      "Resource": ["arn:aws:s3:::citibike", "arn:aws:s3:::citibike/*"]
    }
  ]
}
POLICY

# Write analyst policy
cat > /tmp/analyst-policy.json << 'POLICY'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::citibike/gold/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::citibike"],
      "Condition": {"StringLike": {"s3:prefix": ["gold/*"]}}
    }
  ]
}
POLICY

# Create policies
mc admin policy create local engineer-policy /tmp/engineer-policy.json
mc admin policy create local analyst-policy /tmp/analyst-policy.json

# Create users — using explicit variables
mc admin user add local "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
mc admin user add local "${MINIO_ANALYST_KEY}" "${MINIO_ANALYST_SECRET}"

# Attach policies
mc admin policy attach local engineer-policy --user "${MINIO_ACCESS_KEY}"
mc admin policy attach local analyst-policy --user "${MINIO_ANALYST_KEY}"

echo "MinIO initialisation complete."
mc admin user ls local