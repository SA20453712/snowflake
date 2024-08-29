#!/bin/bash

# Fetch credentials (this example assumes using AWS CLI for STS AssumeRole)
Credentials=$(aws sts assume-role --role-arn arn:aws:iam::793174870720:role/rbac-dv01aws-dev-admin-role --role-session-name "developer_role_Session" --profile dv01aws)

# Extract and export the credentials
export AWS_ACCESS_KEY_ID=$(echo "$Credentials" | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo "$Credentials" | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo "$Credentials" | jq -r '.Credentials.SessionToken')

# Optionally print to confirm
echo "AWS_ACCESS_KEY_ID set to $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY set to $AWS_SECRET_ACCESS_KEY"
echo "AWS_SESSION_TOKEN set to $AWS_SESSION_TOKEN"
