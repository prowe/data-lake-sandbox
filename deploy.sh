#!/bin/bash

set -e

aws cloudformation package \
    --template-file=cloudformation.template.yml \
    --s3-bucket=prowe-sai-sandbox-dev-deploy \
    --output-template-file=cloudformation.transformed.yml

aws cloudformation deploy \
    --stack-name=prowe-serverless-lake \
    --template-file=cloudformation.transformed.yml \
    --capabilities=CAPABILITY_IAM \
    --parameter-overrides \
        DatabaseName=prowe_customers