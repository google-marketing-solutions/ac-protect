#!/bin/bash

# Create the image path
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NAME="ac-protect"
IMAGE_PATH="gcr.io/$PROJECT_ID/$PROJECT_NAME"

# Fetch the latest digest
DIGEST_SERVICE=$(gcloud container images describe ${IMAGE_PATH}:latest --format='get(image_summary.digest)')
DIGEST_JOB=$(gcloud container images describe ${IMAGE_PATH}_job:latest --format='get(image_summary.digest)')

# Output the digest in JSON format
echo "{\"service\":\"${DIGEST_SERVICE}\", \"job\":\"${DIGEST_JOB}\"}"
