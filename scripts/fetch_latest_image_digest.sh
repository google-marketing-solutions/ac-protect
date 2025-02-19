#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Create the image path
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NAME="ac-protect"
IMAGE_PATH="gcr.io/$PROJECT_ID/$PROJECT_NAME"

# Fetch the latest digest
DIGEST_SERVICE=$(gcloud container images describe ${IMAGE_PATH}:latest --format='get(image_summary.digest)')
DIGEST_JOB=$(gcloud container images describe ${IMAGE_PATH}_job:latest --format='get(image_summary.digest)')

# Output the digest in JSON format
echo "{\"service\":\"${DIGEST_SERVICE}\", \"job\":\"${DIGEST_JOB}\"}"
