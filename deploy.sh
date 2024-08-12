#!/bin/bash

COLOR='\033[0;36m' # Cyan
NC='\033[0m' # No Color

# Variables
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NAME=$(gcloud projects describe $PROJECT_ID --format="value(name)")
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID | grep projectNumber | sed "s/.* '//;s/'//g")
SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com
PROJECT_NAME="ac-protect"
TAG="latest" # e.g., "latest"
LOCATION_ID="us-central"

create_default_bucket() {
    echo -e "${COLOR}Creating default bucket $PROJECT_ID...${NC}"
    gsutil mb gs://$PROJECT_ID
}

deploy_config() {
    echo -e "${COLOR}Deploying Configs...${NC}"
    gsutil cp ./config.yaml gs://$PROJECT_ID/$PROJECT_NAME/config.yaml
}

create_image() {
    echo -e "${COLOR}Creating Image...${NC}"
    gcloud services enable cloudbuild.googleapis.com
    gcloud builds submit --config cloudbuild.yaml . --substitutions=_PROJECT_NAME=$PROJECT_NAME
    # gcloud builds submit  --tag gcr.io/$PROJECT_ID/$PROJECT_NAME
}

run_tf() {
    echo -e "${COLOR}Creating Infra...${NC}"
    gcloud services enable googleads.googleapis.com
    gcloud services enable analyticsdata.googleapis.com
    gcloud services enable gmail.googleapis.com
    gcloud services enable run.googleapis.com
    gcloud services enable bigquery.googleapis.com
    gcloud services enable storage.googleapis.com
    gcloud services enable cloudscheduler.googleapis.com
    gcloud services enable iam.googleapis.com
    gcloud services enable cloudresourcemanager.googleapis.com
    gcloud services enable bigquerydatatransfer.googleapis.com

    terraform init -backend-config="bucket=$PROJECT_ID"
    terraform apply -var "project_id=$PROJECT_ID" -var "project_number=$PROJECT_NUMBER" -var "location_id=$LOCATION_ID" -auto-approve
    echo -e "${COLOR}Infra Created!${NC}"

    FRONTEND_SITE=$(terraform output -raw service_url)
    echo -e "${COLOR}Go to - ${FRONTEND_SITE}${NC}"
}

plan_tf() {
    terraform init -backend-config="bucket=$PROJECT_ID"
    terraform plan -var "project_id=$PROJECT_ID" -var "project_number=$PROJECT_NUMBER" -var "location_id=$LOCATION_ID"
}

refresh_tf() {
    terraform refresh -var "project_id=$PROJECT_ID" -var "project_number=$PROJECT_NUMBER" -var "location_id=$LOCATION_ID"
}

destroy_tf() {
    terraform apply -destroy -var "project_id=$PROJECT_ID" -var "project_number=$PROJECT_NUMBER" -var "location_id=$LOCATION_ID"
}

cleanup_builds() {
    gsutil ls -l gs://artifacts.${PROJECT_ID}.appspot.com/containers/images | sort -k2r | tail -n +6 | awk '{print $3}' | gsutil -m rm -I
    gsutil ls -l gs://${PROJECT_ID}_cloudbuild/source | sort -k2r | tail -n +6 | awk '{print $3}' | gsutil -m rm -I
}

deploy_image() {
    create_image
    run_tf
}

deploy_all() {
    create_default_bucket
    deploy_config
    create_image
    run_tf
    cleanup_builds
}


for i in "$@"; do
    "$i"
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
        echo "Breaking script as command '$i' failed"
        exit $exitcode
    fi
done
