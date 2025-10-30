#! /bin/bash
# MAKE SURE GCP PROJECT IS SET
# gcloud config set project PROJECT_ID

# set the project
gcloud config set project $(gcloud projects list --format='value(PROJECT_ID)' | grep playground)

if [[ -z "${GOOGLE_CLOUD_PROJECT}" ]]; then
    echo "Project has not been set! Please run:"
    echo "   gcloud config set project PROJECT_ID"
    echo "(where PROJECT_ID is the desired project)"
else
    echo "Project Name: $GOOGLE_CLOUD_PROJECT"
    gcloud storage buckets create gs://$GOOGLE_CLOUD_PROJECT"-bucket" --soft-delete-duration=0
    # gcloud services disable dataflow.googleapis.com --force
    gcloud services enable dataflow.googleapis.com
    gcloud services enable pubsub.googleapis.com

<<COMMENT
    gcloud iam service-accounts create marssa
    sleep 1
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member serviceAccount:marssa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com --role roles/editor
    sleep 1
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member serviceAccount:marssa@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com --role roles/dataflow.worker
    sleep 1
    gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT --member user:$USER_EMAIL --role roles/iam.serviceAccountUser

    
    gcloud pubsub topics publish activities-topic --message='20251022000002412476,145.196.32.195,BALANCE,GB19GGRY21807231042124,NULL,0,Jeffrey Ramos'

COMMENT
    echo "Creating the mars dataset"
    bq mk mars

    # bq mk --schema timestamp:STRING,ipaddr:STRING,action:STRING,srcacct:STRING,destacct:STRING,amount:NUMERIC,customername:STRING -t mars.activities
    # bq mk --schema message:STRING -t mars.raw
    echo "Creating pubsub topic and subscription"
    gcloud pubsub topics create activities-topic
    gcloud pubsub subscriptions create activities-subscription --topic activities-topic
fi