#!/bin/bash

# Env variables
export PROJECT_ID=phading-dev
export INSTANCE_ID=test
export DATABASE_ID=test
export GCS_VIDEO_REMOTE_BUCKET=phading-dev-video-test
export GCS_VIDEO_LOCAL_DIR=gcs_video_test
export SUBTITLE_TEMP_DIR=subtitle_temp
export MEDIA_TEMP_DIR=media_temp
export R2_VIDEO_REMOTE_BUCKET=video-test

# GCP auth
gcloud auth application-default login

# GCS
mkdir -p gcs_video_test
gcsfuse phading-dev-video-test gcs_video_test

# Spanner
gcloud spanner instances create test --config=regional-us-central1 --description="test" --edition=STANDARD --processing-units=100
gcloud spanner databases create test --instance=test
npx spanage update db/ddl -p phading-dev -i test -d test
