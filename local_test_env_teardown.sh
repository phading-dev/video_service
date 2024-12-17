#!/bin/bash

# GCS
fusermount -u gcs_video_test

# Spanner
gcloud spanner instances delete test --quiet
