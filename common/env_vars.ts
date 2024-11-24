import { getEnvVar } from "@selfage/env_var_getter";

export let PROJECT_ID = getEnvVar("PROJECT_ID").required().asString();
export let INSTANCE_ID = getEnvVar("INSTANCE_ID").required().asString();
export let DATABASE_ID = getEnvVar("DATABASE_ID").required().asString();
export let GCS_VIDEO_REMOTE_BUCKET = getEnvVar("GCS_VIDEO_REMOTE_BUCKET")
  .required()
  .asString();
export let GCS_VIDEO_LOCAL_DIR = getEnvVar("GCS_VIDEO_LOCAL_DIR")
  .required()
  .asString();
export let R2_VIDEO_REMOTE_BUCKET = getEnvVar("R2_VIDEO_REMOTE_BUCKET")
  .required()
  .asString();
export let R2_VIDEO_LOCAL_DIR = getEnvVar("R2_VIDEO_LOCAL_DIR")
  .required()
  .asString();
export let R2_REMOTE_CONFIG_NAME = getEnvVar("R2_CONFIG_NAME")
  .required()
  .asString();
