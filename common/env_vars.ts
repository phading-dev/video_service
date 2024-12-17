import { getEnvVar } from "@selfage/env_var_getter";

export let PROJECT_ID = getEnvVar("PROJECT_ID").required().asString();
export let INSTANCE_ID = getEnvVar("INSTANCE_ID").required().asString();
export let DATABASE_ID = getEnvVar("DATABASE_ID").required().asString();
export let CLOUDFLARE_ACCOUNT_ID = getEnvVar("CLOUDFLARE_ACCOUNT_ID")
  .required()
  .asString();
export let CLOUDFLARE_R2_ACCESS_KEY_ID = getEnvVar(
  "CLOUDFLARE_R2_ACCESS_KEY_ID",
)
  .required()
  .asString();
export let CLOUDFLARE_R2_SECRET_ACCESS_KEY = getEnvVar(
  "CLOUDFLARE_R2_SECRET_ACCESS_KEY",
)
  .required()
  .asString();
export let GCS_VIDEO_REMOTE_BUCKET = getEnvVar("GCS_VIDEO_REMOTE_BUCKET")
  .required()
  .asString();
export let GCS_VIDEO_LOCAL_DIR = getEnvVar("GCS_VIDEO_LOCAL_DIR")
  .required()
  .asString();
export let SUBTITLE_TEMP_DIR = getEnvVar("SUBTITLE_TEMP_DIR")
  .required()
  .asString();
export let MEDIA_TEMP_DIR = getEnvVar("MEDIA_TEMP_DIR").required().asString();
export let R2_VIDEO_REMOTE_BUCKET = getEnvVar("R2_VIDEO_REMOTE_BUCKET")
  .required()
  .asString();
