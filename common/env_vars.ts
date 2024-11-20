import { getEnvVar } from "@selfage/env_var_getter";

export let PROJECT_ID = getEnvVar("PROJECT_ID").required().asString();
export let INSTANCE_ID = getEnvVar("INSTANCE_ID").required().asString();
export let DATABASE_ID = getEnvVar("DATABASE_ID").required().asString();
export let GCS_VIDEO_BUCKET_NAME = getEnvVar("GCS_VIDEO_BUCKET_NAME").required().asString();
export let R2_VIDEO_BUCKET_NAME = getEnvVar("R2_VIDEO_BUCKET_NAME")
  .required()
  .asString();
