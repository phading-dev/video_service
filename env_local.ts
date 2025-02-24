import { ENV_VARS } from "./env";
import "./env_const";
import "@phading/cluster/env_dev";

ENV_VARS.spannerInstanceId = "test";
ENV_VARS.gcsVideoBucketName = "phading-dev-video";
ENV_VARS.r2VideoBucketName = "video-dev";
ENV_VARS.gcsVideoMountedLocalDir = "./gcs_video";
