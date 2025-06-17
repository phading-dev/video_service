import "../env_const";
import "@phading/cluster/dev/env";
import { ENV_VARS } from "../env_vars";

ENV_VARS.spannerInstanceId = "test";
ENV_VARS.r2VideoBucketName = "video-test";
ENV_VARS.gcsVideoBucketName = "phading-test-video";
ENV_VARS.gcsVideoMountedLocalDir = "./gcs_video";
