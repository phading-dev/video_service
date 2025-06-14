import "../env_const";
import "@phading/cluster/dev/env";
import { ENV_VARS } from "../env_vars";

ENV_VARS.spannerInstanceId = ENV_VARS.balancedSpannerInstanceId;
ENV_VARS.gcsVideoBucketName = "phading-dev-video";
ENV_VARS.r2VideoBucketName = "video-dev";
ENV_VARS.gcsVideoMountedLocalDir = "/gcs_video";
ENV_VARS.replicas = 1;
ENV_VARS.cpu = "500m";
ENV_VARS.memory = "512Mi";
