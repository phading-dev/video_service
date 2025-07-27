import "../env_const";
import "@phading/cluster/prod/env";
import { ENV_VARS } from "../env_vars";

ENV_VARS.spannerInstanceId = ENV_VARS.balancedSpannerInstanceId;
ENV_VARS.gcsVideoBucketName = "phading-prod-video";
ENV_VARS.r2VideoBucketName = "video";
ENV_VARS.gcsVideoMountedLocalDir = "/gcs_video";
ENV_VARS.replicas = 2;
ENV_VARS.cpu = "500m";
ENV_VARS.memory = "512Mi";
ENV_VARS.storage = "1Gi";
ENV_VARS.cpuLimit = "2";
ENV_VARS.memoryLimit = "2Gi";
ENV_VARS.storageLimit = "2Gi";
