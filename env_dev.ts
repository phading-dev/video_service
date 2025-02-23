import { ENV_VARS } from "./env";
import "./env_const";
import "@phading/cluster/env_dev";

ENV_VARS.databaseInstanceId = ENV_VARS.balancedDbInstanceId;
ENV_VARS.gcsVideoBucketName = "phading-dev-video";
ENV_VARS.r2VideoBucketName = "video-dev";
