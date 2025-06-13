import { CLUSTER_ENV_VARS, ClusterEnvVars } from "@phading/cluster/env_vars";

export interface EnvVars extends ClusterEnvVars {
  spannerInstanceId?: string;
  spannerDatabaseId?: string;
  gcsVideoBucketName?: string;
  gcsVideoMountedLocalDir?: string;
  r2VideoBucketName?: string;
  releaseServiceName?: string;
  port?: number;
  builderAccount?: string;
  serviceAccount?: string;
  replicas?: number;
}

export let ENV_VARS: EnvVars = CLUSTER_ENV_VARS;
