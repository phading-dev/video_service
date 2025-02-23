import { CLUSTER_ENV_VARS, ClusterEnvVars } from "@phading/cluster/env";

export interface EnvVars extends ClusterEnvVars {
  databaseInstanceId?: string;
  databaseId?: string;
  gcsVideoBucketName?: string;
  gcsVideoMountedLocalDir?: string;
  r2VideoBucketName?: string;
  releaseServiceName?: string;
  port?: number;
  builderAccount?: string;
  serviceAccount?: string;
}

export let ENV_VARS: EnvVars = CLUSTER_ENV_VARS;
