import { ENV_VARS } from "../env_vars";
import { Spanner } from "@google-cloud/spanner";

export let SPANNER_DATABASE = new Spanner({
  projectId: ENV_VARS.projectId,
})
  .instance(ENV_VARS.spannerInstanceId)
  .database(ENV_VARS.spannerDatabaseId);
