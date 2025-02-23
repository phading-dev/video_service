import { ENV_VARS } from "../env";
import { Spanner } from "@google-cloud/spanner";

export let SPANNER_DATABASE = new Spanner({
  projectId: ENV_VARS.projectId,
})
  .instance(ENV_VARS.databaseInstanceId)
  .database(ENV_VARS.databaseId);
