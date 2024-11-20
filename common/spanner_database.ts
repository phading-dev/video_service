import { DATABASE_ID, INSTANCE_ID, PROJECT_ID } from "./env_vars";
import { Spanner } from "@google-cloud/spanner";

export let SPANNER_DATABASE = new Spanner({
  projectId: PROJECT_ID,
})
  .instance(INSTANCE_ID)
  .database(DATABASE_ID);
