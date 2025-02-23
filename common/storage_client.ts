import { ENV_VARS } from "../env";
import { Storage } from "@google-cloud/storage";

export let STORAGE_CLIENT = new Storage({
  projectId: ENV_VARS.projectId,
});
