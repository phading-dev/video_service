import { ENV_VARS } from "./env";
import { spawnSync } from "child_process";
import "./env_local"

async function main() {
  spawnSync("fusermount", ["-u", ENV_VARS.gcsVideoMountedLocalDir], {
    stdio: "inherit",
  });
  spawnSync(
    "gcloud",
    ["spanner", "instances", "delete", ENV_VARS.databaseInstanceId, "--quiet"],
    {
      stdio: "inherit",
    },
  );
}

main();
