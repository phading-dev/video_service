import "./env";
import { ENV_VARS } from "../env_vars";
import { spawnSync } from "child_process";

async function main() {
  spawnSync("fusermount", ["-u", ENV_VARS.gcsVideoMountedLocalDir], {
    stdio: "inherit",
  });
  spawnSync(
    "gcloud",
    ["spanner", "instances", "delete", ENV_VARS.spannerInstanceId, "--quiet"],
    {
      stdio: "inherit",
    },
  );
}

main();
