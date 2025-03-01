import "./env";
import { ENV_VARS } from "../env_vars";
import { spawnSync } from "child_process";

async function main() {
  spawnSync("gcloud", ["auth", "application-default", "login"], {
    stdio: "inherit",
  });
  spawnSync("gcloud", ["config", "set", "project", ENV_VARS.projectId], {
    stdio: "inherit",
  });
  spawnSync("mkdir", ["-p", ENV_VARS.gcsVideoMountedLocalDir], {
    stdio: "inherit",
  });
  spawnSync("gcsfuse", [
    ENV_VARS.gcsVideoBucketName,
    ENV_VARS.gcsVideoMountedLocalDir,
  ]);
  spawnSync(
    "gcloud",
    [
      "spanner",
      "instances",
      "create",
      ENV_VARS.spannerInstanceId,
      `--config=${ENV_VARS.spannerRegion}`,
      `--description=${ENV_VARS.spannerInstanceId}`,
      "--edition=STANDARD",
      "--processing-units=100",
    ],
    {
      stdio: "inherit",
    },
  );
  spawnSync(
    "gcloud",
    [
      "spanner",
      "databases",
      "create",
      ENV_VARS.spannerDatabaseId,
      `--instance=${ENV_VARS.spannerInstanceId}`,
    ],
    {
      stdio: "inherit",
    },
  );
  spawnSync(
    "npx",
    [
      "spanage",
      "update",
      "db/ddl",
      "-p",
      ENV_VARS.projectId,
      "-i",
      ENV_VARS.spannerInstanceId,
      "-d",
      ENV_VARS.spannerDatabaseId,
    ],
    {
      stdio: "inherit",
    },
  );
}

main();
