import { ENV_VARS } from "./env";
import { spawnSync } from "child_process";
import "./env_local";

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
      ENV_VARS.databaseInstanceId,
      `--config=${ENV_VARS.dbRegion}`,
      `--description=${ENV_VARS.databaseInstanceId}`,
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
      ENV_VARS.databaseId,
      `--instance=${ENV_VARS.databaseInstanceId}`,
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
      ENV_VARS.databaseInstanceId,
      "-d",
      ENV_VARS.databaseId,
    ],
    {
      stdio: "inherit",
    },
  );
}

main();
