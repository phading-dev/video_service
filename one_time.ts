import { NodeServiceClient } from "@selfage/node_service_client";
import {newProcessMediaFormattingTaskRequest} from '@phading/video_service_interface/node/client';

async function main() {
  await NodeServiceClient.create("http://localhost:8080").send(
    newProcessMediaFormattingTaskRequest({
      containerId: "show54dc2ef9-31bf-4ab2-aad8-3daf48f51dca",
      gcsFilename: "98aa7721-028a-4f83-be5f-ebcd3535c879",
    })
  );
}

main();