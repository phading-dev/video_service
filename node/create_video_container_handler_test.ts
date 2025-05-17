import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  deleteVideoContainerStatement,
  getVideoContainer,
} from "../db/sql";
import { CreateVideoContainerHandler } from "./create_video_container_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "CreateVideoContainerHandlerTest",
  cases: [
    {
      name: "Create",
      execute: async () => {
        // Prepare
        let handler = new CreateVideoContainerHandler(
          SPANNER_DATABASE,
          () => 1000,
        );

        // Execute
        await handler.handle("CreateVideoContainerHandlerTest", {
          seasonId: "season1",
          episodeId: "episode1",
          accountId: "account1",
          videoContainerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerSeasonId: "season1",
                videoContainerEpisodeId: "episode1",
                videoContainerAccountId: "account1",
                videoContainerCreatedTimeMs: 1000,
                videoContainerData: {
                  r2RootDirname: "container1",
                  masterPlaylist: {
                    synced: {
                      version: 0,
                      r2Filename: "0",
                    },
                  },
                  videoTracks: [],
                  audioTracks: [],
                  subtitleTracks: [],
                },
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
