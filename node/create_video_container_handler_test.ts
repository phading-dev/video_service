import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { CreateVideoContainerHandler } from "./create_video_container_handler";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
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
        let handler = new CreateVideoContainerHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("CreateVideoContainerHandlerTest", {
          seasonId: "season1",
          episodeId: "episode1",
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  seasonId: "season1",
                  episodeId: "episode1",
                  r2RootDirname: "showcontainer1",
                  masterPlaylist: {
                    synced: {
                      version: 0,
                      r2Filename: "0",
                    },
                  },
                  lastProcessingFailures: [],
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
    {
      name: "NoopIfExists",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              seasonId: "season1",
              episodeId: "episode1",
              r2RootDirname: "showcontainer1",
            }),
          ]);
          await transaction.commit();
        });
        let handler = new CreateVideoContainerHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("CreateVideoContainerHandlerTest", {
          seasonId: "season1",
          episodeId: "episode1",
          containerId: "container1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  seasonId: "season1",
                  episodeId: "episode1",
                  r2RootDirname: "showcontainer1",
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
