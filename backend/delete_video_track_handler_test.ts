import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { DeleteVideoTrackHandler } from "./delete_video_track_handler";
import { newNotFoundError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, isArray } from "@selfage/test_matcher";
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
  name: "DeleteVideoTrackHandlerTest",
  cases: [
    {
      name: "Delete",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              videoTracks: [
                {
                  r2TrackDirname: "videoTrack1",
                  committed: {
                    durationSec: 60,
                    resolution: "1920x1080",
                    totalBytes: 100,
                  },
                },
                {
                  r2TrackDirname: "videoTrack2",
                  committed: {
                    durationSec: 120,
                    resolution: "1280x720",
                    totalBytes: 100,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoTrackHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("prefix", {
          containerId: "container1",
          r2TrackDirname: "videoTrack1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  videoTracks: [
                    {
                      r2TrackDirname: "videoTrack1",
                      committed: {
                        durationSec: 60,
                        resolution: "1920x1080",
                        totalBytes: 100,
                      },
                      staging: {
                        toDelete: true,
                      },
                    },
                    {
                      r2TrackDirname: "videoTrack2",
                      committed: {
                        durationSec: 120,
                        resolution: "1280x720",
                        totalBytes: 100,
                      },
                    },
                  ],
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
      name: "NotFound",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              videoTracks: [
                {
                  r2TrackDirname: "videoTrack1",
                  committed: {
                    durationSec: 60,
                    resolution: "1920x1080",
                    totalBytes: 100,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteVideoTrackHandler(SPANNER_DATABASE);

        // Execute
        let error = await assertReject(
          handler.handle("prefix", {
            containerId: "container1",
            r2TrackDirname: "videoTrack2",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newNotFoundError("video track videoTrack2 is not found"),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
