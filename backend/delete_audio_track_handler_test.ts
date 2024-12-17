import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  GET_VIDEO_CONTAINER_ROW,
  deleteVideoContainerStatement,
  getVideoContainer,
  insertVideoContainerStatement,
} from "../db/sql";
import { DeleteAudioTrackHandler } from "./delete_audio_track_handler";
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
  name: "DeleteAudioTrackHandlerTest",
  cases: [
    {
      name: "Delete",
      execute: async () => {
        // Prepare
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            insertVideoContainerStatement("container1", {
              audioTracks: [
                {
                  r2TrackDirname: "audioTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                },
                {
                  r2TrackDirname: "audioTrack2",
                  committed: {
                    name: "name2",
                    isDefault: false,
                    totalBytes: 100,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteAudioTrackHandler(SPANNER_DATABASE);

        // Execute
        await handler.handle("prefix", {
          containerId: "container1",
          r2TrackDirname: "audioTrack1",
        });

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData: {
                  audioTracks: [
                    {
                      r2TrackDirname: "audioTrack1",
                      committed: {
                        name: "name1",
                        isDefault: true,
                        totalBytes: 100,
                      },
                      staging: {
                        toDelete: true,
                      },
                    },
                    {
                      r2TrackDirname: "audioTrack2",
                      committed: {
                        name: "name2",
                        isDefault: false,
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
              audioTracks: [
                {
                  r2TrackDirname: "audioTrack1",
                  committed: {
                    name: "name1",
                    isDefault: true,
                    totalBytes: 100,
                  },
                },
              ],
            }),
          ]);
          await transaction.commit();
        });
        let handler = new DeleteAudioTrackHandler(SPANNER_DATABASE);

        // Execute
        let error = await assertReject(
          handler.handle("prefix", {
            containerId: "container1",
            r2TrackDirname: "audioTrack2",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(newNotFoundError("audio track audioTrack2 is not found")),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
