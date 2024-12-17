import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainerData } from "../db/schema";
import {
  GET_R2_KEY_DELETE_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  GET_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
  deleteR2KeyDeleteTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  getR2KeyDeleteTasks,
  getVideoContainer,
  getVideoContainerSyncingTasks,
  insertVideoContainerStatement,
  insertVideoContainerSyncingTaskStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import {
  SYNC_VIDEO_CONTAINER,
  SYNC_VIDEO_CONTAINER_REQUEST_BODY,
} from "../product_interface";
import { ProcessVideoContainerSyncingTaskHandler } from "./process_video_container_syncing_task_handler";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import {
  assertReject,
  assertThat,
  eq,
  isArray,
  isUnorderedArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function insertVideoContainer(videoContainerData: VideoContainerData) {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement("container1", videoContainerData),
      ...(videoContainerData.masterPlaylist.syncing
        ? [
            insertVideoContainerSyncingTaskStatement(
              "container1",
              videoContainerData.masterPlaylist.syncing.version,
              0,
              0,
            ),
          ]
        : []),
    ]);
    await transaction.commit();
  });
}

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteVideoContainerSyncingTaskStatement("container1", 1),
      deleteR2KeyDeleteTaskStatement("root/m0.m3u8"),
      deleteR2KeyDeleteTaskStatement("root/m1.m3u8"),
      deleteR2KeyDeleteTaskStatement("root/dir1"),
      deleteR2KeyDeleteTaskStatement("root/dir2"),
    ]);
    await transaction.commit();
  });
}

TEST_RUNNER.run({
  name: "ProcessVideoContainerSyncingTaskHandlerTest",
  cases: [
    {
      name: "MultipleTracksAndFilesAndDirsToDelete",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          seasonId: "season1",
          episodeId: "episode1",
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              version: 1,
              r2Filename: "m.m3u8",
              r2FilenamesToDelete: ["m0.m3u8", "m1.m3u8"],
              r2DirnamesToDelete: ["dir1", "dir2"],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "640x480",
                totalBytes: 1200,
              },
            },
            {
              r2TrackDirname: "video2",
              staging: {
                toAdd: {
                  durationSec: 60,
                  resolution: "640x480",
                  totalBytes: 1200,
                },
              },
            },
          ],
          audioTracks: [
            {
              r2TrackDirname: "audio1",
              committed: {
                name: "Eng",
                isDefault: true,
                totalBytes: 600,
              },
            },
            {
              r2TrackDirname: "audio2",
              staging: {
                toAdd: {
                  name: "Jpn",
                  isDefault: false,
                  totalBytes: 600,
                },
              },
            },
            {
              r2TrackDirname: "audio3",
              committed: {
                name: "Spa",
                isDefault: false,
                totalBytes: 600,
              },
              staging: {
                toDelete: true,
              },
            },
          ],
          subtitleTracks: [
            {
              r2TrackDirname: "subtitle1",
              committed: {
                name: "Eng",
                isDefault: true,
                totalBytes: 300,
              },
            },
            {
              r2TrackDirname: "subtitle2",
              staging: {
                toAdd: {
                  name: "Jpn",
                  isDefault: false,
                  totalBytes: 300,
                },
              },
            },
            {
              r2TrackDirname: "subtitle3",
              committed: {
                name: "Spa",
                isDefault: false,
                totalBytes: 300,
              },
              staging: {
                toDelete: true,
              },
            },
          ],
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 1,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          clientMock.request.descriptor,
          eq(SYNC_VIDEO_CONTAINER),
          "RC",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              showId: "show1",
              containerId: "container1",
              container: {
                version: 1,
                r2RootDirname: "root",
                r2MasterPlaylistFilename: "m.m3u8",
                durationSec: 60,
                resolution: "640x480",
                audioTracks: [
                  { name: "Eng", isDefault: true },
                  { name: "Spa", isDefault: false },
                ],
                subtitleTracks: [
                  { name: "Eng", isDefault: true },
                  { name: "Spa", isDefault: false },
                ],
              },
            },
            SYNC_VIDEO_CONTAINER_REQUEST_BODY,
          ),
          "request body",
        );
        videoContainerData.masterPlaylist = {
          synced: {
            version: 1,
            r2Filename: "m.m3u8",
          },
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "video container syncing tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isUnorderedArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/m0.m3u8",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/m1.m3u8",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/dir1",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/dir2",
                r2KeyDeleteTaskExecutionTimestamp: 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
          ]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "OneVideoTrack",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          seasonId: "season1",
          episodeId: "episode1",
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              version: 1,
              r2Filename: "m.m3u8",
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "640x480",
                totalBytes: 1200,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 1,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              showId: "show1",
              containerId: "container1",
              container: {
                version: 1,
                r2RootDirname: "root",
                r2MasterPlaylistFilename: "m.m3u8",
                durationSec: 60,
                resolution: "640x480",
                audioTracks: [],
                subtitleTracks: [],
              },
            },
            SYNC_VIDEO_CONTAINER_REQUEST_BODY,
          ),
          "request body",
        );
        videoContainerData.masterPlaylist = {
          synced: {
            version: 1,
            r2Filename: "m.m3u8",
          },
        };
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "video container syncing tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotInSyncingState",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          seasonId: "season1",
          episodeId: "episode1",
          r2RootDirname: "root",
          masterPlaylist: {
            synced: {
              version: 1,
              r2Filename: "m.m3u8",
            },
          },
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        let error = await assertReject(
          handler.handle("", {
            containerId: "container1",
            version: 1,
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError("container container1 is not in syncing state"),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "FailedToSync",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          seasonId: "season1",
          episodeId: "episode1",
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              version: 1,
              r2Filename: "m.m3u8",
              r2FilenamesToDelete: ["m0.m3u8", "m1.m3u8"],
              r2DirnamesToDelete: ["dir1", "dir2"],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "640x480",
                totalBytes: 1200,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        clientMock.error = new Error("fake error");
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 1,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 1,
                videoContainerSyncingTaskExecutionTimestamp: 301000,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
            ),
          ]),
          "video container syncing tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledSync_ResumeButVersionChanged",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          seasonId: "season1",
          episodeId: "episode1",
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              version: 1,
              r2Filename: "m.m3u8",
              r2FilenamesToDelete: ["m0.m3u8", "m1.m3u8"],
              r2DirnamesToDelete: ["dir1", "dir2"],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "640x480",
                totalBytes: 1200,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((resolve) => {
          clientMock.send = async (request) => {
            resolve();
            await new Promise<void>(
              (stallResolve) => (stallResolveFn = stallResolve),
            );
          };
        });
        let now = 1000;
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => now,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 1,
        });
        await firstEncounter;

        // Verify
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 1,
                videoContainerSyncingTaskExecutionTimestamp: 301000,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
            ),
          ]),
          "delayed video container syncing tasks",
        );

        // Prepare
        now = 2000;
        videoContainerData.masterPlaylist.syncing.version = 2;
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            updateVideoContainerStatement("container1", videoContainerData),
          ]);
          await transaction.commit();
        });

        // Execute
        stallResolveFn();
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, "container1"),
          isArray([
            eqMessage(
              {
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTasks(SPANNER_DATABASE, 1000000),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 1,
                videoContainerSyncingTaskExecutionTimestamp: 301000,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
            ),
          ]),
          "remained video container syncing tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, 1000000),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});