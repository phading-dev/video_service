import "../local/env";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_STORAGE_END_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  GET_VIDEO_CONTAINER_SYNCING_TASK_METADATA_ROW,
  deleteR2KeyDeletingTaskStatement,
  deleteStorageEndRecordingTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  getR2KeyDeletingTask,
  getStorageEndRecordingTask,
  getVideoContainer,
  getVideoContainerSyncingTaskMetadata,
  insertVideoContainerStatement,
  insertVideoContainerSyncingTaskStatement,
  listPendingR2KeyDeletingTasks,
  listPendingStorageEndRecordingTasks,
  listPendingVideoContainerSyncingTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ProcessVideoContainerSyncingTaskHandler } from "./process_video_container_syncing_task_handler";
import {
  CACHE_VIDEO_CONTAINER,
  CACHE_VIDEO_CONTAINER_REQUEST_BODY,
} from "@phading/product_service_interface/show/node/interface";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function insertVideoContainer(videoContainerData: VideoContainer) {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        seasonId: "season1",
        episodeId: "episode1",
        accountId: "account1",
        data: videoContainerData,
      }),
      ...(videoContainerData.masterPlaylist.syncing
        ? [
            insertVideoContainerSyncingTaskStatement({
              containerId: "container1",
              version: videoContainerData.masterPlaylist.syncing.version,
              retryCount: 0,
              executionTimeMs: 0,
              createdTimeMs: 0,
            }),
          ]
        : []),
    ]);
    await transaction.commit();
  });
}

async function cleanupAll() {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteVideoContainerSyncingTaskStatement({
        videoContainerSyncingTaskContainerIdEq: "container1",
        videoContainerSyncingTaskVersionEq: 1,
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/m0.m3u8",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/m1.m3u8",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/dir1",
      }),
      deleteStorageEndRecordingTaskStatement({
        storageEndRecordingTaskR2DirnameEq: "root/dir2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/m0.m3u8",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/m1.m3u8",
      }),
      deleteR2KeyDeletingTaskStatement({ r2KeyDeletingTaskKeyEq: "root/dir1" }),
      deleteR2KeyDeletingTaskStatement({ r2KeyDeletingTaskKeyEq: "root/dir2" }),
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
        let videoContainerData: VideoContainer = {
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
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          version: 1,
        });

        // Verify
        assertThat(
          clientMock.request.descriptor,
          eq(CACHE_VIDEO_CONTAINER),
          "RC",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              seasonId: "season1",
              episodeId: "episode1",
              videoContainer: {
                version: 1,
                r2RootDirname: "root",
                r2MasterPlaylistFilename: "m.m3u8",
                durationSec: 60,
                resolution: "640x480",
              },
            },
            CACHE_VIDEO_CONTAINER_REQUEST_BODY,
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
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerSeasonId: "season1",
                videoContainerEpisodeId: "episode1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingVideoContainerSyncingTasks(SPANNER_DATABASE, {
            videoContainerSyncingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "video container syncing tasks",
        );
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/dir1",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/dir1",
                storageEndRecordingTaskPayload: {
                  accountId: "account1",
                  endTimeMs: 1000,
                },
                storageEndRecordingTaskRetryCount: 0,
                storageEndRecordingTaskExecutionTimeMs: 1000,
                storageEndRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_END_RECORDING_TASK_ROW,
            ),
          ]),
          "storage end recording task for root/dir1",
        );
        assertThat(
          await getStorageEndRecordingTask(SPANNER_DATABASE, {
            storageEndRecordingTaskR2DirnameEq: "root/dir2",
          }),
          isArray([
            eqMessage(
              {
                storageEndRecordingTaskR2Dirname: "root/dir2",
                storageEndRecordingTaskPayload: {
                  accountId: "account1",
                  endTimeMs: 1000,
                },
                storageEndRecordingTaskRetryCount: 0,
                storageEndRecordingTaskExecutionTimeMs: 1000,
                storageEndRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_END_RECORDING_TASK_ROW,
            ),
          ]),
          "storage end recording task for root/dir2",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/m0.m3u8",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/m0.m3u8",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for root/m0.m3u8",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/m1.m3u8",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/m1.m3u8",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for root/m1.m3u8",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/dir1",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/dir1",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for root/dir1",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/dir2",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/dir2",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "r2 key delete task for root/dir2",
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
        let videoContainerData: VideoContainer = {
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
        };
        await insertVideoContainer(videoContainerData);
        let clientMock = new NodeServiceClientMock();
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          clientMock,
          () => 1000,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          version: 1,
        });

        // Verify
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              seasonId: "season1",
              episodeId: "episode1",
              videoContainer: {
                version: 1,
                r2RootDirname: "root",
                r2MasterPlaylistFilename: "m.m3u8",
                durationSec: 60,
                resolution: "640x480",
              },
            },
            CACHE_VIDEO_CONTAINER_REQUEST_BODY,
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
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerSeasonId: "season1",
                videoContainerEpisodeId: "episode1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await listPendingVideoContainerSyncingTasks(SPANNER_DATABASE, {
            videoContainerSyncingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "video container syncing tasks",
        );
        assertThat(
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, {
            storageEndRecordingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "storage end recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: 1000000,
          }),
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
        let videoContainerData: VideoContainer = {
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
          handler.processTask("", {
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
        let videoContainerData: VideoContainer = {
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
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            version: 1,
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("fake error")), "error");
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerSeasonId: "season1",
                videoContainerEpisodeId: "episode1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTaskMetadata(SPANNER_DATABASE, {
            videoContainerSyncingTaskContainerIdEq: "container1",
            videoContainerSyncingTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskRetryCount: 0,
                videoContainerSyncingTaskExecutionTimeMs: 0,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASK_METADATA_ROW,
            ),
          ]),
          "video container syncing tasks",
        );
        assertThat(
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, {
            storageEndRecordingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "storage end recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledAndResumeButVersionChanged",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
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
        let processedPromise = handler.processTask("", {
          containerId: "container1",
          version: 1,
        });
        await firstEncounter;

        // Prepare
        videoContainerData.masterPlaylist.syncing.version = 2;
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            updateVideoContainerStatement({
              videoContainerContainerIdEq: "container1",
              setData: videoContainerData,
            }),
          ]);
          await transaction.commit();
        });

        // Execute
        stallResolveFn();
        let error = await assertReject(processedPromise);

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError("is syncing with a different version than 1"),
          ),
          "error",
        );
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerSeasonId: "season1",
                videoContainerEpisodeId: "episode1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getVideoContainerSyncingTaskMetadata(SPANNER_DATABASE, {
            videoContainerSyncingTaskContainerIdEq: "container1",
            videoContainerSyncingTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskRetryCount: 0,
                videoContainerSyncingTaskExecutionTimeMs: 0,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASK_METADATA_ROW,
            ),
          ]),
          "remained video container syncing tasks",
        );
        assertThat(
          await listPendingStorageEndRecordingTasks(SPANNER_DATABASE, {
            storageEndRecordingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "storage end recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: 1000000,
          }),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ClaimTask",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              version: 1,
              r2Filename: "m.m3u8",
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
        };
        await insertVideoContainer(videoContainerData);
        let handler = new ProcessVideoContainerSyncingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          () => 1000,
        );

        // Execute
        await handler.claimTask("", {
          containerId: "container1",
          version: 1,
        });

        // Verify
        assertThat(
          await getVideoContainerSyncingTaskMetadata(SPANNER_DATABASE, {
            videoContainerSyncingTaskContainerIdEq: "container1",
            videoContainerSyncingTaskVersionEq: 1,
          }),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskRetryCount: 1,
                videoContainerSyncingTaskExecutionTimeMs: 301000,
              },
              GET_VIDEO_CONTAINER_SYNCING_TASK_METADATA_ROW,
            ),
          ]),
          "video container syncing task metadata",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
