import "../local/env";
import { initS3Client, S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { configureRclone } from "../configure_rclone";
import { VideoContainer } from "../db/schema";
import {
  GET_GCS_KEY_DELETING_TASK_ROW,
  GET_MEDIA_UPLOADING_TASK_METADATA_ROW,
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_STORAGE_START_RECORDING_TASK_ROW,
  GET_UPLOADED_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsKeyDeletingTaskStatement,
  deleteMediaUploadingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  deleteStorageStartRecordingTaskStatement,
  deleteUploadedRecordingTaskStatement,
  deleteVideoContainerStatement,
  getGcsKeyDeletingTask,
  getMediaUploadingTaskMetadata,
  getR2Key,
  getR2KeyDeletingTask,
  getStorageStartRecordingTask,
  getUploadedRecordingTask,
  getVideoContainer,
  insertMediaUploadingTaskStatement,
  insertVideoContainerStatement,
  listPendingGcsKeyDeletingTasks,
  listPendingMediaUploadingTasks,
  listPendingR2KeyDeletingTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessMediaUploadingTaskHandler } from "./process_media_uploading_tasks_handler";
import { DeleteObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { cp, rm } from "fs/promises";

let ONE_MONTH_MS = 30 * 24 * 60 * 60 * 1000;
let TWO_YEAR_MS = 2 * 365 * 24 * 60 * 60 * 1000;

async function insertVideoContainer(
  videoContainerData: VideoContainer,
): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement({
        containerId: "container1",
        accountId: "account1",
        data: videoContainerData,
      }),
      ...(videoContainerData.processing?.mediaUploading
        ? [
            insertMediaUploadingTaskStatement({
              containerId: "container1",
              gcsDirname:
                videoContainerData.processing.mediaUploading.gcsDirname,
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

function createProcessMediaUploadingTaskHandler(
  now: () => number,
  getId: () => number,
): ProcessMediaUploadingTaskHandler {
  return new ProcessMediaUploadingTaskHandler(
    SPANNER_DATABASE,
    now,
    () => `uuid${getId()}`,
  );
}

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      deleteMediaUploadingTaskStatement({
        mediaUploadingTaskContainerIdEq: "container1",
        mediaUploadingTaskGcsDirnameEq: "base",
      }),
      deleteGcsKeyDeletingTaskStatement({
        gcsKeyDeletingTaskKeyEq: "base",
      }),
      deleteUploadedRecordingTaskStatement({
        uploadedRecordingTaskGcsKeyEq: "base",
      }),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid0" }),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid1" }),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid2" }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid0",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid1",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid2",
      }),
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "root/uuid0",
      }),
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "root/uuid1",
      }),
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "root/uuid2",
      }),
    ]);
    await transaction.commit();
  });
  let response = await S3_CLIENT.val.send(
    new ListObjectsV2Command({
      Bucket: ENV_VARS.r2VideoBucketName,
      Prefix: "root",
    }),
  );
  await (response.Contents
    ? S3_CLIENT.val.send(
        new DeleteObjectsCommand({
          Bucket: ENV_VARS.r2VideoBucketName,
          Delete: {
            Objects: response.Contents.map((content) => ({ Key: content.Key })),
          },
        }),
      )
    : Promise.resolve());
  await rm(`${ENV_VARS.gcsVideoMountedLocalDir}/base`, {
    recursive: true,
    force: true,
  });
}

TEST_RUNNER.run({
  name: "ProcessMediaUploadingTaskHandlerTest",
  environment: {
    async setUp() {
      await configureRclone();
      await initS3Client();
    },
  },
  cases: [
    {
      name: "UploadVideoAndAudioTracks",
      execute: async () => {
        // Prepare
        await cp(
          "test_data/video_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/video_track`,
          { recursive: true },
        );
        await cp(
          "test_data/audio_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/audio_track`,
          { recursive: true },
        );
        await cp(
          "test_data/audio_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/audio_track_2`,
          { recursive: true },
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            mediaUploading: {
              gcsDirname: "base",
              gcsVideoDirname: "video_track",
              videoInfo: {
                durationSec: 240,
                resolution: "640x360",
              },
              gcsAudioDirnames: ["audio_track", "audio_track_2"],
            },
          },
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaUploadingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsDirname: "base",
        });

        // Verify
        assertThat(id, eq(3), "ids used");
        assertThat(
          (
            await S3_CLIENT.val.send(
              new ListObjectsV2Command({
                Bucket: ENV_VARS.r2VideoBucketName,
                Prefix: "root/uuid0",
              }),
            )
          ).Contents?.length,
          eq(42),
          "video files",
        );
        assertThat(
          (
            await S3_CLIENT.val.send(
              new ListObjectsV2Command({
                Bucket: ENV_VARS.r2VideoBucketName,
                Prefix: "root/uuid1",
              }),
            )
          ).Contents?.length,
          eq(42),
          "audio 1 files",
        );
        assertThat(
          (
            await S3_CLIENT.val.send(
              new ListObjectsV2Command({
                Bucket: ENV_VARS.r2VideoBucketName,
                Prefix: "root/uuid2",
              }),
            )
          ).Contents?.length,
          eq(42),
          "audio 2 files",
        );
        videoContainerData.processing = undefined;
        videoContainerData.videoTracks = [
          {
            r2TrackDirname: "uuid0",
            durationSec: 240,
            resolution: "640x360",
            totalBytes: 19618786,
            staging: {
              toAdd: true,
            },
          },
        ];
        videoContainerData.audioTracks = [
          {
            r2TrackDirname: "uuid1",
            totalBytes: 3158359,
            staging: {
              toAdd: {
                name: "1",
                isDefault: true,
              },
            },
          },
          {
            r2TrackDirname: "uuid2",
            totalBytes: 3158359,
            staging: {
              toAdd: {
                name: "2",
                isDefault: false,
              },
            },
          },
        ];
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid2" }))
            .length,
          eq(1),
          "audio 2 dir r2 key exists",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "r2 key delete tasks",
        );
        assertThat(
          await listPendingMediaUploadingTasks(SPANNER_DATABASE, {
            mediaUploadingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media uploading tasks",
        );
        assertThat(
          await getGcsKeyDeletingTask(SPANNER_DATABASE, {
            gcsKeyDeletingTaskKeyEq: "base",
          }),
          isArray([
            eqMessage(
              {
                gcsKeyDeletingTaskKey: "base",
                gcsKeyDeletingTaskRetryCount: 0,
                gcsKeyDeletingTaskExecutionTimeMs: 1000,
                gcsKeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "GCS key deleting tasks",
        );
        assertThat(
          await getUploadedRecordingTask(SPANNER_DATABASE, {
            uploadedRecordingTaskGcsKeyEq: "base",
          }),
          isArray([
            eqMessage(
              {
                uploadedRecordingTaskGcsKey: "base",
                uploadedRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 19618786 + 3158359 + 3158359,
                },
                uploadedRecordingTaskRetryCount: 0,
                uploadedRecordingTaskExecutionTimeMs: 1000,
                uploadedRecordingTaskCreatedTimeMs: 1000,
              },
              GET_UPLOADED_RECORDING_TASK_ROW,
            ),
          ]),
          "uploaded recording tasks",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "root/uuid0",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid0",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 19618786,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskCreatedTimeMs: 1000,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskRetryCount: 0,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid0",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "root/uuid1",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid1",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 3158359,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskCreatedTimeMs: 1000,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskRetryCount: 0,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid1",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "root/uuid2",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid2",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 3158359,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskCreatedTimeMs: 1000,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskRetryCount: 0,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid2",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "UploadAudioTracks",
      execute: async () => {
        // Prepare
        await cp(
          "test_data/audio_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/audio_track`,
          { recursive: true },
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            mediaUploading: {
              gcsDirname: "base",
              gcsAudioDirnames: ["audio_track"],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video0",
              totalBytes: 1000,
              committed: true,
            },
          ],
          audioTracks: [
            {
              r2TrackDirname: "audio0",
              totalBytes: 1000,
              committed: {
                name: "1",
                isDefault: true,
              },
              staging: {
                toDelete: true,
              },
            },
          ],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaUploadingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsDirname: "base",
        });

        // Verify
        assertThat(id, eq(1), "ids used");
        assertThat(
          (
            await S3_CLIENT.val.send(
              new ListObjectsV2Command({
                Bucket: ENV_VARS.r2VideoBucketName,
                Prefix: "root/uuid0",
              }),
            )
          ).Contents?.length,
          eq(42),
          "audio files",
        );
        videoContainerData.processing = undefined;
        videoContainerData.audioTracks.push({
          r2TrackDirname: "uuid0",
          totalBytes: 3158359,
          staging: {
            toAdd: {
              name: "2",
              isDefault: false,
            },
          },
        });
        assertThat(
          await getVideoContainer(SPANNER_DATABASE, {
            videoContainerContainerIdEq: "container1",
          }),
          isArray([
            eqMessage(
              {
                videoContainerContainerId: "container1",
                videoContainerAccountId: "account1",
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "root/uuid0",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid0",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 3158359,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskCreatedTimeMs: 1000,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskRetryCount: 0,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid0",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotInMediaUploadingState",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            mediaFormatting: {},
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaUploadingTaskHandler(
          () => now,
          () => id++,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsDirname: "base",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "Video container container1 is not in media uploading state",
            ),
          ),
          "",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "UploadFailed",
      execute: async () => {
        // Prepare
        await cp(
          "test_data/video_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/video_track`,
          { recursive: true },
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            mediaUploading: {
              gcsDirname: "base",
              gcsVideoDirname: "video_track",
              videoInfo: {
                durationSec: 240,
                resolution: "640x360",
              },
              gcsAudioDirnames: ["audio_track", "audio_track_2"],
            },
          },
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaUploadingTaskHandler(
          () => now,
          () => id++,
        );
        handler.interfereUpload = () => {
          throw new Error("Fake error");
        };

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsDirname: "base",
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("Fake error")), "error");
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid2" }))
            .length,
          eq(1),
          "audio 2 dir r2 key exists",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid0",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 301000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "R2 key delete task for root/uuid0",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid1",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid1",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 301000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "R2 key delete task for root/uuid1",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid2",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid2",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 301000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "R2 key delete task for root/uuid2",
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
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getMediaUploadingTaskMetadata(SPANNER_DATABASE, {
            mediaUploadingTaskContainerIdEq: "container1",
            mediaUploadingTaskGcsDirnameEq: "base",
          }),
          isArray([
            eqMessage(
              {
                mediaUploadingTaskRetryCount: 0,
                mediaUploadingTaskExecutionTimeMs: 0,
              },
              GET_MEDIA_UPLOADING_TASK_METADATA_ROW,
            ),
          ]),
          "media uploading task metadata",
        );
        assertThat(
          await listPendingGcsKeyDeletingTasks(SPANNER_DATABASE, {
            gcsKeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "GCS key deleting tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledUpload_ResumedButGcsDirnameChanged",
      execute: async () => {
        // Prepare
        await cp(
          "test_data/video_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/video_track`,
          { recursive: true },
        );
        await cp(
          "test_data/audio_track",
          `${ENV_VARS.gcsVideoMountedLocalDir}/base/audio_track`,
          { recursive: true },
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            mediaUploading: {
              gcsDirname: "base",
              gcsVideoDirname: "video_track",
              videoInfo: {
                durationSec: 240,
                resolution: "640x360",
              },
              gcsAudioDirnames: ["audio_track"],
            },
          },
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = createProcessMediaUploadingTaskHandler(
          () => now,
          () => id++,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((resolve1) => {
          handler.interfereUpload = () => {
            resolve1();
            return new Promise<void>((resolve2) => (stallResolveFn = resolve2));
          };
        });

        // Execute
        let processedPromise = handler.processTask("", {
          containerId: "container1",
          gcsDirname: "base",
        });
        await firstEncounter;

        // Verify
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid0" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid0",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: ONE_MONTH_MS + 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "R2 key delete task for root/uuid0",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid1",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid1",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: ONE_MONTH_MS + 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "R2 key delete task for root/uuid1",
        );

        // Prepare
        now = 2000;
        videoContainerData.processing = {
          mediaUploading: {
            gcsDirname: "base2",
          },
        };
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
            newConflictError("is uploading a different directory than base"),
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
                videoContainerData,
              },
              GET_VIDEO_CONTAINER_ROW,
            ),
          ]),
          "video container",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid0",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 302000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "remained R2 key delete tasks for root/uuid0",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid1",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid1",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 302000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "remained R2 key delete tasks for root/uuid1",
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
          processing: {
            mediaUploading: {
              gcsDirname: "base",
            },
          },
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let handler = createProcessMediaUploadingTaskHandler(
          () => now,
          () => 0,
        );

        // Execute
        await handler.claimTask("", {
          containerId: "container1",
          gcsDirname: "base",
        });

        // Verify
        assertThat(
          await getMediaUploadingTaskMetadata(SPANNER_DATABASE, {
            mediaUploadingTaskContainerIdEq: "container1",
            mediaUploadingTaskGcsDirnameEq: "base",
          }),
          isArray([
            eqMessage(
              {
                mediaUploadingTaskRetryCount: 1,
                mediaUploadingTaskExecutionTimeMs: 3601000,
              },
              GET_MEDIA_UPLOADING_TASK_METADATA_ROW,
            ),
          ]),
          "media uploading tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
