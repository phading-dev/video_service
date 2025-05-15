import "../local/env";
import { MEDIA_TEMP_DIR } from "../common/constants";
import { DirectoryStreamUploader } from "../common/r2_directory_stream_uploader";
import { FILE_UPLOADER } from "../common/r2_file_uploader";
import { S3_CLIENT, initS3Client } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  GET_GCS_FILE_DELETING_TASK_ROW,
  GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_STORAGE_START_RECORDING_TASK_ROW,
  GET_VIDEO_CONTAINER_ROW,
  deleteGcsFileDeletingTaskStatement,
  deleteMediaFormattingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  deleteStorageStartRecordingTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeletingTask,
  getMediaFormattingTaskMetadata,
  getR2Key,
  getR2KeyDeletingTask,
  getStorageStartRecordingTask,
  getVideoContainer,
  insertMediaFormattingTaskStatement,
  insertVideoContainerStatement,
  listPendingGcsFileDeletingTasks,
  listPendingMediaFormattingTasks,
  listPendingR2KeyDeletingTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessMediaFormattingTaskHandler } from "./process_media_formatting_task_handler";
import { DeleteObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { ProcessingFailureReason } from "@phading/video_service_interface/node/processing_failure_reason";
import { BlockingLoopMock } from "@selfage/blocking_loop/blocking_loop_mock";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import {
  assertReject,
  assertThat,
  eq,
  eqError,
  isArray,
  isUnorderedArray,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { existsSync } from "fs";
import { copyFile, rm } from "fs/promises";

let ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
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
      ...(videoContainerData.processing?.media?.formatting
        ? [
            insertMediaFormattingTaskStatement({
              containerId: "container1",
              gcsFilename:
                videoContainerData.processing.media.formatting.gcsFilename,
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

let ALL_TEST_GCS_FILE = [
  "h265_opus_codec.mp4",
  "one_video_one_audio.mp4",
  "two_audios.mp4",
  "two_videos_two_audios.mp4",
  "video_only.mp4",
];

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement({
        videoContainerContainerIdEq: "container1",
      }),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteMediaFormattingTaskStatement({
          mediaFormattingTaskContainerIdEq: "container1",
          mediaFormattingTaskGcsFilenameEq: gcsFilename,
        }),
      ),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid1" }),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid2" }),
      deleteR2KeyStatement({ r2KeyKeyEq: "root/uuid3" }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid1",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid2",
      }),
      deleteR2KeyDeletingTaskStatement({
        r2KeyDeletingTaskKeyEq: "root/uuid3",
      }),
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "root/uuid1",
      }),
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "root/uuid2",
      }),
      deleteStorageStartRecordingTaskStatement({
        storageStartRecordingTaskR2DirnameEq: "root/uuid3",
      }),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteGcsFileDeletingTaskStatement({
          gcsFileDeletingTaskFilenameEq: gcsFilename,
        }),
      ),
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
  await rm(MEDIA_TEMP_DIR, { recursive: true, force: true });
  await Promise.all(
    ALL_TEST_GCS_FILE.map((gcsFilename) =>
      rm(`${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`, { force: true }),
    ),
  );
}

TEST_RUNNER.run({
  name: "ProcessMediaFormattingTaskHandlerTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "FormatOneVideoTrackTwoAudioTracks",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_videos_two_audios.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_videos_two_audios.mp4`,
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            media: {
              formatting: {
                gcsFilename: "two_videos_two_audios.mp4",
              },
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "two_videos_two_audios.mp4",
        });

        // Verify
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
          "video files",
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
          "audio 1 files",
        );
        assertThat(
          (
            await S3_CLIENT.val.send(
              new ListObjectsV2Command({
                Bucket: ENV_VARS.r2VideoBucketName,
                Prefix: "root/uuid3",
              }),
            )
          ).Contents?.length,
          eq(42),
          "audio 2 files",
        );
        assertThat(
          existsSync(`${MEDIA_TEMP_DIR}/two_videos_two_audios.mp4/uuid0`),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.videoTracks = [
          {
            r2TrackDirname: "uuid1",
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
            r2TrackDirname: "uuid2",
            totalBytes: 3158359,
            staging: {
              toAdd: {
                name: "1",
                isDefault: true,
              },
            },
          },
          {
            r2TrackDirname: "uuid3",
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
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid2" }))
            .length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid3" }))
            .length,
          eq(1),
          "audio 2 dir r2 key exists",
        );
        assertThat(id, eq(4), "ids used");
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, {
            gcsFileDeletingTaskFilenameEq: "two_videos_two_audios.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "two_videos_two_audios.mp4",
                gcsFileDeletingTaskUploadSessionUrl: "",
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 1000,
                gcsFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcs file delete tasks",
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
                  totalBytes: 19618786,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
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
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid2",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "root/uuid3",
          }),
          isArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid3",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 3158359,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid3",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
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
      name: "AppendOneVideoTrackAndOneAudioTrackToOneVideoTrackAndOneRemovingAudioTrack",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/one_video_one_audio.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/one_video_one_audio.mp4`,
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            media: {
              formatting: {
                gcsFilename: "one_video_one_audio.mp4",
              },
            },
          },
          lastProcessingFailures: [],
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
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });

        // Verify
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
          "video files",
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
          "audio files",
        );
        assertThat(
          existsSync(`${MEDIA_TEMP_DIR}/one_video_one_audio.mp4/uuid0`),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.videoTracks.push({
          r2TrackDirname: "uuid1",
          durationSec: 240,
          resolution: "640x360",
          totalBytes: 19618786,
          staging: {
            toAdd: true,
          },
        });
        videoContainerData.audioTracks.push({
          r2TrackDirname: "uuid2",
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
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid2" }))
            .length,
          eq(1),
          "audio dir r2 key exists",
        );
        assertThat(id, eq(3), "ids used");
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, {
            gcsFileDeletingTaskFilenameEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "one_video_one_audio.mp4",
                gcsFileDeletingTaskUploadSessionUrl: "",
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 1000,
                gcsFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcs file delete tasks",
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
                  totalBytes: 19618786,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
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
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for root/uuid2",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
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
      name: "VideoOnly",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/video_only.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/video_only.mp4`,
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            media: {
              formatting: {
                gcsFilename: "video_only.mp4",
              },
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "video_only.mp4",
        });

        // Verify
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
          "video files",
        );
        assertThat(
          existsSync(`${MEDIA_TEMP_DIR}/video_only.mp4/uuid0`),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.videoTracks.push({
          r2TrackDirname: "uuid1",
          durationSec: 240,
          resolution: "640x360",
          totalBytes: 19618786,
          staging: {
            toAdd: true,
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
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(id, eq(2), "ids used");
        assertThat(
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, {
            gcsFileDeletingTaskFilenameEq: "video_only.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "video_only.mp4",
                gcsFileDeletingTaskUploadSessionUrl: "",
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 1000,
                gcsFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, {
            storageStartRecordingTaskR2DirnameEq: "root/uuid1",
          }),
          isUnorderedArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid1",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 19618786,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording tasks",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, {
            r2KeyDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
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
      name: "ContainerNotInMediaFormattingState",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            subtitle: {},
          },
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsFilename: "one_video_one_audio.mp4",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "Video container container1 is not in media formatting state",
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
      name: "BothVideoCodecAndAudioCodecInvalid",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/h265_opus_codec.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/h265_opus_codec.mp4`,
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            media: {
              formatting: {
                gcsFilename: "h265_opus_codec.mp4",
              },
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "h265_opus_codec.mp4",
        });

        // Verify
        assertThat(
          existsSync(`${MEDIA_TEMP_DIR}/h265_opus_codec.mp4/uuid0`),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.lastProcessingFailures = [
          ProcessingFailureReason.VIDEO_CODEC_REQUIRES_H264,
          ProcessingFailureReason.AUDIO_CODEC_REQUIRES_AAC,
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
          await listPendingMediaFormattingTasks(SPANNER_DATABASE, {
            mediaFormattingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "media formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, {
            gcsFileDeletingTaskFilenameEq: "h265_opus_codec.mp4",
          }),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "h265_opus_codec.mp4",
                gcsFileDeletingTaskUploadSessionUrl: "",
                gcsFileDeletingTaskRetryCount: 0,
                gcsFileDeletingTaskExecutionTimeMs: 1000,
                gcsFileDeletingTaskCreatedTimeMs: 1000,
              },
              GET_GCS_FILE_DELETING_TASK_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "FormattingInterruptedUnexpectedly",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/one_video_one_audio.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/one_video_one_audio.mp4`,
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            media: {
              formatting: {
                gcsFilename: "one_video_one_audio.mp4",
              },
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          new BlockingLoopMock(),
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );
        handler.interfereFormat = () => Promise.reject(new Error("fake error"));

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsFilename: "one_video_one_audio.mp4",
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("fake error")), "error");
        assertThat(
          existsSync(`${MEDIA_TEMP_DIR}/one_video_one_audio.mp4/uuid0`),
          eq(false),
          "temp dir",
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
          await listPendingGcsFileDeletingTasks(SPANNER_DATABASE, {
            gcsFileDeletingTaskExecutionTimeMsLe: TWO_YEAR_MS,
          }),
          isArray([]),
          "gcs file delete tasks",
        );
        assertThat(
          await getMediaFormattingTaskMetadata(SPANNER_DATABASE, {
            mediaFormattingTaskContainerIdEq: "container1",
            mediaFormattingTaskGcsFilenameEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskRetryCount: 0,
                mediaFormattingTaskExecutionTimeMs: 0,
              },
              GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "media formatting tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledFormatting_DelayingTasksFurther_ResumeButAnotherFileIsBeingFormatted",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/one_video_one_audio.mp4",
          `${ENV_VARS.gcsVideoMountedLocalDir}/one_video_one_audio.mp4`,
        );
        let videoContainerData: VideoContainer = {
          r2RootDirname: "root",
          processing: {
            media: {
              formatting: {
                gcsFilename: "one_video_one_audio.mp4",
              },
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let blockingLoop = new BlockingLoopMock();
        let now = 1000;
        let id = 0;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          blockingLoop,
          (loggingPrefix, localDir, remoteBucket, remoteDir) =>
            new DirectoryStreamUploader(
              FILE_UPLOADER,
              loggingPrefix,
              localDir,
              remoteBucket,
              remoteDir,
            ),
          () => now,
          () => `uuid${id++}`,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((resolve1) => {
          handler.interfereFormat = () => {
            resolve1();
            return new Promise<void>((resolve2) => (stallResolveFn = resolve2));
          };
        });

        // Execute
        let processedPromise = handler.processTask("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });
        await firstEncounter;

        // Verify
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid1" }))
            .length,
          eq(1),
          "video dir r2 key exists",
        );
        assertThat(
          (await getR2Key(SPANNER_DATABASE, { r2KeyKeyEq: "root/uuid2" }))
            .length,
          eq(1),
          "audio dir r2 key exists",
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
                r2KeyDeletingTaskExecutionTimeMs: ONE_YEAR_MS + 1000,
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
                r2KeyDeletingTaskExecutionTimeMs: ONE_YEAR_MS + 1000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "R2 key delete task for root/uuid2",
        );

        // Prepare
        now = 2000;

        // Execute
        await blockingLoop.execute();

        // Verify
        assertThat(
          await getMediaFormattingTaskMetadata(SPANNER_DATABASE, {
            mediaFormattingTaskContainerIdEq: "container1",
            mediaFormattingTaskGcsFilenameEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskRetryCount: 0,
                mediaFormattingTaskExecutionTimeMs: 300000,
              },
              GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "delayed formatting tasks",
        );

        // Prepare
        videoContainerData.processing = {
          media: {
            formatting: {
              gcsFilename: "two_audios.mp4",
            },
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
            newConflictError(
              "is formatting a different file than one_video_one_audio.mp4",
            ),
          ),
          "error",
        );
        assertThat(
          existsSync(`${MEDIA_TEMP_DIR}/one_video_one_audio.mp4/uuid0`),
          eq(false),
          "temp dir",
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
          await getMediaFormattingTaskMetadata(SPANNER_DATABASE, {
            mediaFormattingTaskContainerIdEq: "container1",
            mediaFormattingTaskGcsFilenameEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskRetryCount: 0,
                mediaFormattingTaskExecutionTimeMs: 300000,
              },
              GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "remained formatting tasks",
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
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, {
            r2KeyDeletingTaskKeyEq: "root/uuid2",
          }),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid2",
                r2KeyDeletingTaskRetryCount: 0,
                r2KeyDeletingTaskExecutionTimeMs: 302000,
                r2KeyDeletingTaskCreatedTimeMs: 1000,
              },
              GET_R2_KEY_DELETING_TASK_ROW,
            ),
          ]),
          "remained R2 key delete tasks for root/uuid2",
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
            media: {
              formatting: {
                gcsFilename: "one_video_one_audio.mp4",
              },
            },
          },
          lastProcessingFailures: [],
          videoTracks: [],
          audioTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let handler = new ProcessMediaFormattingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          undefined,
          () => now,
          undefined,
        );

        // Execute
        await handler.claimTask("", {
          containerId: "container1",
          gcsFilename: "one_video_one_audio.mp4",
        });

        // Verify
        assertThat(
          await getMediaFormattingTaskMetadata(SPANNER_DATABASE, {
            mediaFormattingTaskContainerIdEq: "container1",
            mediaFormattingTaskGcsFilenameEq: "one_video_one_audio.mp4",
          }),
          isArray([
            eqMessage(
              {
                mediaFormattingTaskRetryCount: 1,
                mediaFormattingTaskExecutionTimeMs: 601000,
              },
              GET_MEDIA_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "media formatting tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
