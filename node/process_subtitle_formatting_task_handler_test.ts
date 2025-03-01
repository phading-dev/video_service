import "../local/env";
import { SUBTITLE_TEMP_DIR } from "../common/constants";
import { FILE_UPLOADER } from "../common/r2_file_uploader";
import { S3_CLIENT, initS3Client } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainer } from "../db/schema";
import {
  GET_GCS_FILE_DELETING_TASK_ROW,
  GET_R2_KEY_DELETING_TASK_ROW,
  GET_STORAGE_START_RECORDING_TASK_ROW,
  GET_SUBTITLE_FORMATTING_TASK_METADATA_ROW,
  GET_VIDEO_CONTAINER_ROW,
  checkR2Key,
  deleteGcsFileDeletingTaskStatement,
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  deleteStorageStartRecordingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeletingTask,
  getR2KeyDeletingTask,
  getStorageStartRecordingTask,
  getSubtitleFormattingTaskMetadata,
  getVideoContainer,
  insertSubtitleFormattingTaskStatement,
  insertVideoContainerStatement,
  listPendingGcsFileDeletingTasks,
  listPendingR2KeyDeletingTasks,
  listPendingSubtitleFormattingTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ENV_VARS } from "../env_vars";
import { ProcessSubtitleFormattingTaskHandler } from "./process_subtitle_formatting_task_handler";
import { DeleteObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { ProcessingFailureReason } from "@phading/video_service_interface/node/processing_failure_reason";
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
      insertVideoContainerStatement(videoContainerData),
      ...(videoContainerData.processing?.subtitle?.formatting
        ? [
            insertSubtitleFormattingTaskStatement(
              "container1",
              videoContainerData.processing.subtitle.formatting.gcsFilename,
              0,
              0,
              0,
            ),
          ]
        : []),
    ]);
    await transaction.commit();
  });
}

let ALL_TEST_GCS_FILE = ["two_subs.zip", "sub_invalid.txt"];

async function cleanupAll(): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      deleteVideoContainerStatement("container1"),
      deleteSubtitleFormattingTaskStatement("container1", "two_subs.zip"),
      deleteR2KeyStatement("root/uuid1"),
      deleteR2KeyStatement("root/uuid2"),
      deleteR2KeyDeletingTaskStatement("root/uuid1"),
      deleteR2KeyDeletingTaskStatement("root/uuid2"),
      deleteStorageStartRecordingTaskStatement("root/uuid1"),
      deleteStorageStartRecordingTaskStatement("root/uuid2"),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteGcsFileDeletingTaskStatement(gcsFilename),
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
  await rm(SUBTITLE_TEMP_DIR, { recursive: true, force: true });
  await Promise.all(
    ALL_TEST_GCS_FILE.map((gcsFilename) =>
      rm(`${ENV_VARS.gcsVideoMountedLocalDir}/${gcsFilename}`, { force: true }),
    ),
  );
}

TEST_RUNNER.run({
  name: "ProcessSubtitleFormattingTaskHandlerTest",
  environment: {
    async setUp() {
      await initS3Client();
    },
  },
  cases: [
    {
      name: "FormatTwoSubs",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_subs.zip",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_subs.zip`,
        );
        let videoContainerData: VideoContainer = {
          containerId: "container1",
          accountId: "account1",
          r2RootDirname: "root",
          processing: {
            subtitle: {
              formatting: {
                gcsFilename: "two_subs.zip",
              },
            },
          },
          lastProcessingFailures: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
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
          eq(2),
          "subtitle uuid1 files",
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
          eq(2),
          "subtitle uuid2 files",
        );
        assertThat(
          existsSync("test_data/two_subs.zip/uuid0"),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.subtitleTracks = [
          {
            r2TrackDirname: "uuid1",
            staging: {
              toAdd: {
                name: "test_data/sub",
                isDefault: true,
                totalBytes: 919,
              },
            },
          },
          {
            r2TrackDirname: "uuid2",
            staging: {
              toAdd: {
                name: "test_data/sub2",
                isDefault: false,
                totalBytes: 919,
              },
            },
          },
        ];
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
          (await checkR2Key(SPANNER_DATABASE, "root/uuid1")).length,
          eq(1),
          "subtitle dir r2 key exists",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid2")).length,
          eq(1),
          "subtitle 2 dir r2 key exists",
        );
        assertThat(id, eq(3), "ids used");
        assertThat(
          await listPendingSubtitleFormattingTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([]),
          "subtitle formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, "two_subs.zip"),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "two_subs.zip",
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
          await getStorageStartRecordingTask(SPANNER_DATABASE, "root/uuid1"),
          isUnorderedArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid1",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 919,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for uuid1",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, "root/uuid2"),
          isUnorderedArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid2",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 919,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for uuid2",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "FormatTwoSubsAndAppendToExistingSubs",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_subs.zip",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_subs.zip`,
        );
        let videoContainerData: VideoContainer = {
          containerId: "container1",
          accountId: "account1",
          r2RootDirname: "root",
          processing: {
            subtitle: {
              formatting: {
                gcsFilename: "two_subs.zip",
              },
            },
          },
          lastProcessingFailures: [],
          subtitleTracks: [
            {
              r2TrackDirname: "uuid0",
              committed: {
                name: "uuid0",
                isDefault: true,
                totalBytes: 919,
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
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
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
          eq(2),
          "subtitle uuid1 files",
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
          eq(2),
          "subtitle uuid2 files",
        );
        assertThat(
          existsSync("test_data/two_subs.zip/uuid0"),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.subtitleTracks.push(
          {
            r2TrackDirname: "uuid1",
            staging: {
              toAdd: {
                name: "test_data/sub",
                isDefault: false,
                totalBytes: 919,
              },
            },
          },
          {
            r2TrackDirname: "uuid2",
            staging: {
              toAdd: {
                name: "test_data/sub2",
                isDefault: false,
                totalBytes: 919,
              },
            },
          },
        );
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
          (await checkR2Key(SPANNER_DATABASE, "root/uuid1")).length,
          eq(1),
          "subtitle dir r2 key exists",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid2")).length,
          eq(1),
          "subtitle 2 dir r2 key exists",
        );
        assertThat(id, eq(3), "ids used");
        assertThat(
          await listPendingSubtitleFormattingTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([]),
          "subtitle formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, "two_subs.zip"),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "two_subs.zip",
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
          await getStorageStartRecordingTask(SPANNER_DATABASE, "root/uuid1"),
          isUnorderedArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid1",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 919,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for uuid1",
        );
        assertThat(
          await getStorageStartRecordingTask(SPANNER_DATABASE, "root/uuid2"),
          isUnorderedArray([
            eqMessage(
              {
                storageStartRecordingTaskR2Dirname: "root/uuid2",
                storageStartRecordingTaskPayload: {
                  accountId: "account1",
                  totalBytes: 919,
                  startTimeMs: 1000,
                },
                storageStartRecordingTaskRetryCount: 0,
                storageStartRecordingTaskExecutionTimeMs: 1000,
                storageStartRecordingTaskCreatedTimeMs: 1000,
              },
              GET_STORAGE_START_RECORDING_TASK_ROW,
            ),
          ]),
          "storage start recording task for uuid2",
        );
        assertThat(
          await listPendingR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ContainerNotInSubtitleFormattingState",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainer = {
          containerId: "container1",
          accountId: "account1",
          processing: {
            media: {
              formatting: {},
            },
          },
          lastProcessingFailures: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsFilename: "two_subs.zip",
          }),
        );

        // Verify
        assertThat(
          error,
          eqHttpError(
            newConflictError(
              "Video container container1 is not in subtitle formatting",
            ),
          ),
          "error",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "CannotGetZipInfo",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/sub_invalid.txt",
          `${ENV_VARS.gcsVideoMountedLocalDir}/sub_invalid.txt`,
        );
        let videoContainerData: VideoContainer = {
          containerId: "container1",
          accountId: "account1",
          r2RootDirname: "root",
          processing: {
            subtitle: {
              formatting: {
                gcsFilename: "sub_invalid.txt",
              },
            },
          },
          lastProcessingFailures: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );

        // Execute
        await handler.processTask("", {
          containerId: "container1",
          gcsFilename: "sub_invalid.txt",
        });

        // Verify
        assertThat(
          existsSync("test_data/two_subs.zip/uuid0"),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.lastProcessingFailures = [
          ProcessingFailureReason.SUBTITLE_ZIP_FORMAT_INVALID,
        ];
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
          await listPendingSubtitleFormattingTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([]),
          "subtitle formatting tasks",
        );
        assertThat(
          await getGcsFileDeletingTask(SPANNER_DATABASE, "sub_invalid.txt"),
          isArray([
            eqMessage(
              {
                gcsFileDeletingTaskFilename: "sub_invalid.txt",
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
          "test_data/two_subs.zip",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_subs.zip`,
        );
        let videoContainerData: VideoContainer = {
          containerId: "container1",
          accountId: "account1",
          r2RootDirname: "root",
          processing: {
            subtitle: {
              formatting: {
                gcsFilename: "two_subs.zip",
              },
            },
          },
          lastProcessingFailures: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );
        handler.interfereFormat = () => Promise.reject(new Error("interfere"));

        // Execute
        let error = await assertReject(
          handler.processTask("", {
            containerId: "container1",
            gcsFilename: "two_subs.zip",
          }),
        );

        // Verify
        assertThat(error, eqError(new Error("interfere")), "Error");
        assertThat(
          existsSync("test_data/two_subs.zip/uuid0"),
          eq(false),
          "temp dir",
        );
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
          await getSubtitleFormattingTaskMetadata(
            SPANNER_DATABASE,
            "container1",
            "two_subs.zip",
          ),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskRetryCount: 0,
                subtitleFormattingTaskExecutionTimeMs: 0,
              },
              GET_SUBTITLE_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "formatting tasks to retry",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/uuid1"),
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
          "R2 key delete task for uuid1",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/uuid2"),
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
          "R2 key delete task for uuid2",
        );
        assertThat(
          await listPendingGcsFileDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "gcs file delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "StalledFormatting_ResumeButAnotherFileIsBeingFormatted",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_subs.zip",
          `${ENV_VARS.gcsVideoMountedLocalDir}/two_subs.zip`,
        );
        let videoContainerData: VideoContainer = {
          containerId: "container1",
          r2RootDirname: "root",
          processing: {
            subtitle: {
              formatting: {
                gcsFilename: "two_subs.zip",
              },
            },
          },
          lastProcessingFailures: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let id = 0;
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((firstEncounterResolve) => {
          handler.interfereFormat = () => {
            firstEncounterResolve();
            return new Promise<void>(
              (stallResolve) => (stallResolveFn = stallResolve),
            );
          };
        });

        // Execute
        let processedPromise = handler.processTask("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
        });
        await firstEncounter;

        // Verify
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid1")).length,
          eq(1),
          "subtitle dir r2 key exists",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid2")).length,
          eq(1),
          "subtitle 2 dir r2 key exists",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/uuid1"),
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
          "R2 key delete task for uuid1",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/uuid2"),
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
          "R2 key delete task for uuid2",
        );

        // Prepare
        now = 2000;
        videoContainerData.processing = {
          subtitle: {
            formatting: {
              gcsFilename: "another_sub.zip",
            },
          },
        };
        await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
          await transaction.batchUpdate([
            updateVideoContainerStatement(videoContainerData),
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
            newConflictError("is formatting a different file than two_subs"),
          ),
          "error",
        );
        assertThat(
          existsSync("test_data/two_subs.zip/uuid0"),
          eq(false),
          "temp dir",
        );
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
          await getSubtitleFormattingTaskMetadata(
            SPANNER_DATABASE,
            "container1",
            "two_subs.zip",
          ),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskRetryCount: 0,
                subtitleFormattingTaskExecutionTimeMs: 0,
              },
              GET_SUBTITLE_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "remained formatting tasks",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/uuid1"),
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
          "remained R2 key delete task for uuid1",
        );
        assertThat(
          await getR2KeyDeletingTask(SPANNER_DATABASE, "root/uuid2"),
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
          "remained R2 key delete task for uuid2",
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
          containerId: "container1",
          accountId: "account1",
          r2RootDirname: "root",
          processing: {
            subtitle: {
              formatting: {
                gcsFilename: "two_subs.zip",
              },
            },
          },
          lastProcessingFailures: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let now = 1000;
        let handler = new ProcessSubtitleFormattingTaskHandler(
          SPANNER_DATABASE,
          undefined,
          () => now,
          undefined,
        );

        // Execute
        await handler.claimTask("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
        });

        // Verify
        assertThat(
          await getSubtitleFormattingTaskMetadata(
            SPANNER_DATABASE,
            "container1",
            "two_subs.zip",
          ),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskRetryCount: 1,
                subtitleFormattingTaskExecutionTimeMs: 301000,
              },
              GET_SUBTITLE_FORMATTING_TASK_METADATA_ROW,
            ),
          ]),
          "claimed task",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
