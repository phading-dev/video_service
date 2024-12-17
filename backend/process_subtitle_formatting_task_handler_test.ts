import {
  GCS_VIDEO_LOCAL_DIR,
  R2_VIDEO_REMOTE_BUCKET,
  SUBTITLE_TEMP_DIR,
} from "../common/env_vars";
import { FILE_UPLOADER } from "../common/r2_file_uploader";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainerData } from "../db/schema";
import {
  GET_GCS_FILE_DELETE_TASKS_ROW,
  GET_R2_KEY_DELETE_TASKS_ROW,
  GET_SUBTITLE_FORMATTING_TASKS_ROW,
  GET_VIDEO_CONTAINER_ROW,
  checkR2Key,
  deleteGcsFileDeleteTaskStatement,
  deleteR2KeyDeleteTaskStatement,
  deleteR2KeyStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  getGcsFileDeleteTasks,
  getR2KeyDeleteTasks,
  getSubtitleFormattingTasks,
  getVideoContainer,
  insertSubtitleFormattingTaskStatement,
  insertVideoContainerStatement,
  updateVideoContainerStatement,
} from "../db/sql";
import { FailureReason } from "../failure_reason";
import { ProcessSubtitleFormattingTaskHandler } from "./process_subtitle_formatting_task_handler";
import { DeleteObjectsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { existsSync } from "fs";
import { copyFile, rm } from "fs/promises";

let ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
let TWO_YEAR_MS = 2 * 365 * 24 * 60 * 60 * 1000;

async function insertVideoContainer(
  videoContainerData: VideoContainerData,
): Promise<void> {
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement("container1", videoContainerData),
      ...(videoContainerData.processing?.subtitle?.formatting
        ? [
            insertSubtitleFormattingTaskStatement(
              "container1",
              videoContainerData.processing.subtitle.formatting.gcsFilename,
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
      deleteR2KeyDeleteTaskStatement("root/uuid1"),
      deleteR2KeyDeleteTaskStatement("root/uuid2"),
      ...ALL_TEST_GCS_FILE.map((gcsFilename) =>
        deleteGcsFileDeleteTaskStatement(gcsFilename),
      ),
    ]);
    await transaction.commit();
  });
  let response = await S3_CLIENT.send(
    new ListObjectsV2Command({
      Bucket: R2_VIDEO_REMOTE_BUCKET,
      Prefix: "root",
    }),
  );
  await (response.Contents
    ? S3_CLIENT.send(
        new DeleteObjectsCommand({
          Bucket: R2_VIDEO_REMOTE_BUCKET,
          Delete: {
            Objects: response.Contents.map((content) => ({ Key: content.Key })),
          },
        }),
      )
    : Promise.resolve());
  await rm(SUBTITLE_TEMP_DIR, { recursive: true, force: true });
  await Promise.all(
    ALL_TEST_GCS_FILE.map((gcsFilename) =>
      rm(`${GCS_VIDEO_LOCAL_DIR}/${gcsFilename}`, { force: true }),
    ),
  );
}

TEST_RUNNER.run({
  name: "ProcessSubtitleFormattingTaskHandlerTest",
  cases: [
    {
      name: "FormatTwoSubs",
      execute: async () => {
        // Prepare
        await copyFile(
          "test_data/two_subs.zip",
          `${GCS_VIDEO_LOCAL_DIR}/two_subs.zip`,
        );
        let videoContainerData: VideoContainerData = {
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
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          (
            await S3_CLIENT.send(
              new ListObjectsV2Command({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
                Prefix: "root/uuid1",
              }),
            )
          ).Contents?.length,
          eq(2),
          "subtitle uuid1 files",
        );
        assertThat(
          (
            await S3_CLIENT.send(
              new ListObjectsV2Command({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
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
          await getSubtitleFormattingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "subtitle formatting tasks",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "two_subs.zip",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
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
          `${GCS_VIDEO_LOCAL_DIR}/two_subs.zip`,
        );
        let videoContainerData: VideoContainerData = {
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
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          (
            await S3_CLIENT.send(
              new ListObjectsV2Command({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
                Prefix: "root/uuid1",
              }),
            )
          ).Contents?.length,
          eq(2),
          "subtitle uuid1 files",
        );
        assertThat(
          (
            await S3_CLIENT.send(
              new ListObjectsV2Command({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
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
          await getSubtitleFormattingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "subtitle formatting tasks",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "two_subs.zip",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
            ),
          ]),
          "gcs file delete tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
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
        let videoContainerData: VideoContainerData = {
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
          handler.handle("", {
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
          `${GCS_VIDEO_LOCAL_DIR}/sub_invalid.txt`,
        );
        let videoContainerData: VideoContainerData = {
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
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "sub_invalid.txt",
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          existsSync("test_data/two_subs.zip/uuid0"),
          eq(false),
          "temp dir",
        );
        videoContainerData.processing = undefined;
        videoContainerData.lastProcessingFailures = [
          FailureReason.SUBTITLE_ZIP_FORMAT_INVALID,
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
          await getSubtitleFormattingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "subtitle formatting tasks",
        );
        assertThat(
          await getGcsFileDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                gcsFileDeleteTaskFilename: "sub_invalid.txt",
                gcsFileDeleteTaskPayload: {},
                gcsFileDeleteTaskExecutionTimestamp: 1000,
              },
              GET_GCS_FILE_DELETE_TASKS_ROW,
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
          `${GCS_VIDEO_LOCAL_DIR}/two_subs.zip`,
        );
        let videoContainerData: VideoContainerData = {
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
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
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
          await getSubtitleFormattingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "two_subs.zip",
                subtitleFormattingTaskExecutionTimestamp: 301000,
              },
              GET_SUBTITLE_FORMATTING_TASKS_ROW,
            ),
          ]),
          "formatting tasks to retry",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/uuid1",
                r2KeyDeleteTaskExecutionTimestamp: 301000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/uuid2",
                r2KeyDeleteTaskExecutionTimestamp: 301000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
          ]),
          "R2 key delete tasks",
        );
        assertThat(
          (await getGcsFileDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS)).length,
          eq(0),
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
          `${GCS_VIDEO_LOCAL_DIR}/two_subs.zip`,
        );
        let videoContainerData: VideoContainerData = {
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
        handler.handle("", {
          containerId: "container1",
          gcsFilename: "two_subs.zip",
        });
        await firstEncounter;

        // Verify
        assertThat(
          await getSubtitleFormattingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "two_subs.zip",
                subtitleFormattingTaskExecutionTimestamp: 301000,
              },
              GET_SUBTITLE_FORMATTING_TASKS_ROW,
            ),
          ]),
          "initial delayed formatting tasks",
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
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/uuid1",
                r2KeyDeleteTaskExecutionTimestamp: ONE_YEAR_MS + 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/uuid2",
                r2KeyDeleteTaskExecutionTimestamp: ONE_YEAR_MS + 1000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
          ]),
          "R2 key delete tasks",
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
            updateVideoContainerStatement("container1", videoContainerData),
          ]);
          await transaction.commit();
        });

        // Execute
        stallResolveFn();
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
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
          await getSubtitleFormattingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                subtitleFormattingTaskContainerId: "container1",
                subtitleFormattingTaskGcsFilename: "two_subs.zip",
                subtitleFormattingTaskExecutionTimestamp: 301000,
              },
              GET_SUBTITLE_FORMATTING_TASKS_ROW,
            ),
          ]),
          "remained delayed formatting tasks",
        );
        assertThat(
          await getR2KeyDeleteTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/uuid1",
                r2KeyDeleteTaskExecutionTimestamp: 302000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
            eqMessage(
              {
                r2KeyDeleteTaskKey: "root/uuid2",
                r2KeyDeleteTaskExecutionTimestamp: 302000,
              },
              GET_R2_KEY_DELETE_TASKS_ROW,
            ),
          ]),
          "remained R2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
