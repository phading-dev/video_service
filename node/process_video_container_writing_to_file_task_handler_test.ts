import { LOCAL_MASTER_PLAYLIST_NAME } from "../common/constants";
import { R2_VIDEO_REMOTE_BUCKET } from "../common/env_vars";
import { FILE_UPLOADER } from "../common/r2_file_uploader";
import { S3_CLIENT } from "../common/s3_client";
import { SPANNER_DATABASE } from "../common/spanner_database";
import { VideoContainerData } from "../db/schema";
import {
  GET_VIDEO_CONTAINER_ROW,
  LIST_R2_KEY_DELETING_TASKS_ROW,
  LIST_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
  LIST_VIDEO_CONTAINER_WRITING_TO_FILE_TASKS_ROW,
  checkR2Key,
  deleteR2KeyDeletingTaskStatement,
  deleteR2KeyStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertVideoContainerStatement,
  insertVideoContainerWritingToFileTaskStatement,
  listR2KeyDeletingTasks,
  listVideoContainerSyncingTasks,
  listVideoContainerWritingToFileTasks,
  updateVideoContainerStatement,
} from "../db/sql";
import { ProcessVideoContainerWritingToFileTaskHandler } from "./process_video_container_writing_to_file_task_handler";
import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { newConflictError } from "@selfage/http_error";
import { eqHttpError } from "@selfage/http_error/test_matcher";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertReject, assertThat, eq, isArray } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";
import { createReadStream } from "fs";

let ONE_YEAR_MS = 365 * 24 * 60 * 60 * 1000;
let TWO_YEAR_MS = 2 * 365 * 24 * 60 * 60 * 1000;

async function insertVideoContainer(videoContainerData: VideoContainerData) {
  await S3_CLIENT.send(
    new PutObjectCommand({
      Bucket: R2_VIDEO_REMOTE_BUCKET,
      Key: `root/video1/${LOCAL_MASTER_PLAYLIST_NAME}`,
      Body: createReadStream("test_data/video_only_master.m3u8"),
    }),
  );
  await SPANNER_DATABASE.runTransactionAsync(async (transaction) => {
    await transaction.batchUpdate([
      insertVideoContainerStatement("container1", videoContainerData),
      ...(videoContainerData.masterPlaylist.writingToFile
        ? [
            insertVideoContainerWritingToFileTaskStatement(
              "container1",
              videoContainerData.masterPlaylist.writingToFile.version,
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
      deleteVideoContainerWritingToFileTaskStatement("container1", 1),
      deleteVideoContainerSyncingTaskStatement("container1", 1),
      deleteVideoContainerWritingToFileTaskStatement("container1", 2),
      deleteVideoContainerSyncingTaskStatement("container1", 2),
      deleteR2KeyStatement("root/uuid0.m3u8"),
      deleteR2KeyDeletingTaskStatement("root/uuid0.m3u8"),
    ]);
    await transaction.commit();
  });
  await S3_CLIENT.send(
    new DeleteObjectCommand({
      Bucket: R2_VIDEO_REMOTE_BUCKET,
      Key: "root/uuid0.m3u8",
    }),
  );
  await S3_CLIENT.send(
    new DeleteObjectCommand({
      Bucket: R2_VIDEO_REMOTE_BUCKET,
      Key: `root/video1/${LOCAL_MASTER_PLAYLIST_NAME}`,
    }),
  );
}

TEST_RUNNER.run({
  name: "ProcessVideoContainerWritingToFileTaskHandlerTest",
  cases: [
    {
      name: "ProcessOneVideoAndTwoAudiosAndTwoSubtitlesAndStagingTracks",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: ["file1"],
              r2DirnamesToDelete: ["dir1"],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "1920x1080",
                totalBytes: 2000000,
              },
              staging: {
                toDelete: true,
              },
            },
            {
              r2TrackDirname: "video2",
              staging: {
                toAdd: {
                  durationSec: 120,
                  resolution: "1920x1080",
                  totalBytes: 2000000,
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
                totalBytes: 123,
              },
            },
            {
              r2TrackDirname: "audio2",
              staging: {
                toAdd: {
                  name: "Jpn",
                  isDefault: false,
                  totalBytes: 456,
                },
              },
            },
            {
              r2TrackDirname: "audio3",
              committed: {
                name: "Kor",
                isDefault: false,
                totalBytes: 789,
              },
              staging: {
                toAdd: {
                  name: "Chn",
                  isDefault: false,
                  totalBytes: 101112,
                },
              },
            },
          ],
          subtitleTracks: [
            {
              r2TrackDirname: "subtitle1",
              committed: {
                name: "Eng",
                isDefault: true,
                totalBytes: 123,
              },
            },
            {
              r2TrackDirname: "subtitle2",
              staging: {
                toAdd: {
                  name: "Jpn",
                  isDefault: false,
                  totalBytes: 456,
                },
              },
            },
            {
              r2TrackDirname: "subtitle3",
              committed: {
                name: "Kor",
                isDefault: false,
                totalBytes: 789,
              },
              staging: {
                toAdd: {
                  name: "Chn",
                  isDefault: false,
                  totalBytes: 101112,
                },
              },
            },
          ],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 1,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await (
            await S3_CLIENT.send(
              new GetObjectCommand({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
                Key: "root/uuid0.m3u8",
              }),
            )
          ).Body.transformToString(),
          eq(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=669168,RESOLUTION=640x360,CODECS="avc1.4d401e"
video1/o.m3u8


#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Eng",DEFAULT=YES,AUTOSELECT=NO,URI="audio1/o.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="Kor",DEFAULT=NO,AUTOSELECT=NO,URI="audio3/o.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Eng",DEFAULT=YES,AUTOSELECT=NO,URI="subtitle1/o.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="Kor",DEFAULT=NO,AUTOSELECT=NO,URI="subtitle3/o.m3u8"`),
          "master playlist content",
        );
        videoContainerData.masterPlaylist = {
          syncing: {
            r2Filename: "uuid0.m3u8",
            version: 1,
            r2FilenamesToDelete: ["file1"],
            r2DirnamesToDelete: ["dir1"],
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
          await listVideoContainerWritingToFileTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([]),
          "writing to file tasks",
        );
        assertThat(
          await listVideoContainerSyncingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 1,
                videoContainerSyncingTaskExecutionTimestamp: 1000,
              },
              LIST_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
            ),
          ]),
          "syncing tasks",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid0.m3u8")).length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "ProcessOneVideoOnly",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 2,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "1920x1080",
                totalBytes: 2000000,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 2,
        });
        await new Promise<void>((resolve) => (handler.doneCallback = resolve));

        // Verify
        assertThat(
          await (
            await S3_CLIENT.send(
              new GetObjectCommand({
                Bucket: R2_VIDEO_REMOTE_BUCKET,
                Key: "root/uuid0.m3u8",
              }),
            )
          ).Body.transformToString(),
          eq(`#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=669168,RESOLUTION=640x360,CODECS="avc1.4d401e"
video1/o.m3u8

`),
          "master playlist content",
        );
        videoContainerData.masterPlaylist = {
          syncing: {
            r2Filename: "uuid0.m3u8",
            version: 2,
            r2FilenamesToDelete: [],
            r2DirnamesToDelete: [],
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
          await listVideoContainerWritingToFileTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([]),
          "writing to file tasks",
        );
        assertThat(
          await listVideoContainerSyncingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                videoContainerSyncingTaskContainerId: "container1",
                videoContainerSyncingTaskVersion: 2,
                videoContainerSyncingTaskExecutionTimestamp: 1000,
              },
              LIST_VIDEO_CONTAINER_SYNCING_TASKS_ROW,
            ),
          ]),
          "syncing tasks",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid0.m3u8")).length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
    {
      name: "NotInWritingState",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          r2RootDirname: "root",
          masterPlaylist: {
            syncing: {
              r2Filename: "uuid0.m3u8",
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
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
            newConflictError(
              "container container1 is not in writing to file state",
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
      name: "WritingInterruptedUnexpectedly",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => 1000,
          () => `uuid${id++}`,
        );
        handler.interfereFn = () => {
          throw new Error("fake error");
        };

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
          await listVideoContainerWritingToFileTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskContainerId: "container1",
                videoContainerWritingToFileTaskVersion: 1,
                videoContainerWritingToFileTaskExecutionTimestamp: 301000,
              },
              LIST_VIDEO_CONTAINER_WRITING_TO_FILE_TASKS_ROW,
            ),
          ]),
          "writing to file tasks",
        );
        assertThat(
          await listVideoContainerSyncingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([]),
          "syncing tasks",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid0.m3u8")).length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0.m3u8",
                r2KeyDeletingTaskExecutionTimestamp: 301000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
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
      name: "StalledWriting_ResumeButVersionChanged",
      execute: async () => {
        // Prepare
        let videoContainerData: VideoContainerData = {
          r2RootDirname: "root",
          masterPlaylist: {
            writingToFile: {
              version: 1,
              r2FilenamesToDelete: [],
              r2DirnamesToDelete: [],
            },
          },
          videoTracks: [
            {
              r2TrackDirname: "video1",
              committed: {
                durationSec: 60,
                resolution: "1920x1080",
                totalBytes: 2000000,
              },
            },
          ],
          audioTracks: [],
          subtitleTracks: [],
        };
        await insertVideoContainer(videoContainerData);
        let id = 0;
        let now = 1000;
        let handler = new ProcessVideoContainerWritingToFileTaskHandler(
          SPANNER_DATABASE,
          S3_CLIENT,
          FILE_UPLOADER,
          () => now,
          () => `uuid${id++}`,
        );
        let stallResolveFn: () => void;
        let firstEncounter = new Promise<void>((firstEncounterResolve) => {
          handler.interfereFn = () => {
            firstEncounterResolve();
            return new Promise<void>(
              (stallResolve) => (stallResolveFn = stallResolve),
            );
          };
        });

        // Execute
        handler.handle("", {
          containerId: "container1",
          version: 1,
        });
        await firstEncounter;

        // Verify
        assertThat(
          await listVideoContainerWritingToFileTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskContainerId: "container1",
                videoContainerWritingToFileTaskVersion: 1,
                videoContainerWritingToFileTaskExecutionTimestamp: 301000,
              },
              LIST_VIDEO_CONTAINER_WRITING_TO_FILE_TASKS_ROW,
            ),
          ]),
          "delayed writing to file tasks",
        );
        assertThat(
          (await checkR2Key(SPANNER_DATABASE, "root/uuid0.m3u8")).length,
          eq(1),
          "r2 key for master playlist exists",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0.m3u8",
                r2KeyDeletingTaskExecutionTimestamp: ONE_YEAR_MS + 1000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
          ]),
          "r2 key delete tasks",
        );

        // Prepare
        now = 2000;
        videoContainerData.masterPlaylist.writingToFile.version = 2;
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
          await listVideoContainerWritingToFileTasks(
            SPANNER_DATABASE,
            TWO_YEAR_MS,
          ),
          isArray([
            eqMessage(
              {
                videoContainerWritingToFileTaskContainerId: "container1",
                videoContainerWritingToFileTaskVersion: 1,
                videoContainerWritingToFileTaskExecutionTimestamp: 301000,
              },
              LIST_VIDEO_CONTAINER_WRITING_TO_FILE_TASKS_ROW,
            ),
          ]),
          "remained writing to file tasks",
        );
        assertThat(
          await listR2KeyDeletingTasks(SPANNER_DATABASE, TWO_YEAR_MS),
          isArray([
            eqMessage(
              {
                r2KeyDeletingTaskKey: "root/uuid0.m3u8",
                r2KeyDeletingTaskExecutionTimestamp: 302000,
              },
              LIST_R2_KEY_DELETING_TASKS_ROW,
            ),
          ]),
          "remained r2 key delete tasks",
        );
      },
      tearDown: async () => {
        await cleanupAll();
      },
    },
  ],
});
