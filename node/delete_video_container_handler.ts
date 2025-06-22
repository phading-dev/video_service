import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteMediaFormattingTaskStatement,
  deleteMediaUploadingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertGcsKeyDeletingTaskStatement,
  insertGcsUploadFileDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
  insertStorageEndRecordingTaskStatement,
} from "../db/sql";
import { Database } from "@google-cloud/spanner";
import { Statement } from "@google-cloud/spanner/build/src/transaction";
import { DeleteVideoContainerHandlerInterface } from "@phading/video_service_interface/node/handler";
import {
  DeleteVideoContainerRequestBody,
  DeleteVideoContainerResponse,
} from "@phading/video_service_interface/node/interface";

export class DeleteVideoContainerHandler extends DeleteVideoContainerHandlerInterface {
  public static create(): DeleteVideoContainerHandler {
    return new DeleteVideoContainerHandler(SPANNER_DATABASE, () => Date.now());
  }

  public constructor(
    private database: Database,
    private getNow: () => number,
  ) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: DeleteVideoContainerRequestBody,
  ): Promise<DeleteVideoContainerResponse> {
    await this.database.runTransactionAsync(async (transaction) => {
      let videoContainerRows = await getVideoContainer(this.database, {
        videoContainerContainerIdEq: body.containerId,
      });
      if (videoContainerRows.length === 0) {
        console.log(
          loggingPrefix,
          `Video container ${body.containerId} has been deleted.`,
        );
        return;
      }

      let { videoContainerAccountId, videoContainerData } =
        videoContainerRows[0];
      let now = this.getNow();
      let statements: Array<Statement> = [
        deleteVideoContainerStatement({
          videoContainerContainerIdEq: body.containerId,
        }),
      ];

      if (videoContainerData.masterPlaylist.synced) {
        let synced = videoContainerData.masterPlaylist.synced;
        statements.push(
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${synced.r2Filename}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        );
      } else if (videoContainerData.masterPlaylist.syncing) {
        let syncing = videoContainerData.masterPlaylist.syncing;
        statements.push(
          deleteVideoContainerSyncingTaskStatement({
            videoContainerSyncingTaskContainerIdEq: body.containerId,
            videoContainerSyncingTaskVersionEq: syncing.version,
          }),
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${syncing.r2Filename}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
          ...syncing.r2FilenamesToDelete.map((filename) =>
            insertR2KeyDeletingTaskStatement({
              key: `${videoContainerData.r2RootDirname}/${filename}`,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          ),
          ...syncing.r2DirnamesToDelete.map((dirname) =>
            insertStorageEndRecordingTaskStatement({
              r2Dirname: `${videoContainerData.r2RootDirname}/${dirname}`,
              payload: {
                accountId: videoContainerAccountId,
                endTimeMs: now,
              },
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          ),
          ...syncing.r2DirnamesToDelete.map((dirname) =>
            insertR2KeyDeletingTaskStatement({
              key: `${videoContainerData.r2RootDirname}/${dirname}`,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          ),
        );
      } else if (videoContainerData.masterPlaylist.writingToFile) {
        let writingToFile = videoContainerData.masterPlaylist.writingToFile;
        statements.push(
          deleteVideoContainerWritingToFileTaskStatement({
            videoContainerWritingToFileTaskContainerIdEq: body.containerId,
            videoContainerWritingToFileTaskVersionEq: writingToFile.version,
          }),

          ...writingToFile.r2FilenamesToDelete.map((filename) =>
            insertR2KeyDeletingTaskStatement({
              key: `${videoContainerData.r2RootDirname}/${filename}`,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          ),
          ...writingToFile.r2DirnamesToDelete.map((dirname) =>
            insertStorageEndRecordingTaskStatement({
              r2Dirname: `${videoContainerData.r2RootDirname}/${dirname}`,
              payload: {
                accountId: videoContainerAccountId,
                endTimeMs: now,
              },
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          ),
          ...writingToFile.r2DirnamesToDelete.map((dirname) =>
            insertR2KeyDeletingTaskStatement({
              key: `${videoContainerData.r2RootDirname}/${dirname}`,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          ),
        );
      }

      if (videoContainerData.processing) {
        let processing = videoContainerData.processing;
        if (processing.uploading) {
          let uploading = processing.uploading;
          statements.push(
            insertGcsUploadFileDeletingTaskStatement({
              filename: uploading.gcsFilename,
              uploadSessionUrl: uploading.uploadSessionUrl,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          );
        } else if (processing.mediaFormatting) {
          let formatting = processing.mediaFormatting;
          statements.push(
            deleteMediaFormattingTaskStatement({
              mediaFormattingTaskContainerIdEq: body.containerId,
              mediaFormattingTaskGcsFilenameEq: formatting.gcsFilename,
            }),
            insertGcsKeyDeletingTaskStatement({
              key: formatting.gcsFilename,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          );
        } else if (processing.mediaUploading) {
          let uploading = processing.mediaUploading;
          statements.push(
            deleteMediaUploadingTaskStatement({
              mediaUploadingTaskContainerIdEq: body.containerId,
              mediaUploadingTaskGcsDirnameEq: uploading.gcsDirname,
            }),
            insertGcsKeyDeletingTaskStatement({
              key: uploading.gcsDirname,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          );
        } else if (processing.subtitleFormatting) {
          let formatting = processing.subtitleFormatting;
          statements.push(
            deleteSubtitleFormattingTaskStatement({
              subtitleFormattingTaskContainerIdEq: body.containerId,
              subtitleFormattingTaskGcsFilenameEq: formatting.gcsFilename,
            }),
            insertGcsKeyDeletingTaskStatement({
              key: formatting.gcsFilename,
              retryCount: 0,
              executionTimeMs: now,
              createdTimeMs: now,
            }),
          );
        }
      }

      for (let videoTrack of videoContainerData.videoTracks) {
        statements.push(
          insertStorageEndRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${videoTrack.r2TrackDirname}`,
            payload: {
              accountId: videoContainerAccountId,
              endTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${videoTrack.r2TrackDirname}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        );
      }
      for (let audioTrack of videoContainerData.audioTracks) {
        statements.push(
          insertStorageEndRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${audioTrack.r2TrackDirname}`,
            payload: {
              accountId: videoContainerAccountId,
              endTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${audioTrack.r2TrackDirname}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        );
      }
      for (let subtitleTrack of videoContainerData.subtitleTracks) {
        statements.push(
          insertStorageEndRecordingTaskStatement({
            r2Dirname: `${videoContainerData.r2RootDirname}/${subtitleTrack.r2TrackDirname}`,
            payload: {
              accountId: videoContainerAccountId,
              endTimeMs: now,
            },
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
          insertR2KeyDeletingTaskStatement({
            key: `${videoContainerData.r2RootDirname}/${subtitleTrack.r2TrackDirname}`,
            retryCount: 0,
            executionTimeMs: now,
            createdTimeMs: now,
          }),
        );
      }

      await transaction.batchUpdate(statements);
      await transaction.commit();
    });
    return {};
  }
}
