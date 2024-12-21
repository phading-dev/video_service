import { SPANNER_DATABASE } from "../common/spanner_database";
import {
  deleteMediaFormattingTaskStatement,
  deleteSubtitleFormattingTaskStatement,
  deleteVideoContainerStatement,
  deleteVideoContainerSyncingTaskStatement,
  deleteVideoContainerWritingToFileTaskStatement,
  getVideoContainer,
  insertGcsFileDeletingTaskStatement,
  insertR2KeyDeletingTaskStatement,
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
      let videoContainerRows = await getVideoContainer(
        this.database,
        body.containerId,
      );
      if (videoContainerRows.length === 0) {
        console.log(
          loggingPrefix,
          `Video container ${body.containerId} has been deleted.`,
        );
        return;
      }

      let videoContainer = videoContainerRows[0].videoContainerData;
      let now = this.getNow();
      let statements: Array<Statement> = [
        deleteVideoContainerStatement(body.containerId),
      ];

      if (videoContainer.masterPlaylist.synced) {
        let synced = videoContainer.masterPlaylist.synced;
        statements.push(
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${synced.r2Filename}`,
            now,
            now,
          ),
        );
      } else if (videoContainer.masterPlaylist.syncing) {
        let syncing = videoContainer.masterPlaylist.syncing;
        statements.push(
          deleteVideoContainerSyncingTaskStatement(
            body.containerId,
            syncing.version,
          ),
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${syncing.r2Filename}`,
            now,
            now,
          ),
          ...syncing.r2FilenamesToDelete.map((filename) =>
            insertR2KeyDeletingTaskStatement(
              `${videoContainer.r2RootDirname}/${filename}`,
              now,
              now,
            ),
          ),
          ...syncing.r2DirnamesToDelete.map((dirname) =>
            insertR2KeyDeletingTaskStatement(
              `${videoContainer.r2RootDirname}/${dirname}`,
              now,
              now,
            ),
          ),
        );
      } else if (videoContainer.masterPlaylist.writingToFile) {
        let writingToFile = videoContainer.masterPlaylist.writingToFile;
        statements.push(
          deleteVideoContainerWritingToFileTaskStatement(
            body.containerId,
            writingToFile.version,
          ),

          ...writingToFile.r2FilenamesToDelete.map((filename) =>
            insertR2KeyDeletingTaskStatement(
              `${videoContainer.r2RootDirname}/${filename}`,
              now,
              now,
            ),
          ),
          ...writingToFile.r2DirnamesToDelete.map((dirname) =>
            insertR2KeyDeletingTaskStatement(
              `${videoContainer.r2RootDirname}/${dirname}`,
              now,
              now,
            ),
          ),
        );
      }

      if (videoContainer.processing) {
        let processing = videoContainer.processing;
        if (processing.media?.uploading) {
          let uploading = processing.media.uploading;
          statements.push(
            insertGcsFileDeletingTaskStatement(
              uploading.gcsFilename,
              {
                uploadSessionUrl: uploading.uploadSessionUrl,
              },
              now,
              now,
            ),
          );
        } else if (processing.media?.formatting) {
          let formatting = processing.media.formatting;
          statements.push(
            deleteMediaFormattingTaskStatement(
              body.containerId,
              formatting.gcsFilename,
            ),
            insertGcsFileDeletingTaskStatement(
              formatting.gcsFilename,
              {},
              now,
              now,
            ),
          );
        } else if (processing.subtitle?.uploading) {
          let uploading = processing.subtitle.uploading;
          statements.push(
            insertGcsFileDeletingTaskStatement(
              uploading.gcsFilename,
              {
                uploadSessionUrl: uploading.uploadSessionUrl,
              },
              now,
              now,
            ),
          );
        } else if (processing.subtitle?.formatting) {
          let formatting = processing.subtitle.formatting;
          statements.push(
            deleteSubtitleFormattingTaskStatement(
              body.containerId,
              formatting.gcsFilename,
            ),
            insertGcsFileDeletingTaskStatement(
              formatting.gcsFilename,
              {},
              now,
              now,
            ),
          );
        }
      }

      for (let videoTrack of videoContainer.videoTracks) {
        statements.push(
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${videoTrack.r2TrackDirname}`,
            now,
            now,
          ),
        );
      }
      for (let audioTrack of videoContainer.audioTracks) {
        statements.push(
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${audioTrack.r2TrackDirname}`,
            now,
            now,
          ),
        );
      }
      for (let subtitleTrack of videoContainer.subtitleTracks) {
        statements.push(
          insertR2KeyDeletingTaskStatement(
            `${videoContainer.r2RootDirname}/${subtitleTrack.r2TrackDirname}`,
            now,
            now,
          ),
        );
      }

      await transaction.batchUpdate(statements);
      await transaction.commit();
    });
    return {};
  }
}
