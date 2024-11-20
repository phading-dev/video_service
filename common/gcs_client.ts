import { PROJECT_ID } from "./env_vars";
import { GoogleAuth } from "google-auth-library";

export interface ResumableUploadProgress {
  urlValid: boolean;
  byteOffset: number;
}

export class CloudStorageClient {
  public static create(projectId?: string): CloudStorageClient {
    return new CloudStorageClient(
      new GoogleAuth({
        projectId,
        scopes: "https://www.googleapis.com/auth/cloud-platform",
      }),
    );
  }

  private static STORAGE_API_DOMAIN = `https://storage.googleapis.com`;
  private static INCOMPLETE_ERROR_CODE = 308;
  private static EXTRACT_BYTE_OFFSET_REGEX = /^bytes=[0-9]+?-([0-9]+?)$/;

  public constructor(
    private googleAuth: GoogleAuth,
    private storageApiDomain = CloudStorageClient.STORAGE_API_DOMAIN,
  ) {}

  public async createResumableUploadUrl(
    bucketName: string,
    filename: string,
    contentType: string,
    contentLength: number,
  ): Promise<string> {
    let response = await this.googleAuth.request({
      method: "POST",
      url: `${this.storageApiDomain}/upload/storage/v1/b/${bucketName}/o?uploadType=resumable&name=${filename}`,
      headers: {
        "Content-Length": 0,
        "X-Upload-Content-Type": contentType,
        "X-Upload-Content-Length": contentLength,
      },
    });
    return response.headers.location;
  }

  public async checkResumableUploadProgress(
    contentLength: number,
    uploadSessionUrl?: string,
  ): Promise<ResumableUploadProgress> {
    if (!uploadSessionUrl) {
      return {
        urlValid: false,
        byteOffset: 0,
      };
    }
    try {
      await this.googleAuth.request({
        method: "PUT",
        url: uploadSessionUrl,
        headers: {
          "Content-Length": 0,
          "Content-Range": `bytes */${contentLength}`,
        },
      });
    } catch (e) {
      if (e.status === CloudStorageClient.INCOMPLETE_ERROR_CODE) {
        let range = e.response.headers.range;
        let matched = CloudStorageClient.EXTRACT_BYTE_OFFSET_REGEX.exec(range);
        if (!matched) {
          return {
            urlValid: true,
            byteOffset: 0,
          };
        } else {
          return {
            urlValid: true,
            byteOffset: parseInt(matched[1]) + 1,
          };
        }
      } else if (e.status >= 400 && e.status < 500) {
        return {
          urlValid: false,
          byteOffset: 0,
        };
      } else {
        throw e;
      }
    }
    // Code 2xx Completed
    return {
      urlValid: true,
      byteOffset: contentLength,
    };
  }
}

export let CLOUD_STORAGE_CLIENT = CloudStorageClient.create(PROJECT_ID);
