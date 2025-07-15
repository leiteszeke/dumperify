import { drive_v3, google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import { GoogleConfig } from "./types";
import { sortBy } from "lodash";

const SCOPES = ["https://www.googleapis.com/auth/drive.file"];

const MAX_DUMP_LIMIT = 2;

export async function authorize(dbConfig: GoogleConfig) {
  const pkey = require(dbConfig.credentialsPath ?? "./credentials.json");

  const jwtClient = new google.auth.JWT({
    email: pkey.client_email,
    key: pkey.private_key,
    scopes: SCOPES,
    subject: dbConfig.googleEmail ?? "ezequiel@leites.dev",
  });

  await jwtClient.authorize();

  return jwtClient;
}

export const uploadToDrive = async (
  auth: drive_v3.Options["auth"],
  filePaths: string[],
  folderId: string
) => {
  const drive = google.drive({ version: "v3", auth });

  for (const filePath of filePaths) {
    console.log("Upload dump to Google Drive: Starting", filePath);

    const file = await drive.files.create({
      media: {
        body: fs.createReadStream(filePath),
      },
      fields: "id",
      requestBody: {
        name: path.basename(filePath),
        parents: [folderId],
      },
    });

    const fileId = file.data.id;

    console.log("Upload dump to Google Drive: Success", fileId);
  }

  return true;
};

export async function deleteOldBackups(
  auth: drive_v3.Options["auth"],
  folderId: string,
  fileName: string,
  maxDumpLimit: number = MAX_DUMP_LIMIT
) {
  console.log("Deleting old backups: Starting");

  const drive = google.drive({ version: "v3", auth });

  try {
    const response = await drive.files.list({
      q: `'${folderId}' in parents`,
      fields: "files(id, name, mimeType, createdTime)",
      orderBy: "createdTime desc",
    });

    const files = sortBy(
      response.data.files.filter((f) => f.name.startsWith(fileName)),
      "name"
    ).reverse();

    if (files.length > maxDumpLimit) {
      for (let i = maxDumpLimit; i < files.length; i++) {
        await drive.files.delete({ fileId: files[i].id });

        console.log(`Deleted old backup: ${files[i].name}`);
      }
    }

    console.log("Deleting old backups: Completed");
  } catch (error) {
    console.error("Error deleting old backups:", error);
  }
}
