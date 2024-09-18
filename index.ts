require("dotenv").config();

import { drive_v3, google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import * as pkey from "./credentials.json";
import mysqldump from "mysqldump";
import { format } from "date-fns/format";

// const execAsync = promisify(exec);

// Configuration
const BACKUP_FOLDER = "./backups";
const MAX_DUMP_LIMIT = 2;
const DRIVE_FOLDER_ID = process.env.GDRIVE_FOLDER_ID;
const DB_HOST = process.env.MYSQL_HOST;
const DB_USER = process.env.MYSQL_USER;
const DB_PASS = process.env.MYSQL_PASSWORD;
const DB_NAME = process.env.MYSQL_DATABASE;
const DB_PORT = process.env.MYSQL_PORT;

const SCOPES = ["https://www.googleapis.com/auth/drive.file"];

async function authorize() {
  const jwtClient = new google.auth.JWT(
    pkey.client_email,
    "",
    pkey.private_key,
    SCOPES
  );

  await jwtClient.authorize();

  return jwtClient;
}

const uploadToDrive = async (
  auth: drive_v3.Options["auth"],
  filePath: string
) => {
  console.log("Upload dump to Google Drive: Starting", filePath);

  const drive = google.drive({ version: "v3", auth });

  const file = await drive.files.create({
    media: {
      body: fs.createReadStream(filePath),
    },
    fields: "id",
    requestBody: {
      name: path.basename(filePath),
      parents: [DRIVE_FOLDER_ID],
    },
  });

  const fileId = file.data.id;

  console.log("Upload dump to Google Drive: Success", fileId);
};

async function deleteOldBackups(auth: drive_v3.Options["auth"]) {
  console.log("Deleting old backups: Starting");

  const drive = google.drive({ version: "v3", auth });

  try {
    const response = await drive.files.list({
      q: `'${DRIVE_FOLDER_ID}' in parents`,
      fields: "files(id, name, mimeType, createdTime)",
      orderBy: "createdTime desc",
    });

    const files = response.data.files;

    if (files.length > MAX_DUMP_LIMIT) {
      for (let i = MAX_DUMP_LIMIT; i < files.length; i++) {
        await drive.files.delete({ fileId: files[i].id });

        console.log(`Deleted old backup: ${files[i].name}`);
      }
    }

    console.log("Deleting old backups: Completed");
  } catch (error) {
    console.error("Error deleting old backups:", error);
  }
}

async function createDatabaseDump(): Promise<string> {
  const timestamp = format(new Date(), "yyyy_MM_dd_HH_mm_ss");
  const backupFileName = `${DB_NAME}_backup_${timestamp}.sql`;
  const backupFilePath = path.join(BACKUP_FOLDER, backupFileName);

  // const command = `mysqldump -h ${DB_HOST} -P${DB_PORT} -u${DB_USER} -p${DB_PASS} ${DB_NAME} > ${backupFilePath}`;

  try {
    console.log(`Creating dump in ${backupFilePath}`);

    // await execAsync(command);
    await mysqldump({
      connection: {
        host: DB_HOST,
        user: DB_USER,
        password: DB_PASS,
        database: DB_NAME,
        port: Number(DB_PORT),
      },
      dumpToFile: backupFilePath,
    });

    console.log(`Database dump created: ${backupFilePath}`);

    return backupFilePath;
  } catch (error) {
    console.error("Error creating database dump:", error);

    throw error;
  }
}

const main = async (auth: drive_v3.Options["auth"]) => {
  try {
    const backupFilePath = await createDatabaseDump();

    await uploadToDrive(auth, backupFilePath);

    await deleteOldBackups(auth);

    fs.rmSync(backupFilePath);

    console.log(
      "Backup created and uploaded successfully. Old backups cleaned up. Local file was deleted."
    );
  } catch (error) {
    console.error("Error during backup and upload process:", error);
  }
};

authorize().then(main);
