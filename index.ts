import { drive_v3, google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import * as pkey from "./credentials.json";
import mysqldump from "mysqldump";
import { format } from "date-fns/format";
import * as cron from "node-cron";
import * as Databases from "./databases.json";

// Configuration
const BACKUP_FOLDER = "./backups";
const MAX_DUMP_LIMIT = 2;

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
  filePath: string,
  folderId: string
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
      parents: [folderId],
    },
  });

  const fileId = file.data.id;

  console.log("Upload dump to Google Drive: Success", fileId);
};

async function deleteOldBackups(
  auth: drive_v3.Options["auth"],
  folderId: string
) {
  console.log("Deleting old backups: Starting");

  const drive = google.drive({ version: "v3", auth });

  try {
    const response = await drive.files.list({
      q: `'${folderId}' in parents`,
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

async function createDatabaseDump(dbName: string): Promise<string> {
  const DatabaseConfig = Databases.find((db) => db.name === dbName);

  if (!DatabaseConfig) {
    console.error(`Config for ${dbName} not found`);

    return;
  }

  const timestamp = format(new Date(), "yyyy_MM_dd_HH_mm_ss");
  const backupFileName = `${DatabaseConfig.name}_backup_${timestamp}.sql`;
  const backupFilePath = path.join(BACKUP_FOLDER, backupFileName);

  try {
    console.log(`Creating dump in ${backupFilePath}`);

    await mysqldump({
      connection: {
        host: DatabaseConfig.host,
        user: DatabaseConfig.user,
        password: DatabaseConfig.password,
        database: DatabaseConfig.database,
        port: Number(DatabaseConfig.port),
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
  for (const dbConfig of Databases) {
    console.log(`Creating cron for ${dbConfig.name}`, dbConfig);

    cron.schedule(
      dbConfig.cronTime,
      async () => {
        try {
          console.log(`Running cron task for ${dbConfig.name}`, dbConfig);

          const backupFilePath = await createDatabaseDump(dbConfig.name);

          await uploadToDrive(auth, backupFilePath, dbConfig.folderId);

          await deleteOldBackups(auth, dbConfig.folderId);

          fs.rmSync(backupFilePath);

          console.log(
            "Backup created and uploaded successfully. Old backups cleaned up. Local file was deleted."
          );
        } catch (error) {
          console.error("Error during backup and upload process:", error);
        }
      },
      {
        name: `backup-${dbConfig.name}`,
      }
    );
  }
};

authorize().then(main);
