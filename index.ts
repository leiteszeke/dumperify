import { drive_v3, google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import mysqldump from "mysqldump";
import { format } from "date-fns/format";
import * as cron from "node-cron";
import * as Databases from "./databases.json";
import { exec } from "child_process";

// Configuration
const BACKUP_FOLDER = "./backups";
const MAX_DUMP_LIMIT = 2;

const SCOPES = ["https://www.googleapis.com/auth/drive.file"];

const IS_LOCAL = process.env.NODE_ENV === "local";

type DbConfig = {
  credentialsPath?: string;
  googleEmail?: string;
  name: string;
  host: string;
  user: string;
  password: string;
  database: string;
  port: number;
  folderId: string;
  cronTime: string;
};

async function authorize(dbConfig: DbConfig) {
  const pkey = require(dbConfig.credentialsPath ?? "./credentials.json");

  const jwtClient = new google.auth.JWT(
    pkey.client_email,
    "",
    pkey.private_key,
    SCOPES,
    dbConfig.googleEmail ?? "ezequiel@leites.dev"
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

  return true;
};

async function deleteOldBackups(
  auth: drive_v3.Options["auth"],
  folderId: string,
  fileName: string
) {
  console.log("Deleting old backups: Starting");

  const drive = google.drive({ version: "v3", auth });

  try {
    const response = await drive.files.list({
      q: `'${folderId}' in parents`,
      fields: "files(id, name, mimeType, createdTime)",
      orderBy: "createdTime desc",
    });

    const files = response.data.files.filter((f) =>
      f.name.startsWith(fileName)
    );

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
  const backupFileName = `${DatabaseConfig.name}_backup_${timestamp}.sql.gz`;
  const backupFilePath = path.join(BACKUP_FOLDER, backupFileName);

  try {
    console.log(`Creating dump in ${backupFilePath}`);

    mysqldump({
      connection: {
        host: DatabaseConfig.host,
        user: DatabaseConfig.user,
        password: DatabaseConfig.password,
        database: DatabaseConfig.database,
        port: Number(DatabaseConfig.port),
      },
      dumpToFile: backupFilePath,
      compressFile: true,
    });

    console.log(`Database dump created: ${backupFilePath}`);

    return backupFilePath;
  } catch (error) {
    console.error("Error creating database dump:", error);

    throw error;
  }
}

const createAndCompressDump = async (dbName: string): Promise<string> => {
  const DatabaseConfig = Databases.find((db) => db.name === dbName);

  if (!DatabaseConfig) {
    console.error(`Config for ${dbName} not found`);

    return;
  }

  const timestamp = format(new Date(), "yyyy_MM_dd_HH_mm_ss");
  const backupFileName = `${DatabaseConfig.name}_backup_${timestamp}.sql`;
  const backupFilePath = path.join(BACKUP_FOLDER, backupFileName);
  const compressedFilePath = `${backupFilePath}.gz`;

  // Comando mysqldump
  const dumpCommand = `mysqldump --column-statistics=0 -h ${DatabaseConfig.host} --port=${DatabaseConfig.port} -u ${DatabaseConfig.user} -p${DatabaseConfig.password} --single-transaction --lock-tables=false ${DatabaseConfig.database} | gzip > ${compressedFilePath}`;

  console.log(`Running mysqldump command`);

  return new Promise((resolve, reject) => {
    exec(dumpCommand, (error, stdout, stderr) => {
      if (error) {
        console.error("Error creating the dump:", error);
        reject(error);

        return;
      }

      console.log(
        "Dump successfully created and compressed at:",
        compressedFilePath
      );

      // Devuelve la ruta del archivo comprimido
      resolve(compressedFilePath);
    });
  });
};

const main = async () => {
  for (const dbConfig of Databases) {
    const auth = await authorize(dbConfig);

    if (!IS_LOCAL) {
      console.log(`Creating cron for ${dbConfig.name}`, dbConfig);

      cron.schedule(
        dbConfig.cronTime,
        async () => {
          console.log(`Running cron task for ${dbConfig.name}`, dbConfig);

          let backupFilePath = "";

          try {
            backupFilePath = await createAndCompressDump(dbConfig.name);

            console.log(`File created ${backupFilePath}`);

            await uploadToDrive(auth, backupFilePath, dbConfig.folderId);

            await deleteOldBackups(auth, dbConfig.folderId, dbConfig.name);

            if (process.env.NODE_ENV !== "local") {
              fs.rmSync(backupFilePath);
            }

            console.log(
              "Backup created and uploaded successfully. Old backups cleaned up"
            );
          } catch (error) {
            console.error("Error during backup and upload process:", error);
          } finally {
            console.log("Deleting local file:");

            fs.rmSync(backupFilePath);

            console.log("Local file deleted");
          }
        },
        {
          name: `backup-${dbConfig.name}`,
        }
      );
    } else {
      console.log(`Running dump for ${dbConfig.name}`, dbConfig);

      let backupFilePath = "";

      try {
        backupFilePath = await createAndCompressDump(dbConfig.name);

        console.log(`File created ${backupFilePath}`);

        await uploadToDrive(auth, backupFilePath, dbConfig.folderId);

        await deleteOldBackups(auth, dbConfig.folderId, dbConfig.name);

        if (process.env.NODE_ENV !== "local") {
          fs.rmSync(backupFilePath);
        }

        console.log(
          "Backup created and uploaded successfully. Old backups cleaned up"
        );
      } catch (error) {
        console.error("Error during backup and upload process:", error);
      } finally {
        console.log(`Deleting local file: ${backupFilePath}`);

        fs.rmSync(backupFilePath);

        console.log("Local file deleted");
      }
    }
  }
};

main();
