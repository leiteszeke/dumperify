import { drive_v3, google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import mysqldump from "mysqldump";
import { format } from "date-fns/format";
import * as cron from "node-cron";
import * as Databases from "./databases.json";
import { exec } from "child_process";
import { authorize, deleteOldBackups, uploadToDrive } from "./drive";

// Configuration
const BACKUP_FOLDER = "./backups";

const IS_LOCAL = process.env.NODE_ENV === "local";

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

            await uploadToDrive(auth, [backupFilePath], dbConfig.folderId);

            await deleteOldBackups(
              auth,
              dbConfig.folderId,
              dbConfig.name,
              dbConfig.maxDumpLimit
            );

            if (!IS_LOCAL) {
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

        await uploadToDrive(auth, [backupFilePath], dbConfig.folderId);

        await deleteOldBackups(
          auth,
          dbConfig.folderId,
          dbConfig.name,
          dbConfig.maxDumpLimit
        );

        if (!IS_LOCAL) {
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
