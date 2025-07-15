export type GoogleConfig = {
  googleEmail?: string;
  credentialsPath: string;
  folderId: string;
};

export type GenericConfig = {
  name: string;
  cronTime: string;
  maxDumpLimit?: number;
};

export type LogConfig = GoogleConfig &
  GenericConfig & {
    apiKey: string;
    sourceId: string;
  };

export type DbConfig = GoogleConfig &
  GenericConfig & {
    host: string;
    user: string;
    password: string;
    database: string;
    port: number;
  };
