# dumperify

## Create Google Service Credentials

- Go to Google Console [https://console.cloud.google.com/apis/credentials](https://console.cloud.google.com/apis/credentials)
- Enable Google Drive API [https://console.cloud.google.com/apis/library/drive.googleapis.com](https://console.cloud.google.com/apis/library/drive.googleapis.com)
- Create a Service Account
- Create a json key
- Delegate domain in [admin.google.com](https://admin.google.com/u/1/ac/owl/domainwidedelegation), set Client ID and OAuth permissions [Ex: https://www.googleapis.com/auth/drive.file](https://www.googleapis.com/auth/drive.file)

## Environment setup

- Set database values
- Create Google Drive folder
- Add service account email as user with permissions in the Google Drive folder
- Add Folder ID in environment file

## Requirements

- mysqldump
