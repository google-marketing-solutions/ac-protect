# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import io
from typing import Any
from typing import Dict

from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from utils.auth import get_credentials

# Function to authenticate and create the Google Drive service
def create_service(config: Dict[str, Any]) -> Resource:
  scopes = ['https://www.googleapis.com/auth/drive.metadata.readonly']
  creds = get_credentials(config, scopes)

  service = build('drive', 'v3', credentials=creds)
  return service


def list_drive_folder(config: Dict[str, Any], folder_id: str) -> list:
  """Shows basic usage of the Drive v3 API.
  Prints the names and ids of the first 10 files the user has access to.
  """
  scopes = ['https://www.googleapis.com/auth/drive']

  creds = get_credentials(config, scopes)

  try:
    service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API
    results = (
        service.files()
        .list(pageSize=100, q=f"'{folder_id}' in parents", fields='nextPageToken, files(id, mimeType, name)')
        .execute()
    )
    items = results.get('files', [])
    return items
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f'An error occurred: {error}')


def download_file_from_drive(config, file_id, file_name):
  """ Download a file from Google Drive """
  print('Downloading file from Drive - ' + file_id)
  scopes = ['https://www.googleapis.com/auth/drive']
  creds = get_credentials(config, scopes)
  drive_service = build('drive', 'v3', credentials=creds)
  request = drive_service.files().get_media(fileId=file_id)

  fh = io.FileIO(file_name, mode='wb')
  downloader = MediaIoBaseDownload(fh, request)
  done = False
  while done is False:
    status, done = downloader.next_chunk()
    print('Download %d%%.' % int(status.progress() * 100))
