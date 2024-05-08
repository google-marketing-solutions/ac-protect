# Copyright 2023 Google LLC
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
''' Environment variables used by the project'''
import pathlib
from os import getenv

from dotenv import find_dotenv
from dotenv import load_dotenv

PROJECT_ID = getenv('PROJECT_ID', '')
APP_ENGINE_URL = getenv('APP_ENGINE_URL', '')
DEBUG_MODE = getenv('DEBUG_MODE', 'True').lower() == 'true'

IS_LOCAL = getenv('IS_LOCAL', 'True').lower() == 'true'
if IS_LOCAL:
  load_dotenv(dotenv_path=find_dotenv(), override=True)

LOCAL_CONFIG_PATH = f'{pathlib.Path(__file__).resolve()}/config.yaml'
REMOTE_CONFIG_PATH = f'gs://{PROJECT_ID}/ac-protect/config.yaml'
CONFIG_FILE_PATH = REMOTE_CONFIG_PATH if PROJECT_ID else LOCAL_CONFIG_PATH
CONFIG_PATH = getenv('CONFIG_PATH', CONFIG_FILE_PATH)
