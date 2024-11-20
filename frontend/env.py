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
"""Environment variables used by the project."""
# pylint: disable=C0330

import os
import pathlib

import dotenv

_PROJECT_ID = os.getenv('PROJECT_ID', '')
_IS_LOCAL = os.getenv('IS_LOCAL', 'True').lower() == 'true'
if _IS_LOCAL:
  dotenv.load_dotenv(dotenv_path=dotenv.find_dotenv(), override=True)
_LOCAL_CONFIG_PATH = f'{pathlib.Path(__file__).parent.resolve()}/config.yaml'
_REMOTE_CONFIG_PATH = f'gs://{_PROJECT_ID}/ac-protect/config.yaml'
_CONFIG_FILE_PATH = _REMOTE_CONFIG_PATH if _PROJECT_ID else _LOCAL_CONFIG_PATH

CONFIG_PATH = os.getenv('CONFIG_PATH', _CONFIG_FILE_PATH)
REDIRECT_URI = os.getenv(
  'REDIRECT_URI', 'https://sdk.cloud.google.com/authcode.html'
)
