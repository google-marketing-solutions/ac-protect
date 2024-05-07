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
import logging

import smart_open

def open_file(file_path, ok_if_not_exists=False):
  """ Open files either local or on GCS. Used to open the config file"""
  try:
    with smart_open.open(file_path, 'rb') as f:
      content = f.read()
    return content
  except BaseException as e:
    logging.error('Config file %s was not found: %s', file_path, str(e))
    if ok_if_not_exists:
      return {}
    raise FileNotFoundError(file_path) from e
