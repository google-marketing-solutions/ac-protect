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
''' Logger setup for the service '''
import logging

from server.env import IS_GCP



logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

console_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

logger.addHandler(console_handler)

if IS_GCP:
  import google.cloud.logging as cloud_logging

  client = cloud_logging.Client()
  cloud_handler = cloud_logging.handlers.CloudLoggingHandler(client)
  cloud_handler.setLevel(logging.DEBUG)  # Set to DEBUG to capture all log levels
  cloud_handler.setFormatter(console_formatter)

  logger.addHandler(cloud_handler)
