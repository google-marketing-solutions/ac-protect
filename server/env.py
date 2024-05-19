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
""" Environment variables used by the backend"""
from os import getenv

IS_CLOUD_RUN_SERVICE = bool(getenv('K_SERVICE'))
IS_CLOUD_RUN_JOB = bool(getenv('CLOUD_RUN_JOB'))
IS_APP_ENGINE = bool(getenv('GAE_APPLICATION'))

IS_GCP = IS_CLOUD_RUN_SERVICE or IS_CLOUD_RUN_JOB or IS_APP_ENGINE
