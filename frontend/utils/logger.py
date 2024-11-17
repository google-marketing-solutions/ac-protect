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
"""Module for logging Frontend."""
# pylint: disable=C0330, g-bad-import-order, g-multiple-import

import logging

logging.basicConfig(
  format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s',
  level=logging.INFO,
  datefmt='%H:%M:%S',
)
logger = logging.getLogger('ac_protect_log')
logging.getLogger('gaarf.report_fetcher').setLevel(logging.WARNING)
