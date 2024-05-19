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
from enum import Enum

class Scopes(Enum):
  ''' Enum that holds all scopes that we are using in the solution (accept
  Google Ads which is implicitly used in gaarf)'''

  GMAIL_SEND = 'https://www.googleapis.com/auth/gmail.send'
  USERINFO_EMAIL = 'https://www.googleapis.com/auth/userinfo.email'
  CLOUD_PLATFORM = 'https://www.googleapis.com/auth/cloud-platform'
  ANALYTICS_READ_ONLY = 'https://www.googleapis.com/auth/analytics.readonly'
