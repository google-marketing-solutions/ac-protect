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
import pathlib
from typing import Dict

from google.ads.googleads.client import GoogleAdsClient
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource

from env import CONFIG_PATH
from server.endpoints import get_config_file
_ADS_API_VERSION = 'v14'
LOCAL_CONFIG_PATH = f'{pathlib.Path(__file__).parent.parent.parent.resolve()}/config.yaml'
SHEETS_SERVICE_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive'
]

class Config:
  """ Class representing the user configuration for the tool """
  def __init__(self) -> None:
    self.file_path = CONFIG_PATH if CONFIG_PATH else LOCAL_CONFIG_PATH
    self.load_config_from_file()
    self.valid = None

  def check_valid_config(self) -> bool:
    """ Check if the config file is valid """

    self.load_config_from_file()
    if not self.auth['client_id'] or not self.auth['refresh_token'] or not self.auth['client_secret']:
      self.valid = False
      return self.valid

    user_info = {
        'client_id': self.auth['client_id'],
        'refresh_token': self.auth['refresh_token'],
        'client_secret': self.auth['client_secret']
    }

    try:
      creds = Credentials.from_authorized_user_info(user_info, [])
      creds.refresh(Request())
      self.valid = creds.valid
    except RefreshError:
      self.valid = False
    return self.valid

  def load_config_from_file(self) -> dict:
    """ Load a config object from a file """
    config_file_path = self.file_path
    config = get_config_file(config_file_path)
    if config is None:
      config = {}

    self.auth = config.get('auth', {
        'client_id': '',
        'client_secret': '',
        'refresh_token': '',
        'login_customer_id': '',
        'developer_token': '',
        'use_proto_plus': '',
        'project_number': '',
        'project_id': ''
    })

    self.collectors = config.get('collectors', {
        'gads': {
            'version': '15',
            'customer_id': '',
            'start_date': '10'
        },
        'ga4': {'properties': []}
    })

    self.bigquery = config.get('bigquery', {
        'location': 'US',
        'dataset': 'ac_protect',
        'last_trigger_log': 'last_trigger_log'
    })

    self.apps = config.get('apps', {})
    return config

  def to_dict(self) -> Dict[str, str]:
    """ Return the core attributes of the object as dict"""
    return {
            'client_id': self.auth['client_id'],
            'client_secret': self.auth['client_secret'],
            'refresh_token': self.auth['refresh_token'],
            'developer_token': self.auth['developer_token'],
            'login_customer_id': self.auth['login_customer_id'],
    }

  def get_ads_client(self) -> GoogleAdsClient:
    """ Create a Client object for interacting with Google Ads. """

    return GoogleAdsClient.load_from_dict({
        'client_id': self.auth['client_id'],
        'client_secret': self.auth['client_secret'],
        'login_customer_id': self.auth['login_customer_id'],
        'developer_token': self.auth['developer_token'],
        'refresh_token': self.auth['refresh_token'],
        'use_proto_plus': True,
    }, version=_ADS_API_VERSION)

  def get_sheets_service(self) -> Resource:
    """ Create a service object (Resource) for communicating with Google Sheets
    """

    user_info = {
        'client_id': self.auth['client_id'],
        'refresh_token': self.auth['refresh_token'],
        'client_secret': self.auth['client_secret']
    }
    creds = Credentials.from_authorized_user_info(user_info,
                                                  SHEETS_SERVICE_SCOPES)
    if creds.expired:
      creds.refresh(Request())

    service = build('sheets', 'v4', credentials=creds)
    return service
