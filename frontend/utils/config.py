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

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from env import CONFIG_PATH
from server.endpoints import get_config_file

LOCAL_CONFIG_PATH = f'{pathlib.Path(__file__).parent.parent.parent.resolve()}/config.yaml'


class Config:
  """ Class representing the user configuration for the tool """

  def __init__(self) -> None:
    self.file_path = CONFIG_PATH if CONFIG_PATH else LOCAL_CONFIG_PATH
    self.load_config_from_file()
    self.valid = None

  def check_valid_config(self) -> bool:
    """ Check if the config file is valid """

    self.load_config_from_file()
    if not self.auth['client_id'] or not self.auth[
        'refresh_token'] or not self.auth['client_secret'] or not self.users:
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

    self.users = config.get(
      'users', []
    )

    self.auth = config.get(
        'auth', {
            'client_id': '',
            'client_secret': '',
            'refresh_token': '',
            'login_customer_id': '',
            'developer_token': '',
            'use_proto_plus': '',
            'project_number': '',
            'project_id': ''
        })

    self.collectors = config.get(
        'collectors', {
            'collector_gads': {
                'version': '15',
                'customer_id': '',
                'start_date': '10'
            },
            'collector_ga4': {
                'properties': []
            }
        })

    self.bigquery = config.get('bigquery', {
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
