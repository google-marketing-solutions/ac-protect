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
"""State management for the app."""
# pylint: disable=C0330, g-bad-import-order

import streamlit as st
from google.auth import exceptions
from google.auth.transport import requests
from google.oauth2 import credentials

from frontend.utils import log
from server import endpoints


class ConfigManager:
  """User configuration for the tool."""

  def __init__(self) -> None:
    """Initialize the ConfigManager."""
    log.logger.info('Initializing ConfigManager')
    self.load_config()
    self.validate_config()

  def load_config(self) -> None:
    """Load a config object from a file."""
    config = endpoints.get_config_endpoint()

    if config is None:
      config = {}

    self.users = config.get('users', [])
    self.auth = config.get('auth', {'use_proto_plus': True})
    self.collectors = config.get('collectors', {})
    self.apps = config.get('apps', {})
    self.bigquery = config.get('bigquery', {})

  def update_config(self) -> None:
    """Update the config file."""
    log.logger.info('Updating config')

    self.validate_config()

    if not self.valid:
      log.logger.info('Config is invalid, skipping update')
      return

    config = {
      'users': self.users,
      'auth': self.auth,
      'collectors': self.collectors,
      'apps': self.apps,
      'bigquery': self.bigquery,
    }
    endpoints.update_config_endpoint(config)

  def validate_config(self) -> bool:
    """Check if the config file is valid."""
    log.logger.info('Validating config')
    required_keys = ['client_id', 'refresh_token', 'client_secret']
    if not all(self.auth.get(key) for key in required_keys) or not self.users:
      self.valid = False
      return self.valid

    user_info = {
      'client_id': self.auth.get('client_id', ''),
      'refresh_token': self.auth.get('refresh_token', ''),
      'client_secret': self.auth.get('client_secret', ''),
    }

    try:
      creds = credentials.Credentials.from_authorized_user_info(user_info, [])
      creds.refresh(requests.Request())
      self.valid = creds.valid
    except exceptions.RefreshError:
      self.valid = False

    st.session_state.ui_state['valid_config'] = self.valid
    return self.valid


def initialize_session_state() -> None:
  """Initializtion of Streamlit session state with default values.

  Sets up the following session state variables if they don't exist:
  - is_auth: Authentication status (bool)
  - ui_state: UI state dict containing valid_config flag
  - config: Config object for app settings
  - errors: List to track error messages
  - ads_data: Dict to store Google Ads data

  If config is valid, fetches initial Google Ads data.
  """
  if 'is_auth' not in st.session_state:
    st.session_state.is_auth = False
  if 'ui_state' not in st.session_state:
    st.session_state.ui_state = {}
    st.session_state.ui_state['valid_config'] = False
  if 'config' not in st.session_state:
    # there's no config on GCS, that's normal
    st.session_state.config = ConfigManager()

  if 'errors' not in st.session_state:
    st.session_state.errors = []
  if 'ads_data' not in st.session_state:
    st.session_state.ads_data = {}


def reset_config() -> None:
  """Resets the application configuration state.

  Sets the configuration validation state and authentication state to False,
  effectively clearing the current configuration settings.
  """
  st.session_state.ui_state['valid_config'] = False
  st.session_state.is_auth = False


def update_app_config(
  app_id: str,
  emails: str,
  version_events_checked: bool,
  time_interval_checked: bool,
) -> None:
  """Update the config for an app."""
  st.session_state.config.apps[app_id] = {
    'alerts': {'emails': emails.replace(' ', '').split(',')},
    'rules': {},
  }

  st.session_state.config.apps[app_id]['rules']['dropped_between_versions'] = (
    version_events_checked
  )

  if time_interval_checked:
    st.session_state.config.apps[app_id]['rules']['time_interval'] = {
      'interval': 24
    }

  st.session_state.config.update_config()
