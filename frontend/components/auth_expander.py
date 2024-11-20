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
"""Frontend component for managing authentication settings.

Provides UI elements for configuring OAuth2 credentials, Google Ads
authentication, GCP project settings, and user access management through a
Streamlit expandable section.
"""

# pylint: disable=C0330, g-multiple-import, g-bad-import-order
import streamlit as st

from frontend.components import google_ads
from frontend.utils import log, state_management

OAUTH_HELP = (
  'Refer to [Create OAuth2 Credentials]'
  '(https://developers.google.com/google-ads/api/docs/client-libs/'
  'python/oauth-web#create_oauth2_credentials) '
  'for more information.'
)


def auth_expander(user_config: state_management.ConfigManager) -> None:
  """Displays and manages authentication settings in an expandable section.

  Creates an expandable UI section for authentication settings. If credentials
  are not set, shows input fields for users and OAuth credentials. If
  credentials are set, displays the current values in disabled fields with an
  option to edit.

  Args:
    user_config: Config object containing the current authentication settings
    and user list.
  """
  text_inputs = {
    'client_id': 'Client ID',
    'client_secret': 'Client Secret',
    'refresh_token': 'Refresh Token',
    'developer_token': 'Developer Token',
    'login_customer_id': 'MCC ID',
    'project_id': 'GCP Project ID',
    'project_number': 'GCP Project Number',
  }

  with st.expander(
    '**Authentication**', expanded=not st.session_state.ui_state['valid_config']
  ):
    if not st.session_state.ui_state['valid_config']:
      users = st.text_input(
        'allowed users emails',
        value=', '.join(st.session_state.config.users),
        key='users',
      )
      users = users.replace(' ', '').split(',')
      valid_users = validate_emails(users)
      if not valid_users:
        st.error('allowed users list contains invalid emails')
      else:
        st.session_state.config.users = users
      st.info(f'Credentials are not set. {OAUTH_HELP}', icon='⚠️')

      values = {}
      for key, label in text_inputs.items():
        values[key] = st.text_input(label, value=user_config.auth[key])

      st.button(
        'Save',
        disabled=not valid_users,
        type='primary',
        on_click=authenticate,
        args=[values],
      )
    else:
      st.success('Credentials successfully set ', icon='✅')
      st.text_input('Users', value=', '.join(user_config.users), disabled=True)
      for key, label in text_inputs.items():
        st.text_input(label, value=user_config.auth[key], disabled=True)
      st.button('Edit Credentials', on_click=state_management.reset_config)


def validate_emails(emails: list[str]) -> bool:
  """Simple validation of a list of email addresses.

  Args:
    emails: A list of strings containing email addresses to validate.

  Returns:
    bool: True if all emails are valid, False if any email is invalid.

  """
  for email in emails:
    e = email.strip()
    if not e or '@' not in e or '.' not in e.split('@')[1]:
      return False
  return True


def authenticate(config_params: dict[str, str]) -> None:
  """Authenticates and initializes Google Ads API connection.

  Updates the session config with provided authentication parameters,
  validates the configuration, and if valid, fetches Google Ads data.

  Args:
    config_params: Dictionary containing Google Ads API authentication
    parameters like client_id, client_secret, refresh_token etc.
  """
  log.logger.info('Authenticating')
  st.session_state.config.auth = config_params
  st.session_state.config.update_config()

  if not st.session_state.config.valid:
    st.session_state.ui_state['valid_config'] = False
    return

  st.session_state.ui_state['valid_config'] = True
  google_ads.get_ads_data(
    st.session_state.config.auth,
    st.session_state.config.collectors['collector_gads'],
    st.session_state.config.bigquery,
  )
