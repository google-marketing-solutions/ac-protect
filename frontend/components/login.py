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
"""Frontend component for handling user login via Google OAuth2.

This module provides a Streamlit component for authenticating users with Google
OAuth2, verifying their email against a list of allowed users, and managing
session state for authenticated users.
"""

# pylint: disable=C0330, g-multiple-import, g-bad-import-order
import requests
import streamlit as st
from google_auth_oauthlib import flow as google_auth_flow

from frontend import env
from frontend.utils import log


def login(user_config: dict[str, str]) -> None:
  """Handles the OAuth2 login flow for user authentication.

  This function initiates the Google OAuth2 authentication flow, prompts the
  user to authenticate via a URL, and validates the returned verification code.
  It also checks if the authenticated user's email is in the allowed users list.

  Args:
    user_config: Configuration object containing auth settings (client_id,
    project_id, client_secret) and allowed users list.

  """
  log.logger.info('starting login flow')
  flow = google_auth_flow.Flow.from_client_config(
    {
      'web': {
        'client_id': user_config.auth['client_id'],
        'project_id': user_config.auth['project_id'],
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': (
          'https://www.googleapis.com/oauth2/v1/certs'
        ),
        'client_secret': user_config.auth['client_secret'],
      }
    },
    scopes=[
      'openid',
      'https://www.googleapis.com/auth/userinfo.email',
    ],
    redirect_uri=env.REDIRECT_URI,
  )

  auth_url, _ = flow.authorization_url(prompt='consent')
  instruct_placeholder = st.empty()
  auth_code_placeholder = st.empty()

  instruct_placeholder.write(
    f'Please go to [this URL]({auth_url}) for authentication '
    'and copy-paste the verification code back here.'
  )
  code = auth_code_placeholder.text_input('Enter your verification code')
  if code:
    try:
      flow.fetch_token(code=code)
      credentials = flow.credentials
    except ValueError as e:
      st.error('Failed to fetch token')
      st.error(e)

    email_address = None
    allowed_users = user_config.users
    try:
      user = get_user_email(credentials.token)
      email_address = user['email']
      log.logger.info('allowed_users: %s', allowed_users)
      log.logger.info('email_address: %s', email_address)
    except Exception as e:  # pylint: disable=broad-exception-caught
      st.error('Failed to get user email')
      st.error(e)

    if email_address in allowed_users:
      st.session_state.is_auth = True
      log.logger.info('session_state.is_auth: %s', st.session_state.is_auth)
      instruct_placeholder.empty()
      auth_code_placeholder.empty()
    else:
      st.error('Login Failed - Invalid email domain')


def get_user_email(access_token: str) -> dict[str, str]:
  """Fetches user information from Google's userinfo endpoint.

  Args:
    access_token: OAuth2 access token to authenticate the request.

  Returns:
    dict: User information containing email and other profile data.

  """
  r = requests.get(
    'https://www.googleapis.com/oauth2/v3/userinfo',
    params={'access_token': access_token},
    timeout=3600,
  )

  return r.json()
