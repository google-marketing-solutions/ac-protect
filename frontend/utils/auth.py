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
""" This file generates a refresh token to use with the Google Ads API, and
populates it inside config.yaml. """
import hashlib
import os
import re
import socket
import webbrowser
from typing import Any
from typing import Dict
from urllib.parse import unquote

import google.auth
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

_SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
_SERVER = '127.0.0.1'
_PORT = 8080
_REDIRECT_URI = f'http://{_SERVER}:{_PORT}'


def generate_refresh_token(config: Dict[str, Any]):
  """ If YAML values are not filled out, return  try to get the default app
  credentials, default app credentials are generated when the user uses the
  following command `gcloud auth application-default login --scopes `"""
  if None in (config['client_id'], config['client_secret']):
    credentials, _ = google.auth.default(scopes=_SCOPES)
    if not credentials.client_id or not credentials.client_secret:
      raise ValueError(
        'Client ID or Client Secret missing. Refer to README for instructions.'
      )
  flow = Flow.from_client_config(
    {
      'installed': {
        'client_id':
          config['client_id'],
        'auth_uri':
          'https://accounts.google.com/o/oauth2/auth',
        'token_uri':
          'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url':
          'https://www.googleapis.com/oauth2/v1/certs',
        'client_secret':
          config['client_secret'],
        'redirect_uris': ['urn:ietf:wg:oauth:2.0:oob', _SERVER]
      }
    },
    scopes=_SCOPES)
  flow.redirect_uri = _REDIRECT_URI

  # Create an anti-forgery state token as described here:
  # https://developers.google.com/identity/protocols/OpenIDConnect#createxsrftoken
  passthrough_val = hashlib.sha256(os.urandom(1024)).hexdigest()
  authorization_url, _ = flow.authorization_url(
      access_type='offline',
      state=passthrough_val,
      prompt='consent',
  )
  webbrowser.open_new(authorization_url)

  # Retrieves an authorization code by opening a socket to receive the
  # redirect request and parsing the query parameters set in the URL.
  # Then pass the code back into the OAuth module to get a refresh token.
  code = unquote(_get_authorization_code(passthrough_val))
  flow.fetch_token(code=code)
  return flow.credentials.refresh_token


def _get_authorization_code(passthrough_val: str) -> str:
  """Opens a socket to handle a single HTTP request containing auth tokens.
  Args:
    passthrough_val: an anti-forgery token used to verify the request
      received by the socket.
  Returns:
    a str access token from the Google Auth service.
  """

  # Open a socket at _SERVER:_PORT and listen for a request
  sock = socket.socket()
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.bind((_SERVER, _PORT))
  sock.listen(1)
  connection, _ = sock.accept()
  data = connection.recv(1024)

  # Parse the raw request to retrieve the URL query parameters.
  params = _parse_raw_query_params(data)
  try:
    if not params.get('code'):

      # If no code is present in the query params then there will be an
      # error message with more details.
      error = params.get('error')
      message = f'Failed to retrieve authorization code. Error: {error}'
      raise ValueError(message)
    elif params.get('state') != passthrough_val:
      message = 'State token does not match the expected state.'
      raise ValueError(message)
    else:
      message = 'Authorization code was successfully retrieved.'
  except ValueError as e:
    raise e
  finally:
    response = (f'''HTTP/1.1 200 OK
                    Content-Type: text/html
                    <div style="text-align:center; font-family:sans-serif">
                        <b>{message}</b>
                        <p>You can close this tab and go back to the app.</p>
                    </div>''')
    connection.sendall(response.encode())
    connection.close()
  return params.get('code')


def _parse_raw_query_params(data: bytes) -> Dict[str,str]:
  """Parses a raw HTTP request to extract its query params as a dict.
  Note that this logic is likely irrelevant if you're building OAuth logic
  into a complete web application, where response parsing is handled by a
  framework.

  Args:
      data: raw request data as bytes.
  Returns:
      a dict of query parameter key value pairs.
  """

  # Decode the request into a utf-8 encoded string
  decoded = data.decode('utf-8')
  # Use a regular expression to extract the URL query parameters string
  match = re.search(r'GET\s\/\?(.*) ', decoded)
  params = match.group(1)
  # Split the parameters to isolate the key/value pairs
  pairs = [pair.split('=') for pair in params.split('&')]
  # Convert pairs to a dict to make it easy to access the values
  return {key: val for key, val in pairs}

def get_credentials(config: Dict[str, Any], scopes=None) -> Credentials:

  if scopes is None:
    scopes = _SCOPES

  creds = None
  user_info = {
      'client_id': config['client_id'],
      'refresh_token': config['refresh_token'],
      'client_secret': config['client_secret']
  }
  creds = Credentials.from_authorized_user_info(user_info, scopes)
  # If credentials are expired, refresh.
  # if creds.expired:
  #     try:
  #         creds.refresh(Request())
  #     except Exception as e:
  #         if 'invalid_scope' in e.args[0]:
  #             user_info["refresh_token"] = generate_refresh_token(config)
  #             creds = Credentials.from_authorized_user_info(user_info, SCOPES)
  #         else:
  #             logger.error(str(e))
  #             raise e
  return creds
