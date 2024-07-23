# Copyright 2023 Google LLC
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
import pandas as pd
import requests
import streamlit as st
from google.api_core.exceptions import BadRequest
from google_auth_oauthlib.flow import Flow

from env import REDIRECT_URI
from frontend.utils.config import Config
from frontend.utils.logger import logger
from server.endpoints import get_ads_app_ids
from server.endpoints import get_ads_data as get_data
from server.endpoints import update_config_file

OAUTH_HELP = """Refer to
    [Create OAuth2 Credentials](https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-web#create_oauth2_credentials)
    for more information"""

MIN_INTERVAL = 24


def validate_config():
  if st.session_state.config.check_valid_config():
    st.session_state.ui_state['valid_config'] = True
  else:
    st.session_state.ui_state['valid_config'] = False
  logger.info(f"validate_config - {st.session_state.ui_state['valid_config']}")


def initialize_session_state():
  if 'is_auth' not in st.session_state:
    st.session_state.is_auth = False
  if 'ui_state' not in st.session_state:
    st.session_state.ui_state = {}
    st.session_state.ui_state['valid_config'] = False
  if 'config' not in st.session_state:
    # there's no config on GCS, that's normal
    st.session_state.config = Config()
    validate_config()
  if 'errors' not in st.session_state:
    st.session_state.errors = []
  if 'ads_data' not in st.session_state:
    st.session_state.ads_data = {}
    if st.session_state.ui_state['valid_config']:
      get_ads_data(st.session_state.config.auth,
                 st.session_state.config.collectors['collector_gads'],
                 st.session_state.config.bigquery)

def get_ads_data(auth, collector_config, bq_config):
  logger.info(f'getting ads data')
  try:
    st.session_state.ads_data = get_data(auth, collector_config, bq_config)
  except BadRequest as e:
    st.session_state.ads_data = pd.DataFrame()
    st.error(f'Bad config:\n{e}')
  return st.session_state.ads_data


def get_user_email(access_token):
  r = requests.get('https://www.googleapis.com/oauth2/v3/userinfo',
    params={'access_token': access_token},
    timeout=3600)

  return r.json()

def login(config):
  logger.info('starting login flow')
  SCOPES = ['openid', 'https://www.googleapis.com/auth/userinfo.email',]
  # Create a flow instance to manage the OAuth 2.0 Authorization Grant Flow steps
  flow = Flow.from_client_config(
    {
    'web': {
      'client_id': config.auth['client_id'],
      'project_id': config.auth['project_id'],
      'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
      'token_uri': 'https://oauth2.googleapis.com/token',
      'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
      'client_secret': config.auth['client_secret']
    }
    },
    scopes=SCOPES,
    redirect_uri=REDIRECT_URI
  )

  auth_url, _ = flow.authorization_url(prompt='consent')
  instruct_placeholder = st.empty()
  auth_code_placeholder = st.empty()

  instruct_placeholder.write('Please go to [this URL](%s) for authentication \
                             and copy-paste the verification code back here' % auth_url)
  code = auth_code_placeholder.text_input('Enter your verification code')
  if code:
    try:
      flow.fetch_token(code=code) #TODO - what exception is sent?
      credentials = flow.credentials
      # Verify if the email belongs to google.com domain
      user = get_user_email(credentials.token)
      email_address = user['email']
      allowed_users = config.users

      if email_address in allowed_users:
        st.session_state.is_auth = True
        instruct_placeholder.empty()
        auth_code_placeholder.empty()
      else:
        st.error('Login Failed - Invalid email domain')
    except Exception as e:  #TODO - change to specific exception
      st.error('Login Failed')
      st.error(e)

def authenticate(config_params):
  config_params['use_proto_plus'] = True
  st.session_state.config.auth = config_params
  update_config_file(st.session_state.config.__dict__)
  validate_config()
  if st.session_state.ui_state['valid_config']:
    get_ads_data(st.session_state.config.auth,
                 st.session_state.config.collectors['collector_gads'],
                 st.session_state.config.bigquery)


def reset_config():
  st.session_state.ui_state['valid_config'] = False
  st.session_state.is_auth = False

def valid_emails(emails):
  for email in emails:
    email = email.replace(' ', '')
    if not email:
      return False
    elif '@' not in email:
      return False
    elif '.' not in email.split('@')[1]:
      return False
  return True


def auth_expander(config):
  with st.expander('**Authentication**', expanded=not st.session_state.ui_state['valid_config']):
    if not st.session_state.ui_state['valid_config']:
      users = st.text_input(
        'allowed users emails',
      value=', '.join(st.session_state.config.users),
        key='users')
      users = users.replace(" ", "")
      users = users.split(',')
      valid_users = valid_emails(users)
      if not valid_users:
        st.error('allowed users list contains unvalid emails')
      else:
        st.session_state.config.users = users
      st.info(f'Credentials are not set. {OAUTH_HELP}', icon='⚠️')
      client_id = st.text_input('Client ID', value=config.auth['client_id'])
      client_secret = st.text_input(
        'Client Secret', value=config.auth['client_secret'])
      refresh_token = st.text_input(
        'Refresh Token', value=config.auth['refresh_token'])
      developer_token = st.text_input(
        'Developer Token', value=config.auth['developer_token'])
      mcc_id = st.text_input('MCC ID', value=config.auth['login_customer_id'])
      project_id = st.text_input(
        'GCP Project ID', value=config.auth['project_id'])
      project_number = st.text_input(
        'GCP PRoject Number', value=config.auth['project_number'])
      st.button(
        'Save',
        disabled=not valid_users,
        type='primary',
        on_click=authenticate,
        args=[{
          'client_id': client_id,
          'client_secret': client_secret,
          'refresh_token': refresh_token,
          'developer_token': developer_token,
          'login_customer_id': mcc_id,
          'project_id': project_id,
          'project_number': project_number,
        }])
    else:
      st.success('Credentials succesfully set ', icon='✅')
      st.text_input('Users', value=', '.join(config.users), disabled=True)
      st.text_input('Client ID', value=config.auth['client_id'], disabled=True)
      st.text_input(
        'Client Secret', value=config.auth['client_secret'], disabled=True)
      st.text_input(
        'Refresh Token', value=config.auth['refresh_token'], disabled=True)
      st.text_input(
        'Developer Token',
        value=config.auth['developer_token'],
        disabled=True)
      st.text_input(
        'MCC ID', value=config.auth['login_customer_id'], disabled=True)
      st.text_input(
        'GCP Project ID', value=config.auth['project_id'], disabled=True)
      st.text_input(
        'GCP PRoject Number',
        value=config.auth['project_number'],
        disabled=True)
      st.button('Edit Credentials', on_click=reset_config)


def show_alerts_expander(df):
  with st.expander('Alerts'):
    st.dataframe(df, use_container_width=True)

def is_module_activated(app_id, module_name):
  return st.session_state.config.apps[app_id][
      'rules'] and module_name in st.session_state.config.apps[app_id]['rules']


def show_dropped_form(app_id):
  module_name = 'dropped_between_versions'
  is_activated = is_module_activated(app_id, module_name)
  is_dropped = st.checkbox(
    'Dropped Between Versions', value=is_activated, key=app_id + 'cb' + '1')
  return is_dropped


def show_mp_form(app_id):
  res = {}
  module_name = 'measurement_protocol'
  is_mp_activated = is_module_activated(app_id, module_name)
  is_mp = st.checkbox(
    'Measurement Protocol', value=is_mp_activated, key=app_id + 'cb' + '2')
  if is_mp:
    if not is_mp_activated:
      res = {'interval': MIN_INTERVAL, 'events': []}
    else:
      res = st.session_state.config.apps[app_id]['rules'][module_name]
    mp_interval = st.number_input(
      'Interval',
      value=res['interval'],
      min_value=MIN_INTERVAL,
      step=1,
      key=app_id + 'ni' + '1')
    events_list = ['event1', 'event2',
                   'event3']  # TODO - replace with google ads event names
    selected = st.multiselect(
      'Select MP events', events_list, key=app_id + 'ms' + '1')
    res = {'interval': mp_interval, 'events': selected}
  return res


def show_time_interval_form(app_id):
  module_name = 'time_interval'
  is_activated = is_module_activated(app_id, module_name)
  is_time = st.checkbox(
    'Time Interval', value=is_activated, key=app_id + 'cb' + '3')
  ti_interval = None
  if is_time:
    if not is_activated:
      interval = MIN_INTERVAL
    else:
      interval = st.session_state.config.apps[app_id]['rules'][module_name][
        'interval']
    ti_interval = st.number_input(
      'Interval',
      value=interval,
      min_value=MIN_INTERVAL,
      step=1,
      key=app_id + 'ni' + '2')
  return ti_interval


def show_app_form(app_id):
  with st.expander(app_id):
    st.subheader(app_id)
    emails = st.text_input(
      'Emails',
      value=', '.join(
        st.session_state.config.apps[app_id]['alerts']['emails']),
      key=app_id + 'emails')
    st.data_editor(
      st.session_state.ads_data[st.session_state.ads_data['app_id'] == app_id]
      ['event_name'],
      hide_index=True,
      use_container_width=True,
      disabled=True,
      key=app_id + 'de' + '1')
    is_dropped = show_dropped_form(app_id)

    is_activated = is_module_activated(app_id, 'time_interval')
    is_time = st.checkbox(
      'Time Interval', value=is_activated, key=app_id + 'cb' + '3')

    is_save = st.button('save', type='primary', key=app_id + 'bt' + '1')
    if is_save:
      st.session_state.config.apps[app_id] = {
        'alerts': {
          'emails': emails.replace(' ', '').split(',')
        },
        'rules': {}
      }
      st.session_state.config.apps[app_id]['rules'][
          'dropped_between_versions'] = is_dropped
      if is_time:
        st.session_state.config.apps[app_id]['rules']['time_interval'] = {
          'interval': 24
        }



      update_config_file(st.session_state.config.__dict__)


def show_ads_data():
  with st.container(border=True):
    st.header('Config')
    for app_id in st.session_state.ads_data['app_id'].unique().tolist():
      show_app_form(app_id)


@st.cache_data
def get_app_ids():
  return get_ads_app_ids(st.session_state.config.auth,
                         st.session_state.config.collectors['collector_gads'],
                         st.session_state.config.bigquery)


def config_expander():
  with st.container():
    app_ids = st.session_state.ads_data['app_id'].unique().tolist()
    for app_id in app_ids:
      if app_id not in st.session_state.config.apps:
        st.session_state.config.apps[app_id] = {
            'alerts': {
              'emails': []
            },
            'rules': {
              'rules': {}
            },
        }
    show_ads_data()


def main():
  st.set_page_config(
    page_title='AC Protect',
    page_icon='🛡',
    layout='centered',
  )
  st.header('🛡AC Protect')
  initialize_session_state()
  config = st.session_state.config
  if not st.session_state.ui_state['valid_config']:
    auth_expander(config)
  if not st.session_state.is_auth and st.session_state.ui_state['valid_config']:
    login(config)
  if st.session_state.is_auth:
    auth_expander(config)
    if 'app_id' in st.session_state.ads_data:
      config_expander()

if __name__ == '__main__':
  main()
