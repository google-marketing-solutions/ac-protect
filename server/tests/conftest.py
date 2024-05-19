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
import dataclasses
import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
import yaml


ANDROID_TEST_APP = 'com.test.app'
IOS_TEST_APP = '1072235449'
PROPERTY_ID = 102016000

@pytest.fixture(scope='session', name='config')
def fixture_config():
  with open(
      'server/tests/test_files/config_test_DO_NOT_UPLOAD.yaml',
      'r',
      encoding='utf-8') as f:
    conf = yaml.safe_load(f)

  return conf


@pytest.fixture(scope='session', name='auth')
def fixture_auth(config):
  return config['auth']


@pytest.fixture(scope='session', name='bq_config')
def fixture_bq_config(config):
  # Imported in function to allow pytest_configure to initialize before import
  from server.db.bq import BigQuery

  bq_config = config['bigquery']
  auth = config['auth']

  db = BigQuery(auth, bq_config)

  return {
      'db': db,
      'project_id': auth['project_id'],
      'test_dataset': 'ac_protect',
      'test_table': 'bq_tests',
  }

@pytest.fixture(scope='module', name='db')
@patch('server.db.bq.Client')
def fixture_db(mock_bq_client, bq_config):
  mock_bq_client.return_value = MagicMock()

  db = bq_config['db']
  db.dataset = bq_config['test_dataset']
  db.connect(bq_config['project_id'])
  return db

@pytest.fixture(scope='session', name='ga4_df')
def fixture_ga4_df():
  # Imported in function to allow pytest_configure to initialize before import
  from server.db.tables import Ga4Table

  columns = [field.name for field in dataclasses.fields(Ga4Table)]
  columns.append('app_id')
  yesterday = (datetime.date.today() -
               datetime.timedelta(days=1)).strftime('%Y-%m-%d')

  data = [[
      PROPERTY_ID, 'Android', '7.24.6', 'first_open', 122,
      'android_102016000_first_open', yesterday, ANDROID_TEST_APP
  ],
          [
              PROPERTY_ID, 'Android', '7.24.4', 'first_open', 47,
              'android_102016000_first_open', '2024-01-01', ANDROID_TEST_APP
          ],
          [
              PROPERTY_ID, 'Android', '7.24.4', 'purchase', 13,
              'android_102016000_purchase', '2024-01-01', ANDROID_TEST_APP
          ],
          [
              PROPERTY_ID, 'Android', '7.24.4', 'first_level', 120,
              'android_102016000_first_level', '2024-01-01', ANDROID_TEST_APP
          ],
          [
              PROPERTY_ID, 'iOS', '3.6.22', 'first_open', 58,
              'ios_102016000_first_open', yesterday, IOS_TEST_APP
          ],
          [
              PROPERTY_ID, 'iOS', '3.5.2', 'first_open', 13,
              'ios_102016000_first_open', '2024-02-01', IOS_TEST_APP
          ],
          [
              PROPERTY_ID, 'iOS', '3.5.2', 'purchase', 13,
              'ios_102016000_purchase', '2024-02-01', IOS_TEST_APP
          ]]
  df = pd.DataFrame(data, columns=columns)
  return df

@pytest.fixture(scope='session', name='ga4_no_app_id_df')
def fixture_ga4_no_app_id_df(ga4_df):
  return ga4_df.drop(columns=['app_id'])

@pytest.fixture(scope='session', name='ga4_pre_process')
def fixture_ga4_pre_process(ga4_no_app_id_df):
  df = ga4_no_app_id_df.drop(columns=['uid', 'date_added'])
  df['event_count'] = df['event_count'].astype(str)
  return df

@pytest.fixture(scope='session', name='gads_df')
def fixture_gads_df():
  # Imported in function to allow pytest_configure to initialize before import
  from server.db.tables import GadsTable

  columns = [field.name for field in dataclasses.fields(GadsTable)]
  data = [
      [
          ANDROID_TEST_APP, PROPERTY_ID, 'Test App', 'first_open',
          'FIREBASE_ANDROID_FIRST_OPEN', '2024-01-01', 'ANDROID',
          'android_102016000_first_open'
      ],
      [
          ANDROID_TEST_APP, PROPERTY_ID, 'Test App', 'purchase',
          'FIREBASE_ANDROID_PURCHASE', '2024-01-01', 'ANDROID',
          'android_102016000_purchase'
      ],
      [
          ANDROID_TEST_APP, PROPERTY_ID, 'Test App', 'first_level',
          'FIREBASE_ANDROID_FIRST_LEVEL', '2024-01-01', 'ANDROID',
          'android_102016000_first_level'
      ],
      [
          IOS_TEST_APP, PROPERTY_ID, 'Test App', 'first_open',
          'FIREBASE_IOS_FIRST_OPEN', '2024-02-01', 'IOS',
          'ios_102016000_first_open'
      ],
      [
          IOS_TEST_APP, PROPERTY_ID, 'Test App', 'purchase',
          'FIREBASE_IOS_PURCHASE', '2024-02-01', 'IOS', 'ios_102016000_purchase'
      ],
  ]

  df = pd.DataFrame(data, columns=columns)
  return df


@pytest.fixture(scope='session', name='customer_ids')
def fixture_customer_ids():
  return ['1234567890', '0987654321']

@pytest.fixture(scope='session', name='gads_campaigns')
def fixture_gads_campaigns():
  columns = [
      'campaign_name',
      'campaign_app_campaign_setting_app_id',
      'campaign_selective_optimization_conversion_actions']

  data = [
      [
          '1234567890_Campaign_1',
          ANDROID_TEST_APP,
          [
              'customers/1234567890/conversionActions/1111111111',
              'customers/1234567890/conversionActions/2222222222'
          ],
      ],
      [
          '1234567890_Campaign_2', IOS_TEST_APP,
          ['customers/1234567890/conversionActions/2222222222']
      ],
      [
          '0987654321_Campaign_1',
          ANDROID_TEST_APP,
          [
              'customers/0987654321/conversionActions/1111111111',
              'customers/0987654321/conversionActions/2222222222'
          ],
      ],
      [
          '0987654321_Campaign_2', IOS_TEST_APP,
          ['customers/0987654321/conversionActions/2222222222']
      ],
  ]

  df = pd.DataFrame(data, columns=columns)
  return df
