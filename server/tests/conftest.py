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
"""Test configuration file."""
# pylint: disable=C0330

import pathlib
from unittest import mock

import pandas as pd
import pytest
import yaml

ANDROID_TEST_APP = 'com.test.app'
IOS_TEST_APP = '1072235449'
PROPERTY_ID = 102016000
TEST_COLLECTORS_CSV_PATH = 'server/tests/test_collector_csvs'


@pytest.fixture(scope='session', name='config')
def fixture_config():  # noqa D103
  with pathlib.Path(
    'server/tests/test_files/config_test_DO_NOT_UPLOAD.yaml'
  ).open('r', encoding='utf-8') as f:
    return yaml.safe_load(f)


@pytest.fixture(scope='session', name='auth')
def fixture_auth(config):  # noqa D103
  return config['auth']


@pytest.fixture(scope='session', name='app_ids')
def fixture_app_ids(config):  # noqa D103
  app_ids = list(config['apps'].keys())
  return [str(x) for x in app_ids]


@pytest.fixture(scope='session', name='bq_config')
def fixture_bq_config(config):  # noqa D103
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
@mock.patch('server.db.bq.bigquery.Client')
def fixture_db(mock_bq_client, bq_config):  # noqa D103
  mock_bq_client.return_value = mock.MagicMock()
  db = bq_config['db']
  db.dataset = bq_config['test_dataset']
  db.connect(bq_config['project_id'])
  return db


@pytest.fixture(scope='session', name='ga4_df')
def fixture_ga4_df():  # noqa D103
  return pd.read_csv(f'{TEST_COLLECTORS_CSV_PATH}/ga4_with_app_id.csv')


@pytest.fixture(scope='session', name='collector_ga4')
def fixture_collector_ga4(ga4_df):  # noqa D103
  return ga4_df.drop(columns=['app_id'])


@pytest.fixture(scope='session', name='ga4_pre_process')
def fixture_ga4_pre_process(collector_ga4):  # noqa D103
  df = collector_ga4.drop(columns=['uid', 'date_added'])
  df['event_count'] = df['event_count'].astype(str)
  return df


@pytest.fixture(scope='session', name='gads_df')
def fixture_gads_df():  # noqa D103
  return pd.read_csv(f'{TEST_COLLECTORS_CSV_PATH}/gads_with_os_uid.csv')


@pytest.fixture(scope='session', name='gads_no_os_uid')
def fixture_gads_no_os_uid(gads_df):  # noqa D103
  return gads_df.drop(columns=['os', 'uid'])


@pytest.fixture(scope='session', name='app_store_df')
def fixture_app_store_df():  # noqa D103
  collector = pd.read_csv(f'{TEST_COLLECTORS_CSV_PATH}/app_store.csv')
  collector['app_id'] = collector['app_id'].astype(str)
  return collector


@pytest.fixture(scope='session', name='play_store_df')
def fixture_play_store_df():  # noqa D103
  return pd.read_csv(f'{TEST_COLLECTORS_CSV_PATH}/play_store.csv')


@pytest.fixture(scope='session', name='customer_ids')
def fixture_customer_ids():  # noqa D103
  return ['1234567890', '0987654321']


@pytest.fixture(scope='session', name='gads_campaigns')
def fixture_gads_campaigns():
  """Fixture mocking a Google Ads campaign."""
  columns = [
    'campaign_name',
    'campaign_app_campaign_setting_app_id',
    'campaign_selective_optimization_conversion_actions',
  ]

  data = [
    [
      '1234567890_Campaign_1',
      ANDROID_TEST_APP,
      [
        'customers/1234567890/conversionActions/1111111111',
        'customers/1234567890/conversionActions/2222222222',
      ],
    ],
    [
      '1234567890_Campaign_2',
      IOS_TEST_APP,
      ['customers/1234567890/conversionActions/2222222222'],
    ],
    [
      '0987654321_Campaign_1',
      ANDROID_TEST_APP,
      [
        'customers/0987654321/conversionActions/1111111111',
        'customers/0987654321/conversionActions/2222222222',
      ],
    ],
    [
      '0987654321_Campaign_2',
      IOS_TEST_APP,
      ['customers/0987654321/conversionActions/2222222222'],
    ],
  ]

  return pd.DataFrame(data, columns=columns)
