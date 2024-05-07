import dataclasses
import datetime

import pandas as pd
import pytest
import yaml

from server.db.tables import Ga4Table
from server.db.tables import GadsTable



@pytest.fixture(scope='session', name='config')
def fixture_config():
  with open(
      'server/tests/test_files/config_test_DO_NOT_UPLOAD.yaml',
      'r',
      encoding='utf-8') as f:
    conf = yaml.safe_load(f)

  return conf

@pytest.fixture(scope='session', name='ga4_df')
def fixture_ga4_df():
  columns = [field.name for field in dataclasses.fields(Ga4Table)]
  columns.append('app_id')
  yesterday = (datetime.date.today() -
               datetime.timedelta(days=1)).strftime('%Y-%m-%d')

  data = [[
      102016000, 'Android', '7.24.6', 'first_open', 122,
      'android_102016000_first_open', yesterday, 'com.test.app'
  ],
          [
              102016000, 'Android', '7.24.4', 'first_open', 47,
              'android_102016000_first_open', '2024-01-01', 'com.test.app'
          ],
          [
              102016000, 'Android', '7.24.4', 'purchase', 13,
              'android_102016000_purchase', '2024-01-01', 'com.test.app'
          ],
          [
              102016000, 'Android', '7.24.4', 'first_level', 120,
              'android_102016000_first_level', '2024-01-01', 'com.test.app'
          ],
          [
              102016000, 'iOS', '3.6.22', 'first_open', 58,
              'ios_102016000_first_open', yesterday, '1072235449'
          ],
          [
              102016000, 'iOS', '3.5.2', 'first_open', 13,
              'ios_102016000_first_open', '2024-02-01', '1072235449'
          ],
          [
              102016000, 'iOS', '3.5.2', 'purchase', 13,
              'ios_102016000_purchase', '2024-02-01', '1072235449'
          ]]
  df = pd.DataFrame(data, columns=columns)
  return df

@pytest.fixture(scope='session', name='ga4_no_app_id_df')
def fixture_ga4_no_app_id_df(ga4_df):
  return ga4_df.drop(columns=['app_id'])


@pytest.fixture(scope='session', name='gads_df')
def fixture_gads_df():
  columns = [field.name for field in dataclasses.fields(GadsTable)]
  data = [
      [
          'com.test.app', 102016000, 'Test App', 'first_open',
          'FIREBASE_ANDROID_FIRST_OPEN', '2024-01-01', 'ANDROID',
          'android_102016000_first_open'
      ],
      [
          'com.test.app', 102016000, 'Test App', 'purchase',
          'FIREBASE_ANDROID_PURCHASE', '2024-01-01', 'ANDROID',
          'android_102016000_purchase'
      ],
      [
          'com.test.app', 102016000, 'Test App', 'first_level',
          'FIREBASE_ANDROID_FIRST_LEVEL', '2024-01-01', 'ANDROID',
          'android_102016000_first_level'
      ],
      [
          '1072235449', 102016000, 'Test App', 'first_open',
          'FIREBASE_IOS_FIRST_OPEN', '2024-02-01', 'IOS',
          'ios_102016000_first_open'
      ],
      [
          '1072235449', 102016000, 'Test App', 'purchase',
          'FIREBASE_IOS_PURCHASE', '2024-02-01', 'IOS',
          'ios_102016000_purchase'
      ],
  ]

  df = pd.DataFrame(data, columns=columns)
  return df
