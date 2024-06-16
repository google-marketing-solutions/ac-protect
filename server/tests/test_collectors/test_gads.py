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
import re
from unittest.mock import patch

import pandas as pd
import pytest
from gaarf.query_executor import AdsReportFetcher
from gaarf.report import GaarfReport

from server.collectors.gads import GAdsCollector
from server.db.bq import BigQuery


@pytest.fixture(scope='module', name='collector_config')
def fixture_collector_config(config):
  return config['collectors']['gads']


class TestGadsCollectorBase:

  def test_init_ga4_collector(self, auth, collector_config, db):
    collector = GAdsCollector(auth, collector_config, db)

    assert collector.name == 'GAds-collector'
    assert collector.type_ == 'collector'
    assert isinstance(collector.bq, BigQuery)


class TestMainFunctions:

  @pytest.fixture(scope='class', name='collector')
  def fixture_collector(self, auth, collector_config, db):
    return GAdsCollector(auth, collector_config, db)

  @patch('server.collectors.gads.AdsReportFetcher.fetch')
  @patch('server.collectors.gads.AdsReportFetcher.expand_mcc')
  def test_collect(self, mock_expand_mcc, mock_fetcher, collector, gads_df,
                   gads_campaigns):
    # def test_collect(self, collector: GAdsCollector):
    mock_expand_mcc.return_value = self._create_mock_expand_mcc()
    mock_fetcher.side_effect = [
        self._create_mock_fetch_campaigns(gads_campaigns),
        self._create_mock_fetch_conversion_action(gads_df)
    ]

    res = collector.collect()
    assert isinstance(res, pd.DataFrame)

    df = gads_df.drop(columns=['os', 'uid'])
    assert res.equals(df)

  def test_process(self, collector, gads_df):
    expected_result_os = ['ANDROID', 'ANDROID', 'ANDROID', 'IOS', 'IOS']
    expected_result_uid = [
        'android_102016000_first_open', 'android_102016000_purchase',
        'android_102016000_first_level', 'ios_102016000_first_open',
        'ios_102016000_purchase'
    ]

    res = collector.process(gads_df)
    assert pd.Series(expected_result_os).equals(res['os'])
    assert pd.Series(expected_result_uid).equals(res['uid'])

  def test_save(self):
    pass

  def _create_mock_expand_mcc(self):
    return [ 1234567890, 9876543210 ]

  def _create_mock_fetch_campaigns(self, gads_campaigns):
    column_names = [
      'campaign_name',
      'campaign_app_campaign_setting_app_id',
      'campaign_selective_optimization_conversion_actions']
    results = gads_campaigns[column_names].values.tolist()
    return GaarfReport(results, column_names)

  def _create_mock_fetch_conversion_action(self, gads_df):
    column_names = [
      'app_id',
      'property_id',
      'property_name',
      'event_name',
      'type',
      'last_conversion_date'
    ]
    results = gads_df[column_names].values.tolist()
    return GaarfReport(results, column_names)



class TestHelperFunctions:

  @pytest.fixture(scope='class', name='collector')
  def fixture_collector(self, auth, collector_config, db):
    return GAdsCollector(auth, collector_config, db)

  def test_create_report_fetcher(self, collector: GAdsCollector):
    resp = collector.create_report_fetcher()
    assert isinstance(resp, AdsReportFetcher)

  def test_create_google_ads_yaml(self, collector: GAdsCollector, auth):
    google_ads_yaml = collector.create_google_ads_yaml(auth)
    required_keys = [
        'client_id', 'client_secret', 'refresh_token', 'login_customer_id',
        'developer_token', 'use_proto_plus']

    assert set(required_keys).issubset(google_ads_yaml.keys())
    for key in required_keys:
      assert auth[key] == google_ads_yaml[key]


  def test_create_campaigns_query(self, collector: GAdsCollector):
    valid_query = '''
      SELECT
        campaign.name,
        campaign.app_campaign_setting.app_id,
        campaign.selective_optimization.conversion_actions
      FROM
        campaign
      WHERE
        campaign.app_campaign_setting.app_id IS NOT NULL
        AND campaign.status = 'ENABLED'
    '''
    valid_query = re.sub(r'\s+', ' ', valid_query).strip()

    query = collector.create_campaigns_query()
    query = re.sub(r'\s+', ' ', query).strip()

    assert valid_query == query

  def test_create_conversion_actions_queries(self, collector: GAdsCollector,
                                             customer_ids: list[str]):
    conversion_action_resource_names = [
        'customers/1234567890/conversionActions/1111111111',
        'customers/1234567890/conversionActions/2222222222',
        'customers/0987654321/conversionActions/1111111111',
        'customers/0987654321/conversionActions/2222222222']
    substr = 'conversion_action.resource_name IN'

    valid_query = '''
          SELECT
            conversion_action.app_id as app_id,
            conversion_action.firebase_settings.property_id as property_id,
            conversion_action.firebase_settings.property_name as property_name,
            conversion_action.firebase_settings.event_name as event_name,
            conversion_action.type as type,
            metrics.conversion_last_conversion_date as last_conversion_date
          FROM
            conversion_action
          WHERE
            conversion_action.status = 'ENABLED'
            AND conversion_action.app_id IS NOT NULL
            AND conversion_action.firebase_settings.property_id != 0
            AND conversion_action.resource_name IN
    '''
    valid_query = re.sub(r'\s+', ' ', valid_query).strip()

    queries_dict = collector.create_conversion_actions_queries(
      customer_ids, conversion_action_resource_names)

    for _, query in queries_dict.items():
      query = re.sub(r'\s+', ' ', query)
      index = query.find(substr)
      query = query[:index + len(substr)].strip()

      assert valid_query == query

  def test_parse_conversion_actions_from_campaigns(self, collector: GAdsCollector):

    conversion_action_resource_names = [
        [
            'customers/1234567890/conversionActions/1111111111',
            'customers/1234567890/conversionActions/2222222222'
        ],
        [
            'customers/1234567890/conversionActions/1111111111',
            'customers/1234567890/conversionActions/2222222222'
        ],
        [
            'customers/0987654321/conversionActions/1111111111',
            'customers/0987654321/conversionActions/2222222222'
        ],
        [
            'customers/0987654321/conversionActions/1111111111',
            'customers/0987654321/conversionActions/2222222222'
        ]
    ]
    df = pd.DataFrame({'campaign_selective_optimization_conversion_actions':
                       conversion_action_resource_names})
    ca = collector._parse_conversion_actions_from_campaigns(df)
    assert len(ca) == 4

  def test_get_os__android(self, collector: GAdsCollector):
    os = collector._get_os('FIREBASE_ANDROID_CUSTOM')
    assert os.lower() == 'android'

  def test_get_os__ios(self, collector: GAdsCollector):
    os = collector._get_os('THIRD_PARTY_APP_ANALYTICS_IOS_CUSTOM')
    assert os.lower() == 'ios'

  def test_get_os__other(self, collector: GAdsCollector):
    os = collector._get_os('FLOODLIGHT_ACTION')
    assert os == ''

  def test_add_uid(self, collector: GAdsCollector, gads_df: pd.DataFrame):
    df = gads_df.copy()

    os = df['os'][0]
    property_id = df['property_id'][0]
    event_name = df['event_name'][0]
    test_uid = f'{os.lower()}_{property_id}_{event_name}'

    uid = df.apply(collector._add_uid, axis=1)

    assert isinstance(uid, pd.Series)
    assert test_uid == uid[0]

  def test_get_conversion_action_ids_for_customer_id(self, collector: GAdsCollector):
    customer_id = '1234567890'
    customer_resources = [
        'customers/1234567890/conversionActions/1111111111',
        'customers/1234567890/conversionActions/2222222222'
    ]
    non_customer_resources = [
        'customers/0987654321/conversionActions/1111111111',
        'customers/0987654321/conversionActions/2222222222'
    ]
    conversion_action_resource_names = customer_resources + non_customer_resources

    resources = collector._get_conversion_action_resource_name_for_customer_id(
      conversion_action_resource_names,
      customer_id
    )

    assert resources == "', '".join(customer_resources)
