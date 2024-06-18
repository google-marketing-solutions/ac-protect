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
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest

from server.collectors.ga4 import GA4Collector
from server.collectors.ga4 import PermissionDenied
from server.collectors.ga4 import RunReportRequest
from server.utils import Scopes

@pytest.fixture(name='collector_config')
def fixture_collector_config(config):
  return config['collectors']['ga4']


class TestGa4CollectorBase:

  def test_init_ga4_collector(self, auth, collector_config, db):
    collector = GA4Collector(auth, collector_config, db)
    assert collector.name == 'GA4-collector'
    assert collector.type_ == 'collector'
    assert list(collector.creds_info.keys()) == [
        'client_id', 'client_secret', 'refresh_token', 'scopes'
    ]

class TestGa4CollecterFunction:

  @pytest.fixture(name='collector')
  def fixture_collector(self, auth, collector_config, db):
    return GA4Collector(auth, collector_config, db)

  @patch('server.collectors.ga4.BetaAnalyticsDataClient.run_report')
  def test_collect(self, mock_run_report, collector):

    os = 'Android'
    app_version = '1.1.2'
    event_name = 'session_start'
    event_count = 2

    mock_query_job_properties = _create_mock_query_job_properties()
    mock_query_job_event_name = _create_mock_query_job_event_name()

    # Set up the mock client to return the mock query job
    collector.bq.client.query.side_effect = [
        mock_query_job_properties, mock_query_job_event_name
    ]

    mock_run_report.return_value = _create_mock_object(
      os, app_version, event_name, event_count)

    resp = collector.collect()

    assert isinstance(resp, pd.DataFrame)
    assert resp.columns.tolist() == [
        'property_id', 'os', 'app_version', 'event_name', 'event_count'
    ]
    assert pd.api.types.is_integer_dtype(resp['property_id'])
    assert resp['property_id'].apply(lambda x: len(str(x)) == 9).all()
    assert (resp['os'] == os).all()
    assert (resp['app_version'] == app_version).all()
    assert (resp['event_name'] == event_name).all()
    assert (resp['event_count'] == event_count).all()

  @patch('server.collectors.ga4.BetaAnalyticsDataClient.run_report')
  @patch('server.collectors.ga4.logger')
  def test_collect__permission_denied(self, mock_logger, mock_run_report,
                                      collector):
    """ Tests that PremissionDenied fails silently and logs the error as info """

    mock_query_job_properties = _create_mock_query_job_properties()
    mock_query_job_event_name = _create_mock_query_job_event_name()

    # Set up the mock client to return the mock query job
    collector.bq.client.query.side_effect = [
        mock_query_job_properties, mock_query_job_event_name
    ]

    mock_run_report.side_effect = PermissionDenied('Error with property_id')
    collector.collect()

    mock_logger.info.assert_called()

  def test_process(self, ga4_pre_process, collector):
    df = collector.process(ga4_pre_process)
    columns = df.columns.tolist()
    assert all(key in columns for key in ['uid', 'date_added'])
    assert pd.api.types.is_integer_dtype(df['event_count'])

  def test_get_creds_info(self, auth, collector):
    creds_info = collector.get_creds_info(auth)
    assert isinstance(creds_info, dict)
    assert creds_info['client_id'] == auth['client_id']
    assert creds_info['client_secret'] == auth['client_secret']
    assert creds_info['refresh_token'] == auth['refresh_token']
    assert creds_info['scopes'] == [Scopes.ANALYTICS_READ_ONLY.value]

  def test_create_request(self, collector):
    property_id = 123456789
    events = ['first_open']
    req = collector.create_request(property_id, events)

    dim_names = [dim.name for dim in req.dimensions]
    met_names = [met.name for met in req.metrics]

    assert isinstance(req, RunReportRequest)
    assert dim_names == ['operatingSystem', 'appVersion', 'eventName']
    assert met_names == ['eventCount']
    assert req.date_ranges[0].start_date == '1daysAgo'
    assert req.date_ranges[0].end_date == 'today'
    assert req.dimension_filter.filter.field_name == 'eventName'
    assert req.dimension_filter.filter.in_list_filter.values == events


  def test_save(self):
    #TODO - implement save test
    pass


# create_request

def _create_mock_object(os, app_version, event_name, event_count):
  mock_response = MagicMock()
  mock_response.dimension_headers = [MagicMock(name='dimensionName')]
  mock_response.metric_headers = [
    MagicMock(name='metricName', type='INTEGER')]
  mock_response.dimension_values=[
    MagicMock(value=os),
    MagicMock(value=app_version),
    MagicMock(value=event_name)
  ]
  mock_response.metric_values=[MagicMock(value=event_count)]

  mock_object = MagicMock()
  mock_object.rows = [mock_response]

  return mock_object


def _create_mock_query_job_properties():
  mock_result_properties = MagicMock()
  mock_result_properties.to_dataframe.return_value = pd.DataFrame(
      {'property_id': [123456789]})

  mock_query_job_properties = MagicMock()
  mock_query_job_properties.total_rows = 1
  mock_query_job_properties.result.return_value = mock_result_properties

  return mock_query_job_properties

def _create_mock_query_job_event_name():

  mock_result_event_name = MagicMock()
  mock_result_event_name.to_dataframe.return_value = pd.DataFrame(
      {'event_name': ['session_start']})

  mock_query_job_event_name = MagicMock()
  mock_query_job_event_name.total_rows = 1
  mock_query_job_event_name.result.return_value = mock_result_event_name

  return mock_query_job_event_name
