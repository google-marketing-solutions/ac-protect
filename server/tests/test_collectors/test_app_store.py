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
import json
from dataclasses import fields
from unittest import mock

import pandas as pd
import pytest
from requests.models import Response

from server.collectors import app_store
from server.db import tables
from server.db.bq import BigQuery

@pytest.fixture(name='content')
def fixture_content():
  # Mock content that holds a small subset of parameters that
  # are actually returned from an App Store lookup function
  return {
      'trackId': 1111111111,
      'version': '1.2.1',
      'price': 0.0,
      'isGameCenterEnabled': False,
      'genres': ['Finance', 'Utilities']
  }

@pytest.fixture(name='collector')
def fixture_collector(db):
  return app_store.AppStoreCollector([1111111111, 2222222222], db)


def _create_mock_appstore_response(content: dict,
                                   success: bool = True) -> Response:
  json_content = json.dumps({'results': [content]})
  response = Response()

  if success:
    response.status_code = 200
    response._content = json_content.encode('utf-8')  #pylint: disable=protected-access
  else:
    response.status_code = 404

  return response

class TestAppStoreCollector:
  def test_init_returns_correct_params(self, collector):
    assert collector.name == 'app-store-collector'
    assert collector.type_ == 'collector'
    assert collector.columns == ['app_id', 'version', 'timestamp']

  @mock.patch('server.collectors.app_store.requests.get')
  def test_collect_returns_df_with_correct_columns(self,
                                                   mock_get,
                                                   collector,
                                                   content):
    mock_get.return_value = _create_mock_appstore_response(content)
    res = collector.collect()
    assert isinstance(res, pd.DataFrame)
    assert set(content.keys()).issubset(res.columns)

  def test_process_returns_df_with_correct_columns(self, collector, content):
    df = pd.DataFrame(content)
    res = collector.process(df)
    assert isinstance(res, pd.DataFrame)
    assert set(
      [field.name for field in fields(tables.AppStoreTable)]
      ).issubset(res.columns)

  @mock.patch('server.collectors.app_store.requests.get')
  def test_lookup_app_in_appstore_returns_correct_columns(self,
                                                          mock_get,
                                                          collector,
                                                          content):
    mock_get.return_value = _create_mock_appstore_response(content)
    data = collector.lookup_app_in_appstore(1111111111)
    assert isinstance(data, dict)
    assert set(content.keys()).issubset(data.keys())
