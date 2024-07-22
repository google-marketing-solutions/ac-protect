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
from dataclasses import fields
from unittest import mock

import pandas as pd
import pytest

from server.collectors import play_store
from server.db import tables


@pytest.fixture(name='test_play_apps')
def fixture_test_play_apps():
  return ['com.test.app', 'com.another-test.app']


@pytest.fixture(name='collector')
def fixture_collector(auth, test_play_apps, db):
  return play_store.PlayStoreCollector(auth, test_play_apps, db)


def _create_insert_response():
  return {'id': '12345678901234567890', 'expiryTimeSeconds': '1234567890'}


def _create_tracks_response():
  return {
      'kind':
          'androidpublisher#tracksListResponse',
      'tracks': [{
          'track': 'production',
          'releases': [{
              'name': 'Test release 1',
              'versionCodes': ['5']
          }]
      }, {
          'track': 'beta',
          'releases': [{
              'name': 'Beta release',
              'versionCodes': ['3']
          }]
      }]
  }


class TestPlayStoreCollector:
  def test_init_returns_correct_params(self, collector):
    assert collector.name == 'play-store-collector'
    assert collector.type_ == 'collector'
    assert collector.columns == ['app_id', 'version', 'track', 'timestamp']

  @mock.patch(
      'server.collectors.play_store.PlayStoreCollector.get_app_from_play_store')
  def test_collect_returns_df_with_correct_app_ids(self, mock_get, collector,
                                                  test_play_apps):
    mock_get.return_value = _create_tracks_response()
    df = collector.collect()
    assert isinstance(df, pd.DataFrame)
    assert set(df['app_id']).issubset(test_play_apps)

  def test_process_returns_df_with_correct_columns(self, collector):
    tracks_resp = _create_tracks_response()
    df = pd.DataFrame(tracks_resp)
    df['app_id'] = 'test_app_id'
    new_df = collector.process(df)
    assert isinstance(new_df, pd.DataFrame)
    assert {field.name for field in fields(tables.PlayStoreTable)
            }.issubset(list(new_df.columns))

  @mock.patch('server.collectors.play_store.discovery.build')
  def test_get_app_from_play_store_returns_correct_results(self, mock_build,
                                                           collector):
    mock_service = mock.MagicMock()
    mock_edits = mock.MagicMock()
    mock_insert_response = _create_insert_response()
    mock_tracks_response = _create_tracks_response()

    mock_service.edits.return_value = mock_edits
    mock_edits.insert.return_value.execute.return_value = mock_insert_response
    mock_edits.tracks.return_value.list.return_value.execute.return_value = (
      mock_tracks_response
    )
    mock_build.return_value.__enter__.return_value = mock_service

    result = collector.get_app_from_play_store('test.package.name')

    assert result == mock_tracks_response
