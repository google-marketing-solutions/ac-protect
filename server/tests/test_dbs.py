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
"""Tests for the DB module."""
# pylint: disable=C0330, missing-function-docstring, g-bad-import-order

import datetime

import pandas as pd
import pytest
import yaml
from google.cloud import bigquery

from server.classes import alerts
from server.db import bq


@pytest.fixture(scope='module', name='config')
def fixture_config():
  file_path = 'server/tests/test_files/config_test_DO_NOT_UPLOAD.yaml'
  with open(file_path, 'r', encoding='utf-8') as f:
    conf = yaml.safe_load(f)
  return conf


@pytest.fixture(scope='module', name='bq_config')
def fixture_bq_config(config):
  bq_config = config['bigquery']
  auth = config['auth']

  db = bq.BigQuery(auth, bq_config)

  return {
    'db': db,
    'project_id': auth['project_id'],
    'test_dataset': 'ac_protect_test_dataset',
    'test_table': 'bq_tests',
  }


@pytest.fixture(scope='module', name='db')
def fixture_db(bq_config):
  db = bq_config['db']
  db.dataset = 'test_dataset'
  db.connect(bq_config['project_id'])
  yield db


@pytest.fixture(scope='module', name='table_name')
def fixture_table_name():
  return 'test_table'


@pytest.fixture(scope='module', name='schema')
def fixture_schema():
  return [
    bigquery.SchemaField('name', 'STRING', 'REQUIRED'),
    bigquery.SchemaField('id', 'STRING', 'REQUIRED'),
  ]


@pytest.fixture(scope='module', name='df')
def fixture_df():
  return pd.DataFrame(
    [
      {'name': 'Phred Phlyntstone', 'id': '32'},
      {'name': 'Wylma Phlyntstone', 'id': '29'},
    ]
  )


class TestDbFunctions:
  """Tests for database functions.

  Tests the core database functionality including:
  - Client connections
  - Dataset operations
  - Table operations (write, append, overwrite)
  - Value retrieval
  - Last run tracking
  - Alert writing
  """

  def test_client_connection(self, bq_config):
    db = bq_config['db']
    db.connect(bq_config['project_id'])
    assert isinstance(db.client, bigquery.Client)

  @pytest.fixture(scope='class', name='db')
  def fixture_db(self, bq_config):
    db = bq_config['db']

    db.connect(bq_config['project_id'])
    yield db

  def test_get_dataset(self, db):
    dataset = db.get_dataset(db.dataset)
    assert isinstance(dataset, bigquery.Dataset)

  def test_write_to_table__success(self, db, table_name, df):
    resp = db.write_to_table(table_name, df)
    assert resp is True

  def test_append_to_table(self, db, table_name, df):
    _ = db.write_to_table(table_name, df)
    _ = db.write_to_table(table_name, df)
    data = db.get_values_from_table(table_name)
    assert len(data) > 2

  def test_overwrite_table(self, db, table_name, df):
    db.write_to_table(table_name, df, overwrite=True)
    data = db.get_values_from_table(table_name)
    assert len(data) == 2

  def test_get_values_from_table(self, db, table_name, df):
    _ = db.write_to_table(table_name, df)
    resp_df = db.get_values_from_table(table_name)
    assert isinstance(resp_df, pd.DataFrame)

  def test_update_last_run(self, db):
    resp = db.update_last_run('Test-Collector', 'Collector')
    assert resp is True

  def test_get_last_run(self, db):
    timestamp = db.get_last_run('Test-Collector', 'Collector')
    assert isinstance(timestamp, datetime.datetime)
    assert datetime.date.today() == timestamp.date()

  def test_write_alerts(self, db):
    alerts_list = [
      alerts.Alert(
        'test_app_1',
        'test_rule',
        'test_trigger',
        {'key': 'value'},
        'test_alert_id_1',
      ),
      alerts.Alert(
        'test_app_2',
        'test_rule',
        'test_trigger_2',
        {'key': 'value', 'key2': 'value2'},
        'test_alert_id_2',
      ),
    ]
    resp = db.write_alerts(alerts_list)
    assert resp is True

  def test_get_alerts_for_app_since_date(self, db):
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    df = db.get_alerts_for_app_since_date_time('test_app_1', yesterday)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
