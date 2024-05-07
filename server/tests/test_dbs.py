from datetime import date
from datetime import datetime
from datetime import timedelta

import pandas as pd
import pytest
import yaml
from google.cloud.bigquery import Client
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import ClientError

from server.classes.alerts import Alert
from server.db.bq import BigQuery

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

  db = BigQuery(auth, bq_config)

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
      SchemaField('name', 'STRING', 'REQUIRED'),
      SchemaField('id', 'STRING', 'REQUIRED')
  ]

@pytest.fixture(scope='module', name='df')
def fixture_df():
  return pd.DataFrame([
    {'name': 'Phred Phlyntstone', 'id': '32'},
    {'name': 'Wylma Phlyntstone', 'id': '29'},
  ])

class TestBaseFunctions:

  def test_client_connection(self, bq_config):
    db = bq_config['db']
    db.connect(bq_config['project_id'])
    assert isinstance(db.client, Client)


class TestDatasetFunctions:

  @pytest.fixture(scope='class', name='db')
  def fixture_db(self, bq_config):
    db = bq_config['db']

    db.connect(bq_config['project_id'])
    yield db

  def test_get_dataset(self, db):
    dataset = db.get_dataset(db.dataset)
    assert isinstance(dataset, Dataset)


class TestTableFunctions:

  def test_write_to_table__success(self, db, table_name, df):
    resp = db.write_to_table(table_name, df)
    assert resp is True

  def test_write_to_table__error(self, db, df):
    #TODO: Find how to trigger ClientError
    pass


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
    assert isinstance(timestamp, datetime)
    assert date.today() == timestamp.date()

  def test_write_alerts(self, db):
    alerts = [
        Alert('test_app_1', 'test_rule', 'test_trigger', {'key': 'value'},
              'test_alert_id_1'),
        Alert('test_app_2', 'test_rule', 'test_trigger_2', {
            'key': 'value',
            'key2': 'value2'
        }, 'test_alert_id_2')
    ]
    resp = db.write_alerts(alerts)
    assert resp is True

  def test_get_alerts_for_app_since_date(self, db):
    yesterday = datetime.now() - timedelta(days=1)
    df = db.get_alerts_for_app_since_date('test_app_1', yesterday)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0

# class TestExceptions:


# def test_exception_table_does_not_exist(bq_config):
#     db = bq_config["db"]
#     with pytest.raises(GoogleAPI)
