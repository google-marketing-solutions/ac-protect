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
''' Defines the DB class for BigQuery.'''
import datetime
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd
from google.cloud import exceptions
from google.cloud.bigquery import Client
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import LoadJobConfig

from server.classes.alerts import Alert
from server.classes.base_db import BaseDb
from server.db.tables import ALERTS_TABLE_NAME
from server.logger import logger

class BigQueryException(Exception):
  pass


class BigQuery(BaseDb):
  '''DB Class for connecting, reading and writing from BigQuery'''

  def __init__(self, auth: Dict, config: Dict):
    self.project_id = auth['project_id']
    self.dataset = config['dataset']
    super().__init__()

  def connect(self, project_id: Optional[str] = None) -> Client:
    ''' Connect to BigQuery

    Args:
      project_id: the Google Cloud project_id
    '''
    if not project_id:
      project_id = self.project_id

    if not hasattr(self, 'client'):
      self.client = Client(project=project_id)
    return self.client

  def get_dataset(self, dataset_id: Optional[str]) -> Dataset:
    ''' Gets the Dataset object from BigQuery

    Args:
      dataset_id: The id of the dataset that we want to get. Defaults to the
      dataset configured during __init__.

    Returns:
      A Dataset object
    '''
    dataset_id = dataset_id or self.dataset
    return self.client.get_dataset(f'{self.project_id}.{dataset_id}')

  def get_values_from_table(self,
                            table_id: str,
                            cols: List[str] = None,
                            where: str = '') -> Optional[pd.DataFrame]:
    ''' Query a table in BigQuery

    Args:
      table_id: The table to query
      cols: specific columns we want returned. Defaults to '*', returning all
      columns.
      where: where clause for query.

    Returns:
      A DataFrame with the values returned from the table.
    '''

    # removed from function header as it leads to "dangerous-default-value"
    if not cols:
      cols = ['*']

    query = f'''
      SELECT {', '.join(cols)}
      FROM `{self.project_id}.{self.dataset}.{table_id}`
    '''

    if where:
      query = f'{query}\nWHERE {where}'

    query_job = self.client.query(query)

    try:
      result = query_job.result()
      if result.total_rows:
        return result.to_dataframe()
      return None

    except exceptions.NotFound:
      logger.error('Missing table %s.%s.%s from BQ.',
                   self.project_id, self.dataset, table_id)
      return None
    except Exception as e:
      raise BigQueryException(e) from e

  def write_to_table(self,
                     table_id: str,
                     df: pd.DataFrame,
                     overwrite: bool = False) -> bool:
    ''' Write data to a BigQuery table.

    Args:
      table_id: The table to query
      df: DataFrame with values to write to the table
      overwrite: should the data overwrite or be appended to the table. Defaults
      to 'False'

    Returns:
      Returns True if successful.

    Raises:
      ClientError: If there are any errors while writing to BigQuery.
    '''
    full_table_id = f'{self.project_id}.{self.dataset}.{table_id}'
    job_config = LoadJobConfig()

    if overwrite:
      job_config.write_disposition = 'WRITE_TRUNCATE'

    load_job = self.client.load_table_from_dataframe(
        dataframe=df, destination=full_table_id, job_config=job_config)

    try:
      load_job.result()
    except exceptions.ClientError as e:
      logger.error('%s', load_job.errors)
      raise e

    return True

  def get_last_run(self, name: str, type_: str) -> Optional[datetime.datetime]:
    ''' Gets the last time the collector, rule or service was run.

    Args:
      name: name of the collector / rule / service
      type_: 'collector', 'rule', 'service'

    Returns:
      The last time that the service ran. If never ran, returns None.
    '''
    df = self.get_values_from_table(
        table_id=self.log,
        cols=['timestamp'],
        where=f'name="{name}" AND type="{type_}" ORDER BY timestamp DESC LIMIT 1'
    )

    if df is None or df.empty or df['timestamp'].empty:
      logger.info('No last run found for - %s, %s', name, type_)
      return None

    last_run = df['timestamp'][0].split('.')[0]
    date_time_format = '%Y-%m-%d %H:%M:%S'


    return datetime.datetime.strptime(last_run, date_time_format)

  def update_last_run(self, name: str, type_: str) -> bool:
    ''' Updates the AC Protect log on the last time the collector, rule or
    service was run.

    Args:
      name: name of the collector / rule / service
      type_: 'collector', 'rule', 'service'

    Returns:
      Returns True if successful.
    '''

    timestamp = datetime.datetime.now()
    log = [[name, type_, str(timestamp)]]

    df = pd.DataFrame(log, columns=['name', 'type', 'timestamp'])
    return self.write_to_table(self.log, df)

  def get_alerts_for_app_since_date_time(
      self, app_id: str, date_time: datetime.datetime) -> Optional[pd.DataFrame]:
    ''' Get all the Alerts triggered for a specific app since "date_time"

    Args:
      app_id: the App Id to search for
      date_time: datetime to search from (until now)

    Returns:
      DataFrame with all alerts matching the criteria. Returns None if no Alerts
      are found.
    '''

    if not date_time:
      logger.info('No date was given using default')
      date_time = datetime.datetime.now() - timedelta(days=1)

    filter_ = f'app_id="{app_id}"'

    date_string = date_time.strftime('%Y-%m-%d %H:%M:%S')
    filter_ += f' AND timestamp >= CAST(\'{date_string}\' AS DATETIME FORMAT \'YYYY-MM-DD HH24:MI:SS\')'

    return self.get_values_from_table(table_id=ALERTS_TABLE_NAME, where=filter_)

  def write_alerts(self, alerts: List[Alert]) -> bool:
    ''' Write new alerts to the Alerts table.

    Args:
      alerts: a list of Alert objects to write to the table

    Returns:
      Returns True if successful.
    '''
    alerts = [alert.to_dict() for alert in alerts]
    return self.write_to_table(ALERTS_TABLE_NAME,
                               pd.DataFrame.from_dict(alerts))
