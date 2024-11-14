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
"""Defines a Collector class for App Store."""
# pylint: disable=C0330

from dataclasses import fields

import pandas as pd
import requests

from server.classes.collector import Collector
from server.db import tables
from server.db.bq import BigQuery
from server.logger import logger


class AppStoreCollector(Collector):
  """Collector class for Apple App Store.

  Attributes:
    apps: a list of app identifiers (formated as strings)
    bq: a BigQuery object used to save the collected data in BQ
    columns: columns associated with the App Store collector table in BQ
  """

  def __init__(self, apps: list[str], bq: BigQuery) -> None:
    """Initializes AppStoreCollector class.

    Args:
      apps: a list of app identifiers (formated as strings)
      bq: a BigQuery object used to save the collected data in BQ
    """

    super().__init__('app-store-collector', 'collector')

    self.apps = apps
    self.bq = bq
    self.columns = [field.name for field in fields(tables.AppStoreTable)]

  def collect(self) -> pd.DataFrame:
    """Create and initiate a 'lookup' request in App Store for each app id.

    Returns:
      Pandas DataFrame with raw data from App Store lookup function for all
      app IDs that are stored in self.apps.
    """

    logger.info('AppStore Collector - Running "collect"')
    data = {}
    app_store_app_ids = [app_id for app_id in self.apps if app_id.isnumeric()]
    for app_id in app_store_app_ids:
      app_data = self.lookup_app_in_appstore(app_id)
      data.update(app_data)
    return pd.json_normalize(data, sep='_')

  def process(self, all_app_data: pd.DataFrame) -> pd.DataFrame:
    """Removes unnecessary columns.

    Removes all columns except trackId and version and adds the timestamp.

    Args:
      df: Pandas DataFrame with raw data from App Store lookup function

    Returns:
      DataFrame with trackId (app Id), version and timestamp
    """
    logger.info('AppStore Collector - processing data')
    df = pd.DataFrame()
    df['app_id'] = all_app_data['trackId'].astype('string')
    df['version'] = all_app_data['version'].astype('string')
    df['timestamp'] = pd.Timestamp.today()

    return df

  def save(self, df: pd.DataFrame, overwrite: bool = False) -> None:
    """Saves App Store data.

    Saves to 'collector_app_store' table in BigQuery and updates the last_run
    table.

    Args:
      df: DataFrame to write to BigQuery.
      overwrite: Whether to overwrite existing data in the table. Defaults to
      False.
    """
    logger.info(
      'AppStore Collector - saving data to %s', tables.APP_STORE_TABLE_NAME
    )
    self.bq.write_to_table(tables.APP_STORE_TABLE_NAME, df, overwrite)
    self.bq.update_last_run(self.name, self.type_)

  def lookup_app_in_appstore(self, app_id: str) -> dict[str, str]:
    """Trigger a request ao App Store Search API.

    Request all information for a specific app id. Includes version and trackId
    (appId). For other data included, please see
    https://performance-partners.apple.com/search-api

    Args:
      app_id: String of application Id to lookup. i.e. '1234567890'

    Returns:
      Dict including all metadata of the app that can be pulled from the App
      Store Search API.
    """

    try:
      response = requests.get(
        f'https://itunes.apple.com/lookup?id={app_id}', timeout=10
      )
      response.raise_for_status()

      data = response.json()

      return data.get('results', [{}])[0]

    except requests.exceptions.HTTPError as e:
      logger.error('HTTP error occurred: %s', e)
      return {}
