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
"""Defines a Collector class for Google Play Store."""
# pylint: disable=C0330, g-bad-import-order, g-multiple-import

from dataclasses import fields

import pandas as pd
from google.oauth2 import credentials
from googleapiclient import discovery
from googleapiclient.errors import HttpError, UnknownApiNameOrVersion

from server import utils
from server.classes.collector import Collector
from server.db import tables
from server.db.bq import BigQuery
from server.logger import logger


class PlayStoreCollector(Collector):
  """Collector class for Google Play Store.

  Attributes:
    apps: a list of app identifiers (formated as strings)
    bq: a BigQuery object used to save the collected data in BQ
    columns: columns associated with the Play Store collector table in BQ
    creds: Credentials for connecting with Google Play Store
  """

  def __init__(
    self, auth: dict[str, str], apps: list[str], bq: BigQuery
  ) -> None:
    """Initializes PlayStoreCollector class.

    Args:
      apps: a list of app identifiers (formated as strings)
      bq: a BigQuery object used to save the collected data in BQ
    """

    super().__init__('play-store-collector', 'collector')

    self.apps = apps
    self.bq = bq
    self.columns = [field.name for field in fields(tables.PlayStoreTable)]
    self.creds = self.create_play_credentials(auth)

  def collect(self) -> pd.DataFrame:
    """Fetches app data from Google Play Store.

    Returns:
      All data fetched from Google Play Store for the requested apps, formated
      as a DataFrame.
    """
    logger.info('Play Store Collector - Running "collect"')
    combined_df = pd.DataFrame()
    for app_id in self.apps:
      try:
        app_data = self.get_app_from_play_store(app_id)
        app_df = pd.DataFrame(app_data)
        app_df['app_id'] = app_id
        combined_df = pd.concat(
          [app_df, combined_df], join='outer', ignore_index=True
        )
      except (HttpError, UnknownApiNameOrVersion) as e:
        logger.error(e)

    return combined_df

  def process(self, df: pd.DataFrame) -> pd.DataFrame:
    """Format collected data.

    Extracts app_id, version and track name from collected play store data and
    adds a timestamp.

    Args:
      df: DataFrame with collecter data.

    Returns:
      DataFrame in the format (columns) required by the db table.
    """

    logger.info('Play Store Collector - processing data')
    new_df = pd.DataFrame()

    new_df['app_id'] = df['app_id']
    new_df[['track', 'version']] = df['tracks'].apply(
      lambda x: pd.Series(self.extract_version_and_track(x))
    )
    new_df['timestamp'] = pd.Timestamp.today()

    return new_df

  def save(self, df: pd.DataFrame, overwrite: bool = False) -> None:
    """Saves Google Play Store data.

    Saves to 'collector_play_store' table in BigQuery and updates the last_run
    table.

    Args:
      df: DataFrame to write to BigQuery.
      overwrite: Whether to overwrite existing data in the table. Defaults to
      False.
    """
    logger.info(
      'Play Store Collector - saving data to %s', tables.APP_STORE_TABLE_NAME
    )
    self.bq.write_to_table(tables.PLAY_STORE_TABLE_NAME, df, overwrite)
    self.bq.update_last_run(self.name, self.type_)

  def create_play_credentials(
    self, auth: dict[str, str]
  ) -> credentials.Credentials:
    return credentials.Credentials(
      None,
      refresh_token=auth['refresh_token'],
      token_uri='https://oauth2.googleapis.com/token',
      client_id=auth['client_id'],
      client_secret=auth['client_secret'],
      scopes=[utils.Scopes.ANDROIDPUBLISHER.value],
    )

  def get_app_from_play_store(self, package_name: str) -> dict[str, str]:
    with discovery.build(
      'androidpublisher', 'v3', credentials=self.creds
    ) as service:
      edit_response = service.edits()

      insert_response = edit_response.insert(
        body={}, packageName=package_name
      ).execute()
      edit_id = insert_response['id']

    return (
      edit_response.tracks()
      .list(packageName=package_name, editId=edit_id)
      .execute()
    )

  def extract_version_and_track(
    self, cell_dict: dict[str, str]
  ) -> tuple[str | None, str | None]:
    try:
      # Convert string to dictionary
      track = cell_dict.get('track')
      releases = cell_dict.get('releases', [])
      if version := releases[-1].get('versionCodes'):
        return track, version[0]
      return track, None
    except (ValueError, SyntaxError, IndexError, AttributeError):
      return None, None
