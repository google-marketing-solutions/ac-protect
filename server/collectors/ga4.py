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
''' Defines a Collector class for GA4.'''
from dataclasses import fields
from datetime import datetime
from typing import Dict
from typing import List

import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.api_core.exceptions import PermissionDenied
from google.oauth2.credentials import Credentials

from server.classes.collector import Collector
from server.db.bq import BigQuery
from server.db.tables import GA4_TABLE_NAME
from server.db.tables import Ga4Table
from server.db.tables import GADS_TABLE_NAME
from server.logger import logger
from server.utils import Scopes


class GA4Collector(Collector):
  ''' Collector class for GA4  '''

  def __init__(self, auth: Dict, config: Dict, bq: BigQuery) -> None:
    super().__init__('GA4-collector', 'collector')

    self.ga4_config = config
    self.creds_info = self.get_creds_info(auth)

    self.bq = bq
    self.columns = [field.name for field in fields(Ga4Table)]

  def collect(self) -> pd.DataFrame:
    ''' Collects data for events in GA4 property ids.

    Data for which events and property ids to run on is extracted from db.

    Returns:
      Pandas DataFrame of collected data.
    '''

    credentials = Credentials.from_authorized_user_info(info=self.creds_info)
    client = BetaAnalyticsDataClient(credentials=credentials)

    properties = self.bq.get_values_from_table(
        GADS_TABLE_NAME, ['property_id'])['property_id'].unique().tolist()
    events = self.bq.get_values_from_table(
        GADS_TABLE_NAME, ['event_name'])['event_name'].unique().tolist()
    rows = []

    for property_id in properties:
      logger.info(f'getting data for property - {property_id}')
      request = self.create_request(property_id, events)
      try:
        response = client.run_report(request)
        for row in response.rows:
          row = list(row.dimension_values) + list(row.metric_values)
          row = [col.value for col in row]
          row = [property_id, *row]
          rows.append(row)
      except PermissionDenied as e:
        logger.info('Error accessing property ID %s - %s', property_id,
                    e.errors)
        #TODO - What happends to the whole process if there is an error in this collector?

    df = pd.DataFrame(rows, columns=self.columns[:-2])
    return df

  def process(self, df: pd.DataFrame) -> pd.DataFrame:
    ''' Adds uid and date_added columns and ensures "event_count" is of type int.

    Args:
      df: Pandas DataFrame with GA4 raw data.

    Returns:
      Original DataFrame with 2 new columns "uid" and "date_added" and updated
      "event_count".
    '''

    df['event_count'] = df['event_count'].astype(int)
    df['uid'] = df.apply(self._add_uid, axis=1)
    df['date_added'] = df.apply(self._add_added_time, axis=1)
    return df

  def save(self, df: pd.DataFrame, overwrite: bool = False) -> None:
    ''' Saves GA4 data to relevant table in BigQuery and updates last_run table

    Args:
      df: DataFrame to write to BigQuery.
      overwrite: Whether to overwrite existing data in the table. Defaults to
      False.
    '''
    logger.info(f'ga4 collector - saving data to {GA4_TABLE_NAME}')
    self.bq.write_to_table(GA4_TABLE_NAME, df, overwrite)
    self.bq.update_last_run(self.name, self.type_)

  def get_creds_info(self, auth: Dict) -> Dict:
    ''' Creates a credentials dictionary in the relevant format for
    "Credentials.from_authorized_user_info"

    Args:
      auth: the auth field from the config file

    Returns:
      A dictionary in the relevant format.
    '''

    return {
        'client_id': auth['client_id'],
        'client_secret': auth['client_secret'],
        'refresh_token': auth['refresh_token'],
        'scopes': [Scopes.ANALYTICS_READ_ONLY.value]
    }

  def create_request(self, property_id: str, events: List[str]):
    ''' Creates the request in GA4 format. Checks data for the last day.

    Args:
      property_id: The GA4 property_id that we are building the request for.
      events: The events that we want to lookup for this property_id.

    Returns:
      A RunReportRequest object.
    '''

    return RunReportRequest(
        property=f'properties/{property_id}',
        dimensions=[
            Dimension(name='operatingSystem'),
            Dimension(name='appVersion'),
            Dimension(name='eventName'),
        ],
        metrics=[Metric(name='eventCount')],
        date_ranges=[DateRange(start_date='1daysAgo', end_date='today')],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name='eventName',
                in_list_filter=Filter.InListFilter(values=events)),))

  def _add_uid(self, df: pd.DataFrame) -> str:
    ''' Helper method to create a uid.

    Args:
      df: Pandas Dataframe with GA4 data for "os", "property_id" and
      "eventName".

    Returns:
      A string representing the uid.

    '''
    os = df['os']
    property_id = df['property_id']
    event_name = df['event_name']

    return f'{os.lower()}_{property_id}_{event_name}'

  def _add_added_time(self, df: pd.DataFrame) -> str:
    ''' Helper method to create a formated string of the current date.

    Returns:
      A string representing the current date.

    '''

    return str(datetime.now().strftime('%Y-%m-%d'))
