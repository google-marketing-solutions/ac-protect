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
''' Defines an Interval Event Rule class.'''
import dataclasses
import datetime
from typing import Dict
from typing import List

import pandas as pd

from server.classes.alerts import Alert
from server.classes.rules import Rule
from server.classes.rules import RuleObject
from server.db.tables import GA4_TABLE_NAME
from server.db.tables import GADS_TABLE_NAME


@dataclasses.dataclass
class IntervalEvent(RuleObject):
  ''' An Interval Event is an event that has not been seen for an app in the
  time frame of the interval
  '''
  event_name: str
  app_id: str
  interval: int


class IntervalEventsRule(Rule):
  ''' For every App, check if there are events missing in a pre determined
  interval.
  '''

  def __init__(self, config: Dict):
    apps_config = config['apps']
    bq_config = config['bigquery']
    auth_config = config['auth']

    super().__init__('IntervalEventsRule', apps_config, auth_config, bq_config)

    self.app_ids = [str(app_id) for app_id in apps_config.keys()]
    self.gads_table = GADS_TABLE_NAME
    self.ga4_table = GA4_TABLE_NAME
    self.interval = 24

  def run(self):
    ''' Function to run the rule and update relevant logs'''
    [gads_data, ga4_data] = self.get_data()
    rule_violations = self.check_rule(gads_data, ga4_data)
    alerts = self.create_alerts(rule_violations)
    self.update_alerts_log(alerts)
    self.update_last_run()

  def get_data(self) -> List[pd.DataFrame]:
    ''' Get all relevant data from tables to assess the rule

    Returns:
      List of 2 DataFrames - one for Google Ads data and a second for Google
      Analytics data.
    '''

    gads_data = self.db_client.get_values_from_table(self.gads_table)
    ga4_data = self.db_client.get_values_from_table(self.ga4_table)

    return [gads_data, ga4_data]

  def check_rule(self, gads_data: pd.DataFrame,
                 ga4_data: pd.DataFrame) -> List[IntervalEvent]:
    '''Find any conversion action that exists in Google Ads but does not
    exist in GA4 table from the last day.

    Args:
      gads_data: DataFrame of the Google Ads data from the DB
      ga4_data: DataFrame of the Google Analytics data from the DB

    Returns:
      A list with all IntervelEvents that were found
    '''
    yesterday = datetime.date.today() + datetime.timedelta(days=-1)

    gads_uids = self.get_uids(self.app_ids, gads_data)

    ga4_data = self.filter_for_uids(gads_uids, ga4_data)
    ga4_data['date_added'] = pd.to_datetime(ga4_data['date_added']).dt.date
    ga4_data = ga4_data[ga4_data['date_added'] >= yesterday]
    ga4_data = self.add_app_ids(gads_data, ga4_data)

    ga4_uids = ga4_data['uid'].unique().tolist()

    rows = gads_data.loc[~gads_data['uid'].isin(ga4_uids)].to_dict('records')
    rule_violations = []
    for row in rows:
      if row['app_id'] in self.app_ids:
        rule_violations.append(
            IntervalEvent(row['event_name'], row['app_id'], self.interval))

    return rule_violations

  def create_alerts(self, data: List[IntervalEvent]) -> List[Alert]:
    '''Create list of Alert objects from missing events

    Args:
      data: list of interval events

    Returns:
      a list of Alert objects
    '''

    alerts = []
    for event in data:
      alerts.append(
          Alert(
              app_id=event.app_id,
              rule_name=self.name,
              trigger='Missing event for interval',
              trigger_value={
                  'Event Name': event.event_name,
                  'Missing for': self.interval,
              },
              alert_id=f'{self.name}_{event.app_id}_{event.event_name}_{self.interval}',
          ))
    return alerts

  def get_uids(self, app_ids: List[str], df: pd.DataFrame) -> List[str]:
    ''' Extract unique uids from data for relevant apps

    Args:
      app_ids: a list of app_ids that we want to get uids for
      df: DataFrame that includes app_id and relevant uids

    Returns:
      a list of uids
    '''
    df = df.loc[df['app_id'].isin(app_ids)]
    uids = df['uid'].to_list()
    return list(set(uids))

  def filter_for_uids(self, uids: List[str], df: pd.DataFrame) -> pd.DataFrame:
    ''' Filter the DataFrame for the selected uids

    Args:
      uids: a list of uids that we want to filter for
      df: the DataFrame that we want to filter on

    Returns:
      DataFrame filtered for the specified uids
    '''
    return df.loc[df['uid'].isin(uids)]

  def add_app_ids(self, gads_df: pd.DataFrame,
                  ga4_df: pd.DataFrame) -> pd.DataFrame:
    ''' Adds app_ids and uids from gads to ga4 data by matching uids

    Args:
      gads_df: DataFrame with Google Ads Data
      ga4_df: DataFrame with Google Analytics Data

    Returns:
      DataFrame with merged data between gads and ga4
    '''
    app_id_df = gads_df[['app_id', 'uid']]
    return pd.merge(ga4_df, app_id_df, on='uid')
