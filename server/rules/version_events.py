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
''' Defines a Versions Event Rule class.'''
import re
from dataclasses import dataclass
from typing import Dict
from typing import List

import pandas as pd
from semantic_version import SimpleSpec
from semantic_version import Version

from server.classes.alerts import Alert
from server.classes.rules import Rule
from server.classes.rules import RuleObject
from server.db.tables import GA4_TABLE_NAME
from server.db.tables import GADS_TABLE_NAME


@dataclass
class VersionEventsEvent(RuleObject):
  ''' A VersionEvent triggers when a conversion event has been in the current
  app version (compared to the previous version)'''
  event_name: str
  app_id: str
  cur_ver: str
  prev_ver: str


class VersionsEventsRule(Rule):
  ''' For every App, check if there are conversion events that we saw in the
  previous version that we dont see in the current version
  '''

  def __init__(self, config: Dict):
    apps_config = config['apps']
    bq_config = config['bigquery']
    auth_config = config['auth']

    super().__init__('VersionEventsRule', apps_config, auth_config, bq_config)

    self.app_ids = [str(app_id) for app_id in apps_config.keys()]
    self.gads_table = GADS_TABLE_NAME
    self.ga4_table = GA4_TABLE_NAME

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
                 ga4_data: pd.DataFrame) -> List[VersionEventsEvent]:
    '''Find any conversion action that exist in the previous app version, but do
    no exist in the current version.

    Args:
      gads_data: DataFrame of the Google Ads data from the DB
      ga4_data: DataFrame of the Google Analytics data from the DB

    Returns:
      A list with all VersionEvents that were found
    '''
    uids = self.get_uids(self.app_ids, gads_data)
    ga4_data = self.filter_for_uids(uids, ga4_data)
    ga4_data = self.add_app_ids(gads_data, ga4_data)
    app_ids_list = ga4_data['app_id'].unique().tolist()

    rule_violations = []
    for app_id in app_ids_list:
      app_df = ga4_data.loc[ga4_data['app_id'] == app_id]

      versions = self.get_versions(app_df)
      cur_ver = self.find_latest_version(versions)
      prev_ver = self.find_previous_version(cur_ver, versions)

      missing_events = self.compare_versions(cur_ver, prev_ver, app_df)

      if missing_events:
        rule_violations += missing_events

    return rule_violations

  def create_alerts(self, data: List[VersionEventsEvent]) -> List[Alert]:
    '''Create list of Alert objects from missing events

    Args:
      data: list of Version Events

    Returns:
      a list of Alert objects
    '''

    alerts = []
    for event in data:
      alerts.append(
          Alert(
              app_id=event.app_id,
              rule_name=self.name,
              trigger='Missing event between versions',
              trigger_value={
                  'Event Name': event.event_name,
                  'Missing from Version': event.cur_ver,
                  'Previous Version': event.prev_ver,
              },
              alert_id=f'{self.name}_{event.app_id}_{event.event_name}_{event.cur_ver}_{event.prev_ver}',
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

    df = df.loc[df['app_id'].astype(str).isin(app_ids)]
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

  def get_versions(self, df: pd.DataFrame) -> List[str]:
    ''' Extracts versions from ga4 data

    Args:
      df: DataFrame with 'app_version' column

    Returns:
      List od unique app_versions from the DataFrame
    '''
    return df['app_version'].unique().tolist()

  def is_version(self, version: str) -> bool:
    ''' Returns only valid Semantic Version main versions (in format X.X.X)

    Args:
      version: a string representing a semantic version number

    Returns:
      Boolean if this is in the format of X.X.X
    '''
    pattern = r'^\d+\.\d+\.\d+$'
    return isinstance(version, str) and re.match(pattern, version)

  def find_latest_version(self, versions: List[str]) -> str:
    ''' Gets the highest version number in a list of versions

    Args:
      versions: list of strings representing version numbers

    Returns:
      String representing the highest version number
    '''
    return str(
        SimpleSpec('>0.0.0').select(
            (Version(v) for v in versions if self.is_version(v))))

  def find_previous_version(self, cur_ver: str, versions: List[str]) -> str:
    ''' Gets the previous version from the version stated in the list

    Args:
      cur_ver: the version number of the current version
      versions: list of strings representing version numbers

    Returns:
      String representing the closest version to the current version
    '''
    return str(
        SimpleSpec(f'<{cur_ver}').select(
            (Version(v) for v in versions if self.is_version(v))))

  def compare_versions(self, cur_ver: str, prev_ver: str,
                       df: pd.DataFrame) -> List[VersionEventsEvent]:
    ''' Extracts data for both versions and compares if there are missing
        events in current version

    Args:
      cur_ver: the version number of the current version
      prev_ver: the version number of the previous version
      df: DataFrame with columns 'app_version' and 'event_name'

    Returns:
      List of Version Events
    '''
    cur_ver_events = self.get_events_for_version(cur_ver, df)
    prev_ver_events = self.get_events_for_version(prev_ver, df)

    app_id = df['app_id'].unique().tolist()[0]

    missing_events = list(set(prev_ver_events).difference(set(cur_ver_events)))

    missing_events = [
        VersionEventsEvent(event, app_id, cur_ver, prev_ver)
        for event in missing_events
        if missing_events
    ]

    return missing_events

  def get_events_for_version(self, version: str, df: pd.DataFrame) -> pd.DataFrame:
    ''' Get all events in data for version

    Args:
      version: a string representing a semantic version number
      df: DataFrame with columns 'app_version' and 'event_name'

    Returns:
      DataFrame filtered for this version
    '''
    #TODO - What happens when there are similar version number in differemt os's?
    return df.loc[df['app_version'] == version]['event_name'].unique().tolist()
