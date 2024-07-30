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
"""Defines Version Events Rule and Event classes."""
import dataclasses
import datetime
import re
from typing import Any
from typing import List

import pandas as pd
import semantic_version

from server.classes import alerts
from server.classes import rules
from server.db import tables
from server.logger import logger


@dataclasses.dataclass
class VersionEventsEvent(rules.RuleObject):
  """Dataclass for VersionEvent rule violation.

  Attributes:
    event_name: The name of the event that was triggered the violation.
    app_id: App Id that the event was triggered in.
    cur_ver: Current version of the app, where the event is missing.
    prev_ver: Previous version of the app, where the event was last seen.

  """
  event_name: str
  app_id: str
  cur_ver: str
  prev_ver: str


class VersionEventsRule(rules.Rule):
  """Checks if events from previous version are missing from current version.

  For every App, check if there are conversion events that we saw in the
  previous version that we dont see in the current version.

  Attributes:
    app_ids: a list of app identifiers (formated as strings).
  """

  def __init__(self, config: dict[str, dict[str, Any]]):
    """Initializes VersionsEventsRule class.

    Args:
      config: The contents of config.yaml in dict format.
    """
    apps_config = config['apps']
    bq_config = config['bigquery']
    auth_config = config['auth']

    super().__init__('VersionEventsRule', apps_config, auth_config, bq_config)

    self.app_ids = [str(app_id) for app_id in apps_config.keys()]

  def run(self):
    """Runs the rule logic and updates relevant tables"""
    collectors_data = self.get_data()
    rule_violations = self.check_rule(collectors_data)
    triggered_alerts = self.create_alerts(rule_violations)
    self.update_alerts_log(triggered_alerts)
    self.update_last_run()

  def get_data(self) -> dict[pd.DataFrame | None]:
    """Fetches values from all relevant tables.

    Fetches all app and events data from the GA4, GAds, Play Store and App Store
    collector tables.

    Returns:
      A dictionary with DataFrames for each of the tables that was pulled
      successfully from the DB, or None of there was an error fetching the
      table.
    """

    collector_tables = {
        'app_store': tables.APP_STORE_TABLE_NAME,
        'ga4': tables.GA4_TABLE_NAME,
        'gads': tables.GADS_TABLE_NAME,
        'play_store': tables.PLAY_STORE_TABLE_NAME,
    }

    collectors_data = {}

    for key, table_name in collector_tables.items():
      collectors_data[key] = self.db_client.get_values_from_table(table_name)

    return collectors_data

  def check_rule(self, collectors_data: dict[str, pd.DataFrame]
                 ) -> List[VersionEventsEvent]:
    """Tests if the current version is missing events from previous version.

    Find any conversion action that exist in the previous app version, but does
    not exist in the current version.

    The logic of how this is done depends on which tables are available:
      - If GA4 or Google Ads data are missing - log an error (alert) and do not
      check the rule.
      - If App Store data is available - if 'version' in App Store is larger
      than 'version' in GA4 and time between versions is larger than the
      preconfigured amount set, create new alert (if one has not been created).
      - If Google Play Store data is available - if new 'version_code' detected
      in Production and time of latest 'version' in GA4 is larger than the
      preconfigured amount set, create new alert (if one has not been
      created). (**)
      - If no new version is detected in App or Google Play Stores, check if
      there are conversion events that were seen in the previous version, but
      are missing from this one.

      ** Note - Google Play Store API does not show the actual version, but a
      numeric version code. That is why we are not comparing them directly

    Args:
      collectors_data: A dictionary of DataFrames that holds app and event data
      from GA4 and Google Ads and can also hold app data from App Store and
      Google Play Store.

    Returns:
      A list with all VersionEvents that were found.
    """

    #Check if GA4 and GAds DataFrames exist in the collector
    if collectors_data.get('ga4') is None:
      logger.error('VersionEventsRule - Missing data from GA4 collector')
      return []

    if collectors_data.get('gads') is None:
      logger.error('VersionEventsRule - Missing data from Google Ads Collector')
      return []

    current_date = datetime.datetime.now()
    time_period = current_date - datetime.timedelta(days=7)

    stores_latest_versions = {}

    # Get the latest versions of apps from the App Store and Google Play Store
    if isinstance(collectors_data.get('app_store'), pd.DataFrame):
      app_store_data = collectors_data['app_store'].copy()
      stores_latest_versions['app_store'] = self.get_latest_versions_of_apps(
          app_store_data, time_period)

    if isinstance(collectors_data.get('play_store'), pd.DataFrame):
      play_store_data = collectors_data['play_store'].copy()
      stores_latest_versions['play_store'] = self.get_latest_versions_of_apps(
          play_store_data, time_period)

    # Check apps for rule violations
    rule_violations = []
    conversion_events = self.get_conversion_events(collectors_data)
    app_ids_list = conversion_events['app_id'].unique().tolist()
    for app_id in app_ids_list:
      missing_events = self.get_app_missing_events(app_id, conversion_events,
                                                   stores_latest_versions)
      if missing_events:
        rule_violations.extend(missing_events)

    return rule_violations

  def create_alerts(self, data: List[VersionEventsEvent]) -> List[alerts.Alert]:
    """Creates a list of Alert objects from missing events.

    Args:
      data: List of Version Events.

    Returns:
      A list of Alert objects.
    """

    triggered_alerts = []
    for event in data:
      triggered_alerts.append(
          alerts.Alert(
              app_id=event.app_id,
              rule_name=self.name,
              trigger='Missing event between versions',
              trigger_value={
                  'Event Name': event.event_name,
                  'Missing from Version': event.cur_ver,
                  'Previous Version': event.prev_ver,
              },
              alert_id=(
                f'{self.name}_{event.app_id}_{event.event_name}_{event.cur_ver}'
                f'_{event.prev_ver}'
              )
          ))

    return triggered_alerts

  def get_uids(self, app_ids: List[str], df: pd.DataFrame) -> List[str]:
    """Extracts unique uids from data for relevant apps.

    Args:
      app_ids: A list of app_ids that we want to get uids for.
      df: DataFrame that includes app_id and relevant uids.

    Returns:
      A list of uids.
    """

    df = df.loc[df['app_id'].astype(str).isin(app_ids)]
    uids = df['uid'].to_list()
    return list(set(uids))

  def filter_by_uids(self, uids: List[str], df: pd.DataFrame) -> pd.DataFrame:
    """Filters the DataFrame for the selected uids.

    Args:
      uids: A list of uids that we want to filter for.
      df: The DataFrame that we want to filter on.

    Returns:
      DataFrame filtered for the specified uids.
    """
    return df.loc[df['uid'].isin(uids)]

  def add_app_ids(self, gads_df: pd.DataFrame,
                  ga4_df: pd.DataFrame) -> pd.DataFrame:
    """Adds app_ids and uids from gads to ga4 data by matching uids.

    Args:
      gads_df: DataFrame with Google Ads Data.
      ga4_df: DataFrame with Google Analytics Data.

    Returns:
      DataFrame with merged data between gads and ga4.
    """
    app_id_df = gads_df[['app_id', 'uid']]
    return pd.merge(ga4_df, app_id_df, on='uid')

  def get_versions(self, df: pd.DataFrame) -> List[str]:
    """Extracts versions from ga4 data.

    Args:
      df: DataFrame with 'app_version' column.

    Returns:
      List of unique app_versions from the DataFrame.
    """
    return df['app_version'].unique().tolist()

  def is_version(self, version: str) -> bool:
    """Returns only valid Semantic Version main versions (in format 'X.X.X').

    Args:
      version: a string representing a semantic version number.

    Returns:
      Boolean if this is in the format of 'X.X.X'.
    """
    pattern = r'^\d+\.\d+\.\d+$'
    return isinstance(version, str) and re.match(pattern, version)

  def find_latest_version(self, versions: List[str]) -> str:
    """Gets the highest version number in a list of versions.

    Args:
      versions: list of strings representing version numbers.

    Returns:
      String representing the highest version number.
    """
    spec = semantic_version.SimpleSpec('>0.0.0')
    valid_versions = [
        semantic_version.Version(v) for v in versions if self.is_version(v)]
    return str(spec.select(valid_versions))

  def find_previous_version(self, cur_ver: str, versions: List[str]) -> str:
    """Gets the previous version from the version stated in the list.

    Args:
      cur_ver: The version number of the current version.
      versions: List of strings representing version numbers.

    Returns:
      String representing the closest version to the current version.
    """
    spec = semantic_version.SimpleSpec(f'<{cur_ver}')
    valid_versions = [
        semantic_version.Version(v) for v in versions if self.is_version(v)]
    return str(spec.select(valid_versions))

  def compare_events_between_versions(self, cur_ver: str, prev_ver: str,
                       df: pd.DataFrame) -> List[VersionEventsEvent]:
    """Compares two lists of events from two different versions.

    Extracts data for both versions and compares if there are events in the
    previous version that are missing from the current version.

    Args:
      cur_ver: The version number of the current version.
      prev_ver: The version number of the previous version.
      df: DataFrame with columns 'app_version' and 'event_name'.

    Returns:
      List of Version Events.
    """
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

  def get_events_for_version(self, version: str, df: pd.DataFrame
                             ) -> pd.DataFrame:
    """Gets all events in data for version.

    Args:
      version: A string representing a semantic version number.
      df: DataFrame with columns 'app_version' and 'event_name'.

    Returns:
      DataFrame filtered for this version.
    """
    #TODO - What happens when there are similar version number in different os's?
    return df.loc[df['app_version'] == version]['event_name'].unique().tolist()

  def get_latest_versions_of_apps(self, apps_data: pd.DataFrame,
                                  time_period: datetime.date
                                  ) -> pd.DataFrame:
    """Gets only the latest versions of all apps in a DataFrame.

    Filters the most up-to-date versions of all the apps in the DataFrame in
    a given time period (i.e. last 7 days).

    Args:
      apps_data: App data from the App/Play Store.
      time_period: The lookback window.

    Returns:
      DataFrame with one row for every app with the highest version number.
    """
    apps_data['timestamp'] = pd.to_datetime(apps_data['timestamp'])
    apps_recent = apps_data[apps_data['timestamp'] >= time_period]
    return apps_recent.loc[apps_recent.groupby('app_id')['timestamp'].idxmax()]

  def get_conversion_events(self, collectors_data: dict[str, pd.DataFrame]
                            ) -> pd.DataFrame:
    """Adds AppIds from GAds to GA4 data.

    Connects AppId with from GAds with GA4 event data making sure that we are
    only taking into account conversion events (events that are in GAds).

    Args:
      collectors_data: Data from rule's relevant collectors.

    Returns:
      DataFrame of GA4 data, filtered only for GAds conversion events, including
      the App Ids.

    """
    gads_uids = self.get_uids(self.app_ids, collectors_data['gads'])
    ga4_conversion_events = self.filter_by_uids(gads_uids,
                                                collectors_data['ga4'])
    return self.add_app_ids(collectors_data['gads'], ga4_conversion_events)

  def get_missing_events_app_store(self, store: pd.DataFrame, app_id: str,
                                     cur_ver: str) -> List[VersionEventsEvent]:
    """Finds if there are version mismatches between GA4 and App Store.

    If 'version' in App Store is larger than 'version' in GA4 and time between
    versions is larger than the preconfigured amount set, this means that there
    is a version mismatch between App Store and GA4 - return a "first_open"
    event violation.

    Args:
      store: App Store collector DataFrame.
      app_id: The app id.
      cur_ver: The current version of the app according to GA4.

    Returns:
      A List with a single "first_open" VersionEventEvent object.
    """
    store_cur_ver = store[store['app_id'] == app_id].reset_index()
    store_ver_num = store_cur_ver.loc[0, 'version']

    if semantic_version.Version(store_ver_num
                              ) > semantic_version.Version(cur_ver):
      return [
        VersionEventsEvent('first_open', app_id, store_ver_num, cur_ver)]
    return []

  def get_missing_events_play_store(self, store: pd.DataFrame, app_id: str,
                                     cur_ver: str,
                                     app_conversion_events: pd.DataFrame
                                     ) -> List[VersionEventsEvent]:
    """Finds if there are version mismatches between GA4 and Google Play Store.

    Google Play Store API does not show the actual version, but instead it shows
    a version code. In order to find version mismatches, we can not compare
    version number directly, but look at a proxy - if the current version in
    Play Store is later than the first instance of the current version in GA4,
    and more than the allowed time has passed - return a "first_open" event
    violation.
    Since we are using time as the metric, we add a 24 hour buffer to make sure
    that we have enough time between app release and users actually using it.

    Args:
      store: Google Play Store collector DataFrame.
      app_id: The app id.
      cur_ver: The current version of the app according to GA4.
      app_conversion_events: conversion events for a specific app.

    Returns:
      A List with a single "first_open" VersionEventEvent object.
    """
    store_cur_ver = store[store['app_id'] == app_id].reset_index()
    store_ver_time = store_cur_ver.loc[0, 'timestamp']
    store_ver_code = store_cur_ver.loc[0, 'version']
    cur_ver_data = app_conversion_events[
      app_conversion_events['app_version'] == cur_ver]
    cur_ver_earliest_time = pd.to_datetime(cur_ver_data['date_added']).min()

    if (store_ver_time - datetime.timedelta(hours=24)) > cur_ver_earliest_time:
      return [VersionEventsEvent('first_open', app_id,
                                               store_ver_code, cur_ver)]
    return []

  def get_missing_events_ga4(
      self, versions: List[str], cur_ver: str,
      app_conversion_events: pd.DataFrame) -> List[VersionEventsEvent]:
    """Finds if there are events missing in current app version.

    Compares GA4 data for current and previous versions of the app and looks
    for conversion events that are missing in the current version.

    Args:
      versions: List of app versions.
      cur_ver: String of current app version (presumes semantic versioning).
      app_conversion_events: conversion events for a specific app.
    """
    prev_ver = self.find_previous_version(cur_ver, versions)
    return self.compare_events_between_versions(cur_ver, prev_ver,
                                                app_conversion_events)

  def get_app_missing_events(
      self, app_id: str, conversion_events: pd.DataFrame,
      stores_latest_versions: dict[str, pd.DataFrame]
      ) -> List[VersionEventsEvent]:
    """Checks for missing events in a new app version.

    Args:
      app_id: The app id.
      conversion_events: GA4 data for events that are classified as conversion
        events in Google Ads. Holds data for all apps that we are following.
      stores_latest_versions: Latest versions of all apps from App Store and
        Google Play Store.

    Returns:
      List of missing events from the current version of the app.
    """
    app_conversion_events = conversion_events.loc[
      conversion_events['app_id'] == app_id]
    versions = self.get_versions(app_conversion_events)
    cur_ver = self.find_latest_version(versions)
    os = app_conversion_events.iloc[0]['os'].lower()

    store_type = 'app_store' if os == 'ios' else 'play_store'
    store = stores_latest_versions.get(store_type)

    if store_type == 'app_store' and isinstance(
      store, pd.DataFrame) and not store.empty:
      return self.get_missing_events_app_store(store, app_id, cur_ver)

    if store_type == 'play_store' and isinstance(
      store, pd.DataFrame) and not store.empty:
      return self.get_missing_events_play_store(store, app_id, cur_ver,
                                                app_conversion_events)

    return self.get_missing_events_ga4(versions, cur_ver, app_conversion_events)
