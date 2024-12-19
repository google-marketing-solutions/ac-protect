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
"""Defines Version Events Rule and Event classes."""
# pylint: disable=C0330, g-multiple-import

import dataclasses
import datetime
import re
from typing import Any, List

import pandas as pd
import semantic_version

from server.classes import alerts, rules
from server.db import tables
from server.logger import logger


@dataclasses.dataclass
class VersionEventsEvent(rules.RuleObject):
  """Dataclass for VersionEvent rule violation.

  A Version Event is an event that was triggered in a previous version, but is
  missing from the current version.

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
    app_ids: A list of app identifiers (formatted as strings).
  """

  def __init__(self, config: dict[str, Any]):
    """Initializes VersionsEventsRule class.

    Args:
      config: The contents of config.yaml in dict format, containing:
        - apps: Configuration for apps to monitor
        - bigquery: BigQuery connection settings
        - auth: Authentication settings
    """
    apps_config = config['apps']
    bq_config = config['bigquery']
    auth_config = config['auth']

    super().__init__('VersionEventsRule', apps_config, auth_config, bq_config)

    self.app_ids = [str(app_id) for app_id in apps_config.keys()]

  def run(self):
    """Runs the rule logic and updates relevant tables."""
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

  def check_rule(
    self, collectors_data: dict[str, pd.DataFrame]
  ) -> List[VersionEventsEvent]:
    """Tests if the current version is missing events from previous version.

    Find any conversion action that exists in the previous app version, but does
    not exist in the current version.

    The logic of how this is done depends on which tables are available:
      - If GA4 or Google Ads data are missing - log an error (alert) and do not
      check the rule.
      - If App Store data is available - if 'version' in App Store is larger
      than 'version' in GA4 and time between versions is larger than 24 hours,
      create new alert.
      - If Google Play Store data is available - if new 'version_code' detected
      in Production and time of latest 'version' in GA4 is larger than 24 hours,
      create new alert. (**)
      - If no new version is detected in App or Google Play Stores, check if
      there are conversion events that were seen in the previous version, but
      are missing from this one.

      ** Note - Google Play Store API does not show the actual version, but a
      numeric version code. That is why we are not comparing them directly.

    Args:
      collectors_data: A dictionary of DataFrames that holds app and event data
      from GA4 and Google Ads and can also hold app data from App Store and
      Google Play Store.

    Returns:
      A list with all VersionEvents that were found.
    """

    # Check if GA4 and GAds DataFrames exist in the collector
    if collectors_data.get('ga4') is None:
      logger.error('VersionEventsRule - Missing data from GA4 collector')
      return []

    if collectors_data.get('gads') is None:
      logger.error('VersionEventsRule - Missing data from Google Ads Collector')
      return []

    stores = {
      'app_store': collectors_data.get('app_store'),
      'play_store': collectors_data.get('play_store'),
    }

    # Check apps for rule violations
    rule_violations = []
    conversion_events = self.get_conversion_events(collectors_data)
    app_ids_list = conversion_events['app_id'].unique().tolist()
    logger.info('VersionEventsRule - looking on app ids - %s', app_ids_list)
    for app_id in app_ids_list:
      missing_events = self.get_app_missing_events(
        app_id, conversion_events, stores
      )
      if missing_events:
        logger.info(
          'VersionEventsRule - found missing events for %s - %s',
          app_id,
          missing_events,
        )
        rule_violations.extend(missing_events)

    return rule_violations

  def create_alerts(
    self, rule_violations: List[VersionEventsEvent]
  ) -> List[alerts.Alert]:
    """Creates a list of Alert objects from missing events.

    Args:
      rule_violations: List of Version Events.

    Returns:
      A list of Alert objects.
    """

    triggered_alerts = []
    for event in rule_violations:
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
          ),
        )
      )

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

  def add_app_ids(
    self, gads_df: pd.DataFrame, ga4_df: pd.DataFrame
  ) -> pd.DataFrame:
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
      semantic_version.Version(v) for v in versions if self.is_version(v)
    ]
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
      semantic_version.Version(v) for v in versions if self.is_version(v)
    ]
    return str(spec.select(valid_versions))

  def compare_events_between_versions(
    self, app_id: str, cur_ver: str, prev_ver: str, df: pd.DataFrame
  ) -> List[VersionEventsEvent]:
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

    missing_events = list(set(prev_ver_events).difference(set(cur_ver_events)))
    logger.info('VersionEvents - cur_ver_events events - %s', cur_ver_events)
    logger.info('VersionEvents - prev_ver_events events - %s', prev_ver_events)
    logger.info('VersionEvents - missing events - %s', missing_events)

    missing_events = [
      VersionEventsEvent(event, app_id, cur_ver, prev_ver)
      for event in missing_events
      if missing_events
    ]

    return missing_events

  def get_events_for_version(
    self, version: str, df: pd.DataFrame
  ) -> pd.DataFrame:
    """Gets all events in data for version.

    Args:
      version: A string representing a semantic version number.
      df: DataFrame with columns 'app_version' and 'event_name'.

    Returns:
      DataFrame filtered for this version.
    """
    return df.loc[df['app_version'] == version]['event_name'].unique().tolist()

  def get_conversion_events(
    self, collectors_data: dict[str, pd.DataFrame]
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
    ga4_conversion_events = self.filter_by_uids(
      gads_uids, collectors_data['ga4']
    )
    return self.add_app_ids(collectors_data['gads'], ga4_conversion_events)

  def get_app_missing_events(
    self, app_id: str, events: pd.DataFrame, stores: dict[str, pd.DataFrame]
  ) -> List[VersionEventsEvent]:
    """Checks for missing events in an app by store or GA4 data.

    Checks if store data is available and sends relevant app sepcific data to
    check for missing events. If store data is unavailable, falls back to look
    only at GA4 event data.

    Args:
      app_id: The id of the specific app we are testing.
      events: Conversion events data from GA4.
      stores: App and Play Store versions data.

    Returns:
      A list of of version event violations.
    """

    app_events = events.loc[events['app_id'] == app_id]
    versions = self.get_versions(app_events)
    cur_ver = self.find_latest_version(versions)
    prev_ver = self.find_previous_version(cur_ver, versions)
    os = app_events.iloc[0]['os'].lower()
    store_type = 'app_store' if os == 'ios' else 'play_store'
    logger.info(
      'VersionEventsEvent - looking for %s.%s.%s.%s',
      app_id,
      cur_ver,
      prev_ver,
      os,
    )

    store = stores.get(store_type)
    store = (
      store
      if isinstance(store, pd.DataFrame)
      else (pd.DataFrame(columns=['app_id']))
    )
    store = store[store['app_id'] == app_id].reset_index(drop=True)

    if not store.empty:
      return self.check_missing_events_for_app(
        app_id, store, app_events, cur_ver, prev_ver, store_type
      )

    return self.compare_events_between_versions(
      app_id, cur_ver, prev_ver, events
    )

  def check_missing_events_for_app(
    self,
    app_id: str,
    store: pd.DataFrame,
    events,
    cur_ver,
    prev_ver,
    store_type,
  ) -> List[VersionEventsEvent]:
    """Checks if there are any missing events between versions for an app.

    Args:
      app_id: The id of the specific app we are testing.
      store: App and Play Store versions data.
      events_data: Conversion events data from GA4.

    Returns:
      A list of of version event violations.
    """

    sorted_store = store.sort_values('timestamp', ascending=False)
    if store_ver := self.has_new_version_been_released(
      store_type, sorted_store, events, cur_ver
    ):
      ver_store = store[store['version'] == store_ver]
      ver_events = events[events['app_version'] == cur_ver]
      if self.is_gap_larger_than_24_hours(ver_store, ver_events):
        logger.info('VersionEvents - missing first open')
        # Potential issue with all store events -> send first_open event Alert.
        return [VersionEventsEvent('first_open', app_id, store_ver, cur_ver)]
      else:
        return self.compare_events_between_versions(
          app_id, cur_ver, prev_ver, events
        )
    return []

  def has_new_version_been_released(
    self,
    store_type: str,
    store: pd.DataFrame,
    events: pd.DataFrame,
    events_cur_ver: str,
  ) -> str | None:
    """Checks if a new app version has been released in the Store.

    Compares app versions depending on the store type:
    - If the app is from App Store, checks if the version in App Store is larger
      than the highest version for the app in GA4 events.
    - If the app is from Play Store, checks which is later - the time that we
      saw a version change in Play Store or in GA4 events. (**)

      ** Note: Google Play Store API does not show the actual version, but a
      numeric version code. That is why we are not comparing them directly.

    Args:
      store_type: Store type is app_store or play_store.
      store: App or Play Store versions data.
      events: Conversion events data from GA4.
      events_cur_ver: The latest version of the app as seen in GA4

    Returns:
      If the version in store is newer, returns the version number. Else returns
      None.
    """
    store_ver_values = store['version'].values.tolist()
    store_ver = store_ver_values[0] if len(store_ver_values) > 0 else None

    if store_ver:
      if store_type == 'app_store':
        if semantic_version.Version(store_ver) > semantic_version.Version(
          events_cur_ver
        ):
          return store_ver

      if store_type == 'play_store':
        store_first = (
          store[store['version'] == store_ver].tail(1).reset_index(drop=True)
        )
        event_first = (
          events[events['app_version'] == events_cur_ver]
          .tail(1)
          .reset_index(drop=True)
        )

        store_timestamp = pd.to_datetime(store_first['timestamp'][0], utc=True)
        event_timestamp = pd.to_datetime(event_first['date_added'][0], utc=True)

        if store_timestamp > event_timestamp:
          return store_ver

    return None

  def is_gap_larger_than_24_hours(
    self, store: pd.DataFrame, events: pd.DataFrame
  ) -> bool:
    """Checks if difference between versions is over 24 hours.

    Determines whether the time gap between the most recent store version update
    and the earliest event version update is greater than 24 hours.

    Args:
      store: App or Play Store versions data.
      events: Conversion events data from GA4.
    Returns:
      True if gap is larger than 24 hours, False otherwise.
    """
    store_last = store.head(1)
    events_first = events.tail(1).reset_index()

    buffer = datetime.timedelta(hours=24)
    store_timestamp = pd.to_datetime(store_last['timestamp'][0]).tz_localize(
      None
    )
    event_timestamp = pd.to_datetime(events_first['date_added'][0]).tz_localize(
      None
    )

    return store_timestamp > event_timestamp + buffer
