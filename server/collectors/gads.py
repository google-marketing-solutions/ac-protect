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
"""Defines a Collector class for Google Ads."""
# pylint: disable=C0330, g-multiple-import

from typing import Any, Dict, List

import pandas as pd
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.report_fetcher import AdsReportFetcher

from server.classes import collector
from server.db import bq, tables
from server.logger import logger

GADS_VERSION = 'v17'


class GAdsCollector(collector.Collector):
  """Collector class for Google Ads."""

  def __init__(
    self,
    auth: Dict[str, str],
    collector_config: Dict[str, Any],
    bq_obj: bq.BigQuery,
  ):
    """Initializes the collector based on the config.

    The general collector is initialized, customer_id and google_ads_yaml
    are extracted from the auth and collector configs and a bq object is
    attached.

    Args:
      auth: The auth configuration from the config.yaml.
      collector_config: The Google Ads configuration from the config.yaml.
      bq_obj: An initialized BQ object.

    """
    super().__init__('GAds-collector', 'collector')
    self.bq = bq_obj
    self.customer_id = str(auth['login_customer_id'])
    self.google_ads_yaml = self.create_google_ads_yaml(auth)

  def collect(self) -> pd.DataFrame:
    """Collects all conversion actions from relevant campaigns in Google Ads.

    Returns:
      Pandas DataFrame of collected data.
    """
    # Init report fetcher and customer_ids

    try:
      report_fetcher = self.create_report_fetcher()
      customer_ids = report_fetcher.expand_mcc(self.customer_id)
    except Exception as e:
      logger.error(
        'Error accessing Google Ads customer id %s - %s', self.customer_id, e
      )
      alert = self.build_connection_alert(self.name, e)
      self.bq.write_alerts([alert])
      return pd.DataFrame([])

    # Get relevant Conversion Actions Ids from Campaigns
    campaigns_query = self.create_campaigns_query()
    campaigns = report_fetcher.fetch(campaigns_query, customer_ids)
    campaigns = campaigns.to_pandas()
    conversion_action_resource_names = (
      self._parse_conversion_actions_from_campaigns(campaigns)
    )

    # Get Conversion Actions
    connection_alerts = []
    conversion_actions = []
    conversion_actions_queries = self.create_conversion_actions_queries(
      customer_ids, conversion_action_resource_names
    )

    for customer_id, query in conversion_actions_queries.items():
      try:
        logger.info(
          'getting conversion actions for customer id - %s', customer_id
        )
        resp = report_fetcher.fetch(query, [customer_id])
        conversion_actions.append(resp.to_pandas())
      except collector.collector_permissions_exeptions as e:
        logger.error(
          'Error accessing customer_id %s - %r', customer_id, e.errors
        )

        alert = self.build_connection_alert(f'customer_{customer_id}', e)
        connection_alerts.append(alert)

    if connection_alerts:
      self.bq.write_alerts(connection_alerts)

    return pd.concat(conversion_actions)

  def process(self, df: pd.DataFrame) -> pd.DataFrame:
    """Adds uid and os columns.

    Args:
      df: Pandas DataFrame with Google Ads raw data.

    Returns:
      Original DataFrame with 2 new columns "uid" and "os".
    """
    df['os'] = df['type'].apply(self._get_os)
    df['uid'] = df.apply(self._add_uid, axis=1)
    return df

  def save(self, df: pd.DataFrame, overwrite: bool = True):
    """Saves Google Ads data to BQ.

    Saves Google Ads data to relevant table in BigQuery and updates
    last_run table.

    Args:
      df: DataFrame to write to BigQuery.
      overwrite: Whether to overwrite existing data in the table. Defaults to
      True.
    """
    logger.info('gads collector - saving data to %s', tables.GADS_TABLE_NAME)
    self.bq.write_to_table(tables.GADS_TABLE_NAME, df, overwrite)
    self.bq.update_last_run(self.name, self.type_)

  def create_report_fetcher(self) -> AdsReportFetcher:
    """Create Gaarf AdsReportFetcher to use for querying GAds.

    Returns:
      Initialized AdsReportFetcher
    """
    self.client = GoogleAdsApiClient(
      config_dict=self.google_ads_yaml, version=GADS_VERSION
    )
    return AdsReportFetcher(self.client)

  def create_google_ads_yaml(self, auth: Dict) -> Dict[str, str]:
    """Creates a dictionary representing the google ads yaml file.

    Args:
      auth: auth object from the config file

    Returns:
      Dictionary for google ads yaml
    """
    return {
      'client_id': auth['client_id'],
      'client_secret': auth['client_secret'],
      'refresh_token': auth['refresh_token'],
      'login_customer_id': auth['login_customer_id'],
      'developer_token': auth['developer_token'],
      'use_proto_plus': auth['use_proto_plus'],
    }

  def create_campaigns_query(self) -> str:
    """Create Google Ads query for campaign data.

    Returns:
      Query string for campaign data, including campaign name, app id, and
      conversion actions in enabled campaigns.
    """
    return (
      'SELECT '
      'campaign.name, '
      'campaign.app_campaign_setting.app_id, '
      'campaign.selective_optimization.conversion_actions '
      'FROM campaign '
      'WHERE campaign.app_campaign_setting.app_id IS NOT NULL '
      "AND campaign.status = 'ENABLED'"
    )

  def create_conversion_actions_queries(
    self, customer_ids: List[str], conversion_action_resource_names: List[str]
  ) -> dict:
    """Create Google Ads query for conversion actions data.

    Returns:
      Dictionary of {customer_id: query} where query is Google Ads query string
      for conversion action data, including app id, property id,
      property name, event name, type, and last conversion date for all
      requested conversion actions.
    """
    conversion_action_queries = {}

    for customer_id in customer_ids:
      conversion_action_ids_for_customer_id = (
        self._get_conversion_action_resource_name_for_customer_id(
          conversion_action_resource_names, customer_id
        )
      )

      if conversion_action_ids_for_customer_id:
        query = (
          'SELECT '
          'conversion_action.app_id AS app_id, '
          'conversion_action.firebase_settings.property_id AS property_id, '
          'conversion_action.firebase_settings.property_name AS property_name, '
          'conversion_action.firebase_settings.event_name AS event_name, '
          'conversion_action.type AS type, '
          'metrics.conversion_last_conversion_date AS last_conversion_date '
          'FROM conversion_action '
          "WHERE conversion_action.status = 'ENABLED' "
          'AND conversion_action.app_id IS NOT NULL '
          'AND conversion_action.firebase_settings.property_id != 0 '
          'AND conversion_action.resource_name IN '
          f"('{conversion_action_ids_for_customer_id}')"
        )

        conversion_action_queries[customer_id] = query
    return conversion_action_queries

  def _parse_conversion_actions_from_campaigns(self, df: pd.DataFrame) -> List:
    """Extracts conversion actions from Google Ads.

    Helper function to extract the conversion actions from the campaign data
    returned from Google Ads.

    Args:
      df: DataFrame containing campaign data from Google Ads, including the
      "campaign_selective_optimization_conversion_actions" column.

    Returns:
      A list of unique conversion action ids.
    """
    conversion_actions_nested = df[
      'campaign_selective_optimization_conversion_actions'
    ].to_list()
    conversion_actions = [
      action for sublist in conversion_actions_nested for action in sublist
    ]

    return list(set(conversion_actions))

  def _get_os(self, conversion_action_type: str) -> str:
    """Parse the OS from the conversion_action_type string.

    Args:
      conversion_action_type: string containing the conversion action type

    Returns:
      'ANDROID', 'IOS' or ''
    """
    split = conversion_action_type.split('_')
    if 'ANDROID' in split:
      os = 'ANDROID'
    elif 'IOS' in split:
      os = 'IOS'
    else:
      os = ''
    return os

  def _add_uid(self, df: pd.DataFrame) -> str:
    """Creates a uid from params in the DataFrame.

    Helper function that creates a uid from the OS, property Id and event
    name fields in the DataFrame.

    Args:
      df: DataFrame that includes the os, property_id and event_name columns

    Returns:
      A unique id compiled from the inputs.
    """
    os = df['os']
    property_id = df['property_id']
    event_name = df['event_name']

    return f'{os.lower()}_{property_id}_{event_name}'

  def _get_conversion_action_resource_name_for_customer_id(
    self, conversion_action_resource_names: List[str], customer_id: str
  ) -> List[str]:
    """Extract the conversion action_ids for this customer_id.

    Args:
      conversion_action_resource_names: a list of conversion actions resource
      names.
      customer_id: the Google Ads customer ID

    Returns:
      A subset of conversion_action_resource_names
    """
    ids = [
      id for id in conversion_action_resource_names if str(customer_id) in id
    ]
    return "', '".join(ids)
