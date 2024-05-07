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
"""Endpoints that are triggered by the UI"""
from typing import Dict
from typing import List

import pandas as pd

from env import CONFIG_FILE_PATH
from server.collectors.gads import GAdsCollector
from server.config import get_config
from server.config import update_config
from server.db.bq import BigQuery
from server.db.bq import BigQueryException
from server.db.tables import GADS_TABLE_NAME

def run_gads_collector(auth, collector_config, bq):
  """Pull, process and save data from Google Ads"""
  collector = GAdsCollector(auth, collector_config, bq)
  data = collector.collect()
  data = collector.process(data)
  collector.save(data, False)
  return data

def get_ads_data(auth: Dict,
                 collector_config: Dict,
                 bq_config: Dict,
                 cols=None) -> pd.DataFrame:
  """Get Google Ads Data From BigQuery table

  Args:
    auth: auth configuration
    collector_config: collector configuration
    bq_config: BigQuery configuration
    cols: specific columns we want returned. Defaults to '*', returning all
      columns.
  Returns:
    A DataFrame with the values returned from the table. Returns None if there
      are no results.
  """
  bq = BigQuery(auth, bq_config)

  if not cols:
    cols = ['*']

  try:
    ads_data = bq.get_values_from_table(GADS_TABLE_NAME, cols)
    if ads_data.empty:
      ads_data = run_gads_collector(auth, collector_config, bq)
    return ads_data
  except BigQueryException:
    return run_gads_collector(auth, collector_config, bq)



def get_ads_app_ids(auth: Dict, collector_config: Dict,
                    bq_config: Dict) -> List[str]:
  """Get all app_ids from the Google Ads Table

  Args:
    auth: auth configuration
    collector_config: collector configuration
    bq_config: BigQuery configuration

  Returns:
    A list of app_ids
  """
  ads_data = get_ads_data(auth, collector_config, bq_config, ['app_id'])
  return ads_data['app_id'].unique().tolist()


def get_ads_event_names(auth: Dict, collector_config: Dict,
                    bq_config: Dict) -> List[str]:
  """Get all event_names from the Google Ads Table

  Args:
    auth: auth configuration
    collector_config: collector configuration
    bq_config: BigQuery configuration

  Returns:
    A list of event_names
  """
  ads_data = get_ads_data(auth, collector_config, bq_config, ['event_name'])
  return ads_data['event_name'].unique().tolist()


def get_config_file(config_path: str=CONFIG_FILE_PATH) -> Dict:
  """Get the config file

  Args:
    config_path: path to the config file. Defaults to CONFIG_FILE_PATH from
    environment

  Returns:
    Dictionary of config
  """
  config = get_config(config_path)
  return config


def update_config_file(config: Dict, config_path: str = CONFIG_FILE_PATH):
  """Update the config file

  Args:
    config: The new configuration we want to update
    config_path: path to the config file. Defaults to CONFIG_FILE_PATH from
    environment
  """

  update_config(config, config_path)
