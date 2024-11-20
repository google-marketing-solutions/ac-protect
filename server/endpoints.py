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
"""Module that provides endpoints to connect the frontend and backend services.

This module contains endpoint functions that serve as the interface between the
frontend UI and the backend services.
"""

# pylint: disable=C0330, g-multiple-import
from typing import Any

import pandas as pd

from server import config, env
from server.collectors import gads
from server.db import bq, tables


def get_ads_data_endpoint(
  auth: dict[str, str],
  collector_config: dict[str, Any],
  bq_config: dict[str, Any],
  cols: list[str] | None = None,
) -> pd.DataFrame:
  """Endpoint to fetch Google Ads Data From BigQuery table.

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
  db = bq.BigQuery(auth, bq_config)
  db.connect()

  if not cols:
    cols = ['*']

  try:
    ads_data = db.get_values_from_table(tables.GADS_TABLE_NAME, cols)
    if ads_data.empty:
      return _run_gads_collector(auth, collector_config, db)
    return ads_data
  except bq.BigQueryException:
    return _run_gads_collector(auth, collector_config, db)


def get_config_endpoint(
  config_path: str = env.CONFIG_FILE_PATH,
) -> dict[str, Any]:
  """Fetch the config file.

  Args:
    config_path: path to the config file. Defaults to CONFIG_FILE_PATH from
    environment

  Returns:
    Dictionary of config
  """
  return config.get_config(config_path)


def update_config_endpoint(
  new_config: dict[str, Any], config_path: str = env.CONFIG_FILE_PATH
) -> None:
  """Update the config file.

  Args:
    new_config: The new configuration we want to update
    config_path: path to the config file. Defaults to CONFIG_FILE_PATH from
    environment
  """
  config.update_config(new_config, config_path)


def _run_gads_collector(
  auth: dict[str, str], collector_config: dict[str, Any], db: bq.BigQuery
) -> pd.DataFrame:
  """Fetch, process and save data from Google Ads.

  Args:
    auth: auth configuration
    collector_config: collector configuration
    db: BigQuery object

  Returns:
    pd.DataFrame: DataFrame containing the fetched ads data.
  """
  collector = gads.GAdsCollector(auth, collector_config, db)
  data = collector.collect()
  data = collector.process(data)
  collector.save(data, False)
  return data
