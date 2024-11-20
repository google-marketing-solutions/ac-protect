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
"""Frontend component for fetching and displaying Google Ads data."""
# pylint: disable=C0330, g-bad-import-order

from typing import Any, Dict

import pandas as pd
import streamlit as st
from google.api_core import exceptions as api_core_exceptions

from frontend.utils import log
from server import endpoints


def get_ads_data(
  auth: Dict[str, str],
  collector_config: Dict[str, Any],
  bq_config: Dict[str, Any],
) -> pd.DataFrame:
  """Fetches Google Ads data using the provided configuration.

  Args:
    auth: Authentication configuration containing credentials.
    collector_config: Configuration for the Google Ads collector.
    bq_config: Configuration for BigQuery.

  Returns:
    pd.DataFrame: DataFrame containing the fetched ads data. Returns empty
    DataFrame if there is a configuration error.
  """

  try:
    log.logger.info('getting ads data')
    ads_data = endpoints.get_ads_data_endpoint(
      auth, collector_config, bq_config
    )
    st.session_state.ads_data = ads_data
  except api_core_exceptions.BadRequest as e:
    log.logger.error(f'Bad config:\n{e}')
    st.session_state.ads_data = pd.DataFrame()
    st.error(f'Bad config:\n{e}')
  return st.session_state.ads_data
