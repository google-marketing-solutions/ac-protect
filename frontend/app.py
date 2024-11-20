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
"""Frontend service for AC-Protect solution.

This module, built with Streamlit, provides a web interface for configuring and
managing AC-Protect settings.
"""
# pylint: disable=C0330, g-multiple-import, g-bad-import-order

import streamlit as st

from frontend.components import (
  auth_expander,
  config_container,
  google_ads,
  login,
)
from frontend.utils import state_management


def main() -> None:
  """Main entry point for the AC Protect application.

  Sets up the Streamlit page configuration and handles the application flow:
  1. Initializes the session state
  2. Handles authentication if needed
  3. Displays the authentication expander
  4. Shows app configuration if authenticated and data is available

  The function manages the overall UI layout and authentication state,
  ensuring proper display of components based on the current state.
  """
  st.set_page_config(
    page_title='AC Protect',
    page_icon='🛡',
    layout='centered',
  )
  st.header('🛡AC Protect')
  state_management.initialize_session_state()
  session_config = st.session_state.config

  if not st.session_state.config.valid:
    auth_expander.auth_expander(session_config)

  if not st.session_state.is_auth and st.session_state.config.valid:
    login.login(session_config)

  if st.session_state.is_auth:
    auth_expander.auth_expander(session_config)
    with st.spinner('Fetching apps ...'):
      google_ads.get_ads_data(
        st.session_state.config.auth,
        st.session_state.config.collectors['collector_gads'],
        st.session_state.config.bigquery,
      )
      config_container.config_container()


if __name__ == '__main__':
  main()
