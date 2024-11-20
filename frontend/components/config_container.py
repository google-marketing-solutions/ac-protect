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
"""Frontend component for configuring app-specific settings."""
# pylint: disable=C0330, g-bad-import-order

import streamlit as st

from frontend.utils import state_management


def config_container() -> None:
  """Initializes and displays configuration for apps in ads data."""
  with st.container(border=True):
    st.header('Config')
    for app_id in st.session_state.ads_data['app_id'].unique().tolist():
      if app_id not in st.session_state.config.apps:
        st.session_state.config.apps[app_id] = {
          'alerts': {'emails': []},
          'rules': {'rules': {}},
        }
      show_app_form(app_id)


def show_app_form(app_id: str) -> None:
  """Shows a form for configuring app-specific settings.

  Displays an expandable form containing configuration options for a specific
  app, including email notifications and rule settings.

  Args:
    app_id: The ID of the app to show the configuration form for.
  """
  with st.expander(app_id):
    st.subheader(app_id)
    emails = st.text_input(
      'Emails',
      value=', '.join(st.session_state.config.apps[app_id]['alerts']['emails']),
      key=app_id + 'emails',
    )
    st.data_editor(
      st.session_state.ads_data[st.session_state.ads_data['app_id'] == app_id][
        'event_name'
      ],
      hide_index=True,
      use_container_width=True,
      disabled=True,
      key=app_id + 'de' + '1',
    )

    is_dropped = st.checkbox(
      'Dropped Between Versions',
      value=module_checked_state(app_id, 'dropped_between_versions'),
      key=app_id + 'cb' + '1',
    )

    is_time = st.checkbox(
      'Time Interval',
      value=module_checked_state(app_id, 'time_interval'),
      key=app_id + 'cb' + '3',
    )

    is_save = st.button('save', type='primary', key=app_id + 'bt' + '1')

    if is_save:
      state_management.update_app_config(app_id, emails, is_dropped, is_time)


def module_checked_state(app_id: str, module_name: str) -> bool:
  """Checks if a specific module is activated for a given app ID.

  Args:
    app_id: The ID of the app to check.
    module_name: The name of the module to check for activation.

  Returns:
    bool: True if the module is activated for the app, False otherwise.

  """
  return (
    st.session_state.config.apps[app_id]['rules']
    and module_name in st.session_state.config.apps[app_id]['rules']
  )
