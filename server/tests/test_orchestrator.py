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
"""Tests for the Orchestrator module."""
# pylint: disable=C0330, missing-function-docstring

import pathlib

import pytest
from pydantic import ValidationError

from server import orchestrator


@pytest.fixture(name='config_yaml_path')
def fixture_config_yaml_path():
  folder = pathlib.Path(__file__).parent.resolve()
  return f'{folder}/test_files/config_test_DO_NOT_UPLOAD.yaml'


class TestConfigValidation:
  """Tests for validating configuration objects.

  Tests that configuration validation works correctly by checking:
  - Valid configurations pass validation
  - Invalid configurations fail validation with appropriate errors
  - Required fields are enforced
  - Field types and formats are validated
  """

  @pytest.fixture
  def config(self):
    return {
      'auth': {
        'client_id': 'test_client_id',
        'client_secret': 'test_client_secret',
        'refresh_token': 'test_refresh_token',
        'login_customer_id': 1,
        'developer_token': 'test_dev_token',
        'use_proto_plus': True,
        'project_number': 1,
        'project_id': 'test_project_id',
      },
      'collectors': {'ga4': {}, 'gads': {'version': 'v17', 'start_date': ''}},
      'bigquery': {'dataset': '', 'last_trigger_log': ''},
      'apps': {
        'app1': {
          'alerts': {'emails': ['']},
          'rules': {
            'dropped_between_versions': {
              'event3': {'interval': 21},
              'event4': {'interval': 22},
            },
            'measurement_protocol': {
              'interval': 23,
              'events': ['event5, event6'],
            },
            'time_interval': {'interval': 24},
          },
        },
      },
    }

  @pytest.fixture
  def failing_config(self):
    return {}

  def test_validate_config_fails(self, failing_config):
    with pytest.raises(ValidationError):
      orchestrator.validate_config(failing_config)

  def test_validate_config_passes(self, config):
    orchestrator.validate_config(config)
