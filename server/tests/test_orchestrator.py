import pathlib

import pytest
from pydantic import ValidationError

from server.orchestrator import orchestrator
from server.orchestrator import validate_config


@pytest.fixture(name='config_yaml_path')
def fixture_config_yaml_path():
  folder = pathlib.Path(__file__).parent.resolve()
  path = f'{folder}/test_files/config_test_DO_NOT_UPLOAD.yaml'
  return path

class TestOrchestrator:
  def test_orchestrator(self, config_yaml_path):

    resp = orchestrator(config_yaml_path)
    assert resp is True

class TestConfig:

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
        'project_id': 'test_project_id'
      },
      'collectors':{
        'ga4': {},
        'gads': {
          'version': 'v15',
          'start_date': ''
        }
      },
      'bigquery':{
        'dataset': '',
        'last_trigger_log': ''
      },
      'apps':{
        'app1': {
          'alerts':{
            'emails': ['']
          },
          'rules':{
            'dropped_between_versions':{
                'event3':{'interval': 21},
                'event4': {'interval': 22}
              },
            'measurement_protocol':{
              'interval': 23,
              'events': ['event5, event6']
            },
            'time_interval':{'interval': 24}
          }
        },
      }
    }

  @pytest.fixture
  def failing_config(self):
    return {}

  def test_validate_config_fails(self, failing_config):
    with pytest.raises(ValidationError):
      validate_config(failing_config)

  def test_validate_config_passes(self, config):
    validate_config(config)
