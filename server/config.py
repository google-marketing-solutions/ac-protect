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
''' All methods to load, update and validate the config file.'''
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import smart_open
import yaml
from pydantic import BaseModel
from pydantic import ValidationError

from env import CONFIG_PATH
from server.logger import logger

class Auth(BaseModel):
  client_id: str
  client_secret: str
  refresh_token: str
  login_customer_id: int
  developer_token: str
  use_proto_plus: bool = True
  project_number: int
  project_id: str

class Alert(BaseModel):
  emails: List[Optional[str]]

class Rule(BaseModel):
  pass

class App(BaseModel):
  alerts: Alert

class Collector(BaseModel):
  pass

class BigQuery(BaseModel):
  dataset: str
  last_trigger_log: str

class Config(BaseModel):
  auth: Auth
  collectors: Dict[str, Collector]
  apps: Dict[str, App]
  bigquery: BigQuery


def get_config(config_yaml_path: str = CONFIG_PATH) -> dict:
  '''Gets the configuration from a YAML file.

  Args:
    config_yaml_path: The path to the YAML file (local or in GCS). Defaults to
    CONFIG_PATH set in environment.

  Returns:
    A dictionary of the configuration. If there is an error in the yaml file, an
    empty dictionary is returned.
  '''
  logger.info(f'getting config at - {config_yaml_path}')
  try:
    with smart_open.open(config_yaml_path, 'r') as f:
      config = yaml.safe_load(f)
    return config
  except (IOError, yaml.YAMLError) as exc:
    logger.error(f'Error getting config: {exc}')
    return {}


def update_config(config: Dict, config_yaml_path=CONFIG_PATH):
  ''' Update the config YAML file.

  Args:
    config: a dictionary of the configuration
    config_yaml_path: path to config YAML file. Default to CONFIG_PATH set in
    environment.

  '''
  logger.info(f'Updating config at - {config_yaml_path}')
  try:
    with smart_open.open(config_yaml_path, 'w', encoding='utf-8') as file:
      yaml.dump(config, file, allow_unicode=True)
  except IOError as exc:
    logger.error(f'Error updating config: {exc}')


def validate_config(config: Dict[str, Any]):
  ''' Validates the configuration.

  Args:
    config: A dictionary of the configuration.

  Raises:
    ValidationError: If the configuration is invalid.
  '''
  try:
    _ = Config(**config)
  except ValidationError as e:
    raise ValidationError.from_exception_data('Error in config file',
                                              e.errors())
