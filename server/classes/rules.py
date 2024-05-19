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
''' Defines the Rule and RuleObject classes.'''
import abc
import dataclasses
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd

from server.classes.alerts import Alert
from server.db.bq import BigQuery
from server.logger import logger

@dataclasses.dataclass
class RuleObject(abc.ABC):
  pass


class Rule(abc.ABC):
  '''  Rules pull data from collector tables, check if the data violates a
  specific rule that we create and creates alerts from them
  '''

  def __init__(self, rule_name: str, app_config: Dict, auth_config: Dict,
               bq_config: Dict):
    self.name = rule_name
    self.type = 'rule'
    self.config = app_config
    #TODO - remove connection to DB from init
    self.db_client = BigQuery(auth_config, bq_config).connect()


  @abc.abstractmethod
  def run(self):
    ''' Trigger the rule.'''
    pass

  @abc.abstractmethod
  def get_data(self) -> List[pd.DataFrame]:
    ''' Pull data from all relevant tables to process in next step.

    Returns:
      A List of DataFrames with all relevant data for the rule to process.
    '''
    pass

  @abc.abstractmethod
  def check_rule(self, df: pd.DataFrame) -> List[RuleObject]:
    ''' Check the rules against the data and return a list of rule violations.

    Args:
      df: The DataFrame with all relevant data for the rule to process.

    Returns:
      A list of rule violations.
    '''
    pass

  @abc.abstractmethod
  def create_alerts(self, data: List[RuleObject]) -> List[Alert]:
    ''' Get a list of rule violations and turn them into alerts.

    Args:
      data: A list of rule violations.

    Returns:
      A list of alerts.
    '''
    pass

  def update_alerts_log(self, alerts: List[Alert]) -> bool:
    ''' Add all triggered alerts to alert log.

    Args:
      alerts: A list of alerts.

    Returns:
      True if alerts were added to the alert log, False otherwise.
    '''
    if not alerts:
      return False

    if not all(isinstance(alert, Alert) for alert in alerts):
      raise TypeError('Alerts must be of type Alert')
    return self.db_client.write_alerts(alerts)

  def update_last_run(self) -> Optional[List[str]]:
    ''' Update the last run time of the rule in the database.

    Returns:
      Name and type of the rule if successful, None otherwise.
    '''
    resp = self.db_client.update_last_run(self.name, self.type)
    if resp is True:
      return [self.name, self.type]
    return None
