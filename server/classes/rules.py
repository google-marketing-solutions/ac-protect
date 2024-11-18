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
"""Defines the Rule and RuleObject classes."""
# pylint: disable=C0330

import abc
import dataclasses
from typing import Dict, List, Optional

import pandas as pd

from server.classes import alerts
from server.db import bq


@dataclasses.dataclass
class RuleObject(abc.ABC):
  """Abstract base class for rule violation objects.

  All rule-specific violation objects should inherit from this class and
  implement required attributes for their specific use case.

  Attributes:
      event_name: The name of the event that triggered the violation.
      app_id: The ID of the app where the violation occurred.
  """

  event_name: str
  app_id: str


class Rule(abc.ABC):
  """Abstract base class for defining monitoring rules.

  Rules are used to monitor data from collector tables by checking for specific
  conditions and generating alerts when violations are found. Each rule
  implementation defines its own logic for:
  - Fetching relevant data from collectors
  - Checking for rule violations in the data
  - Creating alerts for any violations found

  Attributes:
    name: Name of the rule.
    type: Type identifier, always set to 'rule'.
    config: Application configuration dictionary.
    db_client: Connected BigQuery client for database operations.
  """

  def __init__(
    self,
    rule_name: str,
    app_config: Dict[str, any],
    auth_config: Dict[str, str],
    bq_config: Dict[str, str],
  ) -> None:
    """Initialize a new Rule instance.

    Args:
      rule_name: Name identifier for this rule.
      app_config: Application configuration containing collector and rule
      settings.
      auth_config: Authentication configuration containing credentials.
      bq_config: BigQuery configuration containing project and dataset details.
    """
    self.name = rule_name
    self.type = 'rule'
    self.config = app_config
    self.db_client = bq.BigQuery(auth_config, bq_config)
    self.db_client.connect()

  @abc.abstractmethod
  def run(self):
    """Trigger the rule."""

  @abc.abstractmethod
  def get_data(self) -> dict[pd.DataFrame | None]:
    """Pull data from all relevant tables to process in next step.

    Returns:
      A dictionary mapping collector names to their DataFrame data, or None if
      the collector data could not be retrieved.
    """

  @abc.abstractmethod
  def check_rule(self, collectors_data: pd.DataFrame) -> List[RuleObject]:
    """Check the rules against the data and return a list of rule violations.

    Args:
      collectors_data: The DataFrame with all relevant data for the rule to
      process.

    Returns:
      A list of rule violations.
    """

  @abc.abstractmethod
  def create_alerts(
    self, rule_violations: List[RuleObject]
  ) -> List[alerts.Alert]:
    """Get a list of rule violations and turn them into alerts.

    Args:
      rule_violations: A list of rule violations.

    Returns:
      A list of alerts.
    """

  def update_alerts_log(self, triggered_alerts: List[alerts.Alert]) -> bool:
    """Add all triggered alerts to alert log.

    Args:
      triggered_alerts: A list of alerts.

    Returns:
      True if alerts were added to the alert log, False otherwise.
    """
    if not triggered_alerts:
      return False

    if any(
      not isinstance(triggered_alert, alerts.Alert)
      for triggered_alert in triggered_alerts
    ):
      raise TypeError('Alerts must be of type Alert')
    return self.db_client.write_alerts(triggered_alerts)

  def update_last_run(self) -> Optional[List[str]]:
    """Update the last run time of the rule in the database.

    Returns:
      Name and type of the rule if successful, None otherwise.
    """
    resp = self.db_client.update_last_run(self.name, self.type)
    if resp is True:
      return [self.name, self.type]
    return None
