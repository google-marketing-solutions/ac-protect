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
'''Defines the Alert class.'''
import dataclasses
import datetime
import json
from typing import Dict

@dataclasses.dataclass
class Alert:
  '''An alert object.

  Attributes:
    app_id: The ID of the app that triggered the alert.
    rule_name: The name of the rule that triggered the alert.
    trigger: The trigger that caused the alert.
    trigger_value: The value of the trigger that caused the alert.
    alert_id: The ID of the alert.
    timestamp: The timestamp of the alert.
  '''

  app_id: str
  rule_name: str
  trigger: str
  trigger_value: Dict[str, str]
  alert_id: str
  timestamp: datetime.datetime = datetime.datetime.now()

  def to_dict(self) -> Dict[str, str]:
    return {
        'app_id': self.app_id,
        'rule_name': self.rule_name,
        'trigger': self.trigger,
        'trigger_value': str(json.dumps(self.trigger_value)),
        'alert_id': self.alert_id,
        'timestamp': self.timestamp
    }
