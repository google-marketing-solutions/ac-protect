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
''' Defines the tables that are used in AC Protect.'''
import dataclasses
import datetime

GA4_TABLE_NAME = 'collector_ga4'
GADS_TABLE_NAME = 'collector_gads'
ALERTS_TABLE_NAME = 'alerts'

@dataclasses.dataclass
class Ga4Table:
  ''' Defines Google Analytics table schema.'''
  property_id: int
  os: str
  app_version: str
  event_name: str
  event_count: int
  uid: str
  date_added: str


@dataclasses.dataclass
class GadsTable:
  ''' Defines Google Ads table schema.'''
  app_id: str
  property_id: int
  property_name: str
  event_name: str
  type: str
  last_conversion_date: str
  os: str
  uid: str


@dataclasses.dataclass
class AlertsTable:
  ''' Defines Alerts table schema.'''
  app_id: str
  rule_name: str
  trigger: str
  trigger_value: str
  alert_id: str
  timestamp: datetime.datetime


@dataclasses.dataclass
class LastTriggerLogTable:
  ''' Defines Last Trigger Log table schema. This table logs when each collector
   / rule / service last ran.
  '''
  name: str
  type: str
  timestamp: str
