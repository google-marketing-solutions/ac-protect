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
''' Defines the BaseDb class.'''
import abc
from typing import Optional

import pandas as pd

class BaseDb(abc.ABC):
  ''' Base class with main methods that DB classes need to implement'''
  def __init__(self, log: str = 'last_trigger_log'):
    self.log = log

  @abc.abstractmethod
  def connect(self):
    ''' Connect to the db '''
    pass

  @abc.abstractmethod
  def get_values_from_table(self, table_id: str) -> Optional[pd.DataFrame]:
    ''' Get all data from a specific table '''
    pass

  @abc.abstractmethod
  def write_to_table(self, table_id: str, df: pd.DataFrame, overwrite: bool):
    ''' Append data to a specific table '''
    pass

  @abc.abstractmethod
  def update_last_run(self, name: str, type_: str):
    ''' Write to log timestamp of last run '''
    pass
