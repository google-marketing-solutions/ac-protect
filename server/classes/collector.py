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
''' Defines the Collectors class.'''
import abc

import pandas as pd


class Collector():
  ''' Collector extract data from a specific source, (i.e. Google Ads or GA4),
  transform the data and save it to a db for further analysis by Rules.
  '''

  def __init__(self, name: str, type_: str):
    self.name = name
    self.type_ = type_

  @abc.abstractmethod
  def collect(self):
    ''' Collect data from that collector source. '''
    pass

  @abc.abstractmethod
  def process(self, df: pd.DataFrame) -> pd.DataFrame:
    ''' Transform collected data before saving'''
    pass

  @abc.abstractmethod
  def save(self, df: pd.DataFrame, overwrite: bool = True):
    ''' Save collected data '''
    pass
