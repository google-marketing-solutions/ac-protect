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
""" The Orchestrator is in charge of running all relevant functions
- loading configs
- running relevant connectors
- running relevant modules
- sending alerts
"""
from typing import Optional

from google.auth.exceptions import DefaultCredentialsError
from pydantic import ValidationError

from server.collectors.app_store import AppStoreCollector
from server.collectors.ga4 import GA4Collector
from server.collectors.gads import GAdsCollector
from server.collectors.play_store import PlayStoreCollector
from server.config import get_config
from server.config import validate_config
from server.db import tables
from server.db.bq import BigQuery
from server.env import CONFIG_PATH
from server.logger import logger
from server.rules.interval import IntervalEventsRule
from server.rules.version_events import VersionEventsRule
from server.services.email import create_html_email
from server.services.email import get_last_date_time_email_sent
from server.services.email import send_email

RULES = [IntervalEventsRule, VersionEventsRule]


def orchestrator(config_yaml_path: Optional[str] = CONFIG_PATH) -> bool:
  """ The main function to run the orchestrator.

  Args:
    config_yaml_path: The path to the YAML file (local or GCS). Defaults to
    CONFIG_PATH if no path is given.

  Returns:
    True if successful, False otherwise.

  """

  logger.info('---------------- Starting orchestrator ----------------')

  try:
    config = get_config(config_yaml_path)
    validate_config(config)
  except (FileNotFoundError, ValidationError) as e:
    logger.error('Error during orchestration: %s', e)
    return False

  try:
    auth = config['auth']
    bq_config = config['bigquery']
    bq = BigQuery(auth, bq_config)
    bq.connect()
  except (DefaultCredentialsError, KeyError) as e:
    logger.error('Error creating BigQuery client: %s', e)
    return False

  collector_names = list(config['collectors'].keys())
  for collector_name in collector_names:
    logger.info('Running collector - %s',collector_name)
    collector_config = config['collectors'][collector_name]

    if collector_name == tables.GA4_TABLE_NAME:
      collector = GA4Collector(auth, collector_config,bq)
    elif collector_name == tables.GADS_TABLE_NAME:
      collector = GAdsCollector(auth, collector_config, bq)
    elif collector_name == tables.AppStoreTable:
      collector = AppStoreCollector(config['apps'], bq)
    elif collector_name == tables.PlayStoreTable:
      collector = PlayStoreCollector(auth, config['apps'], bq)

    df = collector.collect()
    if not df.empty:
      df = collector.process(df)
      overwrite = bool(
          collector_name == tables.GADS_TABLE_NAME)  # Overwrite only for GAds
      collector.save(df, overwrite)

  for rule in RULES:
    logger.info('Running rule - %s', rule.__name__)
    rule_obj = rule(config)
    rule_obj.run()

  app_ids = list(config['apps'].keys())
  for app_id in app_ids:
    logger.info('Running for app_id - %s', app_id)
    last_date_time_sent = get_last_date_time_email_sent(bq, app_id)
    app_config = config['apps'][app_id]
    recipients = app_config['alerts']['emails']
    df_alerts = bq.get_alerts_for_app_since_date_time(app_id,
                                                        last_date_time_sent)
    if not df_alerts.empty:
      logger.info('found alerts for %s', app_id)
      body = create_html_email(df_alerts)

      logger.info('sending emails to - %s', recipients)
      send_email(
        config=config,
        sender='',
        to=recipients,
        subject=f'Alerts for {app_id}',
        message_text=body,
        bq=bq,
        app_id=app_id)
  return True



if __name__ == '__main__':
  orchestrator()
