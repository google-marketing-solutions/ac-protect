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

from env import CONFIG_PATH
from server.collectors.ga4 import GA4Collector
from server.collectors.gads import GAdsCollector
from server.config import get_config
from server.config import validate_config
from server.db.bq import BigQuery
from server.db.tables import GA4_TABLE_NAME
from server.db.tables import GADS_TABLE_NAME
from server.env import CONFIG_PATH
from server.logger import logger
from server.rules.interval import IntervalEventsRule
from server.rules.version_events import VersionsEventsRule
from server.services.email import create_html_email
from server.services.email import get_last_date_email_sent
from server.services.email import send_email

RULES = [IntervalEventsRule, VersionsEventsRule]


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
    bq_client = BigQuery(auth, bq_config).connect()
  except (DefaultCredentialsError, KeyError) as e:
    logger.error('Error creating BigQuery client: %s', e)
    return False

  collector_names = list(config['collectors'].keys())
  for collector_name in collector_names:
    logger.info(f'Running collector - {collector_name}')
    try:
      collector_config = config['collectors'][collector_name]
      collector = GA4Collector(
          auth, collector_config,
          bq_client) if collector_name == GA4_TABLE_NAME else GAdsCollector(
              auth, collector_config, bq_client)
      df = collector.collect()
      df = collector.process(df)
      overwrite = bool(
          collector_name == GADS_TABLE_NAME)  # Overwrite only for GAds
      collector.save(df, overwrite)
    except Exception as e:  # Catch all exceptions for collectors
      logger.error('Error running collector %s: %s', collector_name, e)

  for rule in RULES:
    logger.info(f'Running rule - {rule.__name__}')
    try:
      rule_obj = rule(config)
      rule_obj.run()
    except Exception as e:  # Catch all exceptions for rules
      logger.error('Error running rule %s: %s', rule.__name__, e)

  app_ids = list(config['apps'].keys())
  for app_id in app_ids:
    logger.info(f'Running for app_id - {app_id}')
    try:
      last_date_email_sent = get_last_date_email_sent(bq_client, app_id)
      app_config = config['apps'][app_id]
      recipients = app_config['alerts']['emails']
      df_alerts = bq_client.get_alerts_for_app_since_date(app_id,
                                                          last_date_email_sent)
      if not df_alerts.empty:
        logger.info(f'found alerts for {app_id}')
        body = create_html_email(df_alerts)

        logger.info(f'sending emails to - {recipients}')
        send_email(
          config=config,
          sender='',
          to=recipients,
          subject=f'Alerts for {app_id}',
          message_text=body,
          bq_client=bq_client,
          app_id=app_id)
    except Exception as e:  # Catch all exceptions for sending emails
      logger.error('Error sending email for app %s: %s', app_id, e)
  return True



if __name__ == '__main__':
  orchestrator()
