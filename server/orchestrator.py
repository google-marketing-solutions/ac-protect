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
from server.logger import logger
from server.rules.interval import IntervalEventsRule
from server.rules.version_events import VersionsEventsRule
from server.services.email import create_alerts_email_body
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
    logger.info('getting config - %s', config_yaml_path)
    config = get_config(config_yaml_path)
    validate_config(config)
    logger.info('config is valid')
  except FileNotFoundError as e:
    logger.error('Error in getting config - File not found - %s',
                 config_yaml_path)
    logger.error(e)
    return False
  except ValidationError as e:
    logger.error('Error in config validation')
    logger.error(e)
    return False

  auth = config['auth']
  bq_config = config['bigquery']
  apps = config['apps']
  collectors = config['collectors']

  try:
    bq_client = BigQuery(auth, bq_config)
  except DefaultCredentialsError as e:
    logger.error('Error in creating BigQuery client')
    logger.error(e)
    return False

  collector_names = list(collectors.keys())
  for collector_name in collector_names:
    logger.info('getting data from collector - %s', collector_name)
    collector_config = collectors[collector_name]
    overwrite = False
    collector = None

    if collector_name == 'gads':
      overwrite = True
      collector = GAdsCollector(auth, collector_config, bq_client)

    if collector_name == 'ga4':
      collector = GA4Collector(auth, collector_config, bq_client)

    if collector:
      df = collector.collect()
      df = collector.process(df)
      collector.save(df, overwrite)

  for rule in RULES:
    logger.info('running rule logic - %s', rule.__name__)
    rule = rule(config)
    rule.run()

  app_ids = list(apps.keys())
  for app_id in app_ids:
    last_date_email_sent = get_last_date_email_sent(bq_client, app_id)
    app_config = apps[app_id]
    recipients = app_config['alerts']['emails']
    df_alerts = bq_client.get_alerts_for_app_since_date(app_id,
                                                        last_date_email_sent)
    if df_alerts:
      logger.log('--- Sending alert emails ---')
      body = create_alerts_email_body(df_alerts)
      send_email(
          subject=f'Alerts for {app_id}',
          recipients=recipients,
          body=body,
          bq_client=bq_client,
          app_id=app_id)

  return True
