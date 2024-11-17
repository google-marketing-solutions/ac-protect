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
"""The Orchestrator is in charge of running all relevant functions.

- loading configs
- running relevant connectors
- running relevant modules
- sending alerts
"""
# pylint: disable=C0330, g-bad-import-order, g-multiple-import

from typing import Any, Dict, Optional

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from pydantic import ValidationError

from server.classes.collector import COLLECTOR_ALERT_ID
from server.collectors import app_store, ga4, gads, play_store
from server.config import get_config, validate_config
from server.db import tables
from server.db.bq import BigQuery
from server.env import CONFIG_PATH
from server.logger import logger
from server.rules.interval import IntervalEventsRule
from server.rules.version_events import VersionEventsRule
from server.services import email

RULES = [IntervalEventsRule, VersionEventsRule]


def handle_alerts(
  config: Dict[str, Any], bq: BigQuery, alert_id: str, recipients: list[str]
) -> None:
  """Sends Alerts if needed.

  Args:
    config: The app config dict.
    bq: BigQuery client.
    alert_id: Id in alerts table
    recipients: Who gets the alert.

  Returns:
    True if alert is sent, False otherwise.

  """
  last_date_time_sent = email.get_last_date_time_email_sent(bq, alert_id)
  df_alerts = bq.get_alerts_for_app_since_date_time(
    alert_id, last_date_time_sent
  )
  if isinstance(df_alerts, pd.DataFrame) and not df_alerts.empty:
    logger.info('found alerts for %s', alert_id)
    body = email.create_html_from_template(df_alerts)

    logger.info('sending emails to - %s', recipients)
    email.send_email(
      config=config,
      sender='',
      to=recipients,
      subject=f'[AC Protect] Alerts for AppId: {alert_id}',
      message_text=body,
      bigquery=bq,
      app_id=alert_id,
    )


def orchestrator(config_yaml_path: Optional[str] = CONFIG_PATH) -> bool:
  """The main function to run the orchestrator.

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
  overwrite = False

  for collector_name in collector_names:
    logger.info('Running collector - %s', collector_name)
    collector_config = config['collectors'][collector_name]

    collector = None
    if collector_name == tables.GA4_TABLE_NAME:
      collector = ga4.GA4Collector(auth, collector_config, bq)
    elif collector_name == tables.GADS_TABLE_NAME:
      collector = gads.GAdsCollector(auth, collector_config, bq)
      overwrite = True
    elif collector_name == tables.APP_STORE_TABLE_NAME:
      collector = app_store.AppStoreCollector(config['apps'], bq)
    elif collector_name == tables.PLAY_STORE_TABLE_NAME:
      collector = play_store.PlayStoreCollector(auth, config['apps'], bq)

    if collector:
      df = collector.collect()
      if not df.empty:
        df = collector.process(df)
        collector.save(df, overwrite)

  for rule in RULES:
    logger.info('Running rule - %s', rule.__name__)
    rule_obj = rule(config)
    rule_obj.run()

  app_ids = list(config['apps'].keys())

  for app_id in app_ids:
    logger.info('Running for app_id - %s', app_id)
    app_config = config['apps'][app_id]
    recipients = app_config['alerts']['emails']
    handle_alerts(config, bq, app_id, recipients)

  logger.info('Running for Collectors - %s', COLLECTOR_ALERT_ID)
  handle_alerts(config, bq, COLLECTOR_ALERT_ID, recipients)

  return True


if __name__ == '__main__':
  orchestrator()
