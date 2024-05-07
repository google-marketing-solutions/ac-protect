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
''' Defines an Email service.'''
from datetime import datetime
from typing import List
from typing import Optional

import pandas as pd
import requests

from env import APP_ENGINE_URL
from server.db.bq import BigQuery


def send_email(subject: str, body: str, recipients: List[str],
               bq_client: BigQuery, app_id: str) -> str:
  ''' Sends an email through the app-engine created service

  Args:
    subject: the email subject
    body: the email body
    recpients: a list of recepients emails
    bq_client: BigQuery client to write to after the email is successfully sent
    app_id: app Id to write to log if email is successfully sent

  Returns:
    The status code of the request
  '''
  data = {'to': ', '.join(recipients), 'subject': subject, 'body': body}

  resp = requests.post(
      url=f'{APP_ENGINE_URL}/send-email', json=data, timeout=30)

  if resp.status_code:
    bq_client.update_last_run('Email', f'service-{app_id}')

  return resp.status_code


def get_last_date_email_sent(bq_client: BigQuery,
                             app_id: str) -> Optional[datetime]:
  ''' Get the date that the last email was sent for a specific app Id

  Args:
    bq_client: BigQuery client
    app_id: App Id to lookup

  Results:
    Date the last email was sent.
  '''
  return bq_client.get_last_run('Email', f'service-{app_id}')


def create_alerts_email_body(df: pd.DataFrame):
  ''' Create the body for the email

  Args:
    df: DataFrame with the Alert data.

  Returns:
    String of the email body
  '''
  app_id = df.iloc[0][0]
  df = df[['trigger', 'trigger_value', 'timestamp']]
  table = df.to_csv()

  return f'''
    The following alerts have been triggered in app {app_id}:

    {table}
  '''
