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
""" Defines an Email service."""
import base64
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List
from typing import Optional

import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from server.db.bq import BigQuery
from server.logger import logger
from server.utils import Scopes


def get_credentials(config: dict) -> Credentials:
  """ Create Credentials object from config file

  Args:
    config: AC Protect config in dict format

  Returns:
    Credentials object in Gmail scope
  """
  creds = None
  user_info = {
      'client_id': config['auth']['client_id'],
      'refresh_token': config['auth']['refresh_token'],
      'client_secret': config['auth']['client_secret']
  }
  creds = Credentials.from_authorized_user_info(user_info,
                                                [Scopes.GMAIL_SEND.value])
  return creds

def create_message(sender: str, to: List, subject: str,
                   body: str) -> MIMEMultipart:
  """ Create a message as a MIMEMultipart object

  Args:
    sender: email of the sender
    to: list of email to send the email to
    subject: subject of the email message
    body: email body

  Returns:
    MIMEMultipart encoded message
  """

  msg = MIMEText(body, 'html')

  message = MIMEMultipart()
  message['to'] = ','.join(to)
  message['from'] = sender  #TODO - is this necessary? if we are sending from 'me'..
  message['subject'] = subject
  message.attach(msg)

  return message

def send_message(config:dict, message:MIMEMultipart) -> Optional[dict]:
  """ Send a message using the Gmail API

  Args:
    config: the AC Protect config dictionary
    message: a MIMEMUltipart formated message

  Returns:
    The message object that was returned from the API call or None if there
    was an error.
  """
  creds = get_credentials(config)
  service = build('gmail', 'v1', credentials=creds)
  user_id = 'me'

  raw = base64.urlsafe_b64encode(message.as_bytes())

  try:
    message = (
        service
        .users()
        .messages()
        .send(
            userId=user_id, body={
                'raw': raw.decode()
            })
            .execute())
    logger.info('Message Id: %s', message['id'])
    return message
  except HttpError as error:
    logger.error('An error occurred when trying to send the email: %s',error)
    return None

def send_email(config: dict, sender: str, to: List[str], subject: str,
               message_text: str, bq: BigQuery, app_id: str):
  """ Main function to send the create, send and log the email message

  Args:
    config: the AC Protect config dictionary
    sender: the email of the sender
    to: list of emails to send the email to
    subject: the subject of the email
    message_text: the body of the email
    bq: a BigQuery object to update the last run
    app_id: the app_id
  """
  message = create_message(sender, to, subject, message_text)
  send_message(config, message)

  # TODO - move all bq updates to orchestrator
  bq.update_last_run('Email', f'service-{app_id}')


def get_last_date_time_email_sent(bq: BigQuery,
                             app_id: str) -> Optional[datetime]:
  """ Get the date that the last email was sent for a specific app Id

  Args:
    bq: BigQuery object
    app_id: App Id to lookup

  Results:
    Date the last email was sent.
  """
  return bq.get_last_run('Email', f'service-{app_id}')


def create_alerts_email_body(df: pd.DataFrame) -> str:
  """ Create the body for the email

  Args:
    df: DataFrame with the Alert data.

  Returns:
    String of the email body
  """
  app_id = df.iloc[0][0]
  df = df[['trigger', 'trigger_value', 'timestamp']]
  table = df.to_csv(index=False)

  return f'''
    The following alerts have been triggered in app {app_id}:

    {table}
  '''

def create_html_email(df: pd.DataFrame) -> str:
  """ Create the body for the email in html format.

  Args:
    df: DataFrame with the Alert data.

  Returns:
    String of the email body
  """
  app_id = df.iloc[0][0]
  df = df[['trigger', 'trigger_value', 'timestamp']]
  table = df.to_html(index=False)

  html = f'''
    <html>
      <body>
        <h1>The following alerts have been triggered in app {app_id}:</h1>
        {table}
      </body>
    </html>
  '''
  return html
