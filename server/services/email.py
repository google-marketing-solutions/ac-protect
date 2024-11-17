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
"""Defines an Email service."""
# pylint: disable=C0330, g-multiple-import

import base64
import json
from datetime import datetime
from email.mime import image, multipart, text
from typing import Any, Optional

import jinja2
import pandas as pd
from google.oauth2 import credentials
from googleapiclient import discovery, errors

from server import utils
from server.db import bq
from server.logger import logger


def get_credentials(config: dict[str, Any]) -> credentials.Credentials:
  """Creates Credentials object from config file.

  Args:
    config: AC Protect config in dict format.

  Returns:
    Credentials object in Gmail scope.
  """
  creds = None
  user_info = {
    'client_id': config['auth']['client_id'],
    'refresh_token': config['auth']['refresh_token'],
    'client_secret': config['auth']['client_secret'],
  }
  creds = credentials.Credentials.from_authorized_user_info(
    user_info, [utils.Scopes.GMAIL_SEND.value]
  )
  return creds


def create_message(
  sender: str, to: list[str], subject: str, body: str
) -> multipart.MIMEMultipart:
  """Creates a message as a MIMEMultipart object.

  Creates the message and attaches the gTech logo to the email via Content-ID.

  Args:
    sender: Email address of the sender.
    to: List of emails to send the email to.
    subject: Subject of the email message.
    body: The email body.

  Returns:
    A MIMEMultipart encoded message.
  """

  msg = multipart.MIMEMultipart('related')
  msg['to'] = ','.join(to)
  msg['from'] = sender
  msg['subject'] = subject

  msg_alternative = multipart.MIMEMultipart('alternative')
  msg.attach(msg_alternative)
  msg_text = text.MIMEText(body, 'html')
  msg_alternative.attach(msg_text)

  with open('server/services/static/gtech_logo.png', 'rb') as img_file:
    img_data = img_file.read()
    img = image.MIMEImage(img_data, name='gtech_logo.png')
    img.add_header('Content-ID', '<gtech-logo>')
    msg.attach(img)

  return msg


def send_message(
  config: dict[str, Any], message: multipart.MIMEMultipart
) -> Optional[dict]:
  """Sends a message using the Gmail API.

  Args:
    config: The AC Protect config dictionary.
    message: A MIMEMultipart formated message.

  Returns:
    The message object that was returned from the API call or None if there
    was an error.
  """
  creds = get_credentials(config)
  service = discovery.build('gmail', 'v1', credentials=creds)
  user_id = 'me'

  raw = base64.urlsafe_b64encode(message.as_bytes())

  try:
    message = (
      service.users()
      .messages()
      .send(userId=user_id, body={'raw': raw.decode()})
      .execute()
    )
    logger.info('Message Id: %s', message['id'])
    return message
  except errors.HttpError as error:
    logger.error('An error occurred when trying to send the email: %s', error)
    return None


def send_email(
  config: dict[str, Any],
  sender: str,
  to: list[str],
  subject: str,
  message_text: str,
  bigquery: bq.BigQuery,
  app_id: str,
) -> None:
  """Creates and sends the email message.

  The main function in this module that creates the email message, sends it and
  logs the action in the db.

  Args:
    config: The AC Protect config dictionary.
    sender: The email address of the sender.
    to: List of emails to send the email to.
    subject: The subject of the email.
    message_text: The body of the email.
    bigquery: A BigQuery object to update the last run.
    app_id: The App id.
  """
  message = create_message(sender, to, subject, message_text)
  send_message(config, message)

  bigquery.update_last_run('Email', f'service-{app_id}')


def get_last_date_time_email_sent(
  bigquery: bq.BigQuery, app_id: str
) -> Optional[datetime]:
  """Gets the date that the last email was sent for a specific app Id.

  Args:
    bigquery: A BigQuery object.
    app_id: The App Id to lookup.

  Results:
    Date the last email was sent.
  """
  return bigquery.get_last_run('Email', f'service-{app_id}')


def create_html_from_template(alerts_data: pd.DataFrame) -> str:
  """Creates an html file from a template.

  Creates the html that will be sent by email, using a jinja2 template.

  Args:
    alerts_data: Dataframe holding alerts data to be sent by email.

  Returns:
    A string representing the html to be sent.
  """
  env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(searchpath='server/services/')
  )
  template = env.get_template('static/email_template.jinja')
  context = create_context(alerts_data)
  return template.render(context)


def create_context(alerts_data: pd.DataFrame) -> dict:
  """Creates a dictionary for each alert.

  Creates a dictionary that can be parsed by the email template.

  Args:
    alerts_data: The contents of the alert to be sent for each alert type.

  Returns:
    A dictionary in the following format:
    {
      app_id: str
      alerts_by_type:[
        {
          alert_name: str,
          alerts: list[dict[str,str]]
          timestamp: str
        }
      ]
    }
  """
  alert_types = alerts_data['trigger'].unique()
  alerts_data['trigger_value_parsed'] = alerts_data['trigger_value'].apply(
    parse_trigger_value
  )

  context = {}
  context['app_id'] = alerts_data.iloc[0][0]
  context['alerts_by_type'] = []

  for alert_type in alert_types:
    alerts = alerts_data['trigger_value_parsed'][
      alerts_data['trigger'] == alert_type
    ]
    context['alerts_by_type'].append(
      {
        'alert_name': alert_type,
        'alerts': alerts.tolist(),
        'timestamp': alerts_data.iloc[0]['timestamp'],
      }
    )

  return context


def parse_trigger_value(trigger_value: str) -> dict:
  """Converts trigger values to dict format.

  The trigger_value columns usually holds key:value pairs in string format.
  The function converts them to a python dictionary. If there isn't a key:value
  pair, the function uses trigger_value just as the value in a new dict.

  Args:
    trigger_value: Values of the alert to be parsed.

  Returns:
    The contents of 'trigger_value' in key:value pairs.
  """
  try:
    return json.loads(trigger_value)
  except json.JSONDecodeError:
    return {'Details': trigger_value}
