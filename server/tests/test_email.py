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
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from server.services.email import create_html_from_template
from server.services.email import create_message
from server.services.email import get_credentials
from server.services.email import send_message
from server.utils import Scopes


@pytest.fixture(scope='module', name='alerts')
def fixture_alerts() -> pd.DataFrame:
  columns = [
      'app_id', 'rule_name', 'trigger', 'trigger_value', 'alert_id', 'timestamp'
  ]
  alerts = [
      [
          'com.josper.gomusic', 'VersionEventsRule',
          'Missing event between versions',
          '{"Event Name": "first_open","Missing from Version": "1.1.2","Previous Version": "1.1.1"}',
          'VersionEventsRule_com.josper.gomusic_first_open_1.1.2_1.1.1',
          '2024-07-25 00:00:00.100000'
      ],
      [
          'com.josper.gomusic', 'VersionEventsRule',
          'Missing event between versions',
          '{"Event Name": "subscribe","Missing from Version": "1.1.2","Previous Version": "1.1.1"}',
          'VersionEventsRule_com.josper.gomusic_subscribe_1.1.2_1.1.1',
          '2024-07-25 00:00:00.100000'
      ],
      [
          'com.josper.gomusic', 'IntervalEventsRule',
          'Missing event for interval',
          '{"Event Name": "first_open","Missing for": "24"}',
          'IntervalEventsRule_com.josper.gomusic_first_open_24',
          '2024-07-25 00:00:00.100000'
      ],
      [
          'com.markdown.editor', 'VersionEventsRule',
          'Missing event between versions',
          '{"Event Name": "first_open","Missing from Version": "2.0.2","Previous Version": "2.0.0"}',
          'VersionEventsRule_com.markdown.editor_first_open_2.0.2_2.0.0',
          '2024-07-25 00:00:00.100000'
      ]
  ]

  return pd.DataFrame(alerts, columns=columns)




class TestEmailService:

  def test_get_credentials(self,config):
    creds = get_credentials(config)
    assert isinstance(creds, Credentials)
    assert creds._scopes == [Scopes.GMAIL_SEND.value]  # pylint: disable=protected-access

  @pytest.mark.parametrize('mime_header, expected_mime_header_value',
                           [('to', lambda message: message['to'][0]),
                            ('from', lambda message: message['sender']),
                            ('subject', lambda message: message['subject']),
                            ('Content-Type', lambda _: 'multipart/related')])
  def test_create_message(self, mime_header, expected_mime_header_value):
    message = {
      'sender': 'this.is@a.test',
      'to': ['test@test.com'],
      'subject': 'Test message',
      'body': ''
    }

    mime_message = create_message(**message)
    headers = dict(mime_message._headers)  # pylint: disable=protected-access

    assert isinstance(mime_message, MIMEMultipart)
    assert headers[mime_header] == expected_mime_header_value(message)

  @patch('server.services.email.build')
  def test_send_message(self, mock_build, config):
    msg = MIMEText('body', 'html')

    mime = MIMEMultipart()
    mime['to'] = 'test@test.com'
    mime['subject'] = 'Test'
    mime.attach(msg)

    mock_service = MagicMock()
    mock_build.return_value = mock_service

    mock_send = mock_service.users().messages().send()
    mock_send.execute.return_value = {
      'id': '0123456789abcdef',
      'thread': '0123456789abcdef'
    }

    resp = send_message(config, mime)

    assert resp['id'] == '0123456789abcdef'
    assert resp['thread'] == '0123456789abcdef'
    mock_send.execute.assert_called_once()

  @patch('server.services.email.build')
  def test_send_message__http_error(self, mock_build, config, caplog):
    mock_service = MagicMock()
    mock_build.return_value = mock_service

    # Mock the send method to raise an HttpError
    mock_send = mock_service.users().messages().send()
    mock_send.execute.side_effect = HttpError(
        resp=MagicMock(status=400),
        content=b'{"error": {"code": 400, "message": "Bad Request"}}')

    resp = send_message(config, MIMEMultipart())

    assert resp is None
    assert caplog.records[0].levelname == 'ERROR'

  def test_create_html_for_template(self, alerts):
    email_html = create_html_from_template(alerts)
    soup = BeautifulSoup(email_html, 'html.parser')

    row_titles = soup.find_all(id='alert-title-row')
    row_content = soup.find_all(id='alert-content-row')

    assert len(row_titles) == 2
    assert len(row_content) == 4
