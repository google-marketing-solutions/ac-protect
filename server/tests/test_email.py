from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError

from server.services.email import create_html_email
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
      'app_id_1', 'rule_name_1', 'trigger_1',
      '{"event":"value", "name":"value"}', 'alert_id_1', '2024-01-01'
    ],
    [
        'app_id_1', 'rule_name_2', 'trigger_2',
        '{"event":"value2", "name":"value2"}', 'alert_id_2',
        '2024-01-02'
    ],
    [
        'app_id_2', 'rule_name', 'trigger', '{"event":"value"}',
        'alert_id_3', '2024-01-02'
    ]]

  return pd.DataFrame(alerts, columns=columns)


class TestEmailService:

  def test_get_credentials(self,config):
    creds = get_credentials(config)
    assert isinstance(creds, Credentials)
    assert creds._scopes == [Scopes.GMAIL_SEND.value]

  def test_create_message(self):
    message = {
      'sender': 'this.is@a.test',
      'to': ['test@test.com'],
      'subject': 'Test message',
      'body': 'This is a test message'
    }

    mime_message = create_message(**message)
    assert isinstance(mime_message, MIMEMultipart)
    assert mime_message._headers[2][1] == message['to'][0]  # pylint: disable=protected-access
    assert mime_message._headers[3][1] == message['sender']  # pylint: disable=protected-access
    assert mime_message._headers[4][1] == message['subject']  # pylint: disable=protected-access
    assert mime_message._payload[0]._payload == message['body']  # pylint: disable=protected-access

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

  def test_create_create_html_email(self, alerts):
    email_html = create_html_email(alerts)

    soup = BeautifulSoup(email_html, 'html.parser')

    table = soup.find('table')
    headers = [th.text for th in table.find_all('th')]

    rows = []
    for tr in table.find_all('tr')[1:]:
      cells = [td.text for td in tr.find_all('td')]
      rows.append(cells)

    df = pd.DataFrame(rows, columns=headers)

    df_origin = alerts[['trigger', 'trigger_value', 'timestamp']]
    assert pd.DataFrame.equals(df, df_origin)
