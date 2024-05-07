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
''' Create an App Engine service for sending emails'''
from flask import Flask
from flask import jsonify
from flask import request
from google.appengine.api import mail
from google.appengine.api import wrap_wsgi_app
from google.appengine.runtime import apiproxy_errors
from google.auth import default


app = Flask(__name__)
app.wsgi_app = wrap_wsgi_app(app.wsgi_app)

def get_project_id() -> str:
  ''' Get the Google Cloud Project Id

  Returns:
    GCP Project Id
  '''
  _, project_id = default()
  return project_id

@app.route('/send-email', methods=['POST'])
def send_email():
  ''' Send an email.
  The function gets the "to", "subject" and "body" from the request object.
  '''
  data = request.get_json()
  to_email = data['to']
  subject_email = data['subject']
  message_email = data['body']

  project_id = get_project_id()
  sender = f'no-reply@{project_id}.appspotmail.com'

  try:
    message = mail.EmailMessage(sender=sender,
                                subject=subject_email,
                                to=to_email,
                                body=message_email)
    message.send()

    return jsonify({'result': f'Successfully sent email to {to_email}!'})
  except apiproxy_errors.ApplicationError as error:
    return jsonify({'result': f'Failed to send email. Error: {error}'})

if __name__ == '__main__':
  app.run()
