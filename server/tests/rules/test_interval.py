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
"""Tests for the IntervalEventsRule class.

The IntervalEventsRule checks if certain events occur within expected time
intervals for specified apps. These tests verify the rule's functionality for
detecting missing events and generating appropriate alerts.
"""
# pylint: disable=C0330, g-multiple-import, missing-function-docstring

import pandas as pd
import pytest

from server.classes import alerts
from server.db import tables
from server.rules import interval


@pytest.fixture(scope='session', name='interval_event')
def fixture_interval_event(config):
  return interval.IntervalEventsRule(config)


@pytest.fixture(name='fake_interval_event_1')
def fixture_fake_interval_event_1():
  return interval.IntervalEvent(
    event_name='test_event', app_id='com.test.app', interval=24
  )


@pytest.fixture(name='fake_interval_event_2')
def fixture_fake_interval_event_2():
  return interval.IntervalEvent(
    event_name='another_test_event', app_id='1072235449', interval=24
  )


@pytest.fixture(name='fake_interval_alert')
def fixture_fake_interval_alert(fake_interval_event_1):
  return alerts.Alert(
    app_id=fake_interval_event_1.app_id,
    rule_name='IntervalEventsRule',
    trigger='Missing event for interval',
    trigger_value={
      'Event Name': fake_interval_event_1.event_name,
      'Missing for': fake_interval_event_1.interval,
    },
    alert_id=(
      f'IntervalEventsRule_{fake_interval_event_1.app_id}_'
      f'{fake_interval_event_1.event_name}_{fake_interval_event_1.interval}'
    ),
  )


@pytest.fixture(name='collectors_data')
def fixture_collectors_data(gads_df, collector_ga4):
  return {'gads': gads_df, 'ga4': collector_ga4}


class TestIntervalRule:
  """Test class for IntervalEventsRule.

  Tests the functionality of the IntervalEventsRule class which monitors events
  that should occur at regular intervals. Verifies initialization, rule
  checking, alert creation, and database interactions.
  """

  def test_init(self, config, app_ids):
    interval_time = 24
    interval_event = interval.IntervalEventsRule(config)

    assert isinstance(interval_event, interval.IntervalEventsRule)
    assert interval_event.name == 'IntervalEventsRule'
    assert interval_event.app_ids == app_ids
    assert interval_event.gads_table == tables.GADS_TABLE_NAME
    assert interval_event.ga4_table == tables.GA4_TABLE_NAME
    assert interval_event.interval == interval_time

  def test_check_rule(self, interval_event, collectors_data):
    num_violations = 15

    violations = interval_event.check_rule(collectors_data)
    assert isinstance(violations, list)
    assert len(violations) == num_violations
    assert all(isinstance(v, interval.IntervalEvent) for v in violations)

  def test_create_alerts(
    self,
    interval_event,
    fake_interval_event_1,
    fake_interval_event_2,
    fake_interval_alert,
  ):
    num_alerts = 2
    events = [fake_interval_event_1, fake_interval_event_2]
    interval_alerts = interval_event.create_alerts(events)

    assert isinstance(interval_alerts, list)
    assert len(interval_alerts) == num_alerts
    assert all(isinstance(a, alerts.Alert) for a in interval_alerts)

    assert interval_alerts[0].app_id == fake_interval_alert.app_id
    assert interval_alerts[0].rule_name == fake_interval_alert.rule_name
    assert interval_alerts[0].trigger == fake_interval_alert.trigger
    assert interval_alerts[0].trigger_value == fake_interval_alert.trigger_value
    assert interval_alerts[0].alert_id == fake_interval_alert.alert_id

  def test_update_last_run(self, interval_event):
    resp = interval_event.update_last_run()
    assert isinstance(resp, list)
    assert resp[0] == 'IntervalEventsRule'
    assert resp[1] == 'rule'

  def test_get_uids(self, interval_event, gads_df, app_ids):
    num_uids = 15
    resp = interval_event.get_uids(app_ids, gads_df)

    assert isinstance(resp, list)
    assert len(resp) == num_uids

  def test_filter_for_uids(self, interval_event, gads_df):
    uids_list = ['android_912210201_first_open', 'ios_881576146_large_cart']
    df = interval_event.filter_for_uids(uids_list, gads_df)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 8)

  def test_add_app_ids(self, interval_event, gads_df, collector_ga4):
    df = interval_event.add_app_ids(gads_df, collector_ga4)
    columns = df.columns.tolist()

    assert isinstance(df, pd.DataFrame)
    assert 'app_id' in columns
