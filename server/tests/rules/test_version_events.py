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
from unittest import mock

import pandas as pd
import pytest

from server.classes import alerts
from server.rules import version_events

@pytest.fixture(scope='class', name='version_events_obj')
def fixture_version_events(config):
  return version_events.VersionEventsRule(config)

@pytest.fixture(name='fake_collector_data')
def fixture_fake_collector_data(app_store_df, collector_ga4, gads_df,
                                play_store_df):
  return {
      'app_store': app_store_df,
      'ga4': collector_ga4,
      'gads': gads_df,
      'play_store': play_store_df
  }

@pytest.fixture(name='fake_version_event_1')
def fixture_fake_version_event_1():
  return version_events.VersionEventsEvent(
      event_name='test_event',
      app_id='com.test.app',
      cur_ver='1.1.0',
      prev_ver='1.0.0')


@pytest.fixture(name='fake_version_event_2')
def fixture_fake_version_event_2():
  return version_events.VersionEventsEvent(
      event_name='another_test_event',
      app_id='1072235449',
      cur_ver='2.0.1',
      prev_ver='2.0.0')

@pytest.fixture(name='fake_version_alert')
def fixture_fake_version_alert(fake_version_event_1):
  return alerts.Alert(
      app_id=fake_version_event_1.app_id,
      rule_name='VersionEventsRule',
      trigger='Missing event between versions',
      trigger_value={
          'Event Name': fake_version_event_1.event_name,
          'Missing from Version': fake_version_event_1.cur_ver,
          'Previous Version': fake_version_event_1.prev_ver,
      },
      alert_id=(
        f'VersionEventsRule_{fake_version_event_1.app_id}_'
        f'{fake_version_event_1.event_name}_{fake_version_event_1.cur_ver}_'
        f'{fake_version_event_1.prev_ver}'
      )
  )

@pytest.fixture(scope='class', name='versions')
def fixture_versions():
  return ['1.1.2', '1.0.1', '0.1.0', '2.0.0', '2.2.2']


class TestVersionEventsRule():

  def test_init(self, config, app_ids):
    version_event_rule = version_events.VersionEventsRule(config)
    assert isinstance(version_event_rule, version_events.VersionEventsRule)
    assert version_event_rule.name == 'VersionEventsRule'
    assert version_event_rule.app_ids == app_ids

  def test_run(self):
    pass

  @mock.patch('server.classes.rules.BigQuery.get_values_from_table')
  def test_get_data(self, mock_get_values, fake_collector_data,
                    version_events_obj):

    mock_get_values.side_effect = [
        fake_collector_data['app_store'], fake_collector_data['ga4'],
        fake_collector_data['gads'], fake_collector_data['play_store']
    ]

    data = version_events_obj.get_data()
    assert data == fake_collector_data

  @mock.patch('server.rules.version_events.logger')
  def test_check_rule_no_ga4_data_logs_error(self, mock_logger,
                                             version_events_obj,
                                             fake_collector_data):
    data = fake_collector_data.copy()
    del data['ga4']
    resp = version_events_obj.check_rule(data)

    assert resp == []
    mock_logger.error.assert_called()

  @mock.patch('server.rules.version_events.logger')
  def test_check_rule_no_gads_data_logs_error(self, mock_logger,
                                              version_events_obj,
                                             fake_collector_data):
    data = fake_collector_data.copy()
    del data['gads']
    resp = version_events_obj.check_rule(data)

    assert resp == []
    mock_logger.error.assert_called()

  def test_check_rule_only_app_store_data(self, version_events_obj,
                                          fake_collector_data):

    collectors = fake_collector_data.copy()
    del collectors['play_store']

    violations = version_events_obj.check_rule(collectors)
    store_violations = [
        violation for violation in violations
        if violation.event_name == 'first_open'
    ]

    assert len(violations) == 4
    assert len(store_violations) == 2
    assert all(
        isinstance(item, version_events.VersionEventsEvent)
        for item in violations)
    assert all(event.app_id.isnumeric() for event in store_violations)

  def test_check_rule_only_play_data_less_than_24_hours(self,
                                                         version_events_obj,
                                                         fake_collector_data):

    collectors = fake_collector_data.copy()
    del collectors['app_store']

    violations = version_events_obj.check_rule(collectors)
    store_violations = list(
        filter(lambda x: x.event_name == 'first_open', violations))

    assert len(violations) == 4
    assert len(store_violations) == 0
    assert all(
        isinstance(item, version_events.VersionEventsEvent)
        for item in violations)

  def test_check_rule_only_play_data__more_than_24_hours(self,
                                                         version_events_obj,
                                                         fake_collector_data):
    collectors = fake_collector_data.copy()
    del collectors['app_store']
    collectors['play_store'].loc[0, 'timestamp'] = '2024-07-29T00:00:00.00000'

    violations = version_events_obj.check_rule(collectors)
    store_violations = list(
        filter(lambda x: x.event_name == 'first_open', violations))

    assert len(violations) == 4
    assert len(store_violations) == 1
    assert all(
        isinstance(item, version_events.VersionEventsEvent)
        for item in violations)

  def test_check_rule_no_app_store_and_play_data(self,
                                                 version_events_obj,
                                                 fake_collector_data):
    collectors = fake_collector_data.copy()
    del collectors['app_store']
    del collectors['play_store']

    violations = version_events_obj.check_rule(collectors)
    store_violations = list(
        filter(lambda x: x.event_name == 'first_open', violations))

    assert len(violations) == 4
    assert len(store_violations) == 0
    assert all(
        isinstance(item, version_events.VersionEventsEvent)
        for item in violations)

  def test_create_alerts(self, version_events_obj, fake_version_event_1,
                         fake_version_event_2, fake_version_alert):

    events = [fake_version_event_1, fake_version_event_2]
    event_alerts = version_events_obj.create_alerts(events)

    assert isinstance(event_alerts, list)
    assert len(event_alerts) == 2
    assert all(isinstance(a, alerts.Alert) for a in event_alerts)
    assert event_alerts[0] == fake_version_alert

  def test_update_alerts_log__pass(self, version_events_obj,
                                   fake_version_alert):
    #TODO: make general test for Rules, not specific to Versions
    resp = version_events_obj.update_alerts_log([fake_version_alert])
    assert resp is True

  def test_update_alerts_log__empty_returns_false(self, version_events_obj):
    #TODO: make general test for Rules, not specific to Versions
    resp = version_events_obj.update_alerts_log([])
    assert resp is False

  def test_update_alerts_log__type_error(self, version_events_obj):
    #TODO: make general test for Rules, not specific to Versions
    with pytest.raises(TypeError) as exc_info:
      version_events_obj.update_alerts_log(['Invalid test value'])
    assert exc_info.value.args[0] == 'Alerts must be of type Alert'

  def test_update_last_run(self, version_events_obj):
    #TODO: make general test for Rules, not specific to Versions
    resp = version_events_obj.update_last_run()
    assert isinstance(resp, list)
    assert resp == ['VersionEventsRule', 'rule']

  def test_get_versions(self, version_events_obj, ga4_df):
    versions = version_events_obj.get_versions(ga4_df)
    assert len(versions) == 12

  def test_is_version__true(self, version_events_obj):
    assert version_events_obj.is_version('1.1.1')

  def test_is_version__false(self, version_events_obj):
    not_versions = ['1.1.1.1', '1.1.1-alpha', 'ver', 100]
    for ver in not_versions:
      assert not version_events_obj.is_version(ver)

  def test_find_latest_version(self, versions, version_events_obj):
    cur_ver = version_events_obj.find_latest_version(versions)
    assert cur_ver == '2.2.2'

  def test_find_previous_version(self, versions, version_events_obj):
    cur_ver = '2.2.2'
    prev_ver = version_events_obj.find_previous_version(cur_ver, versions)
    assert prev_ver == '2.0.0'

  def test_get_events_for_version(self, version_events_obj, ga4_df):
    version = '1.1.0'
    events = version_events_obj.get_events_for_version(version, ga4_df)
    assert len(events) == 5

  def test_compare_events_between_versions(self, version_events_obj, ga4_df):
    cur_ver, prev_ver = '1.1.1', '1.1.0'
    missing_events = version_events_obj.compare_events_between_versions(
        'test-app-id', cur_ver, prev_ver, ga4_df)
    assert len(missing_events) == 1

  def test_get_uids__success(self, app_ids, version_events_obj, gads_df):
    resp = version_events_obj.get_uids(app_ids, gads_df)
    assert isinstance(resp, list)
    assert len(resp) == gads_df.shape[0]

  def test_filter_by_uids__success(self, version_events_obj, gads_df):
    uids_list = ['android_912210201_first_open', 'ios_881576146_large_cart']
    df = version_events_obj.filter_by_uids(uids_list, gads_df)
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2,8)

  def test_add_app_ids__added_app_id_column(self, version_events_obj, gads_df,
                                            collector_ga4):
    df = version_events_obj.add_app_ids(gads_df, collector_ga4)
    columns = df.columns.tolist()
    assert isinstance(df, pd.DataFrame)
    assert 'app_id' in columns
