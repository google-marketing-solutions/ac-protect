import pandas as pd
import pytest

from server.classes.alerts import Alert
from server.db.tables import GA4_TABLE_NAME
from server.db.tables import GADS_TABLE_NAME
from server.rules.interval import IntervalEvent
from server.rules.interval import IntervalEventsRule


@pytest.fixture(scope='session', name='interval_event')
def fixture_interval_event(config):
  return IntervalEventsRule(config)


@pytest.fixture(name='fake_interval_event_1')
def fixture_fake_interval_event_1():
  return IntervalEvent(
      event_name='test_event',
      app_id='com.test.app',
      interval=24)


@pytest.fixture(name='fake_interval_event_2')
def fixture_fake_interval_event_2():
  return IntervalEvent(
      event_name='another_test_event',
      app_id='1072235449',
      interval=24)


@pytest.fixture(name='fake_interval_alert')
def fixture_fake_interval_alert(fake_interval_event_1):
  return Alert(
      app_id=fake_interval_event_1.app_id,
      rule_name='IntervalEventsRule',
      trigger='Missing event for interval',
      trigger_value={
          'Event Name': fake_interval_event_1.event_name,
          'Missing for': fake_interval_event_1.interval,
      },
      alert_id=f'IntervalEventsRule_{fake_interval_event_1.app_id}_{fake_interval_event_1.event_name}_{fake_interval_event_1.interval}'
  )


class TestInit():
  ''' Test the initialization of the rule'''

  def test_init(self, config):
    interval_event = IntervalEventsRule(config)

    assert isinstance(interval_event, IntervalEventsRule)
    assert interval_event.name == 'IntervalEventsRule'
    assert interval_event.app_ids == ['com.test.app', '1072235449']
    assert interval_event.gads_table == GADS_TABLE_NAME
    assert interval_event.ga4_table == GA4_TABLE_NAME
    assert interval_event.interval == 24


class TestMainFunctions():

  def test_run(self):
    pass

  def test_get_data(self):
    # TODO: Mock db and return dataFrame from test data
    pass

  def test_check_rule(self, interval_event, gads_df, ga4_no_app_id_df):
    violations = interval_event.check_rule(gads_df, ga4_no_app_id_df)
    assert isinstance(violations, list)
    assert len(violations) == 3
    assert all(isinstance(v, IntervalEvent) for v in violations)

  def test_create_alerts(self, interval_event, fake_interval_event_1,
                         fake_interval_event_2, fake_interval_alert):

    events = [fake_interval_event_1, fake_interval_event_2]
    alerts = interval_event.create_alerts(events)

    assert isinstance(alerts, list)
    assert len(alerts) == 2
    assert all(isinstance(a, Alert) for a in alerts)

    assert alerts[0].app_id == fake_interval_alert.app_id
    assert alerts[0].rule_name == fake_interval_alert.rule_name
    assert alerts[0].trigger == fake_interval_alert.trigger
    assert alerts[0].trigger_value == fake_interval_alert.trigger_value
    assert alerts[0].alert_id == fake_interval_alert.alert_id

  def test_update_last_run(self, interval_event):
    #TODO: make general test for Rules, not specific to IntervalEventsRule
    resp = interval_event.update_last_run()
    assert isinstance(resp, list)
    assert resp[0] == 'IntervalEventsRule'
    assert resp[1] == 'rule'


class TestHelperFunctions():

  #TODO: Make all these functions a mixin in base Rule class
  def test_get_uids(self, interval_event, gads_df):
    app_ids = ['com.test.app', '1072235449']
    resp = interval_event.get_uids(app_ids, gads_df)

    assert isinstance(resp, list)
    assert len(resp) == 5

  def test_filter_for_uids(self, interval_event, gads_df):
    uids_list = ['android_102016000_purchase', 'ios_102016000_first_open']
    df = interval_event.filter_for_uids(uids_list, gads_df)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 8)

  def test_add_app_ids(self, interval_event, gads_df, ga4_no_app_id_df):
    df = interval_event.add_app_ids(gads_df, ga4_no_app_id_df)
    columns = df.columns.tolist()

    assert isinstance(df, pd.DataFrame)
    assert 'app_id' in columns
