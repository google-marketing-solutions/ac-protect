import pandas as pd
import pytest

from server.classes.alerts import Alert
from server.db.tables import GA4_TABLE_NAME
from server.db.tables import GADS_TABLE_NAME
from server.rules.version_events import VersionEventsEvent
from server.rules.version_events import VersionsEventsRule


@pytest.fixture(scope='class', name='versions_event')
def fixture_versions_event(config):
  return VersionsEventsRule(config)

@pytest.fixture(name='fake_version_event_1')
def fixture_fake_version_event_1():
  return VersionEventsEvent(
      event_name='test_event',
      app_id='com.test.app',
      cur_ver='1.1.0',
      prev_ver='1.0.0')


@pytest.fixture(name='fake_version_event_2')
def fixture_fake_version_event_2():
  return VersionEventsEvent(
      event_name='another_test_event',
      app_id='1072235449',
      cur_ver='2.0.1',
      prev_ver='2.0.0')

@pytest.fixture(name='fake_version_alert')
def fixture_fake_version_alert(fake_version_event_1):
  return Alert(
      app_id=fake_version_event_1.app_id,
      rule_name='VersionEventsRule',
      trigger='Missing event between versions',
      trigger_value={
          'Event Name': fake_version_event_1.event_name,
          'Missing from Version': fake_version_event_1.cur_ver,
          'Previous Version': fake_version_event_1.prev_ver,
      },
      alert_id=f'VersionEventsRule_{fake_version_event_1.app_id}_{fake_version_event_1.event_name}_{fake_version_event_1.cur_ver}_{fake_version_event_1.prev_ver}'
  )

class TestInit():
  ''' Test the initialization of the rule'''

  def test_init(self, config):
    version_event = VersionsEventsRule(config)

    assert isinstance(version_event, VersionsEventsRule)
    assert version_event.name == 'VersionEventsRule'
    assert version_event.app_ids == ['com.test.app', '1072235449']
    assert version_event.gads_table == GADS_TABLE_NAME
    assert version_event.ga4_table == GA4_TABLE_NAME


class TestVersionsLogic():
  ''' Test the version testing logic of the rule'''

  @pytest.fixture(scope='class', name='versions')
  def fixture_versions(self):
    return ['1.1.2', '1.0.1', '0.1.0', '2.0.0', '2.2.2']

  def test_is_version__true(self, versions_event):
    assert versions_event.is_version('1.1.1')

  def test_is_version__false(self, versions_event):
    not_versions = ['1.1.1.1', '1.1.1-alpha', 'ver', 100]
    for ver in not_versions:
      assert not versions_event.is_version(ver)

  def test_find_latest_version(self, versions, versions_event):
    cur_ver = versions_event.find_latest_version(versions)
    assert cur_ver == '2.2.2'

  def test_find_previous_version(self, versions, versions_event):
    cur_ver = '2.2.2'
    prev_ver = versions_event.find_previous_version(cur_ver, versions)
    assert prev_ver == '2.0.0'

  def test_get_versions(self, versions_event, ga4_df):
    versions = versions_event.get_versions(ga4_df)
    assert len(versions) == 4

  def test_get_events_for_version(self, versions_event, ga4_df):
    version = '7.24.4'
    events = versions_event.get_events_for_version(version, ga4_df)
    assert len(events) == 3

  def test_compare_versions(self, versions_event, ga4_df):
    cur_ver = '7.24.6'
    prev_ver = '7.24.4'

    missing_events = versions_event.compare_versions(cur_ver, prev_ver, ga4_df)
    assert len(missing_events) == 2


class TestMainFunctions():

  def test_run(self):
    pass

  def test_get_data(self):
    # TODO: Mock db and return dataFrame from test data
    pass

  def test_check_rule(self, versions_event, gads_df, ga4_no_app_id_df):
    violations = versions_event.check_rule(gads_df, ga4_no_app_id_df)
    assert isinstance(violations, list)
    assert len(violations) == 3
    assert all(isinstance(v, VersionEventsEvent) for v in violations)

  def test_create_alerts(self, versions_event, fake_version_event_1,
                         fake_version_event_2, fake_version_alert):

    events = [fake_version_event_1, fake_version_event_2]
    alerts = versions_event.create_alerts(events)

    assert isinstance(alerts, list)
    assert len(alerts) == 2
    assert all(isinstance(a, Alert) for a in alerts)

    assert alerts[0].app_id == fake_version_alert.app_id
    assert alerts[0].rule_name == fake_version_alert.rule_name
    assert alerts[0].trigger == fake_version_alert.trigger
    assert alerts[0].trigger_value == fake_version_alert.trigger_value
    assert alerts[0].alert_id == fake_version_alert.alert_id

  def test_update_alerts_log__pass(self, versions_event, fake_version_alert):
    #TODO: make general test for Rules, not specific to Versions
    resp = versions_event.update_alerts_log([fake_version_alert])
    assert resp is True

  def test_update_alerts_log__empty_returns_false(self, versions_event):
    #TODO: make general test for Rules, not specific to Versions
    resp = versions_event.update_alerts_log([])
    assert resp is False

  def test_update_alerts_log__type_error(self, versions_event):
    #TODO: make general test for Rules, not specific to Versions
    with pytest.raises(TypeError) as exc_info:
      versions_event.update_alerts_log(['Invalid test value'])
    assert exc_info.value.args[0] == 'Alerts must be of type Alert'

  def test_update_last_run(self, versions_event):
    #TODO: make general test for Rules, not specific to Versions
    resp = versions_event.update_last_run()
    assert isinstance(resp, list)
    assert resp[0] == 'VersionEventsRule'
    assert resp[1] == 'rule'


class TestHelperFunctions():

  def test_get_uids(self, versions_event, gads_df):
    app_ids = ['com.test.app', '1072235449']
    resp = versions_event.get_uids(app_ids, gads_df)

    assert isinstance(resp, list)
    assert len(resp) == 5


  def test_filter_for_uids(self, versions_event, gads_df):
    uids_list = ['android_102016000_purchase', 'ios_102016000_first_open']
    df = versions_event.filter_for_uids(uids_list, gads_df)

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2,8)

  def test_add_app_ids(self, versions_event, gads_df, ga4_no_app_id_df):
    df = versions_event.add_app_ids(gads_df, ga4_no_app_id_df)
    columns = df.columns.tolist()

    assert isinstance(df, pd.DataFrame)
    assert 'app_id' in columns
