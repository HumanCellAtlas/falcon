import json
import os

import pytest

from falcon import settings


class TestSettings(object):
    data_dir = '{}/data/'.format(os.path.split(__file__)[0])
    cromwell_config = 'example_config_cromwell_instance.json'
    caas_config = 'example_config_caas.json'

    with open('{0}{1}'.format(data_dir, cromwell_config)) as f:
        expected_settings_dic_cromwell_instance = json.load(f)
    with open('{0}{1}'.format(data_dir, caas_config)) as f:
        expected_settings_dic_caas = json.load(f)

    def test_get_settings_loads_config_file_for_cromwell_instance_without_exceptions(
        self
    ):
        settings.get_settings('{0}{1}'.format(self.data_dir, self.cromwell_config))

    def test_get_settings_loads_config_file_for_cromwell_instance_correctly(self):
        loaded_settings = settings.get_settings(
            '{0}{1}'.format(self.data_dir, self.cromwell_config)
        )
        assert loaded_settings[
            'cromwell_url'
        ] == self.expected_settings_dic_cromwell_instance.get('cromwell_url')
        assert loaded_settings[
            'use_caas'
        ] == self.expected_settings_dic_cromwell_instance.get('use_caas')
        assert loaded_settings[
            'cromwell_user'
        ] == self.expected_settings_dic_cromwell_instance.get('cromwell_user')
        assert loaded_settings[
            'cromwell_password'
        ] == self.expected_settings_dic_cromwell_instance.get('cromwell_password')
        assert loaded_settings['queue_update_interval'] == int(
            self.expected_settings_dic_cromwell_instance.get('queue_update_interval')
        )
        assert loaded_settings['workflow_start_interval'] == int(
            self.expected_settings_dic_cromwell_instance.get('workflow_start_interval')
        )

    def test_get_settings_cromwell_query_dict_includes_on_hold_status(self):
        loaded_settings = settings.get_settings(
            '{0}{1}'.format(self.data_dir, self.cromwell_config)
        )
        assert loaded_settings['cromwell_query_dict']['status'] == 'On Hold'

    def test_get_settings_loads_config_file_for_caas_throw_exceptions_without_caas_key(
        self
    ):
        with pytest.raises(ValueError):
            settings.get_settings('{0}{1}'.format(self.data_dir, self.caas_config))

    def test_get_settings_loads_config_file_for_caas_for_capital_env_variable_correctly(
        self, monkeypatch
    ):
        with monkeypatch.context() as ctx:
            ctx.setenv(
                'CAAS_KEY', 'encrypted_key_content_string_to_communicate_with_caas'
            )

            loaded_settings = settings.get_settings(
                '{0}{1}'.format(self.data_dir, self.caas_config)
            )
            assert (
                loaded_settings['caas_key']
                == 'encrypted_key_content_string_to_communicate_with_caas'
            )

    def test_get_settings_loads_config_file_for_caas_correctly(self, monkeypatch):
        with monkeypatch.context() as ctx:
            ctx.setenv(
                'caas_key', 'encrypted_key_content_string_to_communicate_with_caas'
            )

            loaded_settings = settings.get_settings(
                '{0}{1}'.format(self.data_dir, self.caas_config)
            )
            assert loaded_settings[
                'cromwell_url'
            ] == self.expected_settings_dic_caas.get('cromwell_url')
            assert loaded_settings['use_caas'] == self.expected_settings_dic_caas.get(
                'use_caas'
            )
            assert (
                loaded_settings['caas_key']
                == 'encrypted_key_content_string_to_communicate_with_caas'
            )
            assert loaded_settings[
                'collection_name'
            ] == self.expected_settings_dic_caas.get('collection_name')
            assert loaded_settings['queue_update_interval'] == int(
                self.expected_settings_dic_caas.get('queue_update_interval')
            )
            assert loaded_settings['workflow_start_interval'] == int(
                self.expected_settings_dic_caas.get('workflow_start_interval')
            )
