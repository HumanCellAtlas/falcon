from falcon import settings
import os
import pytest


class TestSettings(object):

    data_dir = '{}/data/'.format(os.path.split(__file__)[0])
    cromwell_config = 'example_config_cromwell_instance.json'
    caas_config = 'example_config_caas.json'

    def test_get_settings_loads_config_file_for_cromwell_instance_without_exceptions(self):
        settings.get_settings('{0}{1}'.format(self.data_dir, self.cromwell_config))

    def test_get_settings_loads_config_file_for_cromwell_instance_correctly(self):
        loaded_settings = settings.get_settings('{0}{1}'.format(self.data_dir, self.cromwell_config))
        assert loaded_settings['cromwell_url'] == 'https://example.cromwell-instance.org/api/workflows/v1'
        assert loaded_settings['use_caas'] is False
        assert loaded_settings['cromwell_user'] == 'username'
        assert loaded_settings['cromwell_password'] == 'password'
        assert loaded_settings['queue_update_interval'] == 60
        assert loaded_settings['workflow_start_interval'] == 10

    def test_get_settings_loads_config_file_for_caas_throw_exceptions_without_caas_key(self):
        with pytest.raises(ValueError):
            settings.get_settings('{0}{1}'.format(self.data_dir, self.caas_config))

    def test_get_settings_loads_config_file_for_caas_correctly(self, monkeypatch):
        with monkeypatch.context() as ctx:
            ctx.setenv('caas_key', 'encrypted_key_content_string_to_communicate_with_caas')

            loaded_settings = settings.get_settings('{0}{1}'.format(self.data_dir, self.caas_config))
            assert loaded_settings['cromwell_url'] == 'https://example.cromwell-as-a-service.org/api/workflows/v1'
            assert loaded_settings['use_caas'] is True
            assert loaded_settings['caas_key'] == 'encrypted_key_content_string_to_communicate_with_caas'
            assert loaded_settings['collection_name'] == 'test-workflows'
            assert loaded_settings['queue_update_interval'] == 60
            assert loaded_settings['workflow_start_interval'] == 10
