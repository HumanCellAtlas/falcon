import json
import os


def get_settings(config_path):
    with open(config_path, 'r') as f:
        settings = json.load(f)

    # Check Cromwell url
    if not settings['cromwell_url']:
        raise ValueError('A Cromwell URL is required.')

    # Check auth parameters
    if settings['use_caas']:
        if not settings['collection_name']:
            raise ValueError('To use the Cromwell-as-a-Service, you have to pass in a valid collection name.')
        if not os.environ.get('caas_key') and not os.environ.get('CAAS_KEY'):
            raise ValueError('No service account json key provided for cromwell-as-a-service.')
        else:
            settings['caas_key'] = os.environ.get('caas_key')

    # Check other config parameters
    if not settings['queue_update_interval']:
        settings['queue_update_interval'] = 10

    if not settings['workflow_start_interval']:
        settings['workflow_start_interval'] = 1

    return settings
