import json
import os


def get_settings(config_path):
    """This function loads the config.json file based on the path and return the assembled settings dictionary.

    Args:
        config_path (str): Path to the config.json file.

    Returns:
        settings (dict): Dictionary contains the following keys:
            cromwell_url (str): URL to the Cromwell instance.
            use_caas (bool): Whether the Cromwell instance is CaaS or not.
            cromwell_user (str): Cromwell username if HTTPBasicAuth is enabled.
            cromwell_password (str): Cromwell password if HTTPBasicAuth is enabled.
            collection_name (str): The collection name if using CaaS.
            queue_update_interval (int): The sleep time between each time the queue handler retrieves
                workflows from Cromwell.
            workflow_start_interval (int): The sleep time between each time the igniter starts a workflow in Cromwell.
    """
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
    else:
        settings['queue_update_interval'] = int(settings['queue_update_interval'])

    if not settings['workflow_start_interval']:
        settings['workflow_start_interval'] = 1
    else:
        settings['workflow_start_interval'] = int(settings['workflow_start_interval'])

    return settings
