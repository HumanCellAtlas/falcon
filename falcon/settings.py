import json
import os
from cromwell_tools.cromwell_auth import CromwellAuth


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
            cromwell_query_dict (dict): The query used for retrieving cromwell workflows
    """
    with open(config_path, 'r') as f:
        settings = json.load(f)

    # Check Cromwell url
    if not settings['cromwell_url']:
        raise ValueError('A Cromwell URL is required.')

    # Check auth parameters
    if settings['use_caas']:
        if not settings['collection_name']:
            raise ValueError(
                'To use the Cromwell-as-a-Service, you have to pass in a valid collection name.'
            )

        caas_key = os.environ.get('caas_key') or os.environ.get('CAAS_KEY')
        if not caas_key:
            raise ValueError(
                'No service account json key provided for cromwell-as-a-service.'
            )
        else:
            settings['caas_key'] = caas_key

    # Check other config parameters
    settings['queue_update_interval'] = int(settings.get('queue_update_interval', 1))
    settings['workflow_start_interval'] = int(
        settings.get('workflow_start_interval', 1)
    )

    # Check cromwell query parameters
    query_dict = settings.get('cromwell_query_dict', {})
    if ('status', 'On Hold') not in query_dict.items():
        query_dict.update({'status': 'On Hold'})
    settings['cromwell_query_dict'] = query_dict

    return settings


def get_cromwell_auth(settings):
    cromwell_url = settings.get('cromwell_url')
    if settings.get('use_caas'):
        return CromwellAuth.harmonize_credentials(
            url=cromwell_url, service_account_key=settings.get('caas_key')
        )
    return CromwellAuth.harmonize_credentials(
        url=cromwell_url,
        username=settings.get('cromwell_user'),
        password=settings.get('cromwell_password'),
    )
