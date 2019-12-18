#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from dhs import get_countries, generate_datasets_and_showcase, get_tags, generate_resource_view

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-dhs'


def createdataset(dataset):
    dataset.update_from_yaml()
    dataset['license_other'] = dataset['license_other'].replace('\n', '  \n')  # ensure markdown has line breaks
    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    base_url = configuration['base_url']
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
        countries = get_countries(base_url, downloader)
        logger.info('Number of countries: %d' % len(countries))
        for folder, country in progress_storing_tempdir('DHS', countries, 'iso3'):
            tags = get_tags(base_url, downloader, country['dhscode'])
            dataset, subdataset, showcase, bites_disabled = \
                generate_datasets_and_showcase(configuration, base_url, downloader, folder, country, tags)
            if dataset:
                createdataset(dataset)
                resource_view = generate_resource_view(dataset, bites_disabled=bites_disabled['national'])
                resource_view.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)
            if subdataset:
                createdataset(subdataset)
                showcase.add_dataset(subdataset)
                subdataset.generate_resource_view(bites_disabled=bites_disabled['subnational'])


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
