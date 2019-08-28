#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from dhs import get_countriesdata, generate_dataset_and_showcase, generate_resource_view, get_tags

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-dhs'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    base_url = configuration['base_url']
    hxlproxy_url = configuration['hxlproxy_url']
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
        countriesdata = get_countriesdata(base_url, downloader)
        logger.info('Number of countries: %d' % len(countriesdata))
        for countrydata in sorted(countriesdata):
            tags = get_tags(base_url, downloader, countrydata[1])
            dataset, showcase = generate_dataset_and_showcase(base_url, hxlproxy_url, downloader, countrydata, tags)
            if dataset:
                dataset.update_from_yaml()
                dataset['license_other'] = dataset['license_other'].replace('\n', '  \n')  # ensure markdown has line breaks
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                resource_view = generate_resource_view(dataset)
                resource_view.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))
