#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
DHS:
-----

Generates HXlated API urls from the DHS website.

"""
import json
import logging

from hdx.data.dataset import Dataset
from hdx.data.resource_view import ResourceView
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_sets_add
from hdx.utilities.downloader import DownloadError
from slugify import slugify

logger = logging.getLogger(__name__)

description = 'Contains data from the [DHS data portal](https://api.dhsprogram.com/). There is also a dataset containing [%s](%s) on HDX.\n\nThe DHS Program Application Programming Interface (API) provides software developers access to aggregated indicator data from The Demographic and Health Surveys (DHS) Program. The API can be used to create various applications to help analyze, visualize, explore and disseminate data on population, health, HIV, and nutrition from more than 90 countries.'
hxltags = {'ISO3': '#country+code', 'Location': '#loc+name', 'DataId': '#meta+id', 'Indicator': '#indicator+name',
           'Value': '#indicator+value+num', 'Precision': '#indicator+precision', 'CountryName': '#country+name',
           'SurveyYear': '#date+year', 'SurveyId': '#survey+id', 'IndicatorId': '#indicator+code',
           'ByVariableId': '#indicator+label+code', 'ByVariableLabel': '#indicator+label'}


def get_countries(base_url, downloader):
    url = '%scountries' % base_url
    response = downloader.download(url)
    json = response.json()
    countriesdata = list()
    for country in json['Data']:
        countryiso = country['UNSTAT_CountryCode']
        if countryiso:
            countriesdata.append({'iso3': countryiso, 'dhscode': country['DHS_CountryCode']})
    return countriesdata


def get_tags(base_url, downloader, dhscountrycode):
    url = '%stags/%s' % (base_url, dhscountrycode)
    response = downloader.download(url)
    json = response.json()
    return json['Data']


def get_publication(base_url, downloader, dhscountrycode):
    url = '%spublications/%s' % (base_url, dhscountrycode)
    response = downloader.download(url)
    json = response.json()
    publications = json['Data']
    publication = publications[0]
    for publicationdata in publications:
        if publication['SurveyType'] == 'DHS':
            if publicationdata['SurveyType'] != 'DHS':
                continue
            if publicationdata['SurveyYear'] == publication['SurveyYear']:
                if publicationdata['PublicationSize'] > publication['PublicationSize']:
                    publication = publicationdata
            elif publicationdata['SurveyYear'] > publication['SurveyYear']:
                publication = publicationdata
        else:
            if publicationdata['SurveyType'] == 'DHS':
                publication = publicationdata
            elif publicationdata['SurveyYear'] == publication['SurveyYear']:
                if publicationdata['PublicationSize'] > publication['PublicationSize']:
                    publication = publicationdata
            elif publicationdata['SurveyYear'] > publication['SurveyYear']:
                publication = publicationdata
    return publication


def get_dataset(countryiso, tags):
    dataset = Dataset()
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('45e7c1a1-196f-40a5-a715-9d6e934a7f70')
    dataset.set_expected_update_frequency('Every year')
    dataset.add_country_location(countryiso)
    dataset.add_tags(tags)
    return dataset


def set_dataset_date_bites(dataset, years, bites_disabled, national_subnational):
    years = sorted(list(years))
    latest_year = years[-1]
    dataset.set_dataset_year_range(years[0], latest_year)
    new_bites_disabled = [True, True, True]
    ns_bites_disabled = bites_disabled[national_subnational]
    for i, indicator in enumerate(['CM_ECMR_C_IMR', 'HC_ELEC_H_ELC', 'ED_LITR_W_LIT']):
        if indicator in ns_bites_disabled:
            indicator_latest_year = sorted(list(ns_bites_disabled[indicator]))[-1]
            if indicator_latest_year == latest_year:
                new_bites_disabled[i] = False
    bites_disabled[national_subnational] = new_bites_disabled


def process_quickstats_row(row, nationalsubnational):
    indicatorid = row['IndicatorId']
    if indicatorid == 'CM_ECMR_C_IMR':
        if 'ten' in row['ByVariableLabel'].lower():
            dict_of_sets_add(nationalsubnational, indicatorid, int(row['SurveyYear']))
    elif indicatorid in ['HC_ELEC_H_ELC', 'ED_LITR_W_LIT']:
        dict_of_sets_add(nationalsubnational, indicatorid, int(row['SurveyYear']))


def generate_datasets_and_showcase(configuration, base_url, downloader, folder, country, dhstags):
    """
    """
    countryiso = country['iso3']
    dhscountrycode = country['dhscode']
    countryname = Country.get_country_name_from_iso3(countryiso)
    title = '%s - Demographic and Health Data' % countryname
    logger.info('Creating datasets for %s' % title)
    tags = ['hxl', 'health', 'demographics']

    dataset = get_dataset(countryiso, tags)
    if dataset is None:
        return None, None, None, None
    dataset['title'] = title.replace('Demographic', 'National Demographic')
    slugified_name = slugify('DHS Data for %s' % countryname).lower()
    dataset['name'] = slugified_name
    dataset.set_subnational(False)

    subdataset = get_dataset(countryiso, tags)
    if dataset is None:
        return None, None, None, None

    subdataset['title'] = title.replace('Demographic', 'Subnational Demographic')
    subslugified_name = slugify('DHS Subnational Data for %s' % countryname).lower()
    subdataset['name'] = subslugified_name
    subdataset.set_subnational(True)

    dataset['notes'] = description % (subdataset['title'], configuration.get_dataset_url(subslugified_name))
    subdataset['notes'] = description % (dataset['title'], configuration.get_dataset_url(slugified_name))

    bites_disabled = {'national': dict(), 'subnational': dict()}

    def process_national_row(_, row):
        row['ISO3'] = countryiso
        if tagname == 'DHS Quickstats':
            process_quickstats_row(row, bites_disabled['national'])
        return row

    def process_subnational_row(_, row):
        row['ISO3'] = countryiso
        val = row['CharacteristicLabel']
        if val[:2] == '..':
            val = val[2:]
        row['Location'] = val
        if tagname == 'DHS Quickstats':
            process_quickstats_row(row, bites_disabled['subnational'])
        return row

    years = set()
    subyears = set()

    for dhstag in dhstags:
        tagname = dhstag['TagName'].strip()
        resource_name = '%s Data for %s' % (tagname, countryname)
        resourcedata = {
            'name': resource_name,
            'description': 'HXLated csv containing %s data' % tagname
        }

        url = '%sdata/%s?tagids=%s&breakdown=national&perpage=10000&f=csv' % (base_url, dhscountrycode, dhstag['TagID'])
        filename = '%s_national_%s.csv' % (tagname, countryiso)
        _, results = dataset.download_and_generate_resource(
            downloader, url, hxltags, folder, filename, resourcedata, header_insertions=[(0, 'ISO3')],
            row_function=process_national_row, yearcol='SurveyYear')
        years.update(results['years'])

        url = url.replace('breakdown=national', 'breakdown=subnational')
        filename = '%s_subnational_%s.csv' % (tagname, countryiso)
        try:
            insertions = [(0, 'ISO3'), (1, 'Location')]
            _, results = subdataset.download_and_generate_resource(
                downloader, url, hxltags, folder, filename, resourcedata, header_insertions=insertions,
                row_function=process_subnational_row, yearcol='SurveyYear')
            subyears.update(results['years'])
        except DownloadError as ex:
            cause = ex.__cause__
            if cause is not None:
                if 'Variable RET is undefined' not in str(cause):
                    raise ex
            else:
                raise ex
    if len(dataset.get_resources()) == 0:
        dataset = None
    else:
        set_dataset_date_bites(dataset, years, bites_disabled, 'national')
    if len(subdataset.get_resources()) == 0:
        subdataset = None
    else:
        set_dataset_date_bites(subdataset, subyears, bites_disabled, 'subnational')

    publication = get_publication(base_url, downloader, dhscountrycode)
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': publication['PublicationTitle'],
        'notes': publication['PublicationDescription'],
        'url': publication['PublicationURL'],
        'image_url': publication['ThumbnailURL']
    })
    showcase.add_tags(tags)
    return dataset, subdataset, showcase, bites_disabled


def generate_resource_view(dataset, quickchart_resourceno=0, bites_disabled=None):
    if bites_disabled == [True, True, True]:
        return None
    resourceview = ResourceView({'resource_id': dataset.get_resource(quickchart_resourceno)['id']})
    resourceview.update_from_yaml()
    hxl_preview_config = json.loads(resourceview['hxl_preview_config'])
    bites = hxl_preview_config['bites']
    if bites_disabled is not None:
        for i, disable in reversed(list(enumerate(bites_disabled))):
            if disable:
                del bites[i]
    for bite in bites:
        bite['type'] = 'key figure'
        bite['uiProperties']['postText'] = 'percent'
        del bite['ingredient']['aggregateColumn']
    resourceview['hxl_preview_config'] = json.dumps(hxl_preview_config)
    return resourceview
