#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
DHS:
-----

Generates HXlated API urls from the DHS website.

"""
import logging
from urllib.parse import quote_plus

from hdx.data.dataset import Dataset
from hdx.data.resource_view import ResourceView
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)

hxlate_start = '&name=DHSHXL&header-row=1&tagger-match-all=on&tagger-01-header=dataid&tagger-01-tag=%23meta%2Bid&tagger-02-header=indicator&tagger-02-tag=%23indicator%2Bname&tagger-03-header=value&tagger-03-tag=%23indicator%2Bnum&tagger-04-header=precision&tagger-04-tag=%23indicator%2Bprecision&tagger-06-header=countryname&tagger-06-tag=%23country%2Bname&tagger-07-header=surveyyear&tagger-07-tag=%23date%2Byear&tagger-08-header=surveyid&tagger-08-tag=%23survey%2Bid&tagger-09-header=indicatorid&tagger-09-tag=%23indicator%2Bid'

hxlate_sn_tag = '&tagger-10-header=CharacteristicLabel&tagger-10-tag=%23meta%2Bcharacteristic'

hxlate_filter = '&filter01=add&add-tag01=%23country%2Bcode&add-value01=%s&add-header01=ISO3'

hxlate_sn_filters = '&filter02=add&add-tag02=%23adm1%2Bname&add-value02=%7B%7B%23meta%2Bcharacteristic%7D%7D&add-header02=Admin+1+if+relevant&filter03=replace-map&replace-map-url03=https%3A%2F%2Fraw.githubusercontent.com%2Fmcarans%2Fdhs_hxl_quickcharts%2Fmaster%2Fhxl_replacement_values.csv&replace-map-where03=%23adm1%2Bname%21~.%5C.%5C.%2A&filter04=replace&replace-pattern04=%5C.%5C.%28.%2A%29&replace-regex04=on&replace-value04=%5C1&replace-tags04=%23adm1%2Bname&replace-where04=%23adm1%2Bname~%5C.%5C..%2A'

quickchart_resourceno = 1


def get_countriesdata(base_url, downloader):
    url = '%scountries' % base_url
    response = downloader.download(url)
    json = response.json()
    countriesdata = list()
    for country in json['Data']:
        countryiso = country['UNSTAT_CountryCode']
        if countryiso:
            countriesdata.append((countryiso, country['DHS_CountryCode']))
    return countriesdata


def get_tags(base_url, downloader, dhscountrycode):
    url = '%stags/%s' % (base_url, dhscountrycode)
    response = downloader.download(url)
    json = response.json()
    return json['Data']


def get_datecoverage(base_url, downloader, dhscountrycode):
    url = '%ssurveys/%s' % (base_url, dhscountrycode)
    response = downloader.download(url)
    json = response.json()
    years = set()
    for survey in json['Data']:
        years.add(survey['SurveyYear'])
    if len(years) == 0:
        return None, None
    years = sorted(list(years))
    return years[0], years[-1]


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


def generate_dataset_and_showcase(base_url, hxlproxy_url, downloader, countrydata, dhstags):
    """
    """
    countryiso, dhscountrycode = countrydata
    countryname = Country.get_country_name_from_iso3(countryiso)
    title = '%s - Demographic and Health Data' % countryname
    logger.info('Creating dataset: %s' % title)
    slugified_name = slugify('DHS Data for %s' % countryname).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('45e7c1a1-196f-40a5-a715-9d6e934a7f70')
    dataset.set_expected_update_frequency('Live')
    dataset.set_subnational(True)
    dataset.add_country_location(countryiso)
    tags = ['HXL', 'health', 'demographics']
    dataset.add_tags(tags)
    earliest_year, latest_year = get_datecoverage(base_url, downloader, dhscountrycode)
    if earliest_year is None:
        logger.warning('No surveys exists for %s!' % countryname)
        return None, None
    dataset.set_dataset_year_range(earliest_year, latest_year)

    # apikey= downloader.session.params['apiKey']
    for dhstag in dhstags:
        # dhs_country_url = '%sdata/%s?tagids=%s&breakdown=national&perpage=10000&apiKey=%s&f=csv' % (base_url, dhscountrycode, dhstag['TagID'], apikey)
        dhs_country_url = '%sdata/%s?tagids=%s&breakdown=national&perpage=10000&f=csv' % (base_url, dhscountrycode, dhstag['TagID'])
        hxl_url = '%s%s' % (hxlate_start, hxlate_filter % countryiso)
        url = '%surl=%s%s' % (hxlproxy_url, quote_plus(dhs_country_url), hxl_url)

        tagname = dhstag['TagName']
        resource = {
            'name': slugify('%s_national' % tagname),
            'description': 'National Data: %s' % tagname,
            'format': 'csv',
            'url': url
        }
        dataset.add_update_resource(resource)

        hxl_url = '%s%s%s%s' % (hxlate_start, hxlate_sn_tag, hxlate_filter, hxlate_sn_filters)
        url = '%surl=%s%s' % (hxlproxy_url, quote_plus(dhs_country_url.replace('breakdown=national', 'breakdown=subnational')), hxl_url)
        resource = {
            'name': slugify('%s_subnational' % tagname),
            'description': 'Subnational Data: %s' % tagname,
            'format': 'csv',
            'url': url
        }
        dataset.add_update_resource(resource)

    dataset.set_quickchart_resource(quickchart_resourceno)

    publication = get_publication(base_url, downloader, dhscountrycode)
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': publication['PublicationTitle'],
        'notes': publication['PublicationDescription'],
        'url': publication['PublicationURL'],
        'image_url': publication['ThumbnailURL']
    })
    showcase.add_tags(tags)
    return dataset, showcase


def generate_resource_view(dataset):
    resourceview = ResourceView({'resource_id': dataset.get_resource(quickchart_resourceno)['id']})
    resourceview.update_from_yaml()
    return resourceview
