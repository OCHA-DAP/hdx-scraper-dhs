#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for DHS

"""
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dictandlist import read_list_from_csv
from hdx.utilities.downloader import DownloadError
from hdx.utilities.path import temp_dir

from dhs import generate_datasets_and_showcase, get_countries, get_tags, get_datecoverage, get_publication


class TestDHS():
    countrydata = {'UNAIDS_CountryCode': 'AFG', 'SubregionName': 'South Asia', 'WHO_CountryCode': 'AF', 'FIPS_CountryCode': 'AF', 'ISO2_CountryCode': 'AF', 'ISO3_CountryCode': 'AFG', 'RegionOrder': 41, 'DHS_CountryCode': 'AF', 'CountryName': 'Afghanistan', 'UNICEF_CountryCode': 'AFG', 'UNSTAT_CountryCode': 'AFG', 'RegionName': 'South & Southeast Asia'}
    country = {'iso3': 'AFG', 'dhscode': 'AF'}
    tags = [{'TagType': 2, 'TagName': 'DHS Quickstats', 'TagID': 0, 'TagOrder': 0}, {'TagType': 2, 'TagName': 'DHS Mobile', 'TagID': 77, 'TagOrder': 1}]
    datecoverage = '2015'
    publications = [{'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/SR186/SR186.pdf', 'PublicationTitle': 'Mortality Survey Key Findings 2009', 'SurveyId': 'AF2009OTH', 'SurveyType': 'OTH', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/SR186.jpg', 'SurveyYear': 2009, 'PublicationSize': 2189233, 'DHS_CountryCode': 'AF', 'PublicationId': 11072, 'PublicationDescription': 'Afghanistan AMS 2009 Summary Report'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/SR186/SR186.pdf', 'PublicationTitle': 'Mortality Survey Key Findings', 'SurveyId': 'AF2010OTH', 'SurveyType': 'OTH', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/SR186.jpg', 'SurveyYear': 2010, 'PublicationSize': 2189233, 'DHS_CountryCode': 'AF', 'PublicationId': 1107, 'PublicationDescription': 'Afghanistan AMS 2010 Summary Report'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/FR248/FR248.pdf', 'PublicationTitle': 'Mortality Survey Final Report', 'SurveyId': 'AF2010OTH', 'SurveyType': 'OTH', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/FR248.jpg', 'SurveyYear': 2010, 'PublicationSize': 3457803, 'DHS_CountryCode': 'AF', 'PublicationId': 1106, 'PublicationDescription': 'Afghanistan Mortality Survey 2010'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/OF35/OF35.C.pdf', 'PublicationTitle': 'Afghanistan DHS 2014 - 8 Regional Fact Sheets', 'SurveyId': 'AF2014DHS', 'SurveyType': 'DHS', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/OF35.jpg', 'SurveyYear': 2014, 'PublicationSize': 926663, 'DHS_CountryCode': 'AF', 'PublicationId': 17482, 'PublicationDescription': 'Afghanistan DHS 2014 - Capital Region Fact Sheet'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/SR236/SR236.pdf', 'PublicationTitle': 'Key Findings', 'SurveyId': 'AF2015DHS', 'SurveyType': 'DHS', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/SR236.jpg', 'SurveyYear': 2015, 'PublicationSize': 3605432, 'DHS_CountryCode': 'AF', 'PublicationId': 1714, 'PublicationDescription': 'Afghanistan DHS 2015 - Key Findings'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/OF35/OF35.C.pdf', 'PublicationTitle': 'Afghanistan DHS 2015 - 8 Regional Fact Sheets', 'SurveyId': 'AF2015DHS', 'SurveyType': 'DHS', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/OF35.jpg', 'SurveyYear': 2015, 'PublicationSize': 926663, 'DHS_CountryCode': 'AF', 'PublicationId': 1748, 'PublicationDescription': 'Afghanistan DHS 2015 - Capital Region Fact Sheet'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/FR248/FR248.pdf', 'PublicationTitle': 'Mortality Survey Final Report2', 'SurveyId': 'AF2010OTH', 'SurveyType': 'OTH', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/FR248.jpg', 'SurveyYear': 2010, 'PublicationSize': 3457803, 'DHS_CountryCode': 'AF', 'PublicationId': 11062, 'PublicationDescription': 'Afghanistan Mortality Survey 2010'},
                    {'PublicationURL': 'https://www.dhsprogram.com/pubs/pdf/FR323/FR323.pdf', 'PublicationTitle': 'Final Report', 'SurveyId': 'AF2015DHS', 'SurveyType': 'DHS', 'ThumbnailURL': 'https://www.dhsprogram.com/publications/images/thumbnails/FR323.jpg', 'SurveyYear': 2015, 'PublicationSize': 10756438, 'DHS_CountryCode': 'AF', 'PublicationId': 1713, 'PublicationDescription': 'Afghanistan Demographic and Health Survey 2015'}]
    dataset = {'name': 'dhs-data-for-afghanistan', 'title': 'Afghanistan - National Demographic and Health Data',
               'notes': 'Contains data from the [DHS data portal](https://api.dhsprogram.com/). There is also a dataset containing [Afghanistan - Subnational Demographic and Health Data](https://feature-data.humdata.org/dataset/dhs-subnational-data-for-afghanistan) on HDX.\n\nThe DHS Program Application Programming Interface (API) provides software developers access to aggregated indicator data from The Demographic and Health Surveys (DHS) Program. The API can be used to create various applications to help analyze, visualize, explore and disseminate data on population, health, HIV, and nutrition from more than 90 countries.',
               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': '45e7c1a1-196f-40a5-a715-9d6e934a7f70',
               'data_update_frequency': '365', 'subnational': '0', 'groups': [{'name': 'afg'}],
               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
               'dataset_date': '01/01/2015-12/31/2015'}
    resources = [{'name': 'DHS Quickstats Data for Afghanistan', 'description': 'HXLated csv containing DHS Quickstats data', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                 {'name': 'DHS Mobile Data for Afghanistan', 'description': 'HXLated csv containing DHS Mobile data', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]
    subdataset = {'name': 'dhs-subnational-data-for-afghanistan', 'title': 'Afghanistan - Subnational Demographic and Health Data',
               'notes': 'Contains data from the [DHS data portal](https://api.dhsprogram.com/). There is also a dataset containing [Afghanistan - National Demographic and Health Data](https://feature-data.humdata.org/dataset/dhs-data-for-afghanistan) on HDX.\n\nThe DHS Program Application Programming Interface (API) provides software developers access to aggregated indicator data from The Demographic and Health Surveys (DHS) Program. The API can be used to create various applications to help analyze, visualize, explore and disseminate data on population, health, HIV, and nutrition from more than 90 countries.',
               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': '45e7c1a1-196f-40a5-a715-9d6e934a7f70',
               'data_update_frequency': '365', 'subnational': '1', 'groups': [{'name': 'afg'}],
               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
               'dataset_date': '01/01/2015-12/31/2015'}
    subresources = [{'name': 'DHS Quickstats Data for Afghanistan', 'description': 'HXLated csv containing DHS Quickstats data', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'cmr', 'title': 'Cameroon'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'health'}, {'name': 'demographics'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}
        return Configuration.read()

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://haha/countries':
                    def fn():
                        return {'Data': [TestDHS.countrydata]}
                    response.json = fn
                elif url == 'http://haha/tags/AF':
                    def fn():
                        return {'Data': TestDHS.tags}
                    response.json = fn
                elif url == 'http://haha/surveys/AF':
                    def fn():
                        return {'Data': [{'SurveyYear': TestDHS.datecoverage}]}
                    response.json = fn
                elif url == 'http://haha/publications/AF':
                    def fn():
                        return {'Data': TestDHS.publications}
                    response.json = fn
                return response

            @staticmethod
            def get_tabular_rows(url, format):
                if url == 'http://haha/data/AF?tagids=0&breakdown=national&perpage=10000&f=csv':
                    file = 'afg0national.csv'
                elif url == 'http://haha/data/AF?tagids=0&breakdown=subnational&perpage=10000&f=csv':
                    file = 'afg0subnational.csv'
                elif url == 'http://haha/data/AF?tagids=77&breakdown=national&perpage=10000&f=csv':
                    file = 'afg77national.csv'
                elif url == 'http://haha/data/AF?tagids=77&breakdown=subnational&perpage=10000&f=csv':
                    ex = DownloadError()
                    ex.__cause__ = ValueError('Variable RET is undefined')
                    raise ex
                return (x for x in read_list_from_csv(join('tests', 'fixtures', file)))

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countries('http://haha/', downloader)
        assert countriesdata == [TestDHS.country]

    def test_get_tags(self, downloader):
        tags = get_tags('http://haha/', downloader, 'AF')
        assert tags == TestDHS.tags

    def test_get_datecoverage(self, downloader):
        datecoverage = get_datecoverage('http://haha/', downloader, 'AF')
        assert datecoverage == (TestDHS.datecoverage, TestDHS.datecoverage)

    def test_get_publication(self, downloader):
        publication = get_publication('http://haha/', downloader, 'AF')
        assert publication == TestDHS.publications[-1]

    def test_generate_datasets_and_showcase(self, configuration, downloader):
        with temp_dir('DHS') as folder:
            dataset, subdataset, showcase = generate_datasets_and_showcase(configuration, 'http://haha/', downloader, folder, TestDHS.country, TestDHS.tags)
            assert dataset == TestDHS.dataset
            resources = dataset.get_resources()
            assert resources == TestDHS.resources
            assert subdataset == TestDHS.subdataset
            assert subdataset.get_resources() == TestDHS.subresources

            assert showcase == {'name': 'dhs-data-for-afghanistan-showcase', 'title': 'Final Report', 'notes': 'Afghanistan Demographic and Health Survey 2015',
                                'url': 'https://www.dhsprogram.com/pubs/pdf/FR323/FR323.pdf', 'image_url': 'https://www.dhsprogram.com/publications/images/thumbnails/FR323.jpg',
                                'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

            file = 'DHS Quickstats_national_AFG.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
            file = 'DHS Mobile_national_AFG.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
            file = 'DHS Quickstats_subnational_AFG.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
