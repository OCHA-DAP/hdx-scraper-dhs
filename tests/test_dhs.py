#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for DHS

"""
import copy
import datetime
from os.path import join

import pytest
from hdx.data.dataset import Dataset
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country

from dhs import generate_dataset_and_showcase, get_countriesdata, generate_resource_view, get_tags, get_datecoverage, \
    get_publication


class TestDHS():
    countrydata = {'UNAIDS_CountryCode': 'AFG', 'SubregionName': 'South Asia', 'WHO_CountryCode': 'AF', 'FIPS_CountryCode': 'AF', 'ISO2_CountryCode': 'AF', 'ISO3_CountryCode': 'AFG', 'RegionOrder': 41, 'DHS_CountryCode': 'AF', 'CountryName': 'Afghanistan', 'UNICEF_CountryCode': 'AFG', 'UNSTAT_CountryCode': 'AFG', 'RegionName': 'South & Southeast Asia'}
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
    dataset = {'name': 'dhs-data-for-afghanistan', 'title': 'Afghanistan - Demographic and Health Data',
               'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'owner_org': '45e7c1a1-196f-40a5-a715-9d6e934a7f70',
               'data_update_frequency': '0', 'subnational': '1', 'groups': [{'name': 'afg'}],
               'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'},
                        {'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
               'dataset_date': '01/01/2015-12/31/2015', 'dataset_preview': 'resource_id'}
    resources = [{'name': 'dhs-quickstats-national', 'description': 'National Data: DHS Quickstats', 'format': 'csv', 'url': 'https://data.humdata.org/hxlproxy/data.csv?url=http%3A%2F%2Fhaha%2Fdata%2FAF%3Ftagids%3D0%26breakdown%3Dnational%26perpage%3D10000%26f%3Dcsv&name=DHSHXL&header-row=1&tagger-match-all=on&tagger-01-header=dataid&tagger-01-tag=%23meta%2Bid&tagger-02-header=indicator&tagger-02-tag=%23indicator%2Bname&tagger-03-header=value&tagger-03-tag=%23indicator%2Bnum&tagger-04-header=precision&tagger-04-tag=%23indicator%2Bprecision&tagger-06-header=countryname&tagger-06-tag=%23country%2Bname&tagger-07-header=surveyyear&tagger-07-tag=%23date%2Byear&tagger-08-header=surveyid&tagger-08-tag=%23survey%2Bid&tagger-09-header=indicatorid&tagger-09-tag=%23indicator%2Bid&filter01=add&add-tag01=%23country%2Bcode&add-value01=AFG&add-header01=ISO3&add-before01=on', 'resource_type': 'api', 'url_type': 'api', 'dataset_preview_enabled': 'False'},
                 {'name': 'dhs-quickstats-subnational', 'description': 'Subnational Data: DHS Quickstats', 'format': 'csv', 'url': 'https://data.humdata.org/hxlproxy/data.csv?url=http%3A%2F%2Fhaha%2Fdata%2FAF%3Ftagids%3D0%26breakdown%3Dsubnational%26perpage%3D10000%26f%3Dcsv&name=DHSHXL&header-row=1&tagger-match-all=on&tagger-01-header=dataid&tagger-01-tag=%23meta%2Bid&tagger-02-header=indicator&tagger-02-tag=%23indicator%2Bname&tagger-03-header=value&tagger-03-tag=%23indicator%2Bnum&tagger-04-header=precision&tagger-04-tag=%23indicator%2Bprecision&tagger-06-header=countryname&tagger-06-tag=%23country%2Bname&tagger-07-header=surveyyear&tagger-07-tag=%23date%2Byear&tagger-08-header=surveyid&tagger-08-tag=%23survey%2Bid&tagger-09-header=indicatorid&tagger-09-tag=%23indicator%2Bid&tagger-10-header=CharacteristicLabel&tagger-10-tag=%23meta%2Bcharacteristic&filter01=add&add-tag01=%23loc%2Bname&add-value01=%7B%7B%23meta%2Bcharacteristic%7D%7D&add-header01=Location&add-before01=on&filter02=add&add-tag02=%23country%2Bcode&add-value02=AFG&add-header02=ISO3&add-before02=on&filter03=replace&replace-pattern03=%5C.%5C.%28.%2A%29&replace-regex03=on&replace-value03=%5C1&replace-tags03=%23loc%2Bname&replace-where03=%23loc%2Bname~%5C.%5C..%2A', 'resource_type': 'api', 'url_type': 'api', 'dataset_preview_enabled': 'True'},
                 {'name': 'dhs-mobile-national', 'description': 'National Data: DHS Mobile', 'format': 'csv', 'url': 'https://data.humdata.org/hxlproxy/data.csv?url=http%3A%2F%2Fhaha%2Fdata%2FAF%3Ftagids%3D77%26breakdown%3Dnational%26perpage%3D10000%26f%3Dcsv&name=DHSHXL&header-row=1&tagger-match-all=on&tagger-01-header=dataid&tagger-01-tag=%23meta%2Bid&tagger-02-header=indicator&tagger-02-tag=%23indicator%2Bname&tagger-03-header=value&tagger-03-tag=%23indicator%2Bnum&tagger-04-header=precision&tagger-04-tag=%23indicator%2Bprecision&tagger-06-header=countryname&tagger-06-tag=%23country%2Bname&tagger-07-header=surveyyear&tagger-07-tag=%23date%2Byear&tagger-08-header=surveyid&tagger-08-tag=%23survey%2Bid&tagger-09-header=indicatorid&tagger-09-tag=%23indicator%2Bid&filter01=add&add-tag01=%23country%2Bcode&add-value01=AFG&add-header01=ISO3&add-before01=on', 'resource_type': 'api', 'url_type': 'api', 'dataset_preview_enabled': 'False'},
                 {'name': 'dhs-mobile-subnational', 'description': 'Subnational Data: DHS Mobile', 'format': 'csv', 'url': 'https://data.humdata.org/hxlproxy/data.csv?url=http%3A%2F%2Fhaha%2Fdata%2FAF%3Ftagids%3D77%26breakdown%3Dsubnational%26perpage%3D10000%26f%3Dcsv&name=DHSHXL&header-row=1&tagger-match-all=on&tagger-01-header=dataid&tagger-01-tag=%23meta%2Bid&tagger-02-header=indicator&tagger-02-tag=%23indicator%2Bname&tagger-03-header=value&tagger-03-tag=%23indicator%2Bnum&tagger-04-header=precision&tagger-04-tag=%23indicator%2Bprecision&tagger-06-header=countryname&tagger-06-tag=%23country%2Bname&tagger-07-header=surveyyear&tagger-07-tag=%23date%2Byear&tagger-08-header=surveyid&tagger-08-tag=%23survey%2Bid&tagger-09-header=indicatorid&tagger-09-tag=%23indicator%2Bid&tagger-10-header=CharacteristicLabel&tagger-10-tag=%23meta%2Bcharacteristic&filter01=add&add-tag01=%23loc%2Bname&add-value01=%7B%7B%23meta%2Bcharacteristic%7D%7D&add-header01=Location&add-before01=on&filter02=add&add-tag02=%23country%2Bcode&add-value02=AFG&add-header02=ISO3&add-before02=on&filter03=replace&replace-pattern03=%5C.%5C.%28.%2A%29&replace-regex03=on&replace-value03=%5C1&replace-tags03=%23loc%2Bname&replace-where03=%23loc%2Bname~%5C.%5C..%2A', 'resource_type': 'api', 'url_type': 'api', 'dataset_preview_enabled': 'False'}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(user_agent='test', hdx_key='12345',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}, {'name': 'cmr', 'title': 'Cameroon'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'health'}, {'name': 'demographics'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

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

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://haha/', downloader)
        assert countriesdata == [('AFG', 'AF')]

    def test_get_tags(self, downloader):
        tags = get_tags('http://haha/', downloader, 'AF')
        assert tags == TestDHS.tags

    def test_get_datecoverage(self, downloader):
        datecoverage = get_datecoverage('http://haha/', downloader, 'AF')
        assert datecoverage == (TestDHS.datecoverage, TestDHS.datecoverage)

    def test_get_publication(self, downloader):
        publication = get_publication('http://haha/', downloader, 'AF')
        assert publication == TestDHS.publications[-1]

    def test_generate_dataset_and_showcase(self, configuration, downloader):
        hxlproxy_url = Configuration.read()['hxlproxy_url']
        dataset, showcase = generate_dataset_and_showcase('http://haha/', hxlproxy_url, downloader, ('AFG', 'AF'), TestDHS.tags)
        assert dataset == TestDHS.dataset

        resources = dataset.get_resources()
        assert resources == TestDHS.resources

        assert showcase == {'name': 'dhs-data-for-afghanistan-showcase', 'title': 'Final Report', 'notes': 'Afghanistan Demographic and Health Survey 2015',
                            'url': 'https://www.dhsprogram.com/pubs/pdf/FR323/FR323.pdf', 'image_url': 'https://www.dhsprogram.com/publications/images/thumbnails/FR323.jpg',
                            'tags': [{'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'health', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

    def test_generate_resource_view(self):
        dataset = Dataset(TestDHS.dataset)
        resources = copy.deepcopy(TestDHS.resources)
        resources[1]['id'] = '123'
        dataset.add_update_resources(resources)
        result = generate_resource_view(dataset)
        assert result == {'resource_id': '123', 'description': '', 'title': 'Quick Charts', 'view_type': 'hdx_hxl_preview',
                          'hxl_preview_config': '{"configVersion":5,"bites":[{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":"#loc+name","valueColumn":"#indicator+num","aggregateFunction":"sum","dateColumn":null,"comparisonValueColumn":null,"comparisonOperator":null,"filters":{"filterWith":[{"#date+year":"$MAX$"},{"#indicator+id":"HC_ELEC_H_ELC"}]},"title":"Households with Electricity","description":""},"type":"chart","errorMsg":null,"computedProperties":{"explainedFiltersMap":{},"pieChart":false,"dataTitle":"Value"},"uiProperties":{"swapAxis":true,"showGrid":false,"color":"#1ebfb3","sortingByValue1":"ASC","sortingByCategory1":null,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"dataTitle":"Percent"},"dataProperties":{},"displayCategory":"Charts","hashCode":1254518807},{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":"#loc+name","valueColumn":"#indicator+num","aggregateFunction":"sum","dateColumn":null,"comparisonValueColumn":null,"comparisonOperator":null,"filters":{"filterWith":[{"#date+year":"$MAX$"},{"#indicator+id":"CM_ECMR_C_IMR"}]},"title":"Infant Mortality Rate","description":"Rate is for the period of 10 years preceding the survey"},"type":"chart","errorMsg":null,"computedProperties":{"explainedFiltersMap":{},"pieChart":false,"dataTitle":"Value"},"uiProperties":{"swapAxis":true,"showGrid":false,"color":"#1ebfb3","sortingByValue1":"DESC","sortingByCategory1":null,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"dataTitle":"Rate"},"dataProperties":{},"displayCategory":"Charts","hashCode":-1269336625},{"tempShowSaveCancelButtons":false,"ingredient":{"aggregateColumn":"#loc+name","valueColumn":"#indicator+num","aggregateFunction":"sum","dateColumn":null,"comparisonValueColumn":null,"comparisonOperator":null,"filters":{"filterWith":[{"#date+year":"$MAX$"},{"#indicator+id":"ED_LITR_W_LIT"}]},"title":"Women who are Literate","description":""},"type":"chart","errorMsg":null,"computedProperties":{"explainedFiltersMap":{},"pieChart":false,"dataTitle":"Value"},"uiProperties":{"swapAxis":true,"showGrid":false,"color":"#1ebfb3","sortingByValue1":"ASC","sortingByCategory1":null,"internalColorPattern":["#1ebfb3","#0077ce","#f2645a","#9C27B0"],"dataTitle":"Percent"},"dataProperties":{},"displayCategory":"Charts","hashCode":956040626}],"recipeUrl":"https://raw.githubusercontent.com/mcarans/hxl-recipes/dev/recipes/dhs/recipe.json"}'}
