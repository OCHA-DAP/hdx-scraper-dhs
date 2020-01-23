### Collector for DHS's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-scraper-dhs.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdx-scraper-dhs) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-dhs/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-dhs?branch=master)

This script connects to the [DHS API](http://api.dhsprogram.com/#/api-data.cfm) and extracts data country by country creating two datasets per country in HDX (national and subnational). The scraper takes around 10 hours to run. It makes in the order of 200 reads from DHS and 1000 read/writes (API calls) to HDX in total. It creates around 7000 temporary files of at most 1Mb in size and uploads them into HDX. It will be run monthly. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-dhs** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY