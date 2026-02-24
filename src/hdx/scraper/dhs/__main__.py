#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_tempdir,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve
from requests.adapters import HTTPAdapter

from hdx.scraper.dhs.pipeline import (
    generate_datasets_and_showcase,
    get_countries,
    get_tags,
)

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-dhs"


def createdataset(dataset, info):
    dataset.update_from_yaml(
        path=script_dir_plus_file(
            join("config", "hdx_dataset_static.yaml"),
            main,
        )
    )
    dataset["license_other"] = dataset["license_other"].replace(
        "\n", "  \n"
    )  # ensure markdown has line breaks
    dataset.create_in_hdx(
        remove_additional_resources=True,
        updated_by_script=_LOOKUP,
        batch=info["batch"],
    )


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    configuration = Configuration.read()
    base_url = configuration["base_url"]
    with wheretostart_tempdir_batch(_LOOKUP) as info:
        folder = info["folder"]
        dhs_key = getenv("APIKEY")
        if dhs_key:
            extra_params_dict = {"apiKey": dhs_key}
        else:
            extra_params_dict = None
        with Download(
            extra_params_dict=extra_params_dict,
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=_LOOKUP,
        ) as downloader:
            downloader.session.mount(
                "http://",
                HTTPAdapter(max_retries=1, pool_connections=100, pool_maxsize=100),
            )
            downloader.session.mount(
                "https://",
                HTTPAdapter(max_retries=1, pool_connections=100, pool_maxsize=100),
            )
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            countries = get_countries(base_url, retriever)
            logger.info(f"Number of countries: {len(countries)}")

            for info, country in progress_storing_tempdir("DHS", countries, "iso3"):
                tags = get_tags(base_url, retriever, country["dhscode"])
                (
                    dataset,
                    subdataset,
                    showcase,
                    bites_disabled,
                ) = generate_datasets_and_showcase(
                    configuration, base_url, retriever, info["folder"], country, tags
                )
                if dataset:
                    createdataset(dataset, info)
                    if showcase:
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
                if subdataset:
                    createdataset(subdataset, info)
                    if showcase:
                        showcase.add_dataset(subdataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
