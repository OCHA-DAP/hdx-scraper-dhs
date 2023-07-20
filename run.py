#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.utilities.retriever import Retrieve

from dhs import (
    generate_datasets_and_showcase,
    generate_resource_view,
    get_countries,
    get_tags,
)
from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir, \
    wheretostart_tempdir_batch
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-dhs"


def createdataset(dataset, info):
    dataset.update_from_yaml()
    dataset["license_other"] = dataset["license_other"].replace(
        "\n", "  \n"
    )  # ensure markdown has line breaks
    dataset.create_in_hdx(
        remove_additional_resources=True,
        hxl_update=False,
        updated_by_script=lookup,
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
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yml"),
            extra_params_lookup=lookup,
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
                    resource_view = generate_resource_view(
                        dataset, bites_disabled=bites_disabled["national"]
                    )
                    resource_view.create_in_hdx()
                    if showcase:
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
                if subdataset:
                    createdataset(subdataset, info)
                    if showcase:
                        showcase.add_dataset(subdataset)
                    resource_view = generate_resource_view(
                        subdataset, bites_disabled=bites_disabled["subnational"]
                    )
                    resource_view.create_in_hdx()


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
