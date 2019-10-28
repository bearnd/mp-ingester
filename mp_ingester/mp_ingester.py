#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Main module."""

import os
import argparse
import asyncio
from typing import Dict

import wget
from fform.dals_mp import DalMedline

from mp_ingester.parsers import ParserXmlMedlineGroups
from mp_ingester.parsers import ParserXmlMedlineHealthTopic
from mp_ingester.scrapers import ScraperHealthTopicGroupClasses
from mp_ingester.scrapers import ScraperHealthTopicGroupBodyParts
from mp_ingester.scrapers import ScraperMedlineFiles
from mp_ingester.scrapers import TypeHealthTopicGroupClasses
from mp_ingester.scrapers import TypeHealthTopicBodyParts
from mp_ingester.ingesters import IngesterMedlineGroupClasses
from mp_ingester.ingesters import IngesterMedlineBodyParts
from mp_ingester.ingesters import IngesterMedlineGroups
from mp_ingester.ingesters import IngesterMedlineHealthTopics
from mp_ingester.config import import_config
from mp_ingester.sentry import initialize_sentry


def load_config(args):
    if args.config_file:
        cfg = import_config(fname_config_file=args.config_file)
    elif "MP_INGESTER_CONFIG" in os.environ:
        fname_config_file = os.environ["MP_INGESTER_CONFIG"]
        cfg = import_config(fname_config_file=fname_config_file)
    else:
        msg_fmt = "Configuration file path not defined."
        raise ValueError(msg_fmt)

    return cfg


async def _scrape_health_topic_group_classes(
    medline_health_topics_url: str
) -> TypeHealthTopicGroupClasses:
    # Scrape MedlinePlus for the health-topic group classes.
    scraper_classes = ScraperHealthTopicGroupClasses()
    health_topic_group_classes = await scraper_classes.scrape(
        medline_health_topics_url=medline_health_topics_url
    )

    return health_topic_group_classes


async def _scrape_health_topic_body_parts(
    health_topic_group_classes: TypeHealthTopicGroupClasses,
) -> TypeHealthTopicBodyParts:
    tasks = []
    scraper_body_parts = ScraperHealthTopicGroupBodyParts()
    # Scrape MedlinePlus for the health-topic body-parts.
    for health_topic_group_class in health_topic_group_classes:
        health_topic_groups = health_topic_group_class["health_topic_groups"]
        for health_topic_group in health_topic_groups:

            tasks.append(
                scraper_body_parts.scrape(
                    medline_health_topic_group_url=health_topic_group["url"]
                )
            )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    health_topic_body_parts = []
    for result in results:
        if isinstance(result, Exception):
            raise result
        health_topic_body_parts.extend(result)

    return health_topic_body_parts


async def _scrape_medline_files(medline_xml_files_url: str) -> Dict[str, str]:
    # Scrape MedlinePlus for the links to the latest XML files.
    scraper_classes = ScraperMedlineFiles()
    medline_xml_files = await scraper_classes.scrape(
        medline_xml_files_url=medline_xml_files_url
    )

    return medline_xml_files


async def _download_file_groups(medline_xml_files_url: str) -> str:
    filename_output = "/tmp/mplus_topic_groups.xml"
    medline_xml_files = await _scrape_medline_files(
        medline_xml_files_url=medline_xml_files_url
    )

    wget.download(
        url=medline_xml_files["health_topic_group_xml"], out=filename_output
    )

    return filename_output


async def _download_file_topic(medline_xml_files_url: str) -> str:
    filename_output = "/tmp/mplus_topics.xml"
    medline_xml_files = await _scrape_medline_files(
        medline_xml_files_url=medline_xml_files_url
    )

    wget.download(
        url=medline_xml_files["health_topic_xml"], out=filename_output
    )

    return filename_output


async def main(args):
    cfg = load_config(args=args)

    # Initialize the Sentry agent.
    initialize_sentry(cfg=cfg)

    dal = DalMedline(
        sql_username=cfg.sql_username,
        sql_password=cfg.sql_password,
        sql_host=cfg.sql_host,
        sql_port=cfg.sql_port,
        sql_db=cfg.sql_db,
    )

    if arguments.mode == "groups":
        # Scrape MedlinePlus for the health-topic group classes.
        health_topic_group_classes = await _scrape_health_topic_group_classes(
            medline_health_topics_url=cfg.medline.health_topics_url
        )

        # Ingest the MedlinePlus health-topic group classes.
        ingester_classes = IngesterMedlineGroupClasses(dal=dal)
        for health_topic_group_class in health_topic_group_classes:
            ingester_classes.ingest(name=health_topic_group_class["name"])

        # If the filename of the XML file has not been specified then download
        # the file from the website.
        if not arguments.filename:
            filename_xml = await _download_file_groups(
                medline_xml_files_url=cfg.medline.xml_files_url
            )
        else:
            filename_xml = arguments.filename

        # Parse the MedlinePlus health-topic group file.
        parser_groups = ParserXmlMedlineGroups()
        groups = parser_groups.parse(filename_xml=filename_xml)

        # Ingest the MedlinePlus health-topic groups.
        ingester_groups = IngesterMedlineGroups(
            dal=dal, health_topic_group_classes=health_topic_group_classes
        )
        for group in groups:
            ingester_groups.ingest(document=group)
    elif arguments.mode == "topics":
        # Scrape MedlinePlus for the health-topic group classes.
        health_topic_group_classes = await _scrape_health_topic_group_classes(
            medline_health_topics_url=cfg.medline.health_topics_url
        )

        # Scrape MedlinePlus for the health-topic body-parts.
        health_topic_body_parts = await _scrape_health_topic_body_parts(
            health_topic_group_classes=health_topic_group_classes
        )

        # Ingest the MedlinePlus body-parts.
        ingester_body_parts = IngesterMedlineBodyParts(dal=dal)
        for health_topic_body_part in health_topic_body_parts:
            ingester_body_parts.ingest(
                name=health_topic_body_part["name"],
                health_topic_group_url=health_topic_body_part["group_url"],
            )

        # If the filename of the XML file has not been specified then download
        # the file from the website.
        if not arguments.filename:
            filename_xml = await _download_file_topic(
                medline_xml_files_url=cfg.medline.xml_files_url
            )
        else:
            filename_xml = arguments.filename

        # Parse the MedlinePlus health-topic file.
        parser_topics = ParserXmlMedlineHealthTopic()
        topics = parser_topics.parse(filename_xml=filename_xml)

        # Ingest the MedlinePlus health-topics.
        ingester_topics = IngesterMedlineHealthTopics(
            dal=dal, health_topic_body_parts=health_topic_body_parts
        )
        # Ingest without ingesting links to related topics.
        for topic in topics:
            ingester_topics.ingest(document=topic, do_ingest_links=False)

        # Re-ingest, this time including links to related topics.
        topics = parser_topics.parse(filename_xml=filename_xml)
        for topic in topics:
            ingester_topics.ingest(document=topic, do_ingest_links=True)


# main sentinel
if __name__ == "__main__":

    argument_parser = argparse.ArgumentParser(
        description=(
            "mp-ingester: MedlinePlus.gov XML dump parser and SQL " "ingester."
        )
    )
    argument_parser.add_argument(
        "--filename", help="MedlinePlus.gov XML file to ingest.", required=False
    )
    argument_parser.add_argument(
        "--mode",
        dest="mode",
        help="Ingestion mode",
        choices=["groups", "topics"],
        required=True,
    )
    argument_parser.add_argument(
        "--config-file",
        dest="config_file",
        help="configuration file",
        required=False,
    )
    arguments = argument_parser.parse_args()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args=arguments))
    loop.close()
