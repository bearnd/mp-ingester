import io
from lxml import html as lxml_html
from typing import List, Dict, Union

import requests_async as requests

from mp_ingester.loggers import create_logger
from mp_ingester.excs import MedlinePlusHttpRequestGetError


TypeHealthTopicGroupClasses = List[Dict[str, Union[str, List[Dict[str, str]]]]]
TypeHealthTopicBodyParts = List[Dict[str, Union[str, List[Dict[str, str]]]]]


class ScraperBase:
    """ Scraper base-class."""

    def __init__(self, **kwargs):
        """ Constructor and initialization."""

        self.logger = create_logger(
            logger_name=type(self).__name__,
            logger_level=kwargs.get("logger_level", "DEBUG"),
        )


class ScraperMedlineBase(ScraperBase):
    """ MedlinePlus scraper base-class."""

    async def fetch_page(self, url: str) -> requests.Response:
        """ Fetches a given URL and returns the response.

        Args:
            url (str): The URL to fetch.

        Returns:
            requests.Response: The response retrieved when fetching the given
                url.
        """

        self.logger.info(f"Retrieving HTML content under URL {url}.")

        response = await requests.get(url=url)

        if not response.ok:
            msg = (
                f"Could not retrieve HTML content under URL {url}. A response "
                f"with status code of {response.status_code} and content of "
                f"'{response.content}' was received."
            )
            self.logger.error(msg)
            raise (MedlinePlusHttpRequestGetError(msg))

        return response


class ScraperHealthTopicGroupClasses(ScraperMedlineBase):
    """ Class to scrape the MedlinePlus health-topic page and retrieve the
        health-topic group classes and health-topic groups under them.
    """

    async def scrape(
        self, medline_health_topics_url: str
    ) -> TypeHealthTopicGroupClasses:
        """ Scrapes the MedlinePlus health topics page and retrieves the
            MedlinePlus health-topic groups categorized by their assigned class.

        Args:
            medline_health_topics_url (str): The URL of the MedlinePlus health
                topics.

        Returns:
            TypeHealthTopicGroupClasses: The scraped data.
        """

        results = []

        response = await self.fetch_page(url=medline_health_topics_url)

        # Parse the HTML source with the XML parser.
        doc = lxml_html.parse(io.StringIO(response.content.decode("utf-8")))

        # Retrieve the different XML elements with a CSS class of `section`.
        elements_sections = doc.xpath(
            "//article//div[contains(@class, 'col-')]//div[@class='section']"
        )

        for elements_section in elements_sections:
            # Retrieve the name of the health-topic group class.
            health_topic_class_name = elements_section.xpath(
                "div[@class='section-header']/div[@class='section-title']"
                "//h2/text()"
            )[0]

            # Retrieve the health-topic group links under each group class.
            elements_groups = elements_section.xpath(
                "div[@class='section-body']/ul/li/a"
            )

            results.append(
                {
                    "name": health_topic_class_name,
                    "health_topic_groups": [
                        {
                            "name": elements_group.text,
                            "url": elements_group.attrib["href"],
                        }
                        for elements_group in elements_groups
                    ],
                }
            )

        return results


class ScraperHealthTopicGroupBodyParts(ScraperMedlineBase):
    """ Class to scrape a MedlinePlus health-topic group page and retrieve the
        health-topic group body parts and health-topics under them.
    """

    async def scrape(
        self, medline_health_topic_group_url: str
    ) -> TypeHealthTopicBodyParts:
        """ Scrapes the MedlinePlus health topics page and retrieves the
            MedlinePlus health-topic groups categorized by their assigned class.

        Args:
            medline_health_topic_group_url (str): The URL of the MedlinePlus
                health topics.

        Returns:
            TypeHealthTopicBodyParts: The scraped data.
        """

        results = []

        response = await self.fetch_page(url=medline_health_topic_group_url)

        # Parse the HTML source with the XML parser.
        doc = lxml_html.parse(io.StringIO(response.content.decode("utf-8")))

        # Retrieve the body-part elements.
        elements_body_parts = doc.xpath(
            "//div[@class='tp_rdbox_bborder']/div/ul/li/a"
        )

        for element_body_part in elements_body_parts:
            # Retrieve the body part ID for this element.
            element_body_part_id = element_body_part.attrib["id"].replace(
                "_menu", ""
            )

            # Retrieve the links to health-topics under the given body-part.
            elements_health_topics = doc.xpath(
                f"//div[@id='{element_body_part_id}']"
                f"/div[@class='tp_rdbox_bborder']"
                f"/div/div/ul/li/a"
            )

            results.append(
                {
                    "group_url": medline_health_topic_group_url,
                    "name": element_body_part.text,
                    "health_topics": [
                        {
                            "name": element_health_topic.text,
                            "url": element_health_topic.attrib["href"],
                        }
                        for element_health_topic in elements_health_topics
                    ],
                }
            )

        return results


class ScraperMedlineFiles(ScraperMedlineBase):
    """ Class to scrape the MedlinePlus XML Files page and retrieve the links
        to the latest XML files.
    """

    async def scrape(self, medline_xml_files_url: str) -> Dict[str, str]:
        """ Scrapes the MedlinePlus XML Files page and retrieve the links to the
            latest XML files.

        Args:
            medline_xml_files_url (str): The URL of the MedlinePlus XML Files
                page

        Returns:
            Dict[str, str]: The scraped data.
        """

        response = await self.fetch_page(url=medline_xml_files_url)

        # Parse the HTML source with the XML parser.
        doc = lxml_html.parse(io.StringIO(response.content.decode("utf-8")))

        # Retrieve the first `p` section containing the XML file links which
        # should be the links to the latest files.
        elements_latest_section = doc.xpath(
            "//h3[contains(text(),'Files generated on')][1]"
            "/following-sibling::p[1]"
        )[0]

        element_topics = elements_latest_section.xpath(
            "a[contains(text(), 'MedlinePlus Health Topic XML')][1]"
        )[0]

        element_topics_zip = elements_latest_section.xpath(
            "a[contains(text(), 'MedlinePlus Compressed Health Topic XML')][1]"
        )[0]

        element_groups = elements_latest_section.xpath(
            "a[contains(text(), 'MedlinePlus Health Topic Group XML')][1]"
        )[0]

        result = {
            "health_topic_xml": element_topics.attrib["href"],
            "health_topic_compressed_xml": element_topics_zip.attrib["href"],
            "health_topic_group_xml": element_groups.attrib["href"],
        }

        return result
