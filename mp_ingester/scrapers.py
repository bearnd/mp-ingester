import io
from lxml import html as lxml_html
from typing import List, Dict, Union

import requests

from mp_ingester.loggers import create_logger
from mp_ingester.excs import MedlinePlusHttpRequestGetError


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

    def __init__(self, medline_url: str, **kwargs):
        """ Constructor and initialization.

        Args:
            medline_url (str): The URL of the MedlinePlus page that will be
                scraped.
        """

        super(ScraperMedlineBase, self).__init__(**kwargs)

        self.medline_url = medline_url

    def fetch_page(self) -> requests.Response:
        """ Fetches the URL defined under `self.medline_url` and returns the
            response.

        Returns:
            requests.Response: The response retrieved when fetching
                `self.medline_url`.
        """

        self.logger.info(
            f"Retrieving HTML content under URL {self.medline_url}."
        )

        response = requests.get(url=self.medline_url)

        if not response.ok:
            msg = (
                f"Could not retrieve HTML content under URL "
                f"{self.medline_url}. A response with status code"
                f"of {response.status_code} and content of '{response.content}'"
                f"was received."
            )
            self.logger.error(msg)
            raise (MedlinePlusHttpRequestGetError(msg))

        return response


class ScraperHealthTopicGroupClasses(ScraperMedlineBase):
    """ Class to scrape the MedlinePlus health-topic page and retrieve the
        health-topic group classes and health-topic groups under them.
    """

    def __init__(self, medline_health_topics_url: str, **kwargs):
        """Constructor and initialization.

        Args:
            medline_health_topics_url (str): The URL of the MedlinePlus health
                topics.
        """

        super(ScraperHealthTopicGroupClasses, self).__init__(
            medline_url=medline_health_topics_url, **kwargs
        )

        self.medline_health_topics_url = medline_health_topics_url

    def scrape(self) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """ Scrapes the MedlinePlus health topics page and retrieves the
            MedlinePlus health-topic groups categorized by their assigned class.

        Returns:
            List[Dict[str, Union[str, List[Dict[str, str]]]]]: The scraped data.
        """

        results = []

        response = self.fetch_page()

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

    def __init__(self, medline_health_topic_group_url: str, **kwargs):
        """Constructor and initialization.

        Args:
            medline_health_topics_url (str): The URL of the MedlinePlus health
                topics.
        """

        super(ScraperHealthTopicGroupBodyParts, self).__init__(
            medline_url=medline_health_topic_group_url, **kwargs
        )

        self.medline_health_topic_group_url = medline_health_topic_group_url

    def scrape(self) -> List[Dict[str, Union[str, List[Dict[str, str]]]]]:
        """ Scrapes the MedlinePlus health topics page and retrieves the
            MedlinePlus health-topic groups categorized by their assigned class.

        Returns:
            List[Dict[str, Union[str, List[Dict[str, str]]]]]: The scraped data.
        """

        results = []

        response = self.fetch_page()

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