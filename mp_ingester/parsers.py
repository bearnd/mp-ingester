# -*- coding: utf-8 -*-

"""
This module defines XML parser classes meant to parse MedlinePlus XML files
available under https://medlineplus.gov/xml.html and return Python dictionaries
containing the retrieved data.
"""

import abc
import datetime
import gzip
from typing import List, Union, Dict, Optional, Iterable, BinaryIO, Any

from lxml import etree

from mp_ingester.loggers import create_logger


class ParserXmlBase(object):
    """ XML parser base-class"""

    def __init__(self, **kwargs):
        """ Constructor."""

        self.logger = create_logger(
            logger_name=type(self).__name__,
            logger_level=kwargs.get("logger_level", "DEBUG"),
        )

    @staticmethod
    def _et(element: etree.Element) -> Union[str, None]:
        """ Extracts the text out of an XML element.

        Args:
            element (etree.Element): The XML element to extract the text form.

        Returns:
            Union[str, None]: The element's text or `None` if undefined or
                empty.
        """

        text = None
        if element is not None:
            text = element.text

        text = None if not text else text.strip()

        return text

    @staticmethod
    def _eav(element: etree.Element, attribute: str) -> Union[str, None]:
        """ Extracts the value of an XML element attribute.

        Args:
            element (etree.Element): The XML element to extract the attribute
                value from.
            attribute (str): The name of the attribute to extract.

        Returns:
            Union[str, None]: The element's text or `None` if undefined or
                empty.
        """

        value = None
        if element is not None:
            value = element.get(attribute)

        if not value:
            value = None

        return value

    @staticmethod
    def generate_xml_elements(
        file_xml: Union[gzip.GzipFile, BinaryIO],
        element_tag: Optional[str] = None,
    ) -> Iterable[etree.Element]:
        """ Parses an XML file and generates elements of a given type.

        Args:
            file_xml (Union[gzip.GzipFile, BinaryIO]): The opened XML file to
                parse.
            element_tag: (ptional[str] = None): The tag of the elements to
                retrieve and generate.

        Returns:
             Iterable[etree.Element]: An iterable of XML elements of the given
                type.
        """

        document = etree.iterparse(
            file_xml, events=("start", "end"), tag=element_tag
        )

        start_tag = None
        for event, element in document:
            if event == "start" and start_tag is None:
                start_tag = element.tag
            if event == "end" and element.tag == start_tag:
                yield element
                start_tag = None
                element.clear()

    def open_xml_file(
        self, filename_xml: str
    ) -> Union[gzip.GzipFile, BinaryIO]:
        """ Opens an XML file that is either gzipped or not.

        Args:
            filename_xml (str): The path to the XML file to be opened.

        Returns:
            Union[gzip.GzipFile, BinaryIO]: The opened XML file.
        """

        msg_fmt = "Opening XML file '{0}'".format(filename_xml)
        self.logger.info(msg=msg_fmt)

        if filename_xml.endswith(".gz"):
            file_xml = gzip.GzipFile(filename=filename_xml, mode="rb")
        else:
            file_xml = open(filename_xml, "rb")

        return file_xml

    @abc.abstractmethod
    def parse(self, filename_xml: str) -> Dict[str, Any]:
        raise NotImplementedError


class ParserXmlMedlineGroups(ParserXmlBase):
    """ XML parser class meant to parse `MedlinePlus Health Topic Group XML`
        files.
    """

    def __init__(self, **kwargs):
        """ Constructor."""

        super(ParserXmlMedlineGroups, self).__init__(kwargs=kwargs)

    def parse_health_topic_group(
        self, element: etree.Element
    ) -> Dict[str, Union[str, int]]:
        """ Parses an element of type `group` and returns the values of the
            contained elements.

        Args:
            element (etree.Element): The element of type `group`.

        Returns:
            dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        # Skip Spanish entities.
        if self._eav(element=element, attribute="language") == "Spanish":
            return {}

        health_topic_group = {
            "id": int(self._eav(element=element, attribute="id")),
            "url": self._eav(element=element, attribute="url"),
            "name": self._et(element=element),
        }

        return health_topic_group

    def parse(self, filename_xml: str) -> Iterable[Dict[str, Union[str, int]]]:
        """ Parses a `MedlinePlus Health Topic Group XML` file and yields
            Python dictionaries containing the data for each health-topic group.

        Args:
            filename_xml (str): The path to the `MedlinePlus Health Topic Group
                XML` file.

        Returns:
            Iterable[Dict[str, Union[str, int]]]: An iterable yielding Python
                dictionaries containing the data for each health-topic group
                defined under the `MedlinePlus Health Topic Group XML` file.
        """

        self.logger.info(
            f"Parsing MedlinePlus Health Topic Group XML file '{filename_xml}'"
        )

        # Open the XML file.
        file_xml = self.open_xml_file(filename_xml=filename_xml)

        # Retrieve an iterable that yields `<group>` XML elements from the XML
        # file.
        elements = self.generate_xml_elements(
            file_xml=file_xml, element_tag="group"
        )

        # Iterate over the `<group>` elements and yield dictionaries with the
        # parsed data.
        for element in elements:
            health_topic_group = self.parse_health_topic_group(element)

            # Guard against empty documents.
            if not health_topic_group:
                continue

            yield health_topic_group


class ParserXmlMedlineHealthTopic(ParserXmlBase):
    """ XML parser class meant to parse `MedlinePlus Health Topic XML`
        files.
    """

    def __init__(self, **kwargs):
        """ Constructor."""

        super(ParserXmlMedlineHealthTopic, self).__init__(kwargs=kwargs)

    def parse_also_called(self, element: etree.Element) -> Dict:
        """ Parses an element of type `also-called` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `also-called`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        also_called = {"name": self._et(element=element)}

        return also_called

    def parse_also_calleds(
        self, element_health_topic: etree.Element
    ) -> List[Optional[Dict]]:
        """ Extracts and parses `<also-called>` elements from a `<health-topic>`
            element and returns the values of the contained elements.

        Args:
            element_health_topic (etree.Element): The `<health-topic>` element.

        Returns:
            List[Optional[Dict]]: The parsed values of the contained elements.
        """

        also_calleds = []

        if element_health_topic is None:
            return also_calleds

        for element in element_health_topic.findall("also-called"):
            also_calleds.append(self.parse_also_called(element=element))

        return also_calleds

    def parse_group(self, element: etree.Element) -> Dict:
        """ Parses an element of type `group` and returns the values of the
            contained elements.

        Args:
            element (etree.Element): The element of type `group`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        group = {
            "id": int(self._eav(element=element, attribute="id")),
            "url": self._eav(element=element, attribute="url"),
            "name": self._et(element=element),
        }

        return group

    def parse_groups(
        self, element_health_topic: etree.Element
    ) -> List[Optional[Dict]]:
        """ Extracts and parses `<group>` elements from a `<health-topic>`
            element and returns the values of the contained elements.

        Args:
            element_health_topic (etree.Element): The `<health-topic>` element.

        Returns:
            List[Optional[Dict]]: The parsed values of the contained elements.
        """

        groups = []

        if element_health_topic is None:
            return groups

        for element in element_health_topic.findall("group"):
            groups.append(self.parse_group(element=element))

        return groups

    def parse_descriptor(self, element: etree.Element) -> Dict:
        """ Parses an element of type `descriptor` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `descriptor`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        descriptor = {
            "id": self._eav(element=element, attribute="id"),
            "name": self._et(element=element),
        }

        return descriptor

    def parse_qualifier(self, element: etree.Element) -> Dict:
        """ Parses an element of type `qualifier` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `qualifier`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        qualifier = {
            "id": self._eav(element=element, attribute="id"),
            "name": self._et(element=element),
        }

        return qualifier

    def parse_qualifiers(
        self, element_mesh_heading: etree.Element
    ) -> List[Optional[Dict]]:
        """ Extracts and parses `<qualifier>` elements from a `<mesh-heading>`
            element and returns the values of the contained elements.

        Args:
            element_mesh_heading (etree.Element): The `<mesh-heading>` element.

        Returns:
            List[Optional[Dict]]: The parsed values of the contained elements.
        """

        qualifiers = []

        if element_mesh_heading is None:
            return qualifiers

        for element in element_mesh_heading.findall("qualifier"):
            qualifiers.append(self.parse_qualifier(element=element))

        return qualifiers

    def parse_mesh_heading(self, element: etree.Element) -> Dict:
        """ Parses an element of type `mesh-heading` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `mesh-heading`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        mesh_heading = {
            "descriptor": self.parse_descriptor(
                element=element.find("descriptor")
            ),
            "qualifiers": self.parse_qualifiers(element_mesh_heading=element),
        }

        return mesh_heading

    def parse_mesh_headings(
        self, element_health_topic: etree.Element
    ) -> List[Optional[Dict]]:
        """ Extracts and parses `<mesh-heading>` elements from a
            `<health-topic>` element and returns the values of the contained
            elements.

        Args:
            element_health_topic (etree.Element): The `<health-topic>` element.

        Returns:
            List[Optional[Dict]]: The parsed values of the contained elements.
        """

        mesh_headings = []

        if element_health_topic is None:
            return mesh_headings

        for element in element_health_topic.findall("mesh-heading"):
            mesh_headings.append(self.parse_mesh_heading(element=element))

        return mesh_headings

    def parse_primary_institute(self, element: etree.Element) -> Dict:
        """ Parses an element of type `primary-institute` and returns the values
            of the contained elements.

        Args:
            element (etree.Element): The element of type `primary-institute`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        primary_institute = {
            "url": self._eav(element=element, attribute="url"),
            "name": self._et(element=element),
        }

        return primary_institute

    def parse_related_topic(self, element: etree.Element) -> Dict:
        """ Parses an element of type `related-topic` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `related-topic`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        related_topic = {
            "url": self._eav(element=element, attribute="url"),
            "id": self._eav(element=element, attribute="id"),
            "name": self._et(element=element),
        }

        return related_topic

    def parse_related_topics(
        self, element_health_topic: etree.Element
    ) -> List[Optional[Dict]]:
        """ Extracts and parses `<related-topic>` elements from a
            `<health-topic>` element and returns the values of the contained
            elements.

        Args:
            element_health_topic (etree.Element): The `<health-topic>` element.

        Returns:
            List[Optional[Dict]]: The parsed values of the contained elements.
        """

        related_topics = []

        if element_health_topic is None:
            return related_topics

        for element in element_health_topic.findall("related-topic"):
            related_topics.append(self.parse_related_topic(element=element))

        return related_topics

    def parse_see_reference(self, element: etree.Element) -> Dict:
        """ Parses an element of type `see-reference` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `see-reference`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        see_reference = {"name": self._et(element=element)}

        return see_reference

    def parse_see_references(
        self, element_health_topic: etree.Element
    ) -> List[Optional[Dict]]:
        """ Extracts and parses `<see-reference>` elements from a
            `<health-topic>` element and returns the values of the contained
            elements.

        Args:
            element_health_topic (etree.Element): The `<health-topic>` element.

        Returns:
            List[Optional[Dict]]: The parsed values of the contained elements.
        """

        see_references = []

        if element_health_topic is None:
            return see_references

        for element in element_health_topic.findall("see-reference"):
            see_references.append(self.parse_see_reference(element=element))

        return see_references

    def parse_health_topic(self, element: etree.Element) -> Dict:
        """ Parses an element of type `health-topic` and returns the values of
            the contained elements.

        Args:
            element (etree.Element): The element of type `health-topic`.

        Returns:
            Dict: The parsed values of the contained elements.
        """

        if element is None:
            return {}

        # Skip Spanish entities.
        if self._eav(element=element, attribute="language") == "Spanish":
            return {}

        health_topic = {
            "meta-desc": self._eav(element=element, attribute="meta-desc"),
            "title": self._eav(element=element, attribute="title"),
            "url": self._eav(element=element, attribute="url"),
            "id": int(self._eav(element=element, attribute="id")),
            "date-created": datetime.datetime.strptime(
                self._eav(element=element, attribute="date-created"), "%m/%d/%Y"
            ).date(),
            "also_calleds": self.parse_also_calleds(
                element_health_topic=element
            ),
            "full-summary": self._et(element.find("full-summary")),
            "groups": self.parse_groups(element_health_topic=element),
            # Skipping `<language-mapped-topic>` elements.
            "mesh-headings": self.parse_mesh_headings(
                element_health_topic=element
            ),
            # Skipping `<other-language>` elements.
            "primary-institute": self.parse_primary_institute(
                element=element.find("primary-institute")
            ),
            "related-topics": self.parse_related_topics(
                element_health_topic=element
            ),
            "see-references": self.parse_see_references(
                element_health_topic=element
            )
            # Skipping `<site>` elements.
        }

        return health_topic

    def parse(self, filename_xml: str) -> Iterable[Dict]:
        """ Parses a `MedlinePlus Health Topic XML` file and yields Python
            dictionaries containing the data for each health-topic.

        Args:
            filename_xml (str): The path to the `MedlinePlus Health Topic XML`
                file.

        Returns:
            Iterable[Dict]: An iterable yielding Python dictionaries containing
                the data for each health-topic defined under the `MedlinePlus
                Health Topic XML` file.
        """

        self.logger.info(
            f"Parsing MedlinePlus Health Topic XML file '{filename_xml}'"
        )

        # Open the XML file.
        file_xml = self.open_xml_file(filename_xml=filename_xml)

        # Retrieve an iterable that yields `<health-topic>` XML elements from
        # the XML file.
        elements = self.generate_xml_elements(
            file_xml=file_xml, element_tag="health-topic"
        )

        # Iterate over the `<health-topic>` elements and yield dictionaries with
        # the parsed data.
        for element in elements:
            health_topic = self.parse_health_topic(element)

            # Guard against empty documents.
            if not health_topic:
                continue

            yield health_topic
