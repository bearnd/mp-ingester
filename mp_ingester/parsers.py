# -*- coding: utf-8 -*-

"""
This module defines XML parser classes meant to parse MedlinePlus XML files
available under https://medlineplus.gov/xml.html and return Python dictionaries
containing the retrieved data.
"""

import abc
import gzip
from typing import Union, Dict, Optional, Iterable, BinaryIO, Any

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
