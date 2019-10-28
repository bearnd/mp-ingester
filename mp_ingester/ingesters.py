# -*- coding: utf-8 -*-

import abc
from typing import Union, List, Dict, Optional

from fform.dals_mp import DalMedline
from fform.orm_mp import HealthTopicGroupClass
from fform.orm_mp import HealthTopicGroup
from fform.orm_mp import HealthTopic
from fform.orm_mp import BodyPart
from fform.orm_mt import Descriptor

from mp_ingester.loggers import create_logger
from mp_ingester.utils import log_ingestion_of_document


class IngesterDocumentBase(object):
    """ Ingester base-class."""

    def __init__(self, dal: DalMedline, **kwargs):
        """ Constructor.

        Args:
            dal (DalMedline): The DAL to be used to interact with the database.
        """

        self.dal = dal

        self.logger = create_logger(
            logger_name=type(self).__name__,
            logger_level=kwargs.get("logger_level", "DEBUG"),
        )

    @abc.abstractmethod
    def ingest(self, document: Dict) -> int:
        raise NotImplementedError


class IngesterMedlineGroupClasses(IngesterDocumentBase):
    """ Ingester class meant to ingest scraped health-topic group class."""

    @log_ingestion_of_document(document_name="group-class")
    def ingest(self, name: str) -> Optional[int]:
        """ Ingests a health-topic group class and creates a
            `HealthTopicGroupClass` record.

        Args:
            name (int): The name of the health-topic group class.

        Returns:
             int: The primary-key ID of the `HealthTopicGroupClass` record.
        """

        if not name:
            return None

        self.dal.iodi_health_topic_group_class(name=name)


class IngesterMedlineBodyParts(IngesterDocumentBase):
    """ Ingester class meant to ingest scraped body parts."""

    @log_ingestion_of_document(document_name="body-part")
    def ingest(self, name: str, health_topic_group_url: str) -> Optional[int]:
        """ Ingests a body-part and creates a `HealthTopicBodyPart` record.

        Args:
            name (int): The name of the body-part.
            health_topic_group_url (str): The URL of the health-topic group the
                body-part belongs to.

        Returns:
             int: The primary-key ID of the `HealthTopicBodyPart` record.
        """

        if not name:
            return None

        # Retrieve the PK ID of the related `HealthTopicGroup` record.
        # noinspection PyTypeChecker
        health_topic_group = self.dal.get_by_attr(
            orm_class=HealthTopicGroup,
            attr_name="url",
            attr_value=health_topic_group_url,
        )  # type: HealthTopicGroup

        self.dal.iodi_body_part(
            name=name,
            health_topic_group_id=health_topic_group.health_topic_group_id,
        )


class IngesterMedlineGroups(IngesterDocumentBase):
    """ Ingester class meant to ingest parsed `MedlinePlus Health Topic Group
        XML` data.
    """

    def __init__(
        self,
        dal: DalMedline,
        health_topic_group_classes: List[
            Dict[str, Union[str, List[Dict[str, str]]]]
        ],
        **kwargs,
    ):
        """ Constructor.

        Args:
            dal (DalMedline): The DAL to be used to interact with the database.
        """

        super(IngesterMedlineGroups, self).__init__(dal=dal, kwargs=kwargs)

        self.health_topic_group_classes = health_topic_group_classes

    def _get_group_class(self, health_topic_group_name: str) -> Optional[str]:
        """ Retrieve the name of the health-topic group class based on the
            name of the health-topic group from the scraped data.

        Args:
            health_topic_group_name (str): The name of the health-topic group.

        Returns:
            Optional[str]: The name of the encompassing health-topic group class
                or `None` if none was found.
        """

        for entry in self.health_topic_group_classes:
            health_topic_group_class_name = entry["name"]
            for health_topic_group in entry["health_topic_groups"]:
                if health_topic_group["name"] == health_topic_group_name:
                    return health_topic_group_class_name

        return None

    @log_ingestion_of_document(document_name="group")
    def ingest(self, document: Dict) -> Optional[int]:
        """ Ingests a parsed element of type `<group>` and creates a
            `HealthTopicGroup` record.

        Args:
            document (Dict): The element of type `<group`> parsed into a
                dictionary.

        Returns:
             int: The primary-key ID of the `HealthTopicGroup` record.
        """

        if not document:
            return None

        group_name = document["name"]

        # Retrieve the name of the health-topic group class name that includes
        # the given group.
        class_name = self._get_group_class(health_topic_group_name=group_name)

        if not class_name:
            raise ValueError

        # Retrieve the matching `HealthTopicGroupClass` object.
        # noinspection PyTypeChecker
        obj_class = self.dal.get_by_attr(
            orm_class=HealthTopicGroupClass,
            attr_name="name",
            attr_value=class_name,
        )  # type: HealthTopicGroupClass

        if not obj_class:
            raise ValueError

        self.dal.iodu_health_topic_group(
            ui=str(document["id"]),
            name=document["name"],
            url=document["url"],
            health_topic_group_class_id=obj_class.health_topic_group_class_id,
        )

