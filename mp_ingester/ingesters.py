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

