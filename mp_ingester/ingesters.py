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


class IngesterMedlineHealthTopics(IngesterDocumentBase):
    """ Ingester class meant to ingest parsed `MedlinePlus Health Topic XML`
        data.
    """

    def __init__(
        self,
        dal: DalMedline,
        health_topic_body_parts: List[
            Dict[str, Union[str, List[Dict[str, str]]]]
        ],
        **kwargs,
    ):
        """ Constructor.

        Args:
            dal (DalMedline): The DAL to be used to interact with the database.
        """

        super(IngesterMedlineHealthTopics, self).__init__(
            dal=dal, kwargs=kwargs
        )

        self.health_topic_body_parts = health_topic_body_parts

    def _get_topic_body_parts(self, health_topic_name: str) -> List[str]:
        """ Retrieve the names of the body parts based on the name of the
            health-topic from the scraped data.

        Args:
            health_topic_name (str): The name of the health-topic.

        Returns:
            List[str]: The name of the encompassing body parts.
        """

        body_part_names = []

        for entry in self.health_topic_body_parts:
            body_part_name = entry["name"]
            for health_topic in entry["health_topics"]:
                if health_topic["name"] == health_topic_name:
                    body_part_names.append(body_part_name)

        return body_part_names

    @log_ingestion_of_document(document_name="also-called")
    def ingest_also_called(self, document: Dict) -> Optional[int]:
        """ Ingests a parsed element of type `<also-called>` and creates a
            `AlsoCalled` record.

        Args:
            document (Dict): The element of type `<also-called>` parsed into a
                dictionary.

        Returns:
             int: The primary-key ID of the `AlsoCalled` record.
        """

        if not document:
            return None

        obj_id = self.dal.iodi_also_called(name=document["name"])

        return obj_id

    @log_ingestion_of_document(document_name="primary-institute")
    def ingest_primary_institute(self, document: Dict) -> Optional[int]:
        """ Ingests a parsed element of type `<primary-institute>` and creates a
            `PrimaryInstitute` record.

        Args:
            document (Dict): The element of type `<primary-institute>` parsed
                into a dictionary.

        Returns:
             int: The primary-key ID of the `PrimaryInstitute` record.
        """

        if not document:
            return None

        obj_id = self.dal.iodu_primary_institute(
            name=document["name"], url=document["url"]
        )

        return obj_id

    @log_ingestion_of_document(document_name="see-reference")
    def ingest_see_reference(self, document: Dict) -> Optional[int]:
        """ Ingests a parsed element of type `<see-reference>` and creates a
            `SeeReference` record.

        Args:
            document (Dict): The element of type `<see-reference>` parsed into a
                dictionary.

        Returns:
             int: The primary-key ID of the `SeeReference` record.
        """

        if not document:
            return None

        obj_id = self.dal.iodi_see_reference(name=document["name"])

        return obj_id

    @log_ingestion_of_document(document_name="health-topic")
    def ingest(self, document: Dict, do_ingest_links: bool) -> Optional[int]:
        """ Ingests a parsed element of type `<health-topic>` and creates a
            `HealthTopic` record.

        Args:
            document (Dict): The element of type `<health-topic>` parsed into a
                dictionary.
            do_ingest_links (bool): Whether to ingest links to other related
                health-topics. This is set to `False` to perform an initial of
                all topics while the second run should set it to `True` so that
                the links can be set after all topics have been ingested.

        Returns:
             int: The primary-key ID of the `HealthTopic` record.
        """

        if not document:
            return None

        # Upsert the `PrimaryInstitute` record.
        primary_institute_id = self.ingest_primary_institute(
            document=document["primary-institute"]
        )

        health_topic_id = self.dal.iodu_health_topic(
            ui=str(document["id"]),
            title=document["title"],
            url=document["url"],
            description=document["meta-desc"],
            summary=document["full-summary"],
            date_created=document["date-created"],
            primary_institute_id=primary_institute_id,
        )

        # Upsert the `AlsoCalled` records and retrieve their PK IDs.
        also_called_ids = []
        for also_called in document["also-calleds"]:
            also_called_id = self.ingest_also_called(document=also_called)
            also_called_ids.append(also_called_id)

        # Upsert the `HealthTopicAlsoCalled` records.
        for also_called_id in also_called_ids:
            self.dal.iodi_health_topic_also_called(
                health_topic_id=health_topic_id, also_called_id=also_called_id
            )

        # Retrieve the PK IDs of the related `HealthTopicGroup` records.
        health_topic_group_ids = []
        for group in document["groups"]:
            # noinspection PyTypeChecker
            obj_group = self.dal.get_by_attr(
                orm_class=HealthTopicGroup,
                attr_name="name",
                attr_value=group["name"],
            )  # type: HealthTopicGroup
            health_topic_group_ids.append(obj_group.health_topic_group_id)

        # Upsert the `HealthTopicHealthTopicGroup` records.
        for health_topic_group_id in health_topic_group_ids:
            self.dal.iodi_health_topic_health_topic_group(
                health_topic_id=health_topic_id,
                health_topic_group_id=health_topic_group_id,
            )

        # Retrieve the PK IDs of the related `Descriptor` records.
        descriptor_ids = []
        for mesh_heading in document["mesh-headings"]:
            # noinspection PyTypeChecker
            obj_descriptor = self.dal.get_by_attr(
                orm_class=Descriptor,
                attr_name="ui",
                attr_value=mesh_heading["descriptor"]["id"],
            )  # type: Descriptor
            descriptor_ids.append(obj_descriptor.descriptor_id)

        # Upsert the `HealthTopicDescriptor` records.
        for descriptor_id in descriptor_ids:
            self.dal.iodi_health_topic_descriptor(
                health_topic_id=health_topic_id, descriptor_id=descriptor_id
            )


        if do_ingest_links:
            # Retrieve the PK IDs of the related `HealthTopic` records.
            related_health_topic_ids = []
            for related_topic in document["related-topics"]:
                # noinspection PyTypeChecker
                obj_group = self.dal.get_by_attr(
                    orm_class=HealthTopic,
                    attr_name="ui",
                    attr_value=related_topic["id"],
                )  # type: HealthTopic
                related_health_topic_ids.append(obj_group.health_topic_id)

            # Upsert the `HealthTopicRelatedHealthTopic` records.
            for related_health_topic_id in related_health_topic_ids:
                self.dal.iodi_health_topic_related_health_topic(
                    health_topic_id=health_topic_id,
                    related_health_topic_id=related_health_topic_id,
                )

        # Upsert the `SeeReference` records and retrieve their PK IDs.
        see_reference_ids = []
        for see_reference in document["see-references"]:
            see_reference_id = self.ingest_see_reference(document=see_reference)
            see_reference_ids.append(see_reference_id)

        # Upsert the `HealthTopicSeeReference` records.
        for see_reference_id in see_reference_ids:
            self.dal.iodi_health_topic_see_reference(
                health_topic_id=health_topic_id,
                see_reference_id=see_reference_id,
            )

        # Retrieve the names of the body-parts the
        body_part_names = self._get_topic_body_parts(
            health_topic_name=document["title"]
        )
        # Retrieve the PK IDs of the related `BodyPart` records.
        body_part_ids = []
        for body_part_name in body_part_names:
            # noinspection PyTypeChecker
            obj_body_part = self.dal.get_by_attr(
                orm_class=BodyPart, attr_name="name", attr_value=body_part_name
            )  # type: BodyPart
            body_part_ids.append(obj_body_part.body_part_id)

        # Upsert the `HealthTopicBodyPart` records.
        for body_part_id in body_part_ids:
            self.dal.iodi_health_topic_body_part(
                health_topic_id=health_topic_id, body_part_id=body_part_id
            )
