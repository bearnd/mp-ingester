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

