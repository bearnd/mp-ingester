# -*- coding: utf-8 -*-

from typing import Callable


def log_ingestion_of_document(document_name: str) -> Callable:
    """ Decorator that logs a line stating that the ingestion of a records of a
        given XML element type is underway.

    Args:
        document_name (str): The name of the XML element type.
    """

    # Define the actual decorator. This three-tier decorator functions are
    # necessary when defining decorator functions with arguments.
    def log_ingestion_of_document_decorator(func):
        # Define the wrapper function.
        def wrapper(self, *args, **kwargs):

            msg = "Ingesting '{}' document"
            msg_fmt = msg.format(document_name)
            self.logger.debug(msg_fmt)

            # Simply execute the decorated method with the provided arguments
            # and return the result.
            return func(self, *args, **kwargs)

        return wrapper

    return log_ingestion_of_document_decorator
