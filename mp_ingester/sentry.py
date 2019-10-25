# -*- coding: utf-8 -*-

import sentry_sdk
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

import attrdict


def initialize_sentry(cfg: attrdict.AttrDict) -> None:
    """ Initializes the Sentry agent to capture exceptions which are then
        displayed under the sentry.io dashboard and the `mp-ingester`
        project.

    Args:
        cfg (attrdict.AttrDict): The service configuration dictionary.
    """

    # Initialization is skipped if the Sentry configuration has not been
    # defined.
    if not cfg.sentry.dsn:
        return None

    sentry_sdk.init(
        dsn=cfg.sentry.dsn,
        integrations=[SqlalchemyIntegration()],
        send_default_pii=False,
    )
