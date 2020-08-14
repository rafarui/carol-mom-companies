from pycarol import Staging, Carol

import logging, os
logger = logging.getLogger(__name__)


def send_to_carol(results, connector_name, staging_name, login=None):
    """
    Sends Deep Audit results to Carol staging table.

    Parameters
    ----------

    results: list
        List contains a dictionary for each row in procedures.
        Each dictionary contains the rejection probability of the procedure
        as well as aggregated properties of similar training procedures.

    connector_name: str
        Connector to send data to

    staging_name: str
        Staging to send data to

    login: pyCarol.Carol
        If not use the env variables to send.

    Returns
    -------

    True
    """

    if login is None:
        # login
        login = Carol()

    # initialise staging object
    stag = Staging(login)

    # send
    logger.info(f'Sending {len(results)} records to connector/staging {connector_name}/{staging_name}.')
    stag.send_data(staging_name=staging_name, data=results, connector_name=connector_name,
                   async_send=True, step_size=1000, max_workers=3)

    return True