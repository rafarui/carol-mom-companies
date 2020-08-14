from pycarol import Staging, Carol
import logging
logger = logging.getLogger(__name__)


def download_staging_table(staging_name, connector_name, max_workers=None, columns=None, merge_records=True,
                           return_metadata=False, callback=None, env_name=None, org_name=None):
    """
    Ingests data model from Carol.

    Parameters
    ----------

    ...

    Returns
    -------

    query_responses: Pandas DataFrame
        Staging table.
    """

    # login
    login = Carol()
    cds = True

    if env_name is not None:
        login.switch_environment(env_name=env_name, org_name=org_name, app_name=' ')
        logger.debug('switching environment')
        logger.debug(login.get_current())

    # run query
    stag = Staging(login)
    df = stag.fetch_parquet(
        staging_name=staging_name, connector_name=connector_name, max_workers=max_workers,
        backend='pandas', return_dask_graph=False, columns=columns, merge_records=merge_records,
        return_metadata=return_metadata, callback=callback, cds=cds,
    )

    return df


def ingest_cnae(
        staging_name, connector_name, max_workers=None,
        columns=None, merge_records=True,
        return_metadata=False, env_name=None, org_name=None
):
    """
    Ingests records from Medical Form DM.

    Parameters
    ----------

    ...

    Returns
    -------

    forms: Pandas DataFrame
        Ingested records.
    """

    # get data
    df = download_staging_table(
        staging_name=staging_name, connector_name=connector_name, max_workers=max_workers,
        columns=columns, merge_records=merge_records,
        return_metadata=return_metadata, env_name=env_name, org_name=org_name
    )

    df = df.set_index('subclass_cnae')['description_cnae'].to_dict()
    return df
