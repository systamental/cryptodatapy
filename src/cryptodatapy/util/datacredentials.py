# import libraries
import pandas as pd
import os
import logging
import keyring
from dataclasses import dataclass
from typing import Union
from importlib import resources


def set_credential(cred_key: str, cred_val: Union[str, int]) -> None:
    """
    Sets value for data credential key, e.g. api keys, api secrets, db name, ....

    Parameters
    ----------
    cred_key: str
        Credential key for which value is to be stored.
    cred_val: str or int
        Credential value to be stored.

    Examples
    --------
    >>> set_credential('cryptocompare_api_key', 'dcf13983adf7dfa79a0dfa35adf')
    """
    if isinstance(cred_val, int):
        cred_val = str(cred_val)
    try:
        keyring.set_password(cred_key, os.getlogin(), cred_val)
    except Exception as e:
        logging.warning(e)
        logging.warning('Credentials could not be set.')

def get_credential(cred_key: str) -> str:
    """
    Gets value for data credential key, e.g. api keys, api secrets, db name, ....

    Parameters
    ----------
    cred_key: str
        Credential key for which value is to be retrieved.

    Returns
    -------
    cred: str
        Credential value.

    Examples
    --------
    >>> get_credential('cryptocompare_api_key')
    'dcf13983adf7dfa79a0dfa35adf'
    """
    cred = None

    try:
        cred = keyring.get_password(cred_key, os.getlogin())
    except Exception as e:
        logging.warning(e)
        logging.warning('Credentials could not be retrieved.')

    return cred


@dataclass
class DataCredentials:
    """
    Stores data credentials used by the CryptoDataPy project for data extraction, storage, etc.
    """
    # SQL db for structured data
    # postgresql db credentials
    postgresql_db_address: str = get_credential('postgresql_db_address')
    postgresql_db_port: str = get_credential('postgresql_db_port')
    postgresql_db_username: str = get_credential('postgresql_db_username')
    postgresql_db_password: str = get_credential('postgresql_db_password')
    postgresql_db_name: str = get_credential('postgresql_db_name')

    # NoSQL DB for tick/unstructured data
    # Arctic/mongodb credentials
    mongo_db_username: str = get_credential('mongodb_username')
    mongo_db_password: str = get_credential('mongodb_password')
    mongo_db_name: str = get_credential('mongodb_name')

    #  api keys
    cryptocompare_api_key: str = get_credential('cryptocompare_api_key')
    glassnode_api_key: str = get_credential('glassnode_api_key')
    tiingo_api_key: str = get_credential('tiingo_api_key')
    fred_api_key: str = get_credential('fred_api_key')
    ndl_api_key: str = get_credential('quandl_api_key')
    av_api_key: str = get_credential('av_api_key')

    # api limit URLs
    cryptocompare_api_rate_limit: str = 'https://min-api.cryptocompare.com/stats/rate/limit'
    ndl_api_rate_limit: str = 'https://help.data.nasdaq.com/article/' + \
                              '490-is-there-a-rate-limit-or-speed-limit-for-api-usage'

    # base URLs
    cryptocompare_base_url: str = get_credential('cryptocompare_base_url')
    glassnode_base_url: str = get_credential('glassnode_base_url')
    tiingo_base_url: str = get_credential('tiingo_base_url')

    # vendors URLs
    dbnomics_vendors_url: str = 'https://db.nomics.world/providers'
    pdr_vendors_url: str = 'https://pandas-datareader.readthedocs.io/en/latest/readers/index.html'

    # search URLs
    dbnomics_search_url: str = 'https://db.nomics.world/'
