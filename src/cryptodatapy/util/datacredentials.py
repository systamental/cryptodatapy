import os
from dataclasses import dataclass


@dataclass
class DataCredentials:
    """
    Stores data credentials used by the CryptoDataPy project for data extraction, storage, etc.
    """

    # SQL db for structured data
    # postgresql db credentials
    postgresql_db_address: str = None
    postgresql_db_port: str = None
    postgresql_db_username: str = None
    postgresql_db_password: str = None
    postgresql_db_name: str = None

    # NoSQL DB for tick/unstructured data
    # Arctic/mongodb credentials
    mongo_db_username: str = None
    mongo_db_password: str = None
    mongo_db_name: str = None

    #  api keys
    cryptocompare_api_key: str = os.environ['CRYPTOCOMPARE_API_KEY']
    glassnode_api_key: str = os.environ['GLASSNODE_API_KEY']
    tiingo_api_key: str = os.environ['TIINGO_API_KEY']

    # api limit URLs
    cryptocompare_api_rate_limit: str = "https://min-api.cryptocompare.com/stats/rate/limit"

    # base URLs
    cryptocompare_base_url: str = 'https://min-api.cryptocompare.com/data/'
    glassnode_base_url: str = 'https://api.glassnode.com/v1/metrics/'
    tiingo_base_url: str = 'https://api.tiingo.com/tiingo/'

    # vendors URLs
    dbnomics_vendors_url: str = "https://db.nomics.world/providers"
    pdr_vendors_url: str = "https://pandas-datareader.readthedocs.io/en/latest/readers/index.html"

    # search URLs
    dbnomics_search_url: str = "https://db.nomics.world/"
