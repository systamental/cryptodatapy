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

    # API keys
    # cryptocompare api key
    try:
        cryptocompare_api_key: str = os.environ['CRYPTOCOMPARE_API_KEY']
    except KeyError:
        cryptocompare_api_key: str = None
    # glassnode api key
    try:
        glassnode_api_key: str = os.environ['GLASSNODE_API_KEY']
    except KeyError:
        glassnode_api_key: str = None
    # tiingo api key
    try:
        tiingo_api_key: str = os.environ['TIINGO_API_KEY']
    except KeyError:
        tiingo_api_key: str = None
    # coinmetrics api key
    try:
        coinmetrics_api_key: str = os.environ['COINMETRICS_API_KEY']
    except KeyError:
        coinmetrics_api_key: str = None

    # base URLs
    cryptocompare_base_url: str = 'https://min-api.cryptocompare.com/data/'
    glassnode_base_url: str = 'https://api.glassnode.com/v1/metrics/'
    tiingo_base_url: str = 'https://api.tiingo.com/tiingo/'
    aqr_base_url: str = 'https://www.aqr.com/-/media/AQR/Documents/Insights/Data-Sets/'
    if coinmetrics_api_key is not None:
        coinmetrics_base_url: str = 'https://api.coinmetrics.io/v4'
    else:
        coinmetrics_base_url: str = 'https://community-api.coinmetrics.io/v4'

    # api limit URLs
    cryptocompare_api_rate_limit: str = "https://min-api.cryptocompare.com/stats/rate/limit"

    # vendors URLs
    dbnomics_vendors_url: str = "https://db.nomics.world/providers"
    pdr_vendors_url: str = "https://pandas-datareader.readthedocs.io/en/latest/readers/index.html"

    # search URLs
    dbnomics_search_url: str = "https://db.nomics.world/"
