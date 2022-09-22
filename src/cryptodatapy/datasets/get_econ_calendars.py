import logging
from time import sleep
from typing import List

import investpy

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.transform.convertparams import ConvertParams

ctys_dict = {'united states': 'us', 'euro zone': 'ez', 'china': 'cn', 'india': 'in', 'japan': 'jp', 'germany': 'de',
             'russia': 'ru', 'indonesia': 'id', 'brazil': 'br', 'united kingdom': 'gb', 'france': 'fr', 'turkey': 'tr',
             'italy': 'it', 'mexico': 'mx', 'south korea': 'kr', 'canada': 'ca'}


def get_econ_calendar(cty: str) -> None:
    """
    Scrapes econ calendars from Investing.com and creates csv file in datasets.

    data_req: DataRequest
        Parameters of data request in CryptoDataPy format.
    """
    data_req = DataRequest(
        fields=["actual", "expected", "previous", "surprise"],
        cat="macro",
    )
    # convert data req params to InvestPy format
    ip_data_req = ConvertParams(data_req).to_investpy()

    # set number of attempts and bool for while loop
    attempts = 0
    # run a while loop to pull ohlcv prices in case the attempt fails
    while attempts < ip_data_req["trials"]:

        try:
            # get data calendar
            df = investpy.news.economic_calendar(
                countries=cty,
                time_zone="GMT",
                from_date=ip_data_req["start_date"],
                to_date=ip_data_req["end_date"],
            )
            assert not df.empty

        except Exception as e:
            logging.warning(e)
            attempts += 1
            sleep(ip_data_req["pause"])
            if attempts == ip_data_req["trials"]:
                raise Exception(
                    f"Failed to get economic data release calendar for {cty} after many attempts."
                )
        else:
            df.to_csv(ctys_dict[cty] + '_econ_calendar.csv')


def get_all_ctys_econ_calendars(ctys: List[str]) -> None:
    """
    Scrapes econ calendars from Investing.com for each country and creates csv file in datasets.
    """
    # loop through ctys
    for cty in ctys:

        # get calendar
        get_econ_calendar(cty)
