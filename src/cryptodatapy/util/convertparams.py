# import libraries
import pandas as pd


# convert datetime to unix timestamp
def convert_datetime_to_unix_tmsp(date_time):
    """
    Converts date and time from datetime format
    """

    unix_tmsp = round(pd.Timestamp(date_time).timestamp())

    return unix_tmsp
