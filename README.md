![](cryptodatapy_logo.jpeg)

# CryptoDataPy
### _Better data beats advanced algorithms_
<br/>

**CryptoDataPy** is a python library which makes it easy to build high quality data pipelines 
for the analysis of digital assets. By providing easy access to over 100,000 time series for thousands of  assets, 
it facilitates the pre-processing of a wide range of data from different sources.

Cryptoassets generate a huge amount of market, on-chain and off-chain data. 
But unlike legacy financial markets, this data is often fragmented, 
unstructured and dirty. By extracting data from various sources, 
pre-processing it into a user-friendly (tidy) format, detecting and repairing 'bad' data,
and allowing for easy storage and retrieval, CryptoDataPy allows you to spend less time gathering 
and cleaning data, and more time analyzing it.

Our data includes:

- **Market:** market prices of varying granularity (e.g. tick, trade and bar data, aka OHLC),
for spot, futures and options markets, as well as funding rates for the analysis of 
cryptoasset returns.
- **On-chain:** network health and usage data, circulating supply, asset holder positions and 
cost-basis, for the analysis of underlying crypto network fundamentals.
- **Off-chain:** news, social media, developer activity, web traffic and search for project interest and 
sentiment, as well as traditional financial market and macroeconomic data for broader financial and 
economic conditions.

The library's intuitive interface facilitates each step of the ETL/ETL (extract-transform-load) process:

- **Extract**: Extracting data from a wide range of data sources and file formats.
- **Transform**: 
  - Wrangling data into a pandas DataFrame in a structured and user-friendly format, 
  a.k.a [tidy data](https://www.jstatsoft.org/article/view/v059i10). 
  - Detecting, scrubbing and repairing 'bad' data (e.g. outliers, missing values, 0s, etc.) to improve the accuracy and reliability
of machine learning/predictive models.
- **Load**: Storing clean and ready-for-analysis data and metadata for easy access.

## Installation

```bash
$ pip install cryptodatapy
```

## Usage

**CryptoDataPy** allows you to pull ready-to-analyze data from a variety of sources 
with only a few lines of code.

First specify which data you want with a `DataRequest`:

```python
# import DataRequest
from cryptodatapy.extract.datarequest import DataRequest
# specify parameters for data request: tickers, fields, start date, end_date, etc.
data_req = DataRequest(
    source='glassnode',  # name of data source
    tickers=['btc', 'eth'], # list of asset tickers, in CryptoDataPy format, defaults to 'btc'
    fields=['close', 'add_act', 'hashrate'],  # list of fields, in CryptoDataPy, defaults to 'close'
    freq=None,  # data frequency, defaults to daily  
    quote_ccy=None,  # defaults to USD/USDT
    exch=None,  # defaults to exchange weighted average or Binance
    mkt_type= 'spot',  # defaults to spot
    start_date=None,  # defaults to start date for longest series
    end_date=None,  # defaults to most recent 
    tz=None,  # defaults to UTC time
    cat=None,  # optional, should be specified when asset class is not crypto, eg. 'fx', 'rates', 'macro', etc.
)
```
Then get the data :

```python
# import GetData
from cryptodatapy.extract.getdata import GetData
# get data
GetData(data_req).get_series()
```

With the same data request parameters, you can retrieve the same data from a different source:

```python
# modify data source parameter
data_req = DataRequest(
  source='coinmetrics',           
  tickers=['btc', 'eth'], 
  fields=['close', 'add_act', 'hashrate'], 
  req='d',
  start_date='2016-01-01')
# get data
GetData(data_req).get_series()
```

For more detailed code examples and interactive tutorials 
see [here](https://github.com/systamental/cryptodatapy/blob/main/docs/example.ipynb).
## Supported Data Sources

- [CryptoCompare](https://min-api.cryptocompare.com/documentation)
- [CCXT](https://docs.ccxt.com/en/latest/)
- [Glassnode](https://docs.glassnode.com/)
- [Coin Metrics](https://docs.coinmetrics.io/api/v4/)
- [Tiingo](https://api.tiingo.com/documentation/general/overview)
- [Yahoo Finance](https://github.com/ranaroussi/yfinance)
- [Fama-French Data](http://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)
- [AQR](https://www.aqr.com/Insights/Datasets)
- [Federal Reserve Economic Data (FRED)](https://fred.stlouisfed.org/docs/api/fred/)
- [DBnomics](https://db.nomics.world/docs/)
- [WorldBank](https://documents.worldbank.org/en/publication/documents-reports/api)
- [Pandas-datareader](https://pandas-datareader.readthedocs.io/en/latest/)

## Contributing

Interested in contributing? Check out the contributing guidelines and 
contact us at info@systamental.com. Please note that this project is s
released with a Code of Conduct. By contributing to this project, you agree 
to abide by its terms.

## License

`cryptodatapy` was created by Systamental. 
It is licensed under the terms of the Apache License 2.0 license.

