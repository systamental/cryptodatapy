import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import time

import pandas as pd
import requests
import pytz

from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.extract.exchanges.exchange import Exchange
from cryptodatapy.transform.convertparams import ConvertParams
from cryptodatapy.transform.wrangle import WrangleData


class Dydx(Exchange):
    """
    Retrieves data from dydx exchange.
    """
    def __init__(
            self,
            name: str = "dydx",
            exch_type: str = "dex",
            is_active: bool = True,
            categories: Union[str, List[str]] = "crypto",
            assets: Optional[Dict[str, List[str]]] = None,
            markets: Optional[Dict[str, List[str]]] = None,
            market_types: List[str] = ["perpetual_future"],
            fields: Optional[List[str]] = ["open", "high", "low", "close", "volume", "funding_rate"],
            frequencies: Optional[Dict[str, Union[str, int]]] = {
                "1m": "1MIN",
                "5m": "5MINS",
                "15m": "15MINS",
                "1h": "1HOUR",
                "4h": "4HOURS",
                "1d": "1DAY"
            },
            fees: Optional[Dict[str, float]] = {'perpetual_future': {'maker': 0.0, 'taker': 0.0}},
            base_url: Optional[str] = "https://indexer.dydx.trade/v4",
            api_key: Optional[str] = None,
            max_obs_per_call: Optional[int] = 1000,
            rate_limit: Optional[Any] = None
    ):
        """
        Initializes the Dydx class.

        Parameters:
        -----------
        name: str
            The name of the exchange.
        exch_type: str
            The type of the exchange.
        is_active: bool
            Whether the exchange is active.
        categories: Union[str, List[str]]
            The categories of the exchange.
        assets: Optional[Dict[str, List[str]]]
            The assets traded on the exchange.
        markets: Optional[Dict[str, List[str]]]
            The markets traded on the exchange.
        market_types: List[str]
            The types of markets traded on the exchange.
        fields: Optional[List[str]]
            The fields to retrieve from the exchange.
        frequencies: Optional[Dict[str, Union[str, int]]]
            The frequencies of the data to retrieve.
        fees: Optional[Dict[str, float]]
            The fees for the exchange.
        base_url: Optional[str]
            The base url of the exchange.
        api_key: Optional[str]
            The api key for the exchange.
        max_obs_per_call: Optional[int]
            The maximum number of observations per call.
        rate_limit: Optional[Any]
            The rate limit for the exchange.
        """
        super().__init__(
            name=name,
            exch_type=exch_type,
            is_active=is_active,
            categories=categories,
            assets=assets,
            markets=markets,
            market_types=market_types,
            fields=fields,
            frequencies=frequencies,
            fees=fees,
            base_url=base_url,
            api_key=api_key,
            max_obs_per_call=max_obs_per_call,
            rate_limit=rate_limit
        )
        self.data_req = None
        self.data = pd.DataFrame()

    def get_assets_info(self) -> pd.DataFrame:
        """
        Gets info for available assets from dYdX.

        Returns
        -------
        pd.DataFrame
            DataFrame with asset information.
        """
        url = f"{self.base_url}/perpetualMarkets"
        response = requests.get(url)
        response.raise_for_status()
        
        markets_data = response.json()['markets']
        assets_info = []
        seen_assets = set()
        
        for ticker, market in markets_data.items():
            base_currency = ticker.split('-')[0]
            if base_currency not in seen_assets and market['status'] == 'ACTIVE':
                assets_info.append({
                    'asset_id': base_currency,
                    'symbol': base_currency,
                    'decimals': abs(int(market['atomicResolution'])),
                    'status': market['status']
                })
                seen_assets.add(base_currency)
        
        return pd.DataFrame(assets_info)

    def get_markets_info(self, quote_ccy: Optional[str] = None, mkt_type: Optional[str] = None, as_list: bool = False) -> Union[pd.DataFrame, List[str]]:
        """
        Gets info for available markets from dYdX.

        Parameters
        ----------
        quote_ccy: str, optional
            Quote currency to filter by (e.g., 'USD', 'USDC'). For dYdX, this is typically 'USD'.
        mkt_type: str, optional
            Market type to filter by. For dYdX, this is typically 'perpetual_future'.
        as_list: bool, default False
            If True, returns a list of ticker symbols instead of a DataFrame.

        Returns
        -------
        pd.DataFrame or List[str]
            DataFrame with market information or list of ticker symbols.
        """
        url = f"{self.base_url}/perpetualMarkets"
        response = requests.get(url)
        response.raise_for_status()
        
        markets_data = response.json()['markets']
        markets_info = []
        
        for ticker, market in markets_data.items():
            if market['status'] == 'ACTIVE':
                base_currency = ticker.split('-')[0]
                quote_currency = ticker.split('-')[1]
                
                # Apply quote currency filter if specified
                if quote_ccy is not None and quote_currency.upper() != quote_ccy.upper():
                    continue
                
                # Apply market type filter if specified
                # dYdX only has perpetual futures, so we only include if mkt_type is None or 'perpetual_future'
                if mkt_type is not None and mkt_type != 'perpetual_future':
                    continue
                
                markets_info.append({
                    'ticker': ticker,
                    'base_currency': base_currency,
                    'quote_currency': quote_currency,
                    'min_trade_amount': float(market['stepSize']),
                    'price_precision': abs(int(market['atomicResolution'])),
                    'min_price': float(market['tickSize']),
                    'status': market['status'],
                    'type': 'perpetual_future'  # dYdX only has perpetual futures
                })
        
        if not markets_info:
            if as_list:
                return []
            else:
                return pd.DataFrame()
        
        df = pd.DataFrame(markets_info)
        
        if as_list:
            return df['ticker'].tolist()
        else:
            return df

    def get_fields_info(self, data_type: Optional[str] = None) -> pd.DataFrame:
        """
        Gets info for available fields from dYdX.

        Parameters
        ----------
        data_type: str, optional
            Type of data for which to return field information.

        Returns
        -------
        pd.DataFrame
            DataFrame with field information.
        """
        fields = [
            {'field': 'open', 'description': 'Opening price'},
            {'field': 'high', 'description': 'Highest price'},
            {'field': 'low', 'description': 'Lowest price'},
            {'field': 'close', 'description': 'Closing price'},
            {'field': 'volume', 'description': 'Trading volume'},
            {'field': 'funding_rate', 'description': 'Hourly funding rate (dYdX charges funding every hour)'}
        ]
        return pd.DataFrame(fields)

    def get_frequencies_info(self) -> pd.DataFrame:
        """
        Gets info for available frequencies from dYdX.

        Returns
        -------
        pd.DataFrame
            DataFrame with frequency information.
        """
        return pd.DataFrame({
            'frequency': list(self.frequencies.keys()),
            'description': list(self.frequencies.values())
        })

    def get_rate_limit_info(self) -> Dict[str, Any]:
        """
        Gets rate limit information from dYdX.

        Returns
        -------
        Dict[str, Any]
            Dictionary with rate limit information.
        """
        return {
            'requests_per_second': 10,
            'requests_per_minute': 300
        }

    def get_metadata(self) -> Dict[str, Any]:
        """
        Gets metadata about the exchange.

        Returns
        -------
        Dict[str, Any]
            Dictionary with exchange metadata.
        """
        return {
            'name': self.name,
            'type': self.exch_type,
            'status': 'active' if self.is_active else 'inactive',
            'categories': self.categories,
            'market_types': self.market_types,
            'base_url': self.base_url
        }

    def _fetch_ohlcv(self) -> pd.DataFrame:
        """
        Fetches OHLCV data from dYdX for multiple markets with pagination support.
        
        The dYdX candles API has a limit (typically 1000 records) and returns data in 
        reverse chronological order (newest first). For large date ranges, we need to 
        implement pagination to retrieve all historical data.

        Returns
        -------
        pd.DataFrame
            DataFrame with OHLCV data for all requested markets.
        """
        if not self.data_req:
            raise ValueError("Data request not set")

        # Parse date range
        try:
            # source dates are guaranteed to be set by parameter conversion
            start_dt = pd.to_datetime(self.data_req.source_start_date)
            if start_dt.tz is None:
                start_dt = start_dt.tz_localize('UTC')
            
            end_dt = pd.to_datetime(self.data_req.source_end_date)
            if end_dt.tz is None:
                end_dt = end_dt.tz_localize('UTC')
            
            # Add buffer to end date to ensure we get data up to the requested time
            buffered_end_dt = end_dt + pd.Timedelta(hours=1)
            
        except Exception as e:
            logging.error(f"Could not parse date range: {e}")
            return pd.DataFrame()

        all_records = []
        
        for ticker in self.data_req.source_tickers:
            market_symbol = f"{ticker}-USD"
            
            # Initialize pagination variables
            current_end_date = buffered_end_dt
            ticker_records = []
            page_count = 0
            max_pages = 100  # Safety limit for longer date ranges
            
            while page_count < max_pages:
                page_count += 1
                url = f"{self.base_url}/candles/perpetualMarkets/{market_symbol}"
                
                params = {
                    'resolution': self.data_req.source_freq,
                    'fromISO': self.data_req.source_start_date,
                    'toISO': current_end_date.isoformat(),
                    'limit': 1000  # Maximum allowed by dYdX API
                }
                
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Validate API response
                    if 'candles' not in data or not data['candles']:
                        break
                    
                    page_records = data['candles']
                    
                    # Convert timestamps efficiently
                    page_df = pd.DataFrame(page_records)
                    page_df['startedAt'] = pd.to_datetime(page_df['startedAt'])
                    
                    # Ensure timezone consistency
                    if page_df['startedAt'].dt.tz is None:
                        page_df['startedAt'] = page_df['startedAt'].dt.tz_localize('UTC')
                    
                    # Early termination check - if oldest record is before start date
                    oldest_timestamp = page_df['startedAt'].min()
                    if oldest_timestamp < start_dt:
                        # Filter only records within date range before adding
                        mask = (page_df['startedAt'] >= start_dt) & (page_df['startedAt'] <= end_dt)
                        filtered_records = page_df[mask]
                        
                        if not filtered_records.empty:
                            ticker_records.extend(filtered_records.to_dict('records'))
                        
                        break
                    else:
                        # All records on this page are within or after the date range
                        mask = page_df['startedAt'] <= end_dt
                        filtered_records = page_df[mask]
                        
                        if not filtered_records.empty:
                            ticker_records.extend(filtered_records.to_dict('records'))
                    
                    # Check if we got fewer records than requested (end of data)
                    if len(page_records) < 1000:
                        break
                    
                    # Set next pagination point (oldest timestamp from current page minus 1 second)
                    current_end_date = oldest_timestamp - pd.Timedelta(seconds=1)
                    
                    # Rate limiting
                    time.sleep(1.0)
                    
                except requests.exceptions.Timeout:
                    logging.warning(f"Timeout fetching OHLCV data for {market_symbol} on page {page_count}, retrying...")
                    time.sleep(2.0)
                    continue
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch OHLCV data for {market_symbol} on page {page_count}: {str(e)}")
                    break
                except Exception as e:
                    logging.error(f"Error processing OHLCV data for {market_symbol} on page {page_count}: {str(e)}")
                    break
            
            if ticker_records:
                all_records.extend(ticker_records)

        if not all_records:
            return pd.DataFrame()

        # Create final DataFrame
        final_df = pd.DataFrame(all_records)
        final_df = final_df.sort_values(['ticker', 'startedAt']).reset_index(drop=True)
        
        return final_df

    def _fetch_funding_rates(self) -> pd.DataFrame:
        """
        Fetches funding rate data from dYdX for multiple markets with pagination support.
        
        Note: dYdX charges funding every hour, unlike other exchanges that typically 
        use 8-hour funding cycles. This method retrieves the complete historical 
        funding rate data for any requested date range, making multiple API calls as needed.

        Returns
        -------
        pd.DataFrame
            DataFrame with hourly funding rate data for all requested markets.
        """
        if not self.data_req:
            raise ValueError("Data request not set")

        # Parse date range
        try:
            # source dates are guaranteed to be set by parameter conversion
            start_dt = pd.to_datetime(self.data_req.source_start_date)
            if start_dt.tz is None:
                start_dt = start_dt.tz_localize('UTC')
            
            end_dt = pd.to_datetime(self.data_req.source_end_date)
            if end_dt.tz is None:
                end_dt = end_dt.tz_localize('UTC')
            
            buffered_end_dt = end_dt + pd.Timedelta(hours=1)
            
        except Exception as e:
            logging.error(f"Could not parse date range: {e}")
            return pd.DataFrame()

        all_records = []
        
        for ticker in self.data_req.source_tickers:
            market_symbol = f"{ticker}-USD"
            
            # Initialize pagination variables
            current_end_date = buffered_end_dt
            ticker_records = []
            page_count = 0
            max_pages = 100
            
            while page_count < max_pages:
                page_count += 1
                url = f"{self.base_url}/historicalFunding/{market_symbol}"
                
                params = {
                    'effectiveBeforeOrAt': current_end_date.isoformat(),
                    'limit': 1000
                }
                
                try:
                    response = requests.get(url, params=params, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    if 'historicalFunding' not in data or not data['historicalFunding']:
                        break
                    
                    page_records = data['historicalFunding']
                    
                    # Convert timestamps
                    page_df = pd.DataFrame(page_records)
                    page_df['effectiveAt'] = pd.to_datetime(page_df['effectiveAt'])
                    
                    if page_df['effectiveAt'].dt.tz is None:
                        page_df['effectiveAt'] = page_df['effectiveAt'].dt.tz_localize('UTC')
                    
                    # Filter records within date range
                    oldest_timestamp = page_df['effectiveAt'].min()
                    if oldest_timestamp < start_dt:
                        mask = (page_df['effectiveAt'] >= start_dt) & (page_df['effectiveAt'] <= buffered_end_dt)
                        filtered_records = page_df[mask]
                        
                        if not filtered_records.empty:
                            filtered_records = filtered_records.copy()
                            filtered_records['rate'] = pd.to_numeric(filtered_records['rate'], errors='coerce')
                            ticker_records.extend(filtered_records.to_dict('records'))
                        
                        break
                    else:
                        mask = page_df['effectiveAt'] <= buffered_end_dt
                        filtered_records = page_df[mask]
                        
                        if not filtered_records.empty:
                            filtered_records = filtered_records.copy()
                            filtered_records['rate'] = pd.to_numeric(filtered_records['rate'], errors='coerce')
                            ticker_records.extend(filtered_records.to_dict('records'))
                    
                    # Check if we got fewer records than requested (end of data)
                    if len(page_records) < 1000:
                        break
                    
                    # Set next pagination point
                    current_end_date = oldest_timestamp - pd.Timedelta(microseconds=1)
                    
                    # Rate limiting
                    time.sleep(1.0)
                    
                except requests.exceptions.Timeout:
                    logging.warning(f"Timeout fetching funding rate data for {market_symbol}, retrying...")
                    time.sleep(2.0)
                    continue
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch funding rate data for {market_symbol}: {str(e)}")
                    break
                except Exception as e:
                    logging.error(f"Error processing funding rate data for {market_symbol}: {str(e)}")
                    break
            
            if ticker_records:
                all_records.extend(ticker_records)

        if not all_records:
            return pd.DataFrame()

        # Create final DataFrame
        final_df = pd.DataFrame(all_records)
        final_df = final_df.sort_values(['ticker', 'effectiveAt']).reset_index(drop=True)
        
        return final_df

    def _fetch_open_interest(self) -> pd.DataFrame:
        """
        Fetches current open interest from dYdX.
        Note: This implementation only provides current open interest values, not historical data.
        Historical open interest data is not available through the dYdX API.

        Returns
        -------
        pd.DataFrame
            DataFrame with current open interest values.
            The DataFrame has a MultiIndex with 'date' and 'ticker' levels.
            The 'date' index will be the current timestamp for all entries.
        """
        if not self.data_req:
            raise ValueError("Data request not set")

        # Get current timestamp for all entries
        current_time = pd.Timestamp.utcnow()
        
        all_dfs = []
        for ticker in self.data_req.source_tickers:
            market_symbol = f"{ticker}-USD"
            url = f"{self.base_url}/perpetualMarkets/{market_symbol}"
            
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if 'market' not in data:
                    logging.warning(f"No market data found for {market_symbol}")
                    continue

                market_data = data['market']
                if 'openInterest' not in market_data:
                    logging.warning(f"No open interest data found for {market_symbol}")
                    continue

                # Create DataFrame with current open interest data
                df = pd.DataFrame({
                    'oi': [float(market_data['openInterest'])],
                    'date': [current_time],
                    'ticker': [ticker]
                })
                all_dfs.append(df)
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch open interest for {market_symbol}: {str(e)}")
                continue

        if not all_dfs:
            return pd.DataFrame()

        # Combine all DataFrames
        return pd.concat(all_dfs, ignore_index=True)

    def _convert_params(self) -> None:
        """
        Converts parameters for the data request using ConvertParams class.
        """
        if not self.data_req:
            raise ValueError("Data request not set")
        
        # Convert parameters to dYdX format using ConvertParams class
        self.data_req = ConvertParams(self.data_req).to_dydx()

    @staticmethod
    def _wrangle_data_resp(data_req: DataRequest, data_resp: Union[Dict[str, Any], pd.DataFrame]) -> pd.DataFrame:
        """
        Wrangles data response from dYdX using WrangleData class.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request.
        data_resp: Union[Dict[str, Any], pd.DataFrame]
            Data response from dYdX.

        Returns
        -------
        pd.DataFrame
            Wrangled DataFrame.
        """
        # Determine data type based on the actual data structure
        if isinstance(data_resp, pd.DataFrame) and not data_resp.empty:
            # Check columns to determine data type
            if 'effectiveAt' in data_resp.columns or 'rate' in data_resp.columns:
                data_type = 'funding_rates'
            elif 'oi' in data_resp.columns:
                data_type = 'open_interest'
            elif 'startedAt' in data_resp.columns or any(col in data_resp.columns for col in ['open', 'high', 'low', 'close']):
                data_type = 'ohlcv'
            else:
                # Fallback to field-based detection
                if 'funding_rate' in data_req.fields:
                    data_type = 'funding_rates'
                elif 'oi' in data_req.fields:
                    data_type = 'open_interest'
                else:
                    data_type = 'ohlcv'
        else:
            # Empty DataFrame or other format - use field-based detection
            if 'funding_rate' in data_req.fields:
                data_type = 'funding_rates'
            elif 'oi' in data_req.fields:
                data_type = 'open_interest'
            else:
                data_type = 'ohlcv'
        
        # Use dYdX-specific wrangling method
        wrangler = WrangleData(data_req, data_resp)
        return wrangler.dydx(data_type)

    def _fetch_tidy_ohlcv(self) -> pd.DataFrame:
        """
        Fetches and tidies OHLCV data.

        Returns
        -------
        pd.DataFrame
            Tidy DataFrame with OHLCV data.
        """
        df = self._fetch_ohlcv()
        return self._wrangle_data_resp(self.data_req, df)

    def _fetch_tidy_funding_rates(self) -> pd.DataFrame:
        """
        Fetches and tidies funding rates.

        Returns
        -------
        pd.DataFrame
            Tidy DataFrame with funding rates.
        """
        df = self._fetch_funding_rates()
        return self._wrangle_data_resp(self.data_req, df)

    def _fetch_tidy_open_interest(self) -> pd.DataFrame:
        """
        Fetches and tidies open interest.

        Returns
        -------
        pd.DataFrame
            Tidy DataFrame with open interest.
        """
        df = self._fetch_open_interest()
        return self._wrangle_data_resp(self.data_req, df)

    def get_data(self, data_req: DataRequest) -> pd.DataFrame:
        """
        Gets market data from dYdX.

        Parameters
        ----------
        data_req: DataRequest
            Parameters of data request.

        Returns
        -------
        pd.DataFrame
            DataFrame with market data.
        """
        self.data_req = data_req
        self._convert_params()
        
        # Determine what types of data to fetch based on requested fields
        ohlcv_fields = {'open', 'high', 'low', 'close', 'volume'}
        requested_fields = set(data_req.fields)
        
        needs_ohlcv = bool(ohlcv_fields.intersection(requested_fields))
        needs_funding = 'funding_rate' in requested_fields
        needs_oi = 'oi' in requested_fields
        
        dfs_to_combine = []
        
        # Fetch OHLCV data if needed
        if needs_ohlcv:
            ohlcv_df = self._fetch_tidy_ohlcv()
            if not ohlcv_df.empty:
                dfs_to_combine.append(ohlcv_df)
        
        # Fetch funding rates if needed
        if needs_funding:
            funding_df = self._fetch_tidy_funding_rates()
            if not funding_df.empty:
                dfs_to_combine.append(funding_df)
        
        # Fetch open interest if needed
        if needs_oi:
            oi_df = self._fetch_tidy_open_interest()
            if not oi_df.empty:
                dfs_to_combine.append(oi_df)
        
        # Combine all DataFrames
        if not dfs_to_combine:
            return pd.DataFrame()
        elif len(dfs_to_combine) == 1:
            return dfs_to_combine[0]
        else:
            # Combine multiple DataFrames on their common index (date, ticker)
            # Use proper merge strategy for different data frequencies
            combined_df = dfs_to_combine[0]
            for df in dfs_to_combine[1:]:
                # Use merge instead of concat to handle different frequencies better
                combined_df = combined_df.merge(df, left_index=True, right_index=True, how='outer')
            
            # Filter to only requested fields
            available_cols = [col for col in data_req.fields if col in combined_df.columns]
            if available_cols:
                combined_df = combined_df[available_cols]
            
            return combined_df
