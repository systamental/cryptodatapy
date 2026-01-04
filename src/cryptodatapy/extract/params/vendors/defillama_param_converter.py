import logging
from typing import Dict, Any, List
import pandas as pd

from cryptodatapy.core.data_request import DataRequest
from cryptodatapy.extract.params.base_param_converter import BaseParamConverter

logger = logging.getLogger(__name__)


class DefiLlamaParamConverter(BaseParamConverter):
    """
    Converts a standard DataRequest object into the specific set of parameters
    required for DefiLlama data extraction.

    DefiLlama typically provides daily data (e.g., TVL, historical prices)
    and uses protocol slugs or coin IDs in the URL path.
    """

    def __init__(
            self,
            data_req: DataRequest,
            assets: pd.DataFrame,
            fields: pd.DataFrame,
    ):
        """
        Initializes the converter with the data request object.

        Parameters
        ----------
        data_req : DataRequest
            The standard CryptoDataPy data request object.
        assets : pd.DataFrame
            DataFrame containing asset metadata for mapping tickers to DefiLlama-specific identifiers.
        fields : pd.DataFrame
            DataFrame containing field metadata for mapping standard fields
            to DefiLlama-specific query parameters.
        """
        super().__init__(data_req)
        self.assets = assets
        self.fields = fields

    def _convert_tickers(self) -> Dict[str, dict]:
        """
        Converts standard ticker symbols to DefiLlama-specific identifiers.

        Returns
        -------
        Dict[str, dict]
            A dictionary with ticker as key and a dict of DefiLlama-specific identifiers.
        """
        # convert to upper
        tickers = self.data_req.tickers
        self.assets.loc['ALL'] = {'type': 'all', 'category': 'all', 'slug': ''}
        all_tickers = self.assets.index.tolist()
        dl_tickers, missing = [], []

        # check tickers
        for ticker in tickers:
            # case sensitive
            if ticker in all_tickers:
                dl_tickers.append(ticker)
            elif ticker.upper() in all_tickers:
                dl_tickers.append(ticker.upper())
            else:
                missing.append(ticker)

        if missing:
            logger.error(f"Tickers not found in DefiLlama assets: {missing}")
            raise ValueError(f"Tickers not found in DefiLlama assets: {missing}")

        return self.assets.loc[dl_tickers, ['type', 'category', 'slug']].to_dict('index')

    def _convert_fields(self) -> Dict[str, dict]:
        """
        Converts standard fields to DefiLlama-specific query parameters.

        Returns
        -------
        Dict[str, Any]
            A dictionary of DefiLlama-specific query parameters.
        """
        # check fields
        if not all(field in self.fields.index for field in self.data_req.fields):
            missing = [field for field in self.data_req.fields if field not in self.fields.index]
            logger.error(f"Fields not found in DefiLlama fields: {missing}")
            raise ValueError(f"Fields not found in DefiLlama fields: {missing}")

        return self.fields.loc[self.data_req.fields, :].to_dict('index')

    def convert(self) -> Dict[str, Any]:
        """
        Converts the DataRequest object into a dictionary of DefiLlama parameters.

        This dictionary will contain both query parameters and path information
        (like the endpoint and slug) for the Adapter to construct the final URL.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing vendor-specific parameters.
        """
        # mapping data
        ticker_map = self._convert_tickers()
        field_map = self._convert_fields()

        # requests
        request_list: List[Dict[str, Any]] = []

        for ticker, t_meta in ticker_map.items():

            asset_type = t_meta['type']

            for field in self.data_req.fields:

                f_config = field_map.get(field)

                if not f_config:
                    logger.warning(f"Field '{field}' not found for DefiLlama.")
                    continue

                # resolve the vendor-specific path based on asset type
                vendor_endpoint = f_config.get(asset_type)
                query_params = f_config.get('params', {})

                if not vendor_endpoint:
                    logger.error(
                        f"Missing endpoint path for {ticker} with field '{field}' and asset type '{asset_type}'. "
                        f"Skipping request..."
                    )
                    continue

                # Construct the concrete request definition
                request_dict = {
                    "ticker": ticker,
                    "field": field,
                    "type": asset_type,
                    "category": t_meta['category'],
                    "slug": t_meta['slug'],
                    "endpoint": vendor_endpoint,
                    "query_params": query_params.copy()  # Use .copy() for safety
                }

                request_list.append(request_dict)

        return request_list

        # return {"requests": request_list}
