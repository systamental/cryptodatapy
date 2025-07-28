import pandas as pd
from typing import List


def compute_reference_price(dfs: List[pd.DataFrame],
                            method: str = 'median',
                            trim_pct: float = 0.25,
                            ) -> pd.DataFrame:
    """
    Computes the reference price from a list of dataframes.

    Parameters
    ----------
    dfs: pd.DataFrame
        List of dataframes containing price data.
    method: str, optional
        Method to compute the reference price. Options are 'median' or 'trimmed_mean'.
        Default is 'median'.
    trim_pct: float, optional
        Percentage of data to trim from both ends for 'trimmed_mean' method.
        Default is 0.25 (25%).
    Returns
    -------
    pd.DataFrame
        Dataframe with the reference price.
    """
    if not List:
        raise ValueError("The input list is empty.")

    # Concatenate all dataframes in the list
    stacked_df = pd.concat(dfs)

    # Compute ref price based on the specified method
    if method == 'median':
        ref_price = stacked_df.groupby(['date', 'ticker']).median()

    elif method == 'trimmed_mean':
        # Calculate trimmed mean with specified bounds
        lower_bound = stacked_df.groupby(level=[0, 1]).quantile(trim_pct)
        upper_bound = stacked_df.groupby(level=[0, 1]).quantile(1 - trim_pct)

        # Filter out values outside the bounds
        filtered_df = stacked_df[(stacked_df >= lower_bound.reindex(stacked_df.index)) &
                                 (stacked_df <= upper_bound.reindex(stacked_df.index))]

        ref_price = filtered_df.groupby(level=[0, 1]).mean()
    else:
        raise ValueError("Method must be either 'median' or 'trimmed_mean'.")

    return ref_price.sort_index()


def stitch_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Stitches together dataframes with different start dates.

    Parameters
    ----------
    df1: pd.DataFrame
        First dataframe to be stitched.
    df2: pd.DataFrame
        Second dataframe to be stitched.

    Returns
    -------
    combined_df: pd.DataFrame
        Combined or stitched dataframes with extended data.
    """
    # forward fill missing values
    updated_df = df1.reindex(index=df2.index, columns=df2.columns).fillna(df2)
    combined_df = df1.combine_first(updated_df)

    # Convert to float64 to ensure consistent data type
    combined_df = combined_df.convert_dtypes().astype('float64')

    return combined_df


def rebase_fx_to_foreign_vs_usd(df) -> pd.DataFrame:
    """
    Rebase FX rates to foreign currency vs. USD format, so that an increase
    means the foreign currency is appreciating. Works for both MultiIndex
    (date, ticker) and single-index (date index, tickers as columns).

    Parameters
    ----------
    df : pd.DataFrame
        FX DataFrame with either:
        - MultiIndex (date, ticker)
        - Datetime index and tickers as columns

    Returns
    -------
    pd.DataFrame
        Rebased FX rates with tickers as foreign currency (e.g., 'EUR', 'JPY').
    """
    df = df.copy()

    def get_foreign_currency(ticker: str) -> str:
        if ticker.startswith("USD"):
            return ticker[3:]  # USDJPY → JPY
        elif ticker.endswith("USD"):
            return ticker[:3]  # EURUSD → EUR
        else:
            raise ValueError(f"Unexpected ticker format: {ticker}")

    if isinstance(df.index, pd.MultiIndex):
        tickers = df.index.get_level_values(1)
        inverted = tickers.str.startswith("USD")

        # Invert rates for USDXXX
        df[inverted] = 1 / df[inverted]

        # Rename all tickers to just the foreign currency symbol
        new_tickers = tickers.map(get_foreign_currency)
        df.index = pd.MultiIndex.from_arrays(
            [df.index.get_level_values(0), new_tickers],
            names=df.index.names
        )

    else:
        # Single index (datetime), columns = tickers
        rebased = {}
        for col in df.columns:
            fx = get_foreign_currency(col)
            if col.startswith("USD"):
                rebased[fx] = 1 / df[col]
            else:
                rebased[fx] = df[col]

        df = pd.DataFrame(rebased, index=df.index)

    return df.sort_index()
