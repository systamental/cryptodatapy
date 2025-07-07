import pandas as pd


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
        # MultiIndex: (date, ticker)
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
