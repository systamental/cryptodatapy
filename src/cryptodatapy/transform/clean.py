from __future__ import annotations
from typing import Optional, Union
import pandas as pd
from cryptodatapy.transform.od import OutlierDetection
from cryptodatapy.transform.impute import Impute
from cryptodatapy.transform.filter import Filter


def stitch_dataframes(dfs):
    """
    Stitches together dataframes with different start dates.

    Parameters
    ----------
    dfs: list
        List of dataframes to be stitched together.

    Returns
    -------
    combined_df: pd.DataFrame
        Combined dataframe with extended start date.
    """
    # check if dfs is a list
    if not isinstance(dfs, list):
        raise TypeError("Dataframes must be a list.")

    # check index types
    if all([isinstance(df.index, pd.MultiIndex) for df in dfs]):
        dfs.sort(key=lambda df: df.index.levels[0][0], reverse=True)
    elif all([isinstance(df.index, pd.DatetimeIndex) for df in dfs]):
        dfs.sort(key=lambda df: df.index[0], reverse=True)
    else:
        raise TypeError("Dataframes must be pd.MultiIndex or have DatetimeIndex.")

    # most recent start date
    combined_df = dfs[0]

    # combine dfs
    for df in dfs[1:]:
        combined_df = combined_df.combine_first(df)

    # reorder cols
    max_columns = max(len(df.columns) for df in dfs)
    cols = next(df.columns.tolist() for df in dfs if len(df.columns) == max_columns)
    combined_df = combined_df[cols]

    return combined_df


class CleanData:
    """
    Cleans data to improve data quality.
    """
    def __init__(self, df: pd.DataFrame):
        """
        Constructor

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and field (cols) values.
        """
        self.raw_df = df.copy()  # keepy copy of raw dataframe
        self.df = df
        self.excluded_cols = None
        self.outliers = None
        self.yhat = None
        self.repaired_df = None
        self.filtered_df = None
        self.filtered_tickers = None
        self.summary = pd.DataFrame()
        self.initialize_summary()
        self.check_types()

    def initialize_summary(self) -> None:
        """
        Initializes summary dataframe with data quality metrics.
        """
        # add obs and missing vals
        self.summary.loc["n_obs", self.df.unstack().columns] = self.df.unstack().notna().sum().values
        self.summary.loc["%_NaN_start", self.df.unstack().columns] = \
            (self.df.unstack().isnull().sum() / self.df.unstack().shape[0]).values * 100

    def check_types(self) -> None:
        """
        Checks data types of columns and converts them to the appropriate data types.

        Returns
        -------
        CleanData
            CleanData object
        """
        if not isinstance(self.df, pd.DataFrame):
            raise TypeError("Data must be a pandas DataFrame.")

    def filter_outliers(
        self,
        od_method: str = "mad",
        excl_cols: Optional[Union[str, list]] = None,
        **kwargs
    ) -> CleanData:
        """
        Filters outliers.

        Parameters
        ----------
        od_method: str, {'atr', 'iqr', 'mad', 'z_score', 'ewma', 'stl', 'seasonal_decomp', 'prophet'}, default z_score
            Outlier detection method to use for filtering.
        excl_cols: str or list
            Name of columns to exclude from outlier filtering.

        Returns
        -------
        CleanData
            CleanData object
        """
        # outlier detection
        od = OutlierDetection(self.df, excl_cols=excl_cols, **kwargs)
        self.excluded_cols = excl_cols

        # filter outliers
        getattr(od, od_method)()
        self.filtered_df = od.filtered_df
        self.outliers = od.outliers
        self.yhat = od.yhat

        # add to summary
        self.summary.loc["%_outliers", self.outliers.unstack().columns] = (
            self.outliers.unstack().notna().sum() / od.df.unstack().notna().sum()
        ).values * 100

        # filtered df
        self.df = self.filtered_df.sort_index()

        return self

    def repair_outliers(self, imp_method: str = "interpolate", **kwargs) -> CleanData:
        """
        Repairs outliers using an imputation method.

        Parameters
        ----------
        imp_method: str, {"fwd_fill', 'interpolate', 'fcst'}, default 'fwd_fill'
            Imputation method used to replace filtered outliers.

        Returns
        -------
        CleanData
            CleanData object
        """
        # impute missing vals
        if imp_method == "fcst":
            self.repaired_df = getattr(Impute(self.df), imp_method)(self.yhat, **kwargs)
        else:
            self.repaired_df = getattr(Impute(self.df), imp_method)(**kwargs)

        # add repaired % to summary
        rep_vals = self.repaired_df.unstack().notna().sum() - self.df.unstack().notna().sum()
        self.summary.loc["%_imputed", self.df.unstack().columns] = rep_vals / self.df.unstack().notna().sum() * 100

        # repaired df
        if self.excluded_cols is not None:
            self.df = pd.concat([self.repaired_df, self.raw_df[self.excluded_cols]], join="inner", axis=1)
        else:
            self.df = self.repaired_df

        # reorder cols
        self.df = self.df[self.raw_df.columns].sort_index()

        return self

    def filter_avg_trading_val(self, thresh_val: int = 10000000, window_size: int = 30) -> CleanData:
        """
        Filters values below a threshold of average trading value (price * volume/size in quote currency) over some
        lookback window, replacing them with NaNs.

        Parameters
        ----------
        thresh_val: int, default 10,000,000
            Threshold/cut-off for avg trading value.
        window_size: int, default 30
            Size of rolling window.

        Returns
        -------
        CleanData
            CleanData object
        """
        # filter outliers
        self.filtered_df = Filter(self.df).avg_trading_val(thresh_val=thresh_val, window_size=window_size)

        # add to summary
        filtered_vals = self.df.unstack().notna().sum() - self.filtered_df.unstack().notna().sum()
        self.summary.loc["%_below_avg_trading_val", self.df.unstack().columns] = \
            (filtered_vals / self.df.unstack().notna().sum()).values * 100

        # filtered df
        self.df = self.filtered_df.sort_index()

        return self

    def filter_missing_vals_gaps(self, gap_window: int = 30) -> CleanData:
        """
        Filters values before a large gap of missing values, replacing them with NaNs.

        Parameters
        ----------
        gap_window: int, default 30
            Size of window where all values are missing (NaNs).

        Returns
        -------
        CleanData
            CleanData object
        """
        # filter outliers
        self.filtered_df = Filter(self.df).missing_vals_gaps(gap_window=gap_window)

        # add to summary
        missing_vals_gap = (
            self.df.unstack().notna().sum() - self.filtered_df.unstack().notna().sum()
        )
        self.summary.loc["%_missing_vals_gaps", self.df.unstack().columns] = (
            missing_vals_gap / self.df.unstack().notna().sum()
        ).values * 100

        # filtered df
        self.df = self.filtered_df.sort_index()

        return self

    def filter_min_nobs(self, ts_obs: int = 100, cs_obs: int = 2) -> CleanData:
        """
        Removes tickers from dataframe if the ticker has less than a minimum number of observations.

        Parameters
        ----------
        ts_obs: int, default 100
            Minimum number of observations for field/column over time series.
        cs_obs: int, default 5
            Minimum number of observations for tickers over the cross-section.

        Returns
        -------
        CleanData
            CleanData object
        """
        # filter outliers
        self.filtered_df = Filter(self.df).min_nobs(ts_obs=ts_obs, cs_obs=cs_obs)

        # tickers < min obs
        self.filtered_tickers = list(
            set(self.filtered_df.index.droplevel(0).unique()).symmetric_difference(
                set(self.df.index.droplevel(0).unique())
            )
        )

        # add to summary
        self.summary.loc["n_filtered_tickers", self.df.unstack().columns] = len(self.filtered_tickers)

        # filtered df
        self.df = self.filtered_df.sort_index()

        return self

    def filter_delisted_tickers(self, method: str = 'replace') -> CleanData:
        """
        Removes delisted tickers from dataframe.

        Parameters
        ----------
        method: str, {'replace', 'remove'}, default 'replace'
            Method to use for handling delisted tickers.

        Returns
        -------
        CleanData
            CleanData object
        """
        # filter tickers
        self.filtered_df = Filter(self.df).delisted_tickers(method=method)

        # tickers < min obs
        self.filtered_tickers = list(
            set(self.filtered_df.index.droplevel(0).unique()).symmetric_difference(
                set(self.df.index.droplevel(0).unique())
            )
        )

        # add to summary
        filtered_vals = (
            self.df.unstack().notna().sum() - self.filtered_df.unstack().notna().sum()
        )
        self.summary.loc["%_delisted_ticker_vals", self.df.unstack().columns] = (
            filtered_vals / self.df.unstack().notna().sum()
        ).values * 100
        self.summary.loc["n_filtered_tickers", self.df.unstack().columns] = len(self.filtered_tickers)

        # filtered df
        self.df = self.filtered_df.sort_index()

        return self

    def filter_tickers(self, tickers_list) -> CleanData:
        """
        Removes specified tickers from dataframe.

        Parameters
        ----------
        tickers_list: str or list
            List of tickers to be removed. Can be used to remove tickers to be excluded from data analysis,
            e.g. stablecoins or indexes.

        Returns
        -------
        CleanData
            CleanData object
        """
        # filter tickers
        self.filtered_df = Filter(self.df).tickers(tickers_list)

        # tickers < min obs

        self.filtered_tickers = list(
            set(self.filtered_df.index.droplevel(0).unique()).symmetric_difference(
                set(self.df.index.droplevel(0).unique())
            )
        )

        # add to summary
        self.summary.loc["n_filtered_tickers", self.df.unstack().columns] = len(self.filtered_tickers)

        # filtered df
        self.df = self.filtered_df.sort_index()

        return self

    def show_plot(self, plot_series: tuple = ("BTC", "close"), compare_series: bool = True) -> None:
        """
        Plots clean time series and compares it to the raw series.

        Parameters
        ----------
        plot_series: tuple, optional, default('BTC', 'close')
            Plots the time series of a specific (ticker, field) tuple.
        compare_series: bool, default True
            Compares clean time series with raw series
        """
        ax = (
            self.df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
            .droplevel(1)
            .plot(
                linewidth=1,
                figsize=(15, 7),
                color="#1f77b4",
                zorder=0,
                title="Filtered vs. Raw Data",
            )
        )
        if compare_series:
            ax = (
                self.raw_df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
                .droplevel(1)
                .plot(
                    linewidth=1,
                    figsize=(15, 7),
                    linestyle=":",
                    color="#FF8785",
                    zorder=0,
                )
            )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.set_ylabel(plot_series[0])
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend(
            [plot_series[1] + "_filtered", plot_series[1] + "_raw"], loc="upper left"
        )

    def get(self, attr="df") -> pd.DataFrame:
        """
        Returns GetData object attribute.

        Parameters
        ----------
        attr: str, {'df', 'outliers', 'yhat', 'filtered_tickers', 'summary'}, default 'df'
            GetData object attribute to return

        Returns
        -------
        CleanData
            CleanData object
        """
        self.summary.loc["%_NaN_end", self.df.unstack().columns] = (
            self.df.unstack().isnull().sum() / self.df.unstack().shape[0]
        ).values * 100
        self.summary = self.summary.astype(float).round(2)

        return getattr(self, attr)
