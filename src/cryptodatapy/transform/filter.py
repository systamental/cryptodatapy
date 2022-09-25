from typing import Optional, Union

import numpy as np
import pandas as pd


class Filter:
    """
    Filters dataframe in tidy format.

    """

    def __init__(
        self, raw_df: pd.DataFrame, excl_cols: Optional[Union[str, list]] = None
    ):
        """
        Constructor

        Parameters
        ----------
        raw_df: pd.DataFrame - MultiIndex
            Dataframe with raw data. DatetimeIndex (level 0), ticker (level 1) and raw data (cols), in tidy format.
        excl_cols: str or list, default None
            Name of columns to exclude from filtering

        """

        self.raw_df = raw_df
        self.excl_cols = excl_cols

    def outliers(
        self,
        outliers_dict: dict,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> pd.DataFrame:
        """
        Filters outliers, replacing them with NaNs.

        Parameters
        ----------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        filt_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.

        """
        # filter outliers
        filt_df = outliers_dict["filt_vals"]

        # add excl cols
        if self.excl_cols is not None:
            filt_df = pd.concat(
                [filt_df, self.raw_df[self.excl_cols]], join="outer", axis=1
            )

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_filtered(filt_df, plot_series=plot_series)

        return filt_df

    def avg_trading_val(
        self,
        thresh_val: int = 10000000,
        window_size: int = 30,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> pd.DataFrame:
        """
        Filters values below a threshold of average trading value (price * volume/size in quote currency) over some
        lookback window, replacing them with NaNs.

        Parameters
        ----------
        thresh_val: int, default 10,000,000
            Threshold/cut-off for avg trading value.
        window_size: int, default 30
            Size of rolling window.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        filt_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with values below the
            threshold removed.

        """
        # convert string to list
        if self.excl_cols is not None:
            df = self.raw_df.drop(columns=self.excl_cols).copy()
        else:
            df = self.raw_df.copy()

        # compute traded val
        if "close" in df.columns and "volume" in df.columns:
            df["trading_val"] = df.close * df.volume
        elif ("bid" in df.columns and "ask" in df.columns) and (
            "bid_size" in df.columns and "ask_size" in df.columns
        ):
            df["trading_val"] = ((df.bid + df.ask) / 2) * (
                (df.bid_size + df.ask_size) / 2
            )
        elif "trade_size" in df.columns and "trade_price" in df.columns:
            df["trading_val"] = df.trade_price * df.trade_size
        else:
            raise Exception(
                "Dataframe must include at least one price series (e.g. close price, trade price, "
                "ask/bid price) and size series (e.g. volume, trade_size, bid_size/ask_size, ..."
            )

        # compute rolling mean/avg
        df1 = df.groupby(level=1).rolling(window_size).mean().droplevel(0)
        # divide by thresh
        df1 = df1 / thresh_val
        # filter df1
        filt_df = (
            df.loc[df1.trading_val > 1].reindex(df.index).drop(columns="trading_val")
        )
        # add excl cols
        if self.excl_cols is not None:
            filt_df = pd.concat(
                [filt_df, self.raw_df[self.excl_cols]], join="outer", axis=1
            )

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_filtered(filt_df, plot_series=plot_series)

        return filt_df

    def missing_vals_gaps(
        self,
        gap_window: int = 30,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> pd.DataFrame:
        """
        Filters values before a large gap of missing values, replacing them with NaNs.

        Parameters
        ----------
        gap_window: int, default 30
            Size of window where all values are missing (NaNs).
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        filt_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with values before
            missing values gaps removed.

        """
        # convert string to list
        if self.excl_cols is not None:
            df = self.raw_df.drop(columns=self.excl_cols).copy()
        else:
            df = self.raw_df.copy()

        # window obs count
        window_count = (
            df.groupby(level=1)
            .rolling(window=gap_window, min_periods=gap_window)
            .count()
            .droplevel(0)
        )
        gap = window_count[window_count == 0]
        # valid start idx
        for col in gap.unstack().columns:
            start_idx = gap.unstack()[col].last_valid_index()
            if start_idx is not None:
                df.loc[pd.IndexSlice[:start_idx, col[1]], col[0]] = np.nan

        # add excl cols
        if self.excl_cols is not None:
            filt_df = pd.concat([df, self.raw_df[self.excl_cols]], join="outer", axis=1)
        else:
            filt_df = df

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_filtered(filt_df, plot_series=plot_series)

        return filt_df

    def min_nobs(self, min_obs=100) -> pd.DataFrame:
        """
        Removes tickers from dataframe if the ticker has less than a minimum number of observations.

        Parameters
        ----------
        min_obs: int, default 100
            Minimum number of observations for field/column.

        Returns
        -------
        filt_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers with minimum number of observations (level 1)
            and fields (cols).

        """
        # create copy
        df = self.raw_df.copy()

        # drop tickers with nobs < min_obs
        nobs = df.groupby(level=1).count().min(axis=1)
        drop_tickers_list = nobs[nobs < min_obs].index.to_list()
        filt_df = df.drop(drop_tickers_list, level=1, axis=0)

        return filt_df

    def tickers(self, tickers_list) -> pd.DataFrame:
        """
        Removes specified tickers from dataframe.

        Parameters
        ----------
        tickers_list: str or list
            List of tickers to be removed. Can be used to remove tickers to be excluded from data analysis,
            e.g. stablecoins or indexes.

        Returns
        -------
        filt_df: pd.DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols).

        """
        # create copy
        df = self.raw_df.copy()
        # tickers list
        if isinstance(tickers_list, str):
            tickers_list = [tickers_list]
        # drop tickers
        filt_df = df.drop(tickers_list, level=1, axis=0)

        return filt_df

    @staticmethod
    def plot_filtered(
        filt_df: pd.DataFrame, plot_series: Optional[tuple] = None
    ) -> None:
        """
        Plots filtered time series.

        Parameters
        ----------
        filt_df: pd.DataFrame - MultiIndex
            Dataframe MultiIndex with DatetimeIndex (level 0), tickers (level 1) and filtered values (cols).
        plot_series: tuple, optional, default None
            Plots the time series of a specific (ticker, field) tuple.

        """
        ax = (
            filt_df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
            .droplevel(1)
            .plot(linewidth=1, figsize=(15, 7), color="#1f77b4", zorder=0)
        )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.set_ylabel(plot_series[0])
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend([plot_series[1] + "_filtered"], loc="upper left")
