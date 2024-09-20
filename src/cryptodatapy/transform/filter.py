from typing import Optional, Union

import numpy as np
import pandas as pd


class Filter:
    """
    Filters dataframe in tidy format.
    """
    def __init__(self,
                 raw_df: pd.DataFrame,
                 excl_cols: Optional[Union[str, list]] = None,
                 plot: bool = False,
                 plot_series: tuple = ("BTC", "close")
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
        self.plot = plot
        self.plot_series = plot_series
        self.df = raw_df.copy() if excl_cols is None else raw_df.drop(columns=excl_cols).copy()
        self.filtered_df = None

    def avg_trading_val(
        self,
        thresh_val: int = 10000000,
        window_size: int = 30,
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

        Returns
        -------
        filtered_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with values below the
            threshold removed.
        """
        # compute traded val
        if "close" in self.df.columns and "volume" in self.df.columns:
            self.df["trading_val"] = self.df.close * self.df.volume
        elif ("bid" in self.df.columns and "ask" in self.df.columns) and (
            "bid_size" in self.df.columns and "ask_size" in self.df.columns
        ):
            self.df["trading_val"] = ((self.df.bid + self.df.ask) / 2) * (
                (self.df.bid_size + self.df.ask_size) / 2
            )
        elif "trade_size" in self.df.columns and "trade_price" in self.df.columns:
            self.df["trading_val"] = self.df.trade_price * self.df.trade_size
        else:
            raise Exception(
                "Dataframe must include at least one price series (e.g. close price, trade price, "
                "ask/bid price) and size series (e.g. volume, trade_size, bid_size/ask_size, ..."
            )

        # compute rolling mean/avg
        df1 = self.df.groupby(level=1).rolling(window_size).mean().droplevel(0)
        # divide by thresh
        df1 = df1 / thresh_val
        # filter df1
        self.filtered_df = self.df.loc[df1.trading_val > 1].reindex(self.df.index).drop(columns="trading_val")

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_filtered(plot_series=self.plot_series)

        # add excl cols
        if self.excl_cols is not None:
            self.filtered_df = pd.concat([self.filtered_df,
                                          self.raw_df[self.excl_cols].reindex(self.filtered_df.index)], axis=1)

        return self.filtered_df

    def missing_vals_gaps(self, gap_window: int = 30) -> pd.DataFrame:
        """
        Filters values before a large gap of missing values, replacing them with NaNs.

        Parameters
        ----------
        gap_window: int, default 30
            Size of window where all values are missing (NaNs).

        Returns
        -------
        filtered_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with values before
            missing values gaps removed.
        """
        # window obs count
        window_count = (
            self.df.groupby(level=1)
            .rolling(window=gap_window, min_periods=gap_window)
            .count()
            .droplevel(0)
        )
        gap = window_count[window_count == 0]
        # valid start idx
        for col in gap.unstack().columns:
            start_idx = gap.unstack()[col].last_valid_index()
            if start_idx is not None:
                self.df.loc[pd.IndexSlice[:start_idx, col[1]], col[0]] = np.nan

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_filtered(plot_series=self.plot_series)

        # add excl cols
        if self.excl_cols is not None:
            self.filtered_df = pd.concat([self.df,
                                          self.raw_df[self.excl_cols].reindex(self.df)], axis=1)
        else:
            self.filtered_df = self.df

        return self.filtered_df

    def min_nobs(self, ts_obs=100, cs_obs=1) -> pd.DataFrame:
        """
        Removes tickers from dataframe if the ticker has less than a minimum number of observations and removes
        dates if there is less than a minimum number of tickers.

        Parameters
        ----------
        ts_obs: int, default 100
            Minimum number of observations for field/column over time series.
        cs_obs: int, default 1
            Minimum number of observations for tickers over the cross-section.

        Returns
        -------
        filtered_df: DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers with minimum number of observations (level 1)
            and fields (cols).
        """
        # drop tickers with nobs < ts_obs
        obs = self.df.groupby(level=1).count().min(axis=1)
        drop_tickers_list = obs[obs < ts_obs].index.to_list()
        self.filtered_df = self.df.drop(drop_tickers_list, level=1, axis=0)

        # drop tickers with nobs < cs_obs
        obs = self.filtered_df.groupby(level=0).count().min(axis=1)
        idx_start = obs[obs > cs_obs].index[0]
        self.filtered_df = self.filtered_df.loc[idx_start:]

        return self.filtered_df

    def delisted_tickers(self, method: str = 'replace') -> pd.DataFrame:
        """
        Repairs delisted tickers by either removing them or replacing them with NaNs.

        Parameters
        ----------
        method: str, {'replace', 'remove'}, default 'replace'
            Method to repair delisted tickers. Can be 'remove' or 'replace'.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # unchanged rows
        unch_rows = (self.df.subtract(self.df.iloc[:, :4].mean(axis=1), axis=0) == 0).any(axis=1)

        # delisted tickers
        delisted_tickers = unch_rows.unstack().iloc[-1][unch_rows.unstack().iloc[-1]].index.to_list()

        # repair
        if method == 'remove':
            self.filtered_df = self.df.drop(delisted_tickers, level=1)
        else:
            self.filtered_df = self.df.loc[~unch_rows].reindex(self.df.index)

        return self.filtered_df

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
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataFrame with DatetimeIndex (level 0), tickers (level 1) and fields (cols).
        """
        # tickers list
        if isinstance(tickers_list, str):
            tickers_list = [tickers_list]

        # drop tickers
        self.filtered_df = self.df.drop(tickers_list, level=1)

        return self.filtered_df

    def plot_filtered(self, plot_series: Optional[tuple] = None) -> None:
        """
        Plots filtered time series.

        Parameters
        ----------
        plot_series: tuple, optional, default None
            Plots the time series of a specific (ticker, field) tuple.
        """
        ax = (
            self.filtered_df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
            .droplevel(1)
            .plot(linewidth=1, figsize=(15, 7), color="#1f77b4", zorder=0)
        )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.set_ylabel(plot_series[0])
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend([plot_series[1] + "_filtered"], loc="upper left")
