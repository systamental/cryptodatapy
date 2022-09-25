from __future__ import annotations
from typing import Optional, Union

import pandas as pd

from cryptodatapy.transform.filter import Filter
from cryptodatapy.transform.impute import Impute
from cryptodatapy.transform.od import OutlierDetection


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
        self.start_df = df.copy()  # keepy copy of raw dataframe
        self.df = df
        self.outliers = None  # outliers
        self.fcsts = None  # forecasts
        self.filtered_tickers = []  # filtered tickers
        self.summary = pd.DataFrame()  # summary of cleaning ops
        # add obs and missing vals
        self.summary.loc["n_obs", self.df.unstack().columns] = (
            self.df.unstack().notna().sum().values
        )
        self.summary.loc["%_NaN_start", self.df.unstack().columns] = (
            self.df.unstack().isnull().sum() / self.df.unstack().shape[0]
        ).values * 100

    def filter_outliers(
        self,
        excl_cols: Optional[Union[str, list]] = None,
        od_method: str = "z_score",
        **kwargs
    ) -> CleanData:
        """
        Filters outliers.

        Parameters
        ----------
        excl_cols: str or list
            Name of columns to exclude from outlier filtering.
        od_method: str, {'atr', 'iqr', 'mad', 'z_score', 'ewma', 'stl', 'seasonal_decomp', 'prophet'}, default z_score
            Outlier detection method to use for filtering.

        Other Parameters
        ----------------
        log: bool, default False
            Converts series into log of series.
        window_size: int, default 7
            Number of observations in the rolling window.
        model_type: str, {'estimation', 'prediction'}, default 'estimation'
            Estimation models use past, current and future values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t+s].
            Prediction models use only past and current values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t].
        thresh_val: int, default 2
            Value for upper and lower thresholds used in outlier detection.
        period: int, optional, default 7
            periodicity of the sequence.
        model: str, {'additive', 'multiplicative'}, default 'additive'
            Type of seasonal component.
        filt: array-like, optional, default None
            The filter coefficients for filtering out the seasonal component.
            The concrete moving average method used in filtering is determined by two_sided.
        two_sided: bool, optional, default True
            The moving average method used in filtering. If True (default), a centered moving average is
            computed using the filt. If False, the filter coefficients are for past values only.
        extrapolate_trend: int, optional, default 0
            If set to > 0, the trend resulting from the convolution is linear least-squares extrapolated
            on both ends (or the single one if two_sided is False) considering this many (+1) closest points.
            If set to ‘freq’, use freq closest points. Setting this parameter results in no NaN values in trend
            or resid components.
        seasonal_deg: int, optional, default 1
            Degree of seasonal LOESS. 0 (constant) or 1 (constant and trend).
        trend_deg: int, optional, default 1
            Degree of trend LOESS. 0 (constant) or 1 (constant and trend).
        low_pass_deg: int, optional, default 1
            Degree of low pass LOESS. 0 (constant) or 1 (constant and trend).
        robust: bool, optional, default False
            Flag indicating whether to use a weighted version that is robust to some forms of outliers.
        seasonal_jump: int, optional, default 1
            Positive integer determining the linear interpolation step. If larger than 1,
            the LOESS is used every seasonal_jump points and linear interpolation is between fitted points.
            Higher values reduce estimation time.
        trend_jump: int, optional, default 1
            Positive integer determining the linear interpolation step. If larger than 1,
            the LOESS is used every trend_jump points and values between the two are linearly interpolated.
            Higher values reduce estimation time.
        low_pass_jump: int, optional, default 1
            Positive integer determining the linear interpolation step. If larger than 1,
            the LOESS is used every low_pass_jump points and values between the two are linearly interpolated.
            Higher values reduce estimation time.
        interval_width: float, optional, default 0.99
            Uncertainty interval estimated by Monte Carlo simulation. The larger the value,
            the larger the upper/lower thresholds interval for outlier detection.
        plot: bool, default False
            Plots series with outliers highlighted (red dots).
        plot_series: tuple, default ('BTC', 'close')
            The specific time series to plot given by (ticker, field/column) tuple.

        Returns
        -------
        CleanData
            CleanData object

        """
        # outlier detection
        od = getattr(OutlierDetection(self.df), od_method)(**kwargs)
        # add outliers and fcst to obj
        self.outliers = od["outliers"]
        self.fcsts = od["yhat"]
        # filter outliers
        filt_df = Filter(self.df, excl_cols=excl_cols).outliers(od)
        # add to summary
        self.summary.loc["%_outliers", self.df.unstack().columns] = (
            od["outliers"].unstack().notna().sum() / self.df.unstack().shape[0]
        ).values * 100
        # filtered df
        self.df = filt_df

        return self

    def repair_outliers(
        self, imp_method: str = "interpolate", **kwargs
    ) -> CleanData:
        """
        Repairs outliers using an imputation method.

        Parameters
        ----------
        imp_method: str, {"fwd_fill', 'interpolate', 'fcst'}, default 'fwd_fill'
            Imputation method used to replace filtered outliers.

        Other Parameters
        ----------------
        method: str, {'linear', ‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘spline’, ‘barycentric’,
                      ‘polynomial’, ‘krogh’, ‘piecewise_polynomial’, ‘pchip’, ‘akima’, ‘cubicspline’}, default spline
            Interpolation method to use.
        order: int, optional, default None
            Order of polynomial or spline.
        axis: {{0 or ‘index’, 1 or ‘columns’, None}}, default None
            Axis to interpolate along.
        limit: int, optional, default None
            Maximum number of consecutive NaNs to fill. Must be greater than 0.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        CleanData
            CleanData object

        """
        # impute missing vals
        if imp_method == "fcst":
            rep_df = getattr(Impute(self.df), imp_method)(self.fcsts, **kwargs)
        else:
            rep_df = getattr(Impute(self.df), imp_method)(**kwargs)
        # add repaired % to summary
        rep_vals = rep_df.unstack().notna().sum() - self.df.unstack().notna().sum()
        self.summary.loc["%_imputed", self.df.unstack().columns] = (
            rep_vals / self.df.unstack().shape[0]
        ) * 100
        # repaired df
        self.df = rep_df

        return self

    def filter_avg_trading_val(
        self, thresh_val: int = 10000000, window_size: int = 30, **kwargs
    ) -> CleanData:
        """
        Filters values below a threshold of average trading value (price * volume/size in quote currency) over some
        lookback window, replacing them with NaNs.

        Parameters
        ----------
        thresh_val: int, default 10,000,000
            Threshold/cut-off for avg trading value.
        window_size: int, default 30
            Size of rolling window.

        Other Parameters
        ----------------
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        CleanData
            CleanData object

        """
        # filter outliers
        filt_df = Filter(self.df).avg_trading_val(
            thresh_val=thresh_val, window_size=window_size, **kwargs
        )
        # add to summary
        filt_vals = self.df.unstack().notna().sum() - filt_df.unstack().notna().sum()
        self.summary.loc["%_below_avg_trading_val", self.df.unstack().columns] = (
            filt_vals / self.df.unstack().shape[0]
        ).values * 100
        # filtered df
        self.df = filt_df

        return self

    def filter_missing_vals_gaps(self, gap_window: int = 30, **kwargs) -> CleanData:
        """
        Filters values before a large gap of missing values, replacing them with NaNs.

        Parameters
        ----------
        gap_window: int, default 30
            Size of window where all values are missing (NaNs).

        Other Parameters
        ----------------
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        CleanData
            CleanData object

        """
        # filter outliers
        filt_df = Filter(self.df).missing_vals_gaps(gap_window=gap_window, **kwargs)
        # add to summary
        missing_vals_gap = (
            self.df.unstack().notna().sum() - filt_df.unstack().notna().sum()
        )
        self.summary.loc["%_missing_vals_gaps", self.df.unstack().columns] = (
            missing_vals_gap / self.df.unstack().shape[0]
        ).values * 100
        # filtered df
        self.df = filt_df

        return self

    def filter_min_nobs(self, min_obs=100) -> CleanData:
        """
        Removes tickers from dataframe if the ticker has less than a minimum number of observations.

        Parameters
        ----------
        min_obs: int, default 100
            Minimum number of observations for field/column.

        Returns
        -------
        CleanData
            CleanData object

        """
        # filter outliers
        filt_df = Filter(self.df).min_nobs(min_obs=min_obs)
        # tickers < min obs
        filt_tickers = list(
            set(filt_df.index.droplevel(0).unique()).symmetric_difference(
                set(self.df.index.droplevel(0).unique())
            )
        )
        # add to obj
        if len(filt_tickers) != 0:
            self.filtered_tickers.extend(filt_tickers)
        self.summary.loc["n_tickers_below_min_obs", self.df.unstack().columns] = len(
            filt_tickers
        )
        # filtered df
        self.df = filt_df

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
        filt_df = Filter(self.df).tickers(tickers_list)
        # tickers < min obs
        filt_tickers = list(
            set(filt_df.index.droplevel(0).unique()).symmetric_difference(
                set(self.df.index.droplevel(0).unique())
            )
        )
        # add to obj properties
        if len(filt_tickers) != 0:
            self.filtered_tickers.extend(filt_tickers)
        self.summary.loc["n_filtered_tickers", self.df.unstack().columns] = len(
            filt_tickers
        )
        # filtered df
        self.df = filt_df

        return self

    def show_plot(
        self, plot_series: tuple = ("BTC", "close"), compare_series: bool = True
    ) -> None:
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
                self.start_df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
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
        attr: str, {'df', 'outliers', 'fcst', 'filtered_tickers', 'summary'}, default 'df'
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
