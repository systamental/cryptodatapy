from typing import Optional

import numpy as np
import pandas as pd


class Impute:
    """
    Handles missing values.

    """

    def __init__(self, filt_df: pd.DataFrame):

        """
        Constructor

        Parameters
        ----------
        filt_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with filtered values.

        """
        self.filt_df = filt_df

    def fwd_fill(
        self, plot: bool = False, plot_series: tuple = ("BTC", "close")
    ) -> pd.DataFrame:
        """
        Imputes missing values by imputing missing values with latest non-missing values.

        Parameters
        ----------
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        imp_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values
            using forward fill method.

        """
        # copy df
        filt_df = self.filt_df.copy()

        # ffill
        imp_df = filt_df.groupby(level=1).ffill()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_imputed(imp_df, plot_series=plot_series)

        return imp_df

    def interpolate(
        self,
        method: str = "linear",
        order: Optional[int] = None,
        axis=0,
        limit: Optional[int] = None,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> pd.DataFrame:
        """
        Imputes missing values by interpolating using various methods.

        Parameters
        ----------
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
        imp_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values
            using interpolation method.

        """
        # copy df and convert to float for interpolation (code will break if type int64)
        filt_df = self.filt_df.astype(float).copy()

        # add order if spline or polynomial
        if (method == "spline" or method == "polynomial") and order is None:
            order = 3

        # interpolate
        imp_df = (
            filt_df.unstack()
            .interpolate(method=method, order=order, axis=axis, limit=limit)
            .stack()
            .reindex(filt_df.index)
        )

        # type conversion
        imp_df = imp_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_imputed(imp_df, plot_series=plot_series)

        return imp_df

    def fcst(
        self,
        fcst_df: pd.DataFrame,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> pd.DataFrame:
        """
        Imputes missing values with forecasts from outlier detection algorithm.

        Parameters
        ----------
        fcst_df: pd.DataFrame - MultiIndex
            Multiindex dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols)
            with forecasted values.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        imp_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values
            using forecasts from outlier detection method.

        """
        # copy filtered and forecast dfs
        filt_df, yhat_df = self.filt_df.copy(), fcst_df.copy()

        # impute missing vals in filtered df with fcst vals
        imp_yhat = np.where(filt_df.isna(), yhat_df, filt_df)
        # create df
        imp_df = pd.DataFrame(imp_yhat, index=filt_df.index, columns=filt_df.columns)

        # type conversion
        imp_df = imp_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_imputed(imp_df, plot_series=plot_series)

        return imp_df

    @staticmethod
    def plot_imputed(imp_df: pd.DataFrame, plot_series: Optional[tuple] = None) -> None:
        """
        Plots filtered time series.

        Parameters
        ----------
        imp_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values.
        plot_series: tuple, optional, default None
            Plots the time series of a specific (ticker, field) tuple.

        """
        ax = (
            imp_df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
            .droplevel(1)
            .plot(linewidth=1, figsize=(15, 7), color="#1f77b4", zorder=0)
        )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.set_ylabel(plot_series[0])
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend([plot_series[1] + "_repaired"], loc="upper left")
