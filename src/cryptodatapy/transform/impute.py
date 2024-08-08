from typing import Optional

import numpy as np
import pandas as pd


class Impute:
    """
    Handles missing values.
    """
    def __init__(self, filtered_df: pd.DataFrame, plot: bool = False, plot_series: tuple = ("BTC", "close")):
        """
        Constructor

        Parameters
        ----------
        filtered_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with filtered values.
        """
        self.filtered_df = filtered_df.astype(float)
        self.plot = plot
        self.plot_series = plot_series
        self.imputed_df = None

    def fwd_fill(self) -> pd.DataFrame:
        """
        Imputes missing values by imputing missing values with latest non-missing values.

        Returns
        -------
        imputed_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values
            using forward fill method.
        """
        # ffill
        self.imputed_df = self.filtered_df.groupby(level=1).ffill()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_imputed()

        return self.imputed_df

    def interpolate(
        self,
        method: str = "linear",
        order: Optional[int] = None,
        axis: int = 0,
        limit: Optional[int] = None,
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

        Returns
        -------
        imputed_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values
            using interpolation method.
        """
        # add order if spline or polynomial
        if (method == "spline" or method == "polynomial") and order is None:
            order = 3

        # interpolate
        self.imputed_df = self.filtered_df.unstack().interpolate(method=method, order=order, axis=axis,
                                                                 limit=limit).stack().reindex(self.filtered_df.index)

        # type conversion
        self.imputed_df = self.imputed_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_imputed()

        return self.imputed_df

    def fcst(
        self,
        yhat_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Imputes missing values with forecasts from outlier detection algorithm.

        Parameters
        ----------
        yhat_df: pd.DataFrame - MultiIndex
            Multiindex dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols)
            with forecasted values.

        Returns
        -------
        imputed_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and fields (cols) with imputed values
            using forecasts from outlier detection method.
        """
        # impute missing vals in filtered df with fcst vals
        imp_yhat = np.where(self.filtered_df.isna(), yhat_df, self.filtered_df)
        # create df
        self.imputed_df = pd.DataFrame(imp_yhat, index=self.filtered_df.index, columns=self.filtered_df.columns)

        # type conversion
        self.imputed_df = self.imputed_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to plot (ticker, column)."
                )
            else:
                self.plot_imputed()

        return self.imputed_df

    def plot_imputed(self) -> None:
        """
        Plots filtered time series.
        """
        ax = (
            self.imputed_df.loc[pd.IndexSlice[:, self.plot_series[0]], self.plot_series[1]]
            .droplevel(1)
            .plot(linewidth=1, figsize=(15, 7), color="#1f77b4", zorder=0)
        )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.set_ylabel(self.plot_series[0])
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend([self.plot_series[1] + "_repaired"], loc="upper left")
