from typing import Dict, Optional

import numpy as np
import pandas as pd
from prophet import Prophet
from statsmodels.tsa.seasonal import STL, seasonal_decompose


class OutlierDetection:
    """
    Detects outliers.

    """

    def __init__(self, raw_df: pd.DataFrame):
        """
        Constructor

        Parameters
        ----------
        raw_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and raw data/values (cols).

        """
        self.raw_df = raw_df

    def atr(
        self,
        log: bool = False,
        window_size: int = 7,
        model_type: str = "estimation",
        thresh_val: int = 2,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers using OHLC values and H-L range.

        Parameters
        ----------
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
        plot: bool, default False
            Plots series with outliers highlighted (red dots).
        plot_series: tuple, default ('BTC', 'close')
            The specific time series to plot given by (ticker, field/column) tuple.

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # sort index and create df copy
        df = self.raw_df.sort_index(level=1)

        # log
        if log:
            df0 = np.log(df).copy()
        else:
            df0 = df.copy()

        # ohlc
        if not all(col in df.columns for col in ["open", "high", "low", "close"]):
            raise Exception("Dataframe must have OHLC prices to compute ATR.")

        # compute true range
        df0["hl"], df0["hc"], df0["lc"] = (
            (df0.high - df.low).abs(),
            (df0.high - df.close.shift(1)).abs(),
            (df0.low - df.close.shift(1)).abs(),
        )
        df0["tr"] = df0.loc[:, "hl":"lc"].max(axis=1)

        # compute ATR for estimation and prediction models
        if model_type == "estimation":
            df0["atr"] = (
                df0.tr.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .mean()
                .sort_index()
            )
            med = (
                df0.loc[:, :"volume"]
                .groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .median()
                .sort_index()
            )
        else:
            df0["atr"] = (
                df0.tr.groupby(level=1).ewm(span=window_size).mean().droplevel(0)
            )
            med = (
                df0.loc[:, :"volume"]
                .groupby(level=1)
                .rolling(window_size)
                .median()
                .droplevel(0)
            )

        # compute dev, score and upper/lower
        dev = df0.loc[:, :"volume"] - med
        score = dev.divide(df0.atr, axis=0)
        upper, lower = thresh_val, thresh_val * -1

        # outliers
        out_df = df[(score > upper) | (score < lower)]
        filt_df = df[(score < upper) & (score > lower)]

        # log
        if log:
            med = np.exp(med)

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": med.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def iqr(
        self,
        log: bool = True,
        window_size: int = 7,
        model_type: str = "estimation",
        thresh_val: int = 1.5,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers using interquartile range (IQR) method.

        Parameters
        ----------
        log: bool, default True
            Converts series into log of series.
        window_size: int, default 7
            Number of observations in the rolling window.
        model_type: str, {'estimation', 'prediction'}, default 'estimation'
            Estimation models use past, current and future values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t+s].
            Prediction models use only past and current values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t].
        thresh_val: int, default 1.5
            Value for upper and lower thresholds used in outlier detection.
            Computed as: IQR x thresh_val +/- 75th/25th percentiles (upper/lower bands), respectively.
        plot: bool, default False
            Plots series with outliers highlighted (red dots).
        plot_series: tuple, default ('BTC', 'close')
            The specific time series to plot given by (ticker, field/column) tuple.

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # sort index and create df copy
        df = self.raw_df.sort_index(level=1)

        # log
        if log:
            df0 = np.log(df).copy()
        else:
            df0 = df.copy()

        # compute 75th, 50th and 25th percentiles for estimation and prediction models
        if model_type == "estimation":
            perc_75th = (
                df0.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .quantile(0.75)
            )
            perc_25th = (
                df0.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .quantile(0.25)
            )
            med = (
                df0.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .median()
            )
        else:
            perc_75th = (
                df0.groupby(level=1).rolling(window_size).quantile(0.75).droplevel(0)
            )
            perc_25th = (
                df0.groupby(level=1).rolling(window_size).quantile(0.25).droplevel(0)
            )
            med = df0.groupby(level=1).rolling(window_size).median().droplevel(0)

        # compute iqr and upper/lower thresholds
        iqr = perc_75th - perc_25th
        upper = perc_75th.add(thresh_val * iqr, axis=1)
        lower = perc_25th.subtract(thresh_val * iqr, axis=1)

        # detect outliers
        out_df = df[(df0 > upper) | (df0 < lower)]
        filt_df = df[(df0 < upper) & (df0 > lower)]

        # log
        if log:
            med = np.exp(med)

        # type conversion
        med = med.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": med.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def mad(
        self,
        log: bool = True,
        window_size: int = 7,
        model_type: str = "estimation",
        thresh_val: int = 10,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers using a median absolute deviation method, aka Hampler filter.

        Parameters
        ----------
        log: bool, default True
            Converts series into log of series.
        window_size: int, default 7
            Number of observations in the rolling window.
        model_type: str, {'estimation', 'prediction'}, default 'estimation'
            Estimation models use past, current and future values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t+s].
            Prediction models use only past and current values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t].
        thresh_val: int, default 10
            Value for upper and lower thresholds used in outlier detection.
            Computed as: [median - thresh_val * mad, median + thresh_val * mad] for lower/upper thresholds.
        plot: bool, default False
            Plots series with outliers highlighted (red dots).
        plot_series: tuple, default ('BTC', 'close')
            The specific time series to plot given by (ticker, field/column) tuple.

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # sort index and create df copy
        df = self.raw_df.sort_index(level=1).copy()

        # log
        if log:
            df0 = np.log(df)
        else:
            df0 = df

        # compute median for estimation and prediction models
        if model_type == "estimation":
            med = (
                df0.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .median()
            )
        else:
            med = df0.groupby(level=1).rolling(window_size).median().droplevel(0)

        # compute dev, mad, upper/lower thresholds
        dev = df0 - med
        mad = dev.abs().groupby(level=1).rolling(window_size).median().droplevel(0)
        upper = med.add(thresh_val * mad, axis=1)
        lower = med.subtract(thresh_val * mad, axis=1)

        # outliers
        out_df = df[(df0 > upper) | (df0 < lower)]
        filt_df = df[(df0 < upper) & (df0 > lower)]

        # log
        if log:
            med = np.exp(med)

        # type conversion
        med = med.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": med.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def z_score(
        self,
        log: bool = True,
        window_size: int = 7,
        model_type: str = "estimation",
        thresh_val: int = 2,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers using a z-score method, aka simple moving average.

        Parameters
        ----------
        log: bool, default True
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
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific ticker/field combination (tuple).

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # sort index and create copy
        df = self.raw_df.sort_index(level=1).copy()

        # log
        if log:
            df0 = np.log(df)
        else:
            df0 = df

        # compute rolling mean and std for estimation and prediction models
        if model_type == "estimation":
            roll_mean = (
                df0.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .mean()
            )
            roll_std = (
                df0.groupby(level=1)
                .shift(-1 * int((window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(window_size, min_periods=1)
                .std()
            )
        else:
            roll_mean = (
                df0.groupby(level=1)
                .rolling(window_size, min_periods=1)
                .mean()
                .droplevel(0)
            )
            roll_std = (
                df0.groupby(level=1)
                .rolling(window_size, min_periods=1)
                .std()
                .droplevel(0)
            )

        # compute z-score and upper/lower thresh
        z = (df0 - roll_mean) / roll_std
        upper = thresh_val
        lower = thresh_val * -1

        # outliers
        out_df = df[(z > upper) | (z < lower)]
        filt_df = df[(z < upper) & (z > lower)]

        # log
        if log:
            roll_mean = np.exp(roll_mean)

        # type conversion
        roll_mean = roll_mean.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": roll_mean.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def ewma(
        self,
        log: bool = True,
        window_size: int = 7,
        thresh_val: int = 1.5,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers using an exponential moving average method.

        Parameters
        ----------
        log: bool, default True
            Converts series into log of series.
        window_size: int, default 7
            Number of observations in the rolling window.
        thresh_val: int, default 1.5
            Value for upper and lower thresholds used in outlier detection.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific ticker/field combination (tuple).

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # sort index and create copy
        df = self.raw_df.sort_index(level=1).copy()

        # log
        if log:
            df0 = np.log(df)
        else:
            df0 = df

        # compute ew ma and std for estimation and prediction models
        ewma = df0.groupby(level=1).ewm(span=window_size).mean().droplevel(0)
        ewstd = df0.groupby(level=1).ewm(span=window_size).std().droplevel(0)

        # compute z-score and upper/lower thresh
        z = (df0 - ewma) / ewstd
        upper = thresh_val
        lower = thresh_val * -1

        # outliers
        out_df = df[(z > upper) | (z < lower)]
        filt_df = df[(z < upper) & (z > lower)]

        # log
        if log:
            ewma = np.exp(ewma)

        # type conversion
        ewma = ewma.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": ewma.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def seasonal_decomp(
        self,
        log: bool = True,
        thresh_val: int = 5,
        period: int = 7,
        model: str = "additive",
        filt: Optional[np.array] = None,
        two_sided: Optional[bool] = True,
        extrapolate_trend: Optional[int] = 0,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers with seasonal decomposition moving averages from statsmodels.

        https://www.statsmodels.org/dev/generated/statsmodels.tsa.seasonal.seasonal_decompose.html#statsmodels.tsa.seasonal.seasonal_decompose

        Parameters
        ----------
        log: bool, default True
            Converts series into log of series.
        thresh_val: int, default 5
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
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # unstack
        df = self.raw_df.unstack().copy()
        # original idx, unstacked idx
        mult_idx, idx = self.raw_df.index, df.index

        # log
        if log:
            df0 = np.log(df)
        else:
            df0 = df

        # store resid dfs in dict
        resid_dict, yhat_dict = {}, {}
        for field in df0.columns.get_level_values(0).unique():
            resid_df, yhat_df = pd.DataFrame(index=idx), pd.DataFrame(index=idx)
            for ticker in df0[field].columns:
                # decompose
                res = seasonal_decompose(
                    df0[field][ticker].dropna(),
                    period=period,
                    model=model,
                    filt=filt,
                    two_sided=two_sided,
                    extrapolate_trend=extrapolate_trend,
                )
                resid_vals, trend_vals = res.resid.fillna(0), res.trend.ffill()
                # add to dfs
                resid_df = pd.concat(
                    [resid_df, resid_vals], join="outer", axis=1
                ).rename(columns={"resid": ticker})
                yhat_df = pd.concat([yhat_df, trend_vals], join="outer", axis=1).rename(
                    columns={"trend": ticker}
                )
                # convert to datetimeindex and rename
                resid_df.index, yhat_df.index = pd.to_datetime(
                    resid_df.index
                ), pd.to_datetime(yhat_df.index)
                resid_df.index.name, yhat_df.index.name = "date", "date"

            # normalize resid using mad
            dev = resid_df - resid_df.median()
            mad = dev.abs().median()
            dev_df = dev / mad

            # add fcst and resid dfs to dict
            resid_dict[field], yhat_dict[field] = dev_df, yhat_df

        # convert dict to multiindex
        resid_df, yhat_df = pd.concat(resid_dict, axis=1), pd.concat(yhat_dict, axis=1)

        # log
        if log:
            yhat_df = np.exp(yhat_df)

        # filter outliers
        out_df = df[resid_df.abs() > thresh_val]
        filt_df = df[resid_df.abs() < thresh_val]

        # stack and reindex
        out_df = out_df.stack().reindex(mult_idx)
        filt_df = filt_df.stack().reindex(mult_idx)
        yhat_df = yhat_df.stack().reindex(mult_idx)

        # convert dtypes
        yhat_df = yhat_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": yhat_df.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def stl(
        self,
        log: bool = True,
        thresh_val: int = 5,
        period: Optional[int] = 7,
        seasonal: Optional[int] = 7,
        trend: Optional[int] = None,
        low_pass: Optional[int] = None,
        seasonal_deg: Optional[int] = 1,
        trend_deg: Optional[int] = 1,
        low_pass_deg: Optional[int] = 1,
        robust: Optional[bool] = False,
        seasonal_jump: Optional[int] = 1,
        trend_jump: Optional[int] = 1,
        low_pass_jump: Optional[int] = 1,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers with seasonal decomposition moving averages from statsmodels.

        https://www.statsmodels.org/dev/generated/statsmodels.tsa.seasonal.seasonal_decompose.html#statsmodels.tsa.seasonal.seasonal_decompose

        Parameters
        ----------
        log: bool, default True
            Converts series into log of series.
        thresh_val: int, default 5
            Value for upper and lower thresholds used in outlier detection.
        period: int, optional, default 7
            Periodicity of the sequence.
        seasonal: int, optional, default 7
            Length of the seasonal smoother. Must be an odd integer, and should normally be >= 7.
        trend: int, optional, default None
            Length of the trend smoother. Must be an odd integer.
            If not provided uses the smallest odd integer greater than 1.5 * period / (1 - 1.5 / seasonal),
            following the suggestion in the original implementation.
        low_pass: int, optional, default None
            Length of the low-pass filter. Must be an odd integer >=3. If not provided,
            uses the smallest odd integer > period.
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
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # unstack
        df = self.raw_df.unstack().copy()
        # original idx, unstacked idx
        mult_idx, idx = self.raw_df.index, df.index

        # log
        if log:
            df0 = np.log(df)
        else:
            df0 = df

        # store resid dfs in dict
        resid_dict, yhat_dict = {}, {}
        for field in df0.columns.get_level_values(0).unique():
            resid_df, yhat_df = pd.DataFrame(index=idx), pd.DataFrame(index=idx)
            for ticker in df0[field].columns:
                # decompose
                res = STL(
                    df0[field][ticker].dropna(),
                    period=period,
                    seasonal=seasonal,
                    trend=trend,
                    low_pass=low_pass,
                    seasonal_deg=seasonal_deg,
                    trend_deg=trend_deg,
                    low_pass_deg=low_pass_deg,
                    robust=robust,
                    seasonal_jump=seasonal_jump,
                    trend_jump=trend_jump,
                    low_pass_jump=low_pass_jump,
                ).fit()
                resid_vals, trend_vals, seas_vals = res.resid, res.trend, res.seasonal
                # add to dfs
                resid_df = pd.concat(
                    [resid_df, resid_vals], join="outer", axis=1
                ).rename(columns={"resid": ticker})
                yhat_df = pd.concat([yhat_df, trend_vals], join="outer", axis=1).rename(
                    columns={"trend": ticker}
                )
                # convert to datetimeindex and rename
                resid_df.index, yhat_df.index = pd.to_datetime(
                    resid_df.index
                ), pd.to_datetime(yhat_df.index)
                resid_df.index.name, yhat_df.index.name = "date", "date"

            # normalize resid using mad
            dev = resid_df - resid_df.median()
            mad = dev.abs().median()
            dev_df = dev / mad

            # add fcst and resid dfs to dict
            resid_dict[field], yhat_dict[field] = dev_df, yhat_df

        # convert dict to multiindex
        resid_df, yhat_df = pd.concat(resid_dict, axis=1), pd.concat(yhat_dict, axis=1)

        # log
        if log:
            yhat_df = np.exp(yhat_df)

        # filter outliers
        out_df = df[resid_df.abs() > thresh_val]
        filt_df = df[resid_df.abs() < thresh_val]

        # stack and reindex
        out_df = out_df.stack().reindex(mult_idx)
        filt_df = filt_df.stack().reindex(mult_idx)
        yhat_df = yhat_df.stack().reindex(mult_idx)

        # convert dtypes
        yhat_df = yhat_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": yhat_df.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def prophet(
        self,
        log: bool = True,
        interval_width: Optional[float] = 0.99,
        plot: bool = False,
        plot_series: tuple = ("BTC", "close"),
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers using Prophet, a time series forecasting algorithm published by Facebook.

        Parameters
        ----------
        log: bool, default True
            Converts series into log of series.
        interval_width: float, optional, default 0.99
            Uncertainty interval estimated by Monte Carlo simulation. The larger the value,
            the larger the upper/lower thresholds interval for outlier detection.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.

        Returns
        -------
        outliers_dict: Dictionary of pd.DataFrame - MultiIndex
            Dictionary of forecasts (yhat), outliers (outliers) and filtered values (filt_vals) multiindex dataframes
            with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with forecasted, outlier or filtered
            values.

        """
        # unstack
        df = self.raw_df.unstack().copy()
        # original idx, unstacked idx
        mult_idx, idx = self.raw_df.index, df.index

        # log
        if log:
            df0 = np.log(df)
        else:
            df0 = df

        # store predictions for fields dfs in dict
        upper_dict, lower_dict, yhat_dict = {}, {}, {}
        for field in df0.columns.get_level_values(0).unique():
            upper_df, lower_df, yhat_df = (
                pd.DataFrame(index=idx),
                pd.DataFrame(index=idx),
                pd.DataFrame(index=idx),
            )
            for ticker in df0[field].columns:
                # format cols for prophet
                df1 = (
                    df0[field][ticker]
                    .to_frame()
                    .reset_index()
                    .rename(columns={"date": "ds", ticker: "y"})
                )
                # fit model
                m = Prophet(interval_width=interval_width)
                m = m.fit(df1)
                # forecast
                pred = m.predict(df1)
                # convert to datetimeindex and rename
                pred["date"] = pd.to_datetime(pred.ds)
                pred.set_index("date", inplace=True)
                # add to dfs
                upper_df = pd.concat(
                    [upper_df, pred["yhat_upper"]], join="outer", axis=1
                ).rename(columns={"yhat_upper": ticker})
                lower_df = pd.concat(
                    [lower_df, pred["yhat_lower"]], join="outer", axis=1
                ).rename(columns={"yhat_lower": ticker})
                yhat_df = pd.concat(
                    [yhat_df, pred["yhat"]], join="outer", axis=1
                ).rename(columns={"yhat": ticker})

            # add fcst adfs to dict
            upper_dict[field], lower_dict[field], yhat_dict[field] = (
                upper_df,
                lower_df,
                yhat_df,
            )

        # convert dict to multiindex
        yhat_upper, yhat_lower, yhat = (
            pd.concat(upper_dict, axis=1),
            pd.concat(lower_dict, axis=1),
            pd.concat(yhat_dict, axis=1),
        )
        # rename cols
        yhat_upper.columns.names = [None, "ticker"]
        yhat_lower.columns.names = [None, "ticker"]
        yhat.columns.names = [None, "ticker"]

        # transform log
        if log:
            yhat_upper = np.exp(yhat_upper)
            yhat_lower = np.exp(yhat_lower)
            yhat = np.exp(yhat)

        # filter outliers
        yhat_df = yhat
        out_df = df[df.gt(yhat_upper) | df.lt(yhat_lower)]
        filt_df = df[df.lt(yhat_upper) & df.gt(yhat_lower)]

        # stack and reindex
        yhat_df = yhat_df.stack().reindex(mult_idx)
        out_df = out_df.stack().reindex(mult_idx)
        filt_df = filt_df.stack().reindex(mult_idx)

        # convert dtypes
        yhat_df = yhat_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        # plot
        if plot:
            if not isinstance(plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers(out_df, plot_series=plot_series)

        outliers_dict = {
            "yhat": yhat_df.sort_index(),
            "outliers": out_df.sort_index(),
            "filt_vals": filt_df.sort_index(),
        }

        return outliers_dict

    def plot_outliers(
        self, outliers_df: pd.DataFrame, plot_series: Optional[tuple] = None
    ) -> None:
        """
        Plots time series with outliers highlighted (red dots).

        Parameters
        ----------
        outliers_df: pd.DataFrame - MultiIndex
            Dataframe MultiIndex with DatetimeIndex (level 0), tickers (level 1) and fields (cols) outlier values.
        plot_series: tuple, optional, default None
            Plots the time series of a specific (ticker, field) tuple.

        """
        ax = (
            self.raw_df.loc[pd.IndexSlice[:, plot_series[0]], plot_series[1]]
            .droplevel(1)
            .plot(linewidth=1, figsize=(15, 7), color="#1f77b4", zorder=0)
        )
        outliers_df.unstack()[plot_series[1]].reset_index().plot(
            kind="scatter",
            x="date",
            y=plot_series[0],
            color="#E64E53",
            ax=ax,
            label="outliers",
        )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend(
            [plot_series[1] + "_raw", plot_series[1] + "_outliers"], loc="upper left"
        )
