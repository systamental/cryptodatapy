from typing import Dict, Optional, Union

import numpy as np
np.float_ = np.float64
import pandas as pd
from prophet import Prophet
from statsmodels.tsa.seasonal import STL, seasonal_decompose


class OutlierDetection:
    """
    Detects outliers.
    """
    def __init__(self,
                 raw_df: pd.DataFrame,
                 excl_cols: Optional[Union[str, list]] = None,
                 log: bool = False,
                 window_size: int = 7,
                 model_type: str = 'estimation',
                 thresh_val: int = 5,
                 plot: bool = False,
                 plot_series: tuple = ('BTC', 'close')
                 ):
        """
        Constructor

        Parameters
        ----------
        raw_df: pd.DataFrame - MultiIndex
            DataFrame MultiIndex with DatetimeIndex (level 0), ticker (level 1) and raw data/values (cols).
        excl_cols: str or list, optional, default None
            Columns to exclude from outlier detection.
        log: bool, default False
            Log transform the series.
        window_size: int, default 7
            Number of observations in the rolling window.
        model_type: str, {'estimation', 'prediction'}, default 'estimation'
            Estimation models use past, current and future values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t+s].
            Prediction models use only past and current values to estimate the expected value of a series,
            e.g. expected x_t of series x at time t uses values from [x_t-s, x_t].
        thresh_val: int, default 5
            Value for upper and lower thresholds used in outlier detection.
        plot: bool, default False
            Plots series with outliers highlighted with red dots.
        plot_series: tuple, default ('BTC', 'close')
            Plots the time series of a specific (ticker, field/column) tuple.
        """
        self.raw_df = raw_df
        self.excl_cols = excl_cols
        self.log = log
        self.window_size = window_size
        self.model_type = model_type
        self.thresh_val = thresh_val
        self.plot = plot
        self.plot_series = plot_series
        self.df = raw_df.copy() if excl_cols is None else raw_df.drop(columns=excl_cols).copy()
        self.yhat = None
        self.outliers = None
        self.filtered_df = None
        self.log_transform()

    def log_transform(self) -> None:
        """
        Log transform the dataframe.
        """
        if self.log:
            # remove negative values
            self.df[self.df <= 0] = np.nan
            # log and replace inf
            self.df = np.log(self.df).replace([np.inf, -np.inf], np.nan)

    def atr(self) -> pd.DataFrame:
        """
        Detects outliers using OHLC values and H-L range.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # ohlc
        if not all(col in self.df.columns for col in ["open", "high", "low", "close"]):
            raise Exception("Dataframe must have OHLC prices to compute ATR.")

        # df copy
        df0 = self.df.copy()

        # compute true range
        df0["hl"], df0["hc"], df0["lc"] = (
            (df0.high - df0.low).abs(),
            (df0.high - df0.close.groupby(level=1).shift(1)).abs(),
            (df0.low - df0.close.groupby(level=1).shift(1)).abs(),
        )
        df0["tr"] = df0.loc[:, "hl":"lc"].max(axis=1)

        # compute ATR for estimation and prediction models
        if self.model_type == "estimation":
            df0["atr"] = (
                df0.tr.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .mean()
                .sort_index()
            )
            med = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .median()
                .sort_index()
            )
        else:
            df0["atr"] = (
                df0.tr.groupby(level=1).ewm(span=self.window_size).mean().droplevel(0)
            )
            med = (
                df0.groupby(level=1)
                .rolling(self.window_size)
                .median()
                .droplevel(0)
            )

        # compute dev and score for outliers
        dev = df0 - med
        score = dev.divide(df0.atr, axis=0)

        # outliers
        self.outliers = self.df[score.abs() > self.thresh_val].sort_index()
        self.filtered_df = self.df[score.abs() < self.thresh_val].sort_index()

        # log to original scale
        if self.log:
            self.yhat = np.exp(med).sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def iqr(self) -> pd.DataFrame:
        """
        Detects outliers using interquartile range (IQR) method.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # sort index
        df0 = self.df.sort_index(level=1)

        # compute 75th, 50th and 25th percentiles for estimation and prediction models
        if self.model_type == "estimation":
            perc_75th = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .quantile(0.75)
            )
            perc_25th = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .quantile(0.25)
            )
            med = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .median()
            )
        else:
            perc_75th = (
                df0.groupby(level=1).rolling(self.window_size).quantile(0.75).droplevel(0)
            )
            perc_25th = (
                df0.groupby(level=1).rolling(self.window_size).quantile(0.25).droplevel(0)
            )
            med = df0.groupby(level=1).rolling(self.window_size).median().droplevel(0)

        # compute iqr and upper/lower thresholds
        iqr = perc_75th - perc_25th
        upper = perc_75th.add(self.thresh_val * iqr, axis=1)
        lower = perc_25th.subtract(self.thresh_val * iqr, axis=1)

        # detect outliers
        out_df = self.df[(df0 > upper) | (df0 < lower)]
        filt_df = self.df[(df0 < upper) & (df0 > lower)]

        # log to original scale
        if self.log:
            med = np.exp(med)

        # type conversion
        self.yhat = med.apply(pd.to_numeric, errors='coerce').convert_dtypes().sort_index()
        self.outliers = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes().sort_index()
        self.filtered_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes().sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def mad(self) -> pd.DataFrame:
        """
        Detects outliers using a median absolute deviation method, aka Hampler filter.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # sort index and create df copy
        df0 = self.df.sort_index(level=1).copy()

        # compute median for estimation and prediction models
        if self.model_type == "estimation":
            med = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .median()
            )
        else:
            med = df0.groupby(level=1).rolling(self.window_size).median().droplevel(0)

        # compute dev, mad, upper/lower thresholds
        dev = df0 - med
        mad = dev.abs().groupby(level=1).rolling(self.window_size).median().droplevel(0)
        upper = med.add(self.thresh_val * mad, axis=1)
        lower = med.subtract(self.thresh_val * mad, axis=1)

        # outliers
        out_df = self.df[(df0 > upper) | (df0 < lower)]
        filt_df = self.df[(df0 < upper) & (df0 > lower)]

        # log to original scale
        if self.log:
            med = np.exp(med)

        # type conversion
        med = med.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        self.yhat = med.sort_index()
        self.outliers = out_df.sort_index()
        self.filtered_df = filt_df.sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def z_score(self) -> pd.DataFrame:
        """
        Detects outliers using a z-score method, aka simple moving average.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # sort index and create copy
        df0 = self.df.sort_index(level=1).copy()

        # compute rolling mean and std for estimation and prediction models
        if self.model_type == "estimation":
            roll_mean = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .mean()
            )
            roll_std = (
                df0.groupby(level=1)
                .shift(-1 * int((self.window_size + 1) / 2))
                .sort_index(level=1)
                .rolling(self.window_size, min_periods=1)
                .std()
            )
        else:
            roll_mean = (
                df0.groupby(level=1)
                .rolling(self.window_size, min_periods=1)
                .mean()
                .droplevel(0)
            )
            roll_std = (
                df0.groupby(level=1)
                .rolling(self.window_size, min_periods=1)
                .std()
                .droplevel(0)
            )

        # compute z-score and upper/lower thresh
        z = (df0 - roll_mean) / roll_std

        # outliers
        out_df = self.df[z.abs() > self.thresh_val]
        filt_df = self.df[z.abs() < self.thresh_val]

        # log to original scale
        if self.log:
            roll_mean = np.exp(roll_mean)

        # type conversion
        roll_mean = roll_mean.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        self.yhat = roll_mean.sort_index()
        self.outliers = out_df.sort_index()
        self.filtered_df = filt_df.sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def ewma(self) -> pd.DataFrame:
        """
        Detects outliers using an exponential moving average method.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # sort index and create copy
        df0 = self.df.sort_index(level=1).copy()

        # compute ew ma and std for estimation and prediction models
        ewma = df0.groupby(level=1).ewm(span=self.window_size).mean().droplevel(0)
        ewstd = df0.groupby(level=1).ewm(span=self.window_size).std().droplevel(0)

        # compute z-score and upper/lower thresh
        z = (df0 - ewma) / ewstd

        # outliers
        out_df = self.df[z.abs() > self.thresh_val]
        filt_df = self.df[z.abs() < self.thresh_val]

        # log to original scale
        if self.log:
            ewma = np.exp(ewma)

        # type conversion
        ewma = ewma.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors='coerce').convert_dtypes()

        self.yhat = ewma.sort_index()
        self.outliers = out_df.sort_index()
        self.filtered_df = filt_df.sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def seasonal_decomp(
        self,
        period: int = 7,
        model: str = "additive",
        filt: Optional[np.array] = None,
        two_sided: Optional[bool] = True,
        extrapolate_trend: Optional[int] = 0,
    ) -> Dict[str, pd.DataFrame]:
        """
        Detects outliers with seasonal decomposition moving averages from statsmodels.

        https://www.statsmodels.org/dev/generated/statsmodels.tsa.seasonal.seasonal_decompose.html#statsmodels.tsa.seasonal.seasonal_decompose

        Parameters
        ----------
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

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # unstack
        df0 = self.df.unstack().copy()
        # original idx, unstacked idx
        mult_idx, idx = self.raw_df.index, self.df.unstack().index

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

        # log to original scale
        if self.log:
            yhat_df = np.exp(yhat_df)

        # filter outliers
        out_df = self.df.unstack()[resid_df.abs() > self.thresh_val]
        filt_df = self.df.unstack()[resid_df.abs() < self.thresh_val]

        # stack and reindex
        out_df = out_df.stack().reindex(mult_idx)
        filt_df = filt_df.stack().reindex(mult_idx)
        yhat_df = yhat_df.stack().reindex(mult_idx)

        # convert dtypes
        yhat_df = yhat_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        self.yhat = yhat_df.sort_index()
        self.outliers = out_df.sort_index()
        self.filtered_df = filt_df.sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def stl(
        self,
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
    ) -> pd.DataFrame:
        """
        Detects outliers with seasonal decomposition moving averages from statsmodels.

        https://www.statsmodels.org/dev/generated/statsmodels.tsa.seasonal.seasonal_decompose.html#statsmodels.tsa.seasonal.seasonal_decompose

        Parameters
        ----------
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

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # unstack
        df0 = self.df.unstack().copy()
        # original idx, unstacked idx
        mult_idx, idx = self.df.index, self.df.unstack().index

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

        # log to original scale
        if self.log:
            yhat_df = np.exp(yhat_df)

        # filter outliers
        out_df = self.df.unstack()[resid_df.abs() > self.thresh_val]
        filt_df = self.df.unstack()[resid_df.abs() < self.thresh_val]

        # stack and reindex
        out_df = out_df.stack().reindex(mult_idx)
        filt_df = filt_df.stack().reindex(mult_idx)
        yhat_df = yhat_df.stack().reindex(mult_idx)

        # convert dtypes
        yhat_df = yhat_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        self.yhat = yhat_df.sort_index()
        self.outliers = out_df.sort_index()
        self.filtered_df = filt_df.sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def prophet(self, interval_width: Optional[float] = 0.999) -> pd.DataFrame:
        """
        Detects outliers using Prophet, a time series forecasting algorithm published by Facebook.

        Parameters
        ----------
        interval_width: float, optional, default 0.99
            Uncertainty interval estimated by Monte Carlo simulation. The larger the value,
            the larger the upper/lower thresholds interval for outlier detection.

        Returns
        -------
        filtered_df: pd.DataFrame - MultiIndex
            Filtered dataframe with DatetimeIndex (level 0), tickers (level 1) and fields (cols) with outliers removed.
        """
        # unstack
        df0 = self.raw_df.unstack().copy()
        # original idx, unstacked idx
        mult_idx, idx = self.raw_df.index, df0.index

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
        if self.log:
            yhat_upper = np.exp(yhat_upper)
            yhat_lower = np.exp(yhat_lower)
            yhat = np.exp(yhat)

        # filter outliers
        yhat_df = yhat
        out_df = self.df.unstack()[self.df.unstack().gt(yhat_upper) | self.df.unstack().lt(yhat_lower)]
        filt_df = self.df.unstack()[self.df.unstack().lt(yhat_upper) & self.df.unstack().gt(yhat_lower)]

        # stack and reindex
        yhat_df = yhat_df.stack().reindex(mult_idx)
        out_df = out_df.stack().reindex(mult_idx)
        filt_df = filt_df.stack().reindex(mult_idx)

        # convert dtypes
        yhat_df = yhat_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        out_df = out_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()
        filt_df = filt_df.apply(pd.to_numeric, errors="ignore").convert_dtypes()

        self.yhat = yhat_df.sort_index()
        self.outliers = out_df.sort_index()
        self.filtered_df = filt_df.sort_index()

        # plot
        if self.plot:
            if not isinstance(self.plot_series, tuple):
                raise TypeError(
                    "Plot_series must be a tuple specifying the ticker and column/field to "
                    "plot (ticker, column)."
                )
            else:
                self.plot_outliers()

        return self.filtered_df

    def plot_outliers(self) -> None:
        """
        Plots time series with outliers highlighted (red dots).
        """
        ax = (
            self.df.loc[pd.IndexSlice[:, self.plot_series[0]], self.plot_series[1]]
            .droplevel(1)
            .plot(linewidth=1, figsize=(15, 7), color="#1f77b4", zorder=0)
        )
        self.outliers.unstack()[self.plot_series[1]].reset_index().plot(
            kind="scatter",
            x="date",
            y=self.plot_series[0],
            color="#E64E53",
            ax=ax,
            label="outliers",
        )
        ax.grid(color="black", linewidth=0.05)
        ax.xaxis.grid(False)
        ax.ticklabel_format(style="plain", axis="y")
        ax.set_facecolor("whitesmoke")
        ax.legend(
            [self.plot_series[1] + "_raw", self.plot_series[1] + "_outliers"], loc="upper left"
        )
