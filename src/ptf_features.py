import pandas as pd


def build_ptf_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shared PTF feature engineering for training and inference.

    Callers are responsible for calling dropna() after this function returns,
    since lag/rolling columns introduce NaN rows during the warm-up window and
    the appropriate subset to drop differs between training and inference.
    """
    df = df.copy()
    df["hour"]        = df.index.hour
    df["day_of_week"] = df.index.dayofweek
    df["month"]       = df.index.month
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)

    df["ptf_lag_24h"]  = df["ptf_try"].shift(24)
    df["ptf_lag_168h"] = df["ptf_try"].shift(168)

    # Supply shock columns come from a LEFT JOIN keyed by outage *start* date, so
    # most rows are NULL.  Forward-fill propagates the last known value; fillna(0)
    # handles the case where no outage history exists at all.
    for col in ["supply_shock_index", "total_outage_mwh", "total_available_capacity_mwh"]:
        if col in df.columns:
            df[col] = df[col].ffill().fillna(0.0)

    if "supply_shock_index" in df.columns:
        df["supply_shock_trend_7d"] = df["supply_shock_index"].rolling(168).mean()
    else:
        df["supply_shock_trend_7d"] = 0.0

    return df
