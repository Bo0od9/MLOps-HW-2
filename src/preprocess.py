import logging
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from haversine import haversine_vector

logger = logging.getLogger(__name__)


def preprocess_data(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    logger.info("Старт препроцессинга...")
    df = df.copy()

    df["transaction_time"] = pd.to_datetime(df["transaction_time"])
    df["hour"] = df["transaction_time"].dt.hour
    df["dayofweek"] = df["transaction_time"].dt.dayofweek

    coords1 = np.column_stack([df["lat"].to_numpy(), df["lon"].to_numpy()])
    coords2 = np.column_stack([df["merchant_lat"].to_numpy(), df["merchant_lon"].to_numpy()])
    df["distance_km"] = haversine_vector(coords1, coords2)

    arts = Path(cfg["paths"]["artifacts_dir"])
    feat_order_path = arts / "feature_order.joblib"
    if feat_order_path.exists():
        feature_order = joblib.load(feat_order_path)
    else:
        feature_order = cfg["data"]["cat_cols"] + cfg["data"]["text_cols"] + cfg["data"]["num_cols"]

    missing = [c for c in feature_order if c not in df.columns]
    if missing:
        raise ValueError(f"Отсутствуют колонки: {missing}")
    df = df[feature_order]
    logger.info(f"Финальный shape = {df.shape}")
    return df
