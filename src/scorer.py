import logging
from pathlib import Path

import pandas as pd
import yaml
from catboost import CatBoostClassifier

from .preprocess import preprocess_data

logger = logging.getLogger(__name__)
cfg = yaml.safe_load(open("configs/config.yaml"))
model = CatBoostClassifier()
model.load_model(Path(cfg["paths"]["models_dir"]) / "model.cbm")
threshold = cfg["inference"]["threshold"]

logger.info("Pretrained model imported successfully...")


def make_pred(raw_df: pd.DataFrame, source_info: str = "kafka"):
    X = preprocess_data(raw_df, cfg=cfg)
    proba = model.predict_proba(X)[:, 1]
    fraud = (proba >= threshold).astype(int)
    logger.info(f"Prediction complete for data from {source_info}")
    return proba, fraud
