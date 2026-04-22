import argparse
import pandas as pd
import numpy as np
import yaml
import json
import logging
import time
import sys
import os

def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    return config

def load_data(input_path):
    if not os.path.exists(input_path):
        raise FileNotFoundError("Input CSV not found")

    df = pd.read_csv(input_path)

    if df.empty:
        raise ValueError("CSV file is empty")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logger(args.log_file)
    start_time = time.time()

    try:
        logging.info("Job started")

        # Load config
        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)
        logging.info(f"Config loaded: {config}")

        # Load data
        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        # Rolling mean
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        # Signal generation
        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        # Drop NaNs for calculation
        valid_df = df.dropna()

        rows_processed = len(valid_df)
        signal_rate = valid_df["signal"].mean()

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        logging.info(f"Metrics: {metrics}")

        with open(args.output, "w") as f:
            json.dump(metrics, f, indent=2)

        print(json.dumps(metrics, indent=2))
        logging.info("Job completed successfully")

        sys.exit(0)

    except Exception as e:
        logging.error(str(e))

        error_metrics = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        with open(args.output, "w") as f:
            json.dump(error_metrics, f, indent=2)

        print(json.dumps(error_metrics, indent=2))

        sys.exit(1)

if __name__ == "__main__":
    main()