import argparse
import yaml
import pandas as pd
import numpy as np
import json
import logging
import time
import sys
from typing import Any, Dict


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def write_metrics(output_path, data):
    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)


def load_and_validate_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("Invalid config structure: expected YAML mapping")

    required_fields = ["seed", "window", "version"]
    missing = [field for field in required_fields if field not in config]
    if missing:
        raise ValueError(f"Invalid config structure: missing fields {missing}")

    seed = config["seed"]
    window = config["window"]
    version = config["version"]

    if not isinstance(seed, int):
        raise ValueError("Invalid config: 'seed' must be an integer")
    if not isinstance(window, int) or window <= 0:
        raise ValueError("Invalid config: 'window' must be a positive integer")
    if not isinstance(version, str) or not version.strip():
        raise ValueError("Invalid config: 'version' must be a non-empty string")

    return {"seed": seed, "window": window, "version": version}


def load_and_validate_dataset(input_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(input_path)
    except FileNotFoundError:
        raise ValueError(f"Missing input file: {input_path}")
    except pd.errors.EmptyDataError:
        raise ValueError("Empty file: no CSV content found")
    except pd.errors.ParserError as exc:
        raise ValueError(f"Invalid CSV format: {exc}")

    if len(df.columns) == 1 and "," in str(df.columns[0]):
        # Fallback for files where each line is quoted as one CSV field.
        df = pd.read_csv(input_path, sep=",", quotechar="'", engine="python")
        df.columns = [str(col).strip('"') for col in df.columns]
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.strip('"')

    if df.empty:
        raise ValueError("Empty dataset")

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

    start_time = time.time()
    config_version = "unknown"

    setup_logging(args.log_file)
    logging.info("Job started")

    try:
        config = load_and_validate_config(args.config)

        seed = config["seed"]
        window = config["window"]
        version = config["version"]
        config_version = version

        np.random.seed(seed)

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        df = load_and_validate_dataset(args.input)

        logging.info(f"Rows loaded: {len(df)}")

        logging.info("Processing step: computing rolling mean")
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        logging.info("Processing step: generating binary signal")
        df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)

        # Exclude initial rows where rolling_mean is NaN.
        valid_df = df.dropna()

        signal_rate = valid_df["signal"].mean()
        rows_processed = len(df)

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(float(signal_rate), 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        logging.info(f"Metrics: {metrics}")

        write_metrics(args.output, metrics)

        print(json.dumps(metrics, indent=4))

        logging.info("Job completed successfully")

    except Exception as e:
        logging.exception(f"Job failed: {str(e)}")

        error_metrics = {
            "version": config_version,
            "status": "error",
            "error_message": str(e)
        }

        write_metrics(args.output, error_metrics)

        print(json.dumps(error_metrics, indent=4))

        logging.info("Job completed with status=error")

        sys.exit(1)


if __name__ == "__main__":
    main()