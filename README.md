# MLOps Task 0

Minimal batch job for rolling-mean trading signal generation with deterministic config, structured metrics, and Dockerized execution.

## Run Locally

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the batch job:

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Run With Docker

Build image:

```bash
docker build -t mlops-task .
```

Run container:

```bash
docker run --rm mlops-task
```

The container command is configured to run:

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Example metrics.json

```json
{
	"version": "v1",
	"rows_processed": 10000,
	"metric": "signal_rate",
	"value": 0.4952,
	"latency_ms": 34,
	"seed": 42,
	"status": "success"
}
```

## Artifacts

- `metrics.json`: machine-readable output (written on both success and error)
- `run.log`: execution log including validation, processing, and status