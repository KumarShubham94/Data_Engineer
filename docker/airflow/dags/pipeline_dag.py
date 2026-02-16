from prefect import flow, task
import subprocess
import os


@task(retries=1, retry_delay_seconds=10)
def run_gold():
    subprocess.run(
        ["python3.11", "src/consumers/gold_ge1.py"],
        check=True,
        env={**os.environ, "PYTHONPATH": "."}
    )


@flow(name="kafka-streaming-pipeline")
def kafka_pipeline():
    run_gold()


if __name__ == "__main__":
    kafka_pipeline()
