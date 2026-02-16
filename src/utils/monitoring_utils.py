from prometheus_client import Counter, Histogram, start_http_server
import time

PIPELINE_RUNS = Counter(
    "pipeline_runs_total",
    "Total number of pipeline executions"
)

PIPELINE_FAILURES = Counter(
    "pipeline_failures_total",
    "Total pipeline failures"
)

PIPELINE_DURATION = Histogram(
    "pipeline_duration_seconds",
    "Pipeline execution duration"
)


def start_metrics_server(port=8000):
    start_http_server(port)


class pipeline_timer:
    def __enter__(self):
        self.start = time.time()
        PIPELINE_RUNS.inc()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        duration = time.time() - self.start
        PIPELINE_DURATION.observe(duration)
        if exc_type:
            PIPELINE_FAILURES.inc()
