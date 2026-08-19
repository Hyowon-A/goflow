# Local Benchmarks

Local benchmark artifacts for GoFlow.

Keep this directory small:

- Commit curated JSON summaries generated from public fixtures.
- Commit Markdown reports derived from those summaries.
- Do not commit raw logs, secrets, private document text or generated answers
  from private material.

Planned result files live under `results/`.

Run the baseline:

```sh
scripts/local_baseline.sh
```

Run the scaling matrix:

```sh
scripts/local_scaling.sh
```

Both scripts start PostgreSQL and Redis with Docker Compose, then stop them on
exit. The scaling script runs worker counts `1`, `3` and `5` for loadcheck and
Recallify fake mode.

Latest scaling results:

| Scenario | Workers | Runs | Completed | Failed | p50 | p95 | Elapsed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| loadcheck | 1 | 50 | 50 | 0 | 2.44s | 2.58s | 2.93s |
| loadcheck | 3 | 50 | 50 | 0 | 1.08s | 1.11s | 1.51s |
| loadcheck | 5 | 50 | 50 | 0 | 0.84s | 0.89s | 1.10s |
| recallify fake | 1 | 10 | 10 | 0 | 0.60s | 0.60s | 0.72s |
| recallify fake | 3 | 10 | 10 | 0 | 0.23s | 0.25s | 0.53s |
| recallify fake | 5 | 10 | 10 | 0 | 0.15s | 0.17s | 0.35s |

Conclusion: local happy-path throughput improves as worker count increases, and
p95 stays close to p50 for both scenarios. These numbers show the worker pool
scales on this local setup; they are not production capacity claims.

## Durations

Benchmark commands report p50 and p95 workflow durations from stored workflow
timestamps. Live Prometheus duration metrics are intentionally skipped until a
dashboard needs live averages or percentile buckets.
