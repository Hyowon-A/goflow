# Local Benchmarks

Local benchmark artifacts for GoFlow.

Keep this directory small:

- Commit curated JSON summaries generated from public fixtures.
- Commit Markdown reports derived from those summaries.
- Do not commit raw logs, secrets, private document text or generated answers
  from private material.

Planned result files live under `results/`.

## Durations

Benchmark commands report p50 and p95 workflow durations from stored workflow
timestamps. Live Prometheus duration metrics are intentionally skipped until a
dashboard needs live averages or percentile buckets.
