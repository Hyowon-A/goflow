#!/usr/bin/env bash
set -euo pipefail

dry_run=0
out_dir="benchmarks/local/results"

for arg in "$@"; do
	case "$arg" in
		--dry-run|-n)
			dry_run=1
			;;
		--output-dir=*)
			out_dir="${arg#*=}"
			;;
		*)
			echo "usage: $0 [--dry-run|-n] [--output-dir=DIR]" >&2
			exit 2
			;;
	esac
done

run() {
	printf '+'
	printf ' %q' "$@"
	printf '\n'
	if [ "$dry_run" -eq 0 ]; then
		"$@"
	fi
}

cleanup() {
	run docker compose stop postgres redis
}

if [ "$dry_run" -eq 0 ]; then
	mkdir -p "$out_dir"
	trap cleanup EXIT
	run docker compose up -d postgres redis
	run docker compose exec -T postgres pg_isready -U goflow -d goflow
	run docker compose exec -T redis redis-cli ping
else
	printf '+ mkdir -p %q\n' "$out_dir"
	printf '+ docker compose up -d postgres redis\n'
	printf '+ docker compose exec -T postgres pg_isready -U goflow -d goflow\n'
	printf '+ docker compose exec -T redis redis-cli ping\n'
fi

stamp="$(date +%Y%m%d%H%M%S)"

run go run ./cmd/loadcheck \
	-runs 25 \
	-workers 1 \
	-timeout 90s \
	-failure-probability 0 \
	-stream "goflow:tasks:baseline-loadcheck-$stamp" \
	-json \
	-output "$out_dir/loadcheck-baseline.json" \
	-tag local-baseline-loadcheck

run go run ./cmd/recallify \
	-runs 5 \
	-workers 1 \
	-timeout 90s \
	-stream "goflow:tasks:baseline-recallify-$stamp" \
	-json \
	-output "$out_dir/recallify-baseline.json" \
	-tag local-baseline-recallify

if [ "$dry_run" -eq 1 ]; then
	printf '+ docker compose stop postgres redis\n'
fi
