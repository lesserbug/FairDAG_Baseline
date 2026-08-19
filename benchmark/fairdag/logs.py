import csv
import json
import re
import statistics
import sys
from pathlib import Path


log_root = Path(sys.argv[1])
rows = []
for run_dir in sorted(log_root.iterdir()):
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        continue
    with open(metadata_path, encoding="utf-8") as file:
        metadata = json.load(file)

    warmup_windows = (metadata["warmup"] + 4) // 5
    measurement_windows = max(1, metadata["duration"] // 5)
    replica_averages = []
    for index in range(1, metadata["nodes"] + 1):
        log_path = run_dir / f"result_{index}_log"
        values = [
            int(value)
            for value in re.findall(r"\btxn:(\d+)", log_path.read_text(errors="replace"))
        ]
        first_positive = next((i for i, value in enumerate(values) if value > 0), 0)
        start = first_positive + warmup_windows
        measured = values[start : start + measurement_windows]
        if measured:
            replica_averages.append(statistics.mean(measured))

    client_latency_sum = 0
    client_latency_samples = 0
    offered_transactions = 0
    offered_reports = 0
    for index in range(
        metadata["nodes"] + 1,
        metadata["nodes"] + metadata["clients"] + 1,
    ):
        log_path = run_dir / f"result_{index}_log"
        text = log_path.read_text(errors="replace")
        values = [
            (float(latency), int(samples))
            for latency, samples in re.findall(
                r"req client latency:([0-9.eE+-]+) samples:(\d+)", text
            )
        ]
        for latency, samples in values:
            client_latency_sum += latency * samples
            client_latency_samples += samples
        offered = re.findall(r"offered_transactions:(\d+)", text)
        if offered:
            offered_transactions += int(offered[-1])
            offered_reports += 1

    summary = {
        **metadata,
        "actual_offered_rate": (
            offered_transactions / (metadata["warmup"] + metadata["duration"])
            if offered_reports == metadata["clients"]
            else None
        ),
        "output_tps": (
            statistics.median(replica_averages) if replica_averages else 0
        ),
        "latency_ms": (
            client_latency_sum / client_latency_samples * 1000
            if client_latency_samples
            else None
        ),
    }
    with open(run_dir / "summary.json", "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2)
    rows.append(summary)

with open(log_root / "results.csv", "w", newline="", encoding="utf-8") as file:
    fields = [
        "variant",
        "nodes",
        "clients",
        "tx_size",
        "configured_input_rate",
        "actual_offered_rate",
        "output_tps",
        "latency_ms",
        "run",
    ]
    writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
