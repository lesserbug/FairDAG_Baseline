import json
import os
import subprocess
import tempfile
from pathlib import Path

from fabric import task


ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "scripts" / "deploy"
LOG_ROOT = Path(__file__).resolve().parent / "logs"


@task
def remote(
    ctx,
    inventory,
    nodes="5,10,20,50",
    rates="10000,20000,30000,40000,50000,60000,70000,80000,90000,100000",
    clients=5,
    variant="ab",
    duration=60,
    warmup=15,
    drain_duration=30,
    runs=3,
):
    with open(inventory, encoding="utf-8") as file:
        hosts = json.load(file)

    node_counts = [int(value) for value in str(nodes).split(",")]
    input_rates = [int(value) for value in str(rates).split(",")]
    client_count = int(clients)
    duration = int(duration)
    warmup = int(warmup)
    drain_duration = int(drain_duration)
    runs = int(runs)
    script = {
        "ab": "performance/fair_performance.sh",
        "rl": "performance/fairrl_performance.sh",
    }[variant]

    LOG_ROOT.mkdir(parents=True, exist_ok=True)
    for run in range(runs):
        for node_count in node_counts:
            replicas = []
            for index in range(node_count):
                region = hosts["regions"][index % len(hosts["regions"])]
                replicas.append(region[index // len(hosts["regions"])])
            client_hosts = hosts["clients"][:client_count]

            for aggregate_rate in input_rates:
                if aggregate_rate % client_count != 0:
                    raise ValueError(
                        "aggregate rate must be divisible by the client count"
                    )
                client_rate = aggregate_rate // client_count
                run_name = (
                    f"{variant}-n{node_count}-r{aggregate_rate}-run{run}"
                )
                run_dir = LOG_ROOT / run_name
                run_dir.mkdir(parents=True, exist_ok=False)

                with open(run_dir / "metadata.json", "w", encoding="utf-8") as file:
                    json.dump(
                        {
                            "run": run,
                            "variant": variant,
                            "nodes": node_count,
                            "clients": client_count,
                            "configured_input_rate": aggregate_rate,
                            "configured_client_rate": client_rate,
                            "warmup": warmup,
                            "duration": duration,
                            "drain_duration": drain_duration,
                            "replicas": replicas,
                            "client_hosts": client_hosts,
                        },
                        file,
                        indent=2,
                    )

                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".conf",
                    dir=DEPLOY / "config",
                    delete=False,
                    encoding="utf-8",
                ) as config:
                    config.write("iplist=(\n")
                    for host in replicas + client_hosts:
                        config.write(f'  "{host}"\n')
                    config.write(")\n")
                    config.write(f"client_num={client_count}\n")
                    config_path = Path(config.name)

                environment = os.environ.copy()
                environment["USE_BAZEL_VERSION"] = "5.0.0"
                environment["FAIRDAG_CLIENT_RATE"] = str(client_rate)
                environment["FAIRDAG_SEND_DURATION"] = str(warmup + duration)
                environment["FAIRDAG_TOTAL_DURATION"] = str(
                    warmup + duration + drain_duration
                )
                environment["FAIRDAG_BURST_HZ"] = "20"
                environment["FAIRDAG_MAX_BATCH_DELAY_MS"] = "200"
                environment["FAIRDAG_RESULT_DIR"] = str(run_dir)

                try:
                    subprocess.run(
                        ["bash", script, str(config_path)],
                        cwd=DEPLOY,
                        env=environment,
                        check=True,
                    )
                finally:
                    config_path.unlink(missing_ok=True)

    subprocess.run(
        ["python3", str(Path(__file__).resolve().parent / "logs.py"), str(LOG_ROOT)],
        check=True,
    )


@task
def logs(ctx):
    subprocess.run(
        ["python3", str(Path(__file__).resolve().parent / "logs.py"), str(LOG_ROOT)],
        check=True,
    )
