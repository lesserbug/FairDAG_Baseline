import json
import os
import subprocess
import tempfile
from pathlib import Path

from fabric import task


ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "scripts" / "deploy"
LOG_ROOT = Path(__file__).resolve().parent / "logs"
DEFAULT_AWS_CONFIG = Path(__file__).resolve().parent / "aws.json"
DEFAULT_INVENTORY = Path(__file__).resolve().parent / "inventory.json"
MANAGED_BY = "fairdag-fab"


def _load_aws(config):
    config_path = Path(config).expanduser()
    with open(config_path, encoding="utf-8") as file:
        settings = json.load(file)

    try:
        import boto3
    except ImportError as error:
        raise RuntimeError("boto3 is required: python3 -m pip install boto3") from error

    session = boto3.Session(profile_name=settings.get("profile"))
    clients = {
        region["name"]: session.client("ec2", region_name=region["name"])
        for region in settings["regions"]
    }
    return settings, clients


def _managed_instances(ec2, project):
    response = ec2.describe_instances(
        Filters=[
            {"Name": "tag:Project", "Values": [project]},
            {"Name": "tag:ManagedBy", "Values": [MANAGED_BY]},
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping", "stopped"],
            },
        ]
    )
    return [
        instance
        for reservation in response["Reservations"]
        for instance in reservation["Instances"]
    ]


def _ubuntu_image(ec2):
    images = ec2.describe_images(
        Owners=["099720109477"],
        Filters=[
            {
                "Name": "name",
                "Values": ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"],
            },
            {"Name": "architecture", "Values": ["x86_64"]},
            {"Name": "state", "Values": ["available"]},
            {"Name": "root-device-type", "Values": ["ebs"]},
            {"Name": "virtualization-type", "Values": ["hvm"]},
        ],
    )["Images"]
    if not images:
        raise RuntimeError("no Canonical Ubuntu 22.04 x86_64 AMI found")
    return max(images, key=lambda image: image["CreationDate"])["ImageId"]


def _tags(instance):
    return {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}


def _write_inventory(path, regions, replicas, clients, controller_private_ip=None):
    inventory = {
        "regions": [
            [
                instance["PrivateIpAddress"]
                for instance in sorted(
                    replicas.get(region["name"], []),
                    key=lambda instance: int(_tags(instance)["NodeSlot"]),
                )
            ]
            for region in regions
        ],
        "clients": [
            instance["PrivateIpAddress"]
            for region in regions
            for instance in sorted(
                clients.get(region["name"], []),
                key=lambda instance: int(_tags(instance)["NodeSlot"]),
            )
        ],
    }
    if controller_private_ip:
        inventory["clients"].insert(0, controller_private_ip)

    inventory_path = Path(path).expanduser()
    inventory_path.parent.mkdir(parents=True, exist_ok=True)
    with open(inventory_path, "w", encoding="utf-8") as file:
        json.dump(inventory, file, indent=2)
        file.write("\n")
    print(f"wrote {inventory_path}")


@task
def create(
    ctx,
    config=str(DEFAULT_AWS_CONFIG),
    inventory=str(DEFAULT_INVENTORY),
    replicas_per_region=1,
    clients_per_region=0,
    controller_as_client=False,
):
    """Create tagged FairDAG participants in existing VPC subnets."""
    settings, ec2_clients = _load_aws(config)
    project = settings.get("project", "FairDAG")
    replicas_per_region = int(replicas_per_region)
    clients_per_region = int(clients_per_region)
    controller_private_ip = settings.get("controller_private_ip")
    if replicas_per_region < 1 or clients_per_region < 0:
        raise ValueError(
            "replicas-per-region must be positive and clients-per-region non-negative"
        )
    if controller_as_client and not controller_private_ip:
        raise ValueError("controller_private_ip is required with --controller-as-client")
    if clients_per_region == 0 and not controller_as_client:
        raise ValueError("create at least one client or pass --controller-as-client")

    existing = []
    for region in settings["regions"]:
        existing.extend(_managed_instances(ec2_clients[region["name"]], project))
    if existing:
        raise RuntimeError(
            f"found {len(existing)} existing {project} instances managed by {MANAGED_BY}; "
            "use 'fab info', 'fab start', 'fab stop', or 'fab destroy --yes'"
        )

    replicas = {}
    clients = {}
    created = []
    for region_slot, region in enumerate(settings["regions"]):
        ec2 = ec2_clients[region["name"]]
        image_id = region.get("image_id") or _ubuntu_image(ec2)
        replicas[region["name"]] = []
        clients[region["name"]] = []
        for role, count, destination in (
            ("replica", replicas_per_region, replicas),
            ("client", clients_per_region, clients),
        ):
            if count == 0:
                continue
            response = ec2.run_instances(
                ImageId=image_id,
                InstanceType=settings.get(
                    f"{role}_instance_type", settings["instance_type"]
                ),
                KeyName=region.get("key_name", settings["key_name"]),
                MinCount=count,
                MaxCount=count,
                NetworkInterfaces=[
                    {
                        "DeviceIndex": 0,
                        "SubnetId": region["subnet_id"],
                        "Groups": [region["security_group_id"]],
                        "AssociatePublicIpAddress": False,
                    }
                ],
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Project", "Value": project},
                            {"Key": "ManagedBy", "Value": MANAGED_BY},
                            {"Key": "Role", "Value": role},
                            {"Key": "RegionSlot", "Value": str(region_slot)},
                        ],
                    }
                ],
            )
            instances = sorted(response["Instances"], key=lambda item: item["InstanceId"])
            for node_slot, instance in enumerate(instances):
                ec2.create_tags(
                    Resources=[instance["InstanceId"]],
                    Tags=[
                        {"Key": "NodeSlot", "Value": str(node_slot)},
                        {
                            "Key": "Name",
                            "Value": f"{project}-{role}-{region_slot}-{node_slot}",
                        },
                    ],
                )
            destination[region["name"]] = instances
            created.extend((ec2, instance["InstanceId"]) for instance in instances)
            print(f"created {count} {role}(s) in {region['name']} using {image_id}")

    for ec2 in ec2_clients.values():
        instance_ids = [instance_id for client, instance_id in created if client is ec2]
        if instance_ids:
            ec2.get_waiter("instance_running").wait(InstanceIds=instance_ids)
            ec2.get_waiter("instance_status_ok").wait(InstanceIds=instance_ids)

    for region in settings["regions"]:
        ec2 = ec2_clients[region["name"]]
        instances = _managed_instances(ec2, project)
        replicas[region["name"]] = [
            instance for instance in instances if _tags(instance).get("Role") == "replica"
        ]
        clients[region["name"]] = [
            instance for instance in instances if _tags(instance).get("Role") == "client"
        ]
    _write_inventory(
        inventory,
        settings["regions"],
        replicas,
        clients,
        controller_private_ip if controller_as_client else None,
    )


@task
def info(ctx, config=str(DEFAULT_AWS_CONFIG)):
    """List EC2 instances created by this Fabric collection."""
    settings, ec2_clients = _load_aws(config)
    project = settings.get("project", "FairDAG")
    print("REGION\tROLE\tSLOT\tINSTANCE\tSTATE\tPRIVATE_IP\tPUBLIC_IP")
    for region in settings["regions"]:
        instances = _managed_instances(ec2_clients[region["name"]], project)
        for instance in sorted(
            instances,
            key=lambda item: (
                _tags(item).get("Role", ""),
                int(_tags(item).get("NodeSlot", 0)),
            ),
        ):
            tags = _tags(instance)
            print(
                f"{region['name']}\t{tags.get('Role', '-')}\t{tags.get('NodeSlot', '-')}\t"
                f"{instance['InstanceId']}\t{instance['State']['Name']}\t"
                f"{instance.get('PrivateIpAddress', '-')}\t{instance.get('PublicIpAddress', '-')}"
            )


@task
def start(ctx, config=str(DEFAULT_AWS_CONFIG)):
    """Start stopped FairDAG participant instances."""
    settings, ec2_clients = _load_aws(config)
    project = settings.get("project", "FairDAG")
    for region in settings["regions"]:
        ec2 = ec2_clients[region["name"]]
        instance_ids = [
            instance["InstanceId"]
            for instance in _managed_instances(ec2, project)
            if instance["State"]["Name"] == "stopped"
        ]
        if instance_ids:
            ec2.start_instances(InstanceIds=instance_ids)
            ec2.get_waiter("instance_running").wait(InstanceIds=instance_ids)
            ec2.get_waiter("instance_status_ok").wait(InstanceIds=instance_ids)
            print(f"started {len(instance_ids)} instance(s) in {region['name']}")


@task
def stop(ctx, config=str(DEFAULT_AWS_CONFIG)):
    """Stop running FairDAG participant instances; keep network and controller."""
    settings, ec2_clients = _load_aws(config)
    project = settings.get("project", "FairDAG")
    for region in settings["regions"]:
        ec2 = ec2_clients[region["name"]]
        instance_ids = [
            instance["InstanceId"]
            for instance in _managed_instances(ec2, project)
            if instance["State"]["Name"] == "running"
        ]
        if instance_ids:
            ec2.stop_instances(InstanceIds=instance_ids)
            ec2.get_waiter("instance_stopped").wait(InstanceIds=instance_ids)
            print(f"stopped {len(instance_ids)} instance(s) in {region['name']}")


@task
def destroy(
    ctx,
    config=str(DEFAULT_AWS_CONFIG),
    inventory=str(DEFAULT_INVENTORY),
    yes=False,
):
    """Terminate only tagged FairDAG participants; keep network and controller."""
    if not yes:
        raise ValueError("pass --yes to terminate the tagged FairDAG participant instances")
    settings, ec2_clients = _load_aws(config)
    project = settings.get("project", "FairDAG")
    for region in settings["regions"]:
        ec2 = ec2_clients[region["name"]]
        instance_ids = [
            instance["InstanceId"]
            for instance in _managed_instances(ec2, project)
        ]
        if instance_ids:
            ec2.terminate_instances(InstanceIds=instance_ids)
            ec2.get_waiter("instance_terminated").wait(InstanceIds=instance_ids)
            print(f"terminated {len(instance_ids)} instance(s) in {region['name']}")
    Path(inventory).expanduser().unlink(missing_ok=True)


@task
def remote(
    ctx,
    inventory=str(DEFAULT_INVENTORY),
    nodes="5,10,20,50",
    rates="10000,20000,30000,40000,50000,60000,70000,80000,90000,100000",
    clients=5,
    variant="ab",
    duration=60,
    warmup=15,
    drain_duration=30,
    tx_size=512,
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
    tx_size = int(tx_size)
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
                            "tx_size": tx_size,
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
                environment["FAIRDAG_WARMUP_DURATION"] = str(warmup)
                environment["FAIRDAG_TOTAL_DURATION"] = str(
                    warmup + duration + drain_duration
                )
                environment["FAIRDAG_BURST_HZ"] = "20"
                environment["FAIRDAG_MAX_BATCH_DELAY_MS"] = "200"
                environment["FAIRDAG_TX_SIZE"] = str(tx_size)
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
