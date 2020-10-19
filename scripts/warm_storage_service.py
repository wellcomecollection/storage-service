#!/usr/bin/env python3
"""
This is a script to scale up prod/stage when running migrations to get
results in a timely fashion.

You will need to ensure that it is run with the correct AWS credentials.
You can provide an appropriate "AWS_PROFILE" environment.
"""

import json
import sys

import boto3
import termcolor


if __name__ == "__main__":
    stack = sys.argv[1]

    if stack not in ("staging", "prod"):
        sys.exit(f"Usage: {__file__} (staging | prod)")

    s3 = boto3.client("s3")

    state_file = json.load(
        s3.get_object(
            Bucket="wellcomecollection-storage-infra",
            Key=f"terraform/storage-service/stack_{stack}.tfstate"
        )["Body"]
    )

    cluster_name = None
    service_names = set()
    service_min_capacities = {}

    for res in state_file["resources"]:
        if res["type"] == "aws_ecs_cluster":
            assert cluster_name is None
            assert len(res["instances"]) == 1
            cluster_name = res["instances"][0]["attributes"]["name"]

        elif res["type"] == "aws_ecs_service":
            assert len(res["instances"]) == 1
            service_names.add(res["instances"][0]["attributes"]["name"])

        elif res["type"] == "aws_appautoscaling_target":
            assert len(res["instances"]) == 1
            name = res["instances"][0]["attributes"]["id"].split("/")[-1]
            min_capacity = res["instances"][0]["attributes"]["min_capacity"]
            service_min_capacities[name] = min_capacity

    assert set(service_min_capacities.keys()) - service_names == set()

    no_scaling = service_names - set(service_min_capacities.keys())
    if no_scaling:
        print("The following services do not have scaling configured:")
        for name in sorted(no_scaling):
            print(f"- {name}")
        print("")

    client = boto3.client("application-autoscaling")

    for name, min_capacity in service_min_capacities.items():
        resp = client.describe_scalable_targets(
            ResourceIds=[f"service/{cluster_name}/{name}"],
            ServiceNamespace="ecs"
        )
        assert len(resp["ScalableTargets"]) == 1

        target = resp["ScalableTargets"][0]

        if target["MinCapacity"] == min_capacity and min_capacity == 0:
            print(f"Warming {termcolor.colored(name, 'yellow')} to min capacity {termcolor.colored(1, 'green')}")
            target["MinCapacity"] = max(1, min_capacity)
        elif target["MinCapacity"] > min_capacity:
            print(f"Cooling {termcolor.colored(name, 'yellow')} to min capacity {termcolor.colored(min_capacity, 'blue')}")
            target["MinCapacity"] = min_capacity

        del target["CreationTime"]

        client.register_scalable_target(**target)