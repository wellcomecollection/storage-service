from boto3.dynamodb.types import TypeDeserializer
import concurrent
import itertools

from common import get_aws_client


def gather_table_items(role_arn, table_name):
    dynamo_client = get_aws_client("dynamodb", role_arn=role_arn)
    scanner = parallel_scan_table(dynamo_client, TableName=table_name)

    for dynamo_format_data in scanner:
        deserializer = TypeDeserializer()
        item = {k: deserializer.deserialize(v) for k, v in dynamo_format_data.items()}
        yield item


def parallel_scan_table(dynamo_client, *, TableName, **kwargs):
    total_segments = 25
    max_scans_in_parallel = 5

    tasks_to_do = [
        {
            **kwargs,
            "TableName": TableName,
            "Segment": segment,
            "TotalSegments": total_segments,
        }
        for segment in range(total_segments)
    ]

    scans_to_run = iter(tasks_to_do)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(dynamo_client.scan, **scan_params): scan_params
            for scan_params in itertools.islice(scans_to_run, max_scans_in_parallel)
        }

        while futures:
            # Wait for the first future to complete.
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                yield from fut.result()["Items"]

                scan_params = futures.pop(fut)

                try:
                    scan_params["ExclusiveStartKey"] = fut.result()["LastEvaluatedKey"]
                except KeyError:
                    break
                tasks_to_do.append(scan_params)

            for scan_params in itertools.islice(scans_to_run, len(done)):
                futures[
                    executor.submit(dynamo_client.scan, **scan_params)
                ] = scan_params