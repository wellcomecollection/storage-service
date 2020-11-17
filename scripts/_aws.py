import boto3

from helpers import write_secret


READ_ONLY_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-read_only"
DEV_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"


sts_client = boto3.client("sts")


def get_aws_resource(resource, *, role_arn=READ_ONLY_ROLE_ARN):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.resource(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_aws_client(resource, *, role_arn=READ_ONLY_ROLE_ARN):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.client(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_elastic_ip():
    """
    Our VPCs have exactly one elastic IP associated with them.

    Because our services run in private subnets and use a NAT Gateway to connect
    to the public Internet through an elastic IP, this is the address from which
    all our service traffic will originate.

    Returns the IPv4 address of our elastic IP.
    """
    ec2_client = get_aws_client("ec2")
    resp = ec2_client.describe_addresses()

    ipv4_addresses = [addr["PublicIp"] for addr in resp["Addresses"]]

    if len(ipv4_addresses) == 0:
        raise RuntimeError("No Elastic IPs found!")
    elif len(ipv4_addresses) > 1:
        address_string = ", ".join(ipv4_addresses)
        raise RuntimeError(f"More than one Elastic IP found: {address_string}")
    else:
        return ipv4_addresses[0]


def store_secret(*, secret_id, secret_string):
    """
    Store a SecretString in Secrets Manager.
    """
    secrets_client = get_aws_client("secretsmanager", role_arn=DEV_ROLE_ARN)
    write_secret(secrets_client, id=secret_id, value=secret_string)
