from botocore.exceptions import ClientError


def find_elastic_ip():
    """
    Our VPCs have exactly one elastic IP associated with them.

    Because our services run in private subnets and use a NAT Gateway to connect
    to the public Internet through an elastic IP, this is the address from which
    all our service traffic will originate.

    Returns the IPv4 address of our elastic IP.
    """
    resp = ec2_client.describe_addresses()

    ipv4_addresses = [addr["PublicIp"] for addr in resp["Addresses"]]

    if len(ipv4_addresses) == 0:
        raise RuntimeError("No Elastic IPs found!")
    elif len(ipv4_addresses) > 1:
        address_string = ", ".join(ipv4_addresses)
        raise RuntimeError(f"More than one Elastic IP found: {address_string}")
    else:
        return ipv4_addresses[0]


def store_secret(secrets_client, *, secret_id, secret_string):
    """
    Store a SecretString in Secrets Manager.
    """
    try:
        secrets_client.put_secret_value(SecretId=secret_id, SecretString=secret_string)
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            secrets_client.create_secret(Name=secret_id, SecretString=secret_string)
        else:
            raise
