import subprocess


def az(*args, **kwargs):
    """
    Run a command with the Azure CLI.
    """
    output = subprocess.check_output(["az"] + list(args), **kwargs)
    return output.decode("utf8").strip()
