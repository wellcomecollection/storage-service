import os
import shutil
import settings
import logging


def clean_working_dir():
    for f in os.listdir(settings.WORKING_DIRECTORY):
        path = os.path.join(
            settings.WORKING_DIRECTORY, f
        )

        if os.path.isfile(path):
            os.remove(path)

        elif os.path.isdir(path):
            shutil.rmtree(path)


def cleanup(bag_details):
    directory = bag_details['directory']

    logging.debug(
        "deleting %s",
        directory
    )

    if os.path.isdir(directory):
        shutil.rmtree(directory)

    zipfile = bag_details['zip_file_path']

    logging.debug("deleting %s", zipfile)

    if os.path.isfile(zipfile):
        os.remove(zipfile)


def prepare_bag_dir(b_number):
    bag_details = {
        "b_number": b_number,
        "directory": os.path.join(
            settings.WORKING_DIRECTORY,
            b_number
        ),
        "mets_partial_path": get_mets_partial_path(
            b_number
        ),
    }

    shutil.rmtree(
        bag_details["directory"],
        ignore_errors=True
    )

    os.makedirs(
        os.path.join(
            bag_details["directory"],
            "objects"
        )
    )

    return bag_details


def get_mets_partial_path(b_number):
    separator = get_separator()
    pathparts = separator.join(
        b_number[9:4:-1]
    )

    return "{0}{1}{2}".format(
        settings.METS_ROOT_PREFIX,
        pathparts,
        separator
    )


def get_separator():
    # Not necessarily the OS we're running on!
    if settings.READ_METS_FROM_FILESHARE:
        return "\\"
    return "/"


def ensure_directory(destination):
    destination_dir = os.path.dirname(destination)
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
        # TODO: beware race condition if threaded
