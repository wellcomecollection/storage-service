import logging
import sys

import daiquiri


def get_logger(name):
    if "--debug" in sys.argv:
        level = logging.DEBUG
    else:
        level = logging.INFO

    daiquiri.setup(
        level=level,
        outputs=[
            daiquiri.output.Stream(
                formatter=daiquiri.formatter.ColorFormatter(
                    fmt="%(color)s[%(levelname)s] %(message)s%(color_stop)s",
                    datefmt="%H:%M:%S",
                )
            )
        ],
    )

    return daiquiri.getLogger(name)
