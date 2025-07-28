"""A module providing the main command line interface for the maintenance tool."""

import argparse
import sys
import textwrap
from pathlib import Path

from ve_data_science_tool import LOGGER
from ve_data_science_tool.config import load_config
from ve_data_science_tool.data import check_data_directory
from ve_data_science_tool.globus import globus_sync
from ve_data_science_tool.scripts import check_scripts


def ve_data_science_tool_cli(args_list: list[str] | None = None) -> int:
    """A maintenance tool for use with ve_data_science.

    Args:
        args_list: This is a developer and testing facing argument that is used to
            simulate command line arguments, allowing this function to be called
            directly in tests.

    Returns:
        An integer indicating success (0) or failure (1)
    """

    # If no arguments list is provided
    if args_list is None:
        args_list = sys.argv[1:]

    # Check function docstring exists to safeguard against -OO mode, and strip off the
    # description of the function args_list, which should not be included in the command
    # line docs
    if ve_data_science_tool_cli.__doc__ is not None:
        desc = textwrap.dedent(
            "\n".join(ve_data_science_tool_cli.__doc__.splitlines()[:-10])
        )
    else:
        desc = "Python in -OO mode: no docs"

    fmt = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc, formatter_class=fmt)

    subparsers = parser.add_subparsers(dest="subcommand", metavar="")

    check_script_directory_subparser = subparsers.add_parser(
        "scripts",
        description="Check script and notebook metadata",
        help="Check script and notebook metadata",
    )

    check_script_directory_subparser.add_argument(
        "directory", type=Path, help="Specific directory to check", nargs="?"
    )

    check_data_directory_subparser = subparsers.add_parser(
        "data",
        description="Check data directories",
        help="Check data directories",
    )

    check_data_directory_subparser.add_argument(
        "directory", type=Path, help="Specific directory to check"
    )

    # TODO Currently no argument but probably will want them
    globus_sync_subparser = subparsers.add_parser(  # noqa: F841
        "sync",
        description="Synchronise data with GLOBUS",
        help="Synchronise data with GLOBUS",
    )

    args = parser.parse_args(args=args_list)

    # Get the configuration
    try:
        config = load_config()
    except RuntimeError as excep:
        LOGGER.error(f"Could not load configuration\n{excep!s}")
        return 0

    match args.subcommand:
        case "scripts":
            check_scripts(config=config, directory=args.directory)
        case "data":
            root = Path.cwd() if args.repository_root is None else args.repository_root
            check_data_directory(directory=args.directory, repository_root=root)
        case "sync":
            globus_sync()

    return 1
