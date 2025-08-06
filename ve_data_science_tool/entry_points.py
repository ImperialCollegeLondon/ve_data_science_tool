"""A module providing the main command line interface for the maintenance tool."""

import argparse
import sys
import textwrap
from pathlib import Path

from ve_data_science_tool import LOGGER
from ve_data_science_tool.config import configure, load_config
from ve_data_science_tool.data import check_data, update_manifests
from ve_data_science_tool.globus import globus_status, globus_sync
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
        description="""Validate script and notebook metadata recursively within a 
            directory. If a directory is not provided then the default is to validate 
            all scripts and notebooks within the `analysis` directory.
            
            The --check-file-locations option can be used to turn on validation of file
            paths provided in the `input_files` and `output_files` metadata.
            """,
        help="Check script and notebook metadata",
    )

    check_script_directory_subparser.add_argument(
        "directory", type=Path, help="Specific directory to check", nargs="?"
    )

    check_script_directory_subparser.add_argument(
        "-c",
        "--check-file-locations",
        help="Validate file locations",
        action="store_true",
        default=False,
    )

    check_data_directory_subparser = subparsers.add_parser(
        "data",
        description="""Validate metadata in data directories recursively within a 
            directory. If a directory is not provided then the default is to validate 
            all scripts and notebooks within the `data` directory.""",
        help="Check data directories",
    )

    check_data_directory_subparser.add_argument(
        "directory", type=Path, help="Specific directory to check", nargs="?"
    )

    update_manifests_subparser = subparsers.add_parser(
        "manifests",
        description="""Update manifest files within data directories recursively within
            a directory. If a directory is not provided then the default is to update
            manifests starting in the root `data` directory. This command does not check
            manifest metadata - just updates the manifests to ensure all files are
            included. It does not remove files from the manifest.""",
        help="Update data manifests",
    )

    update_manifests_subparser.add_argument(
        "directory", type=Path, help="Specific directory to update", nargs="?"
    )

    # TODO Currently no argument but probably will want them
    globus_sync_subparser = subparsers.add_parser(  # noqa: F841
        "globus_sync",
        description="Synchronise data with GLOBUS",
        help="Synchronise data with GLOBUS",
    )

    # TODO Currently no argument but probably will want them
    globus_status_subparser = subparsers.add_parser(  # noqa: F841
        "globus_status",
        description="Check the file synchronisation status with GLOBUS",
        help="Check the file synchronisation status with GLOBUS",
    )

    configure_subparser = subparsers.add_parser(
        "configure",
        description="Configure the tool from the ve_data_science repo root.",
        help="Configure the tool from the ve_data_science repo root.",
    )

    required_config = configure_subparser.add_argument_group("Required named arguments")
    required_config.add_argument(
        "--client-uuid", type=str, help="GLOBUS app client UUID", required=True
    )

    required_config.add_argument(
        "--remote-uuid", type=str, help="GLOBUS remote endpoint UUID", required=True
    )

    args = parser.parse_args(args=args_list)

    # Handle initial configuration step
    if args.subcommand == "configure":
        try:
            configure(client_uuid=args.client_uuid, remote_uuid=args.remote_uuid)
            return 1
        except RuntimeError as excep:
            LOGGER.error(f"Could not create configuration\n{excep!s}")
            return 0

    # Get the configuration
    try:
        config = load_config()
    except RuntimeError as excep:
        LOGGER.error(f"Could not load configuration\n{excep!s}")
        return 0

    match args.subcommand:
        case "scripts":
            check_scripts(
                config=config,
                directory=args.directory,
                check_file_locations=args.check_file_locations,
            )
        case "manifests":
            update_manifests(config=config, directory=args.directory)
        case "data":
            check_data(config=config, directory=args.directory)
        case "globus_sync":
            globus_sync(config=config)
        case "globus_status":
            globus_status(config=config)

    return 1
