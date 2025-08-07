"""GLOBUS synchronisation tool.

This module provides functionality to synchronise local data in Globus Personal
Connect collection with the central shared Guest Collection attached to the Imperial
HPC project store.

The listing code comes from
https://globus-sdk-python.readthedocs.io/en/stable/examples/recursive_ls.html
"""

import time
from collections import deque
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Literal

import globus_sdk

from ve_data_science_tool import LOGGER
from ve_data_science_tool.config import Config


def get_authenticated_transfer_client(config: Config) -> globus_sdk.TransferClient:
    """Generate an authenticated GLOBUS Transfer client object.

    The authentication process has multiple steps, requiring authorisation of several
    different permissions to access the collection. This function was developed with
    support by the GLOBUS development team to interact with the web authorisation
    process for GLOBUS against the Imperial College London high assurance endpoints.

    Args:
        config: A SyncConfig instance providing connection data.

    """

    # Create a user application
    user_app = globus_sdk.UserApp(
        app_name=config.app_client_name, client_id=config.app_client_uuid
    )

    # Use that to create a GLOBUS transfer client
    client = globus_sdk.TransferClient(app=user_app)

    # Try to run an operation on the client and then handle authorisation errors
    try:
        _ = client.operation_ls(config.remote_collection_uuid)

    except globus_sdk.TransferAPIError as err:
        # Look for the specific case of additional authorisation parameters required in
        # the exception information and then use that to drive a fresh authentication
        # sequence via the web API. This requires user interaction at the command line.
        if err.info.authorization_parameters:
            print("An authorization requirement was not met. Logging in again...")

            # Convert the authentication error information in the exception to the
            # Globus Auth Requirements Errors (GARE) class, which provides an interface
            # to the required parameters that need to be used to login.
            gare = globus_sdk.gare.to_gare(err)
            params = gare.authorization_parameters  # type: ignore [union-attr]

            # Explicitly set 'prompt=login' to guarantee a fresh login without reliance
            # on any existing browser session.
            params.prompt = "login"

            # Pass these additional parameters into client via a login flow for the user
            # application
            user_app.login(auth_params=params)

        # otherwise, there are no authorization parameters, so reraise the error
        else:
            raise

    # The client should now be authorized
    try:
        _ = client.operation_ls(config.remote_collection_uuid)
    except Exception:
        raise RuntimeError("Could not connect to GLOBUS transfer")

    return client


def globus_transfer(
    transfer_client: globus_sdk.TransferClient,
    source_endpoint: str,
    destination_endpoint: str,
    source_path: str,
    destination_path: str,
    filters: tuple[
        tuple[Literal["include", "exclude"], str, Literal["file", "dir"]], ...
    ] = (
        ("exclude", ".*", "file"),
        ("exclude", "MANIFEST.yaml", "file"),
    ),
) -> bool:
    """Synchronize files with GLOBUS.

    This function sets up and runs a transfer between two endpoints. See the SDK
    documentation at https://docs.globus.org/api/transfer/task/#task_document for
    information on the task info data that is being monitored.

    By default, the transfer filters out MANIFEST.yaml files and hidden files.

    Args:
        transfer_client: An authenticated GLOBUS transfer client.
        source_endpoint: The source endpoint
        destination_endpoint: The destination endpoint
        source_path: The source path
        destination_path: The destination path
        filters: Details of filters to be applied to the transfer.
    """

    # Set up the Transfer Data object, synchronizing only files with newer modification
    # times
    tdata = globus_sdk.TransferData(
        transfer_client=transfer_client,
        source_endpoint=source_endpoint,
        destination_endpoint=destination_endpoint,
        preserve_timestamp=True,
        sync_level="mtime",
    )

    # Add the remote data directory to the transfer data request
    tdata.add_item(
        source_path=source_path,
        destination_path=destination_path,
        recursive=True,
    )

    # Apply any filters
    for method, name, obj_type in filters:
        tdata.add_filter_rule(method=method, name=name, type=obj_type)

    # Submit the request and monitor progress
    try:
        submit_result = transfer_client.submit_transfer(tdata)
        task_id = submit_result["task_id"]
        running = True
        interval = 2
        start_time = time.time()
        n_files_transferred = -1
        queue_message_emitted = False

        while running:
            task_info = transfer_client.get_task(task_id)
            runtime = f"[{(time.time() - start_time):6.1f} s]"

            # Handle errors
            if (task_info["status"] == "ACTIVE") & (not task_info["is_ok"]):
                # Record the status information from the task to give a clue about the
                # failure mode and then explicitly cancel the task and exit
                LOGGER.error(f"{runtime} Globus sync error: {task_info['nice_status']}")
                cancel_outcome = transfer_client.cancel_task(task_id)
                LOGGER.error(
                    f"{runtime} Globus sync cancelled: {cancel_outcome['status']}"
                )
                return False

            # Submitted jobs (even failed ones) are Active - nice status reports queued
            # or OK for actively transferring
            if task_info["nice_status"] == "Queued":
                if not queue_message_emitted:
                    LOGGER.info(f"{runtime} Globus sync queued")
                    queue_message_emitted = True

            if task_info["nice_status"] == "OK":
                # Total number of files - this is static once populated but is only
                # populated during the queuing process
                n_files = task_info["files"]
                # Report when the number of files transferred increases
                if n_files_transferred < task_info["files_transferred"]:
                    n_files_transferred = task_info["files_transferred"]
                    LOGGER.info(
                        f"{runtime} Globus sync active: "
                        f"{n_files_transferred}/{n_files} files transferred"
                    )

            if task_info["status"] == "SUCCEEDED":
                LOGGER.info(f"{runtime} Globus sync complete.")
                running = False
                continue

            time.sleep(interval)

    except globus_sdk.services.transfer.errors.TransferAPIError as excep:
        raise RuntimeError(f"GLOBUS transfer API error: {excep.raw_json}")

    # Report files
    # https://docs.globus.org/api/transfer/task/#get_task_successful_transfers
    transferred = transfer_client.task_successful_transfers(task_id)

    if transferred["DATA"]:
        LOGGER.info("Files transferred")
        for data in sorted(transferred["DATA"]):
            LOGGER.info(f" - {data['source_path']}")

    return True


def globus_sync(config: Config) -> bool:
    """Synchronise data with GLOBUS collection.

    Args:
        config: A config object.
    """
    # Get the transfer client
    transfer_client = get_authenticated_transfer_client(config=config)

    # Update local from remote
    LOGGER.info("Starting GLOBUS synchronisation: remote to local.")
    local_from_remote_success = globus_transfer(
        transfer_client=transfer_client,
        source_endpoint=config.remote_collection_uuid,
        destination_endpoint=config.local_collection_uuid,
        source_path="ve_data_science/data",
        destination_path=str(Path(config.repository_path) / "data"),
    )

    if not local_from_remote_success:
        LOGGER.error("Remote to local update failed.")
        return False

    # Update remote from local
    LOGGER.info("Starting GLOBUS synchronisation: local to remote.")
    remote_from_local_success = globus_transfer(
        transfer_client=transfer_client,
        source_endpoint=config.local_collection_uuid,
        destination_endpoint=config.remote_collection_uuid,
        source_path=str(Path(config.repository_path) / "data"),
        destination_path="ve_data_science/data",
    )

    if not remote_from_local_success:
        LOGGER.error("Local to remote update failed.")
        return False

    return True


def _recursive_ls_helper(
    transfer_client: globus_sdk.TransferClient,
    endpoint: str,
    queue: deque,
    max_depth: int,
    sleep_frequency: int,
    sleep_duration: float,
    ls_params: dict = {},
    top_level_ls_params: dict = {},
) -> Generator:
    """Helper function for recursive listing of GLOBUS endpoint."""

    call_count = 0
    while queue:
        abs_path, rel_path, depth = queue.pop()
        path_prefix = rel_path + "/" if rel_path else ""

        use_params = ls_params
        if call_count == 0:
            use_params = {**ls_params, **top_level_ls_params}

        res = transfer_client.operation_ls(endpoint, path=abs_path, **use_params)

        call_count += 1
        if call_count % sleep_frequency == 0:
            time.sleep(sleep_duration)

        if depth < max_depth:
            queue.extend(
                (
                    res["path"] + item["name"],
                    path_prefix + item["name"],
                    depth + 1,
                )
                for item in res["DATA"]
                if item["type"] == "dir"
            )
        for item in res["DATA"]:
            item["name"] = path_prefix + item["name"]
            yield item


def recursive_ls(
    transfer_client: globus_sdk.TransferClient,
    endpoint: str,
    path: str,
    max_depth: int = 3,
    sleep_frequency: int = 10,
    sleep_duration: float = 0.5,
    ls_params: dict | None = None,
    top_level_ls_params: dict | None = None,
) -> Generator:
    """A function generating a recursive listing of files on an endpoint."""
    ls_params = ls_params or {}
    top_level_ls_params = top_level_ls_params or {}
    queue: deque = deque()
    queue.append((path, "", 0))
    yield from _recursive_ls_helper(
        transfer_client,
        endpoint,
        queue,
        max_depth,
        sleep_frequency,
        sleep_duration,
        ls_params,
        top_level_ls_params,
    )


def globus_ls(
    transfer_client: globus_sdk.TransferClient,
    config: Config,
    remote: bool = True,
    ls_filter: str = "",
) -> list[dict]:
    """Retrieve a file listing of a GLOBUS collection.

    Note that if this function is used to retrieve the file listing of a local
    collection, then the GLOBUS servers are remotely querying the local endpoint and
    passing the data back in. This is useful for identifying what the GLOBUS system sees
    on an endpoint, but is not an efficient way to get a local file listing.

    Args:
        transfer_client: An authenticated GLOBUS transfer client.
        config: A config object.
        remote: Use the remote collection or the local one
        ls_filter: Filters to pass on to the listing process
    """

    # Create the generator - need to set the endpoint and path for remote vs local
    if remote:
        # Log the listing start
        LOGGER.info("Starting GLOBUS list on remote endpoint.")
        endpoint = config.remote_collection_uuid
        path = "ve_data_science/data"
    else:
        LOGGER.info("Starting GLOBUS list on local endpoint.")
        endpoint = config.local_collection_uuid
        path = str(Path(config.repository_path) / "data")

    file_list_generator = recursive_ls(
        transfer_client=transfer_client,
        endpoint=endpoint,
        path=path,
        ls_params={"filter": ls_filter},
    )

    # Retrieve the data by consuming from the generator
    file_list = list(file_list_generator)

    return file_list


def get_sync_status(
    transfer_client: globus_sdk.TransferClient,
    config: Config,
    ls_filter: str = "name:!~.*/name:!~MANIFEST.yaml",
) -> dict[str, list[str]]:
    """Generate a report on the synchronization of the remote and local endpoints.

    This function runs a recursive search of the files on both the remote and local
    endpoints and then returns a dictionary with keys providing lists of file path that
    are:

    * ``local_only``:  only present on the local endpoint
    * ``remote_only``:  only present on the remote endpoint
    * ``up_to_date``:  present on both with the same modification time
    * ``remote_outdated``:  present on both but the local file is newer
    * ``local_outdated``: present on both but the remote file is newer

    The status check explicitly ignores hidden files and MANIFEST.yaml files. For
    details of the filter string syntax, see:
    https://docs.globus.org/api/transfer/file_operations/#dir_listing_filtering

    Args:
        transfer_client: An authenticated GLOBUS transfer client.
        config: A config object.
        ls_filter: Filters to pass on to the listing process. The default is to ignore
            hidden files and MANIFEST.yaml files.
    """

    # Get the file listings for each endpoint
    remote = globus_ls(
        transfer_client=transfer_client, config=config, ls_filter=ls_filter
    )
    local = globus_ls(
        transfer_client=transfer_client,
        config=config,
        remote=False,
        ls_filter=ls_filter,
    )

    # Reduce to simple dictionaries of filename and modification date, dropping
    # directory entries. We can't filter the `TransferClient.operation_ls` using
    # the type:'file' filter string because then the dir entries aren't followed in the
    # recursive search.
    remote_names = {
        Path(f["name"]): datetime.fromisoformat(f["last_modified"])
        for f in local
        if f["type"] == "file"
    }

    local_names = {
        Path(f["name"]): datetime.fromisoformat(f["last_modified"])
        for f in remote
        if f["type"] == "file"
    }

    # Identify paths on both or only one endpoint
    both_endpoints = set(remote_names).intersection(local_names)
    local_only = set(remote_names).difference(local_names)
    remote_only = set(local_names).difference(remote_names)

    # Split files on both endpoints into outdated and up to date
    both_endpoints_times = {}
    for file in both_endpoints:
        both_endpoints_times[file] = (remote_names[file], local_names[file])

    up_to_date = [str(f) for f, (rd, ld) in both_endpoints_times.items() if rd == ld]
    remote_outdated = [
        str(f) for f, (rd, ld) in both_endpoints_times.items() if rd < ld
    ]
    local_outdated = [str(f) for f, (rd, ld) in both_endpoints_times.items() if rd > ld]

    return dict(
        local_only=sorted([str(file) for file in local_only]),
        remote_only=sorted([str(file) for file in remote_only]),
        up_to_date=sorted(up_to_date),
        remote_outdated=sorted(remote_outdated),
        local_outdated=sorted(local_outdated),
    )


def globus_status(config: Config) -> None:
    """Get and print the GLOBUS status.

    Args:
        config: A Config object
    """
    # Get the transfer client
    transfer_client = get_authenticated_transfer_client(config=config)

    # Get the status
    LOGGER.info("Starting GLOBUS status check")
    status = get_sync_status(transfer_client=transfer_client, config=config)

    output_sections = (
        ("Remote only:", "remote_only"),
        ("Local only:", "local_only"),
        ("Remote outdated:", "remote_outdated"),
        ("Local outdated:", "local_outdated"),
        ("Up to date:", "up_to_date"),
    )

    for msg, key in output_sections:
        LOGGER.info(msg)
        if status[key]:
            for file in status[key]:
                LOGGER.info(f" - {file}")
        else:
            LOGGER.info(" - No files")
