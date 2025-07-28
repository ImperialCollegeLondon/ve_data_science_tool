"""GLOBUS synchronisation tool.

This module providesfunctions to synchronise local data in Globus Personal
Connect collection with the central shared Guest Collection attached to the Imperial
HPC project store.
"""

import globus_sdk

from ve_data_science_tool.config import Config


def get_authenticated_transfer_client(config: Config) -> globus_sdk.TransferClient:
    """Generate an authenticated GLOBUS Transfer client object.

    The authentication process has multiple steps, requiring authorisation of several
    different permissions to access the collection. This function was developed with
    support by the GLOBUS development team to interact with the web authorisation
    process for GLOBUS.

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


def globus_sync(config: Config, transfer_client: globus_sdk.TransferClient) -> None:
    """Synchronize files to and from GLOBUS.

    In progress.

    Args:
        config: A SyncConfig instance providing connection data.
        transfer_client: An authenticated GLOBUS transfer client.

    """

    # Set up the Transfer Data object from remote to local
    tdata = globus_sdk.TransferData(
        transfer_client=transfer_client,
        source_endpoint=config.remote_collection_uuid,
        destination_endpoint=config.local_collection_uuid,
        preserve_timestamp=True,
    )

    # Add the remote data directory to the transfer data request
    tdata.add_item(
        "/ve_data_science/data/",
        "/Users/dorme/Research/Virtual_Rainforest/ve_data_science/data/testing",
        recursive=True,
    )

    # Submit the request and handle errors
    try:
        submit_result = transfer_client.submit_transfer(tdata)  # noqa: F841
    except globus_sdk.services.transfer.errors.TransferAPIError as excep:
        print(excep.raw_json)
        raise
