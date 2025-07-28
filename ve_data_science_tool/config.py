"""Tools to create and load the tool configuration."""

from pathlib import Path
from typing import ClassVar

import globus_sdk
import platformdirs
import yaml
from marshmallow import Schema
from marshmallow.exceptions import ValidationError
from marshmallow_dataclass import dataclass


@dataclass
class Config:
    """Configuration dataclass.

    This dataclass is used to maintain the configuration identities and tokens needed to
    interact with the GLOBUS system, along with the local repository path and other
    details.

    The app and remote collection details are required but the other attributes can be
    populated by authentication.
    """

    repository_path: str
    app_client_uuid: str
    app_client_name: str
    remote_collection_uuid: str
    local_collection_uuid: str | None = None

    Schema: ClassVar[type[Schema]]


def configure(
    client_uuid: str, remote_uuid: str, repository_dir: str | None = None
) -> Path:
    """Generate the ve_data_science_tool configuration.

    This function sets up the configuration file for the ve_data_science tool. Two
    arguments are required to set UUIDs required by GLOBUS. These should be kept secure
    so are not recorded in the code. The local ve_data_science repository path is also
    needed - if this is not set, the function will check if the current working
    directory is the repo root.

    The file is saved to a platform appropriate configuration directory, and the path to
    the file is returned.

    Arg:
        client_uuid: The UUID of the client application.
        remote_uuid: The UUID of the remote data collection.
        repository_dir: The local path to the ve_data_science repository
    """

    config_directory = platformdirs.user_config_path(appname="ve_data_science_tool")
    config_file = config_directory / "config.yaml"

    if config_file.exists():
        raise RuntimeError(f"Configuration file already exists at: {config_file}")

    if repository_dir is None:
        repository_path = Path(".").absolute()
    else:
        repository_path = Path(repository_dir).absolute()

    # Check for the root directory of a ve_data_science repo clone
    if not (repository_path / ".ve_data_science").exists():
        raise RuntimeError(
            "Cannot confirm ve_data_science directory: run configuration in repository "
            "root or provide path."
        )

    # Retrieve the local collection UUID using the globus_sdk and set it
    local = globus_sdk.LocalGlobusConnectPersonal()

    config = Config(
        repository_path=str(repository_path),
        app_client_uuid=client_uuid,
        app_client_name="ve_data_science",
        remote_collection_uuid=remote_uuid,
        local_collection_uuid=local.endpoint_id,
    )

    # Save the config to the configuration directory
    if not config_directory.exists():
        config_directory.mkdir(parents=True)

    with open(config_file, "w") as cfg_out:
        yaml.safe_dump(
            data=Config.Schema().dump(config),
            stream=cfg_out,
        )

    return config_file


def load_config() -> Config:
    """Load the configuration file.

    This method loads the configuration file and returns a Config instance.

    """

    config_directory = platformdirs.user_config_path(appname="ve_data_science_tool")
    config_file = config_directory / "config.yaml"

    if not config_file.exists():
        raise ValueError("Configuration file not found: run `configure`")

    with open(config_file) as cfp:
        try:
            config_data = yaml.safe_load(cfp)
        except yaml.YAMLError as excep:
            raise ValueError("Error reading configuration YAML: " + str(excep))

    try:
        config: Config = Config.Schema().load(data=config_data)
    except ValidationError as excep:
        raise ValueError("Invalid configuration data: " + str(excep))

    return config
