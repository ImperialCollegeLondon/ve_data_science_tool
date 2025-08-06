"""Module to maintain data directories."""

from pathlib import Path
from pprint import pformat
from textwrap import indent
from typing import ClassVar, Literal

import yaml
from marshmallow import Schema
from marshmallow.exceptions import ValidationError
from marshmallow_dataclass import dataclass
from marshmallow_dataclass.typing import Url

from ve_data_science_tool import LOGGER
from ve_data_science_tool.config import Config


@dataclass
class ManifestFile:
    """A dataclass for file details in data directory manifests."""

    name: str
    url: Url | None = None
    script: str | None = None
    md5: str | None = None


@dataclass
class Manifest:
    """A validation schema for data directory manifests."""

    directory: str
    files: list[ManifestFile]

    # Type the Schema attribute
    Schema: ClassVar[type[Schema]]


def load_manifest(file: Path) -> Manifest:
    """Load a manifest file.

    Args:
        file: A Path to a manifest file.

    Raises:
        YAMLError: File contents not valid YAML.
        ValidationError: File contents not congruent with Manifest schema.
    """

    with open(file) as manifest_io:
        try:
            manifest_data: dict = yaml.safe_load(manifest_io)
        except yaml.error.YAMLError:
            raise

    # Does it conform to the Schema
    try:
        manifest = Manifest.Schema().load(data=manifest_data)
    except ValidationError:
        raise

    return manifest


def check_data_directory(config: Config, directory: Path) -> bool:
    """Validate a data directory.

    This function checks that a data directory has a MANIFEST.yaml file and that the
    contents of the manifest are congruent with the directory contents and provide
    complete metadata.

    The function logs the validation process and returns True or False to indicate
    success or failure of the validation.

    Args:
        config: A Config object.
        directory: A path to a data directory.
    """

    # Check that the directory a subpath within the repository
    try:
        directory_relative = directory.resolve().relative_to(config.repository_path)
    except ValueError:
        LOGGER.error(
            f" \u2717 The directory is not within the "
            f"ve_data_science repo: {directory!s}"
        )
        return False

    LOGGER.info(f"Checking {directory_relative}")

    # Do we have a directory to validate
    if not directory.exists():
        LOGGER.error(" \u2717 Directory not found")
        return False

    if not directory.is_dir():
        LOGGER.error(" \u2717 Directory path is a file not a directory")
        return False

    # Get the directory file as a set
    actual_files = {
        f.name
        for f in directory.iterdir()
        if not f.name.startswith(".") and f.is_file() and f.name != "MANIFEST.yaml"
    }

    # Check the MANIFEST.yaml file is present if files are present and that no MANIFEST
    # is found if there are no files
    manifest_file = directory / "MANIFEST.yaml"

    if actual_files and not manifest_file.exists():
        LOGGER.error(" \u2717 MANIFEST.yaml not found")
        return False

    if not actual_files and manifest_file.exists():
        LOGGER.error(" \u2717 MANIFEST.yaml file in empty directory")
        return False

    if not actual_files and not manifest_file.exists():
        LOGGER.error(" \u2713 Directory empty: no manifest required.")
        return True

    # Load the manifest, logging errors.
    try:
        manifest = load_manifest(manifest_file)
    except yaml.error.YAMLError as excep:
        LOGGER.error(" \u2717 Cannot parse MANIFEST.yaml")
        LOGGER.error(excep)
        return False
    except ValidationError as excep:
        LOGGER.error(" \u2717 MANIFEST.yaml structure incorrect:")
        LOGGER.error(indent(pformat(excep.messages, indent=1, compact=True), "   "))
        return False

    # Are the contents valid and complete.
    # - These checks should all run before returning to give a complete assessment.
    return_value = True
    logged_errors = []

    # Is the relative directory path in the manifest file congruent with its location?
    if directory_relative != Path(manifest.directory):
        logged_errors.append(
            f"   \u2717 MANIFEST.yaml directory does not match "
            f"location: {manifest.directory}"
        )
        return_value = False

    manifest_files = {entry.name for entry in manifest.files}

    if not manifest_files == actual_files:
        only_in_manifest = manifest_files.difference(actual_files)
        only_in_directory = actual_files.difference(manifest_files)

        if only_in_manifest:
            logged_errors.append(
                f"   \u2717 Unknown files in manifest: {', '.join(only_in_manifest)}"
            )
        if only_in_directory:
            logged_errors.append(
                f"   \u2717 Files missing from manifest: {', '.join(only_in_directory)}"
            )

        return_value = False

    # Is the script or url attribute set for each file. Although this check could be in
    # the dataclass schema using the @validates_schema decorator, we want to be able to
    # read and write partially complete manifests, so it is only implemented as part of
    # explicit metadata validation.

    for file in manifest.files:
        if not (file.url is None) ^ (file.script is None):
            logged_errors.append(
                f"   \u2717 File does not provide _one_ of url or script: {file.name}"
            )
            return_value = False

    if return_value is True:
        LOGGER.info(" \u2713 Valid manifest")
    else:
        LOGGER.error(" \u2717 Manifest contains errors")
        for msg in logged_errors:
            LOGGER.error(msg)

    return return_value


def check_data(config: Config, directory: Path | None = None) -> bool:
    """Recursively validate metadata in data directories.

    This function checks that a data directory and its subdirectories contain valid
    metadata. The function logs the validation process and returns True or False to
    indicate success or failure of the validation.

    Args:
        config: A config object
        directory: A directory within which to carry out script validation.

    """

    # Default is to search the data directory
    if directory is None:
        directory = Path(config.repository_path) / "data"

    LOGGER.info(f"Checking all data directories within : {directory}")

    # Walk the directories. Future note pathlib.Path.walk() in 3.12+
    directories = [path for path in directory.rglob("*") if path.is_dir()]
    directories.insert(0, directory)

    for each_dir in directories:
        check_data_directory(config=config, directory=each_dir)

    return True


def populate_manifest(
    config: Config, directory: Path
) -> tuple[Literal["empty", "created", "updated"], list[Path]]:
    """Populates a manifest file in a directory.

    If the directory does not contain a MANIFEST.yaml file, then one is created within
    the directory and the ``files`` attribute is populated from the directory contents.
    If the directory does contain a MANIFEST.yaml file, then it is opened and any new
    files are added to the manifest. Hidden files are not included in manifest files.

    This function does not do any metadata checking or population - it just creates
    manifest entries for files within the directory. The resulting manifest files will
    not pass data metadata validation and need further checking to do so. Critically:

    * The url or script attributes for newly added files are not populated.
    * The function does not remove manifest entries for files that are not present in
      the directory. These files might be missing in the local directory or might
      genuinely have been removed, but that is something to solve at the data
      metadata validation step.

    The function does not create a manifest in a directory that does not contain files.
    The function returns a tuple containing the status of the population and a list of
    the paths added to the manifest.

    Args:
        config: A config object
        directory: A directory within which to carry out script validation.
    """

    # Check that the directory a subpath within the repository
    try:
        directory_relative = directory.resolve().relative_to(config.repository_path)
    except ValueError:
        raise ValueError(
            f"The directory is not within the ve_data_science repo: {directory!s}"
        )

    # Get the expected manifest file path and the non-hidden files within the directory
    # that are not the MANIFEST file itself (if one exists) paths in the directory
    manifest_path = directory / "MANIFEST.yaml"
    # TODO - filter to data files?
    files = [
        f
        for f in directory.glob("*")
        if not (f.is_dir() or f.name.startswith(".") or f.name == "MANIFEST.yaml")
    ]

    if not (manifest_path).exists():
        # Do not create manifest files in empty directories
        if not files:
            return "empty", files

        # If a manifest file does not exist then create one, using the existing files
        # within the directory. This does not attempt to populate the url or script
        # attribute.
        manifest = Manifest(
            directory=str(directory_relative),
            files=[ManifestFile(name=f.name) for f in files],
        )

        with open(manifest_path, "w") as outfile:
            yaml.safe_dump(data=Manifest.Schema().dump(manifest), stream=outfile)

        return "created", files

    # Otherwise, update the existing manifest with any new files
    try:
        manifest = load_manifest(manifest_path)
    except (yaml.error.YAMLError, ValidationError):
        raise

    existing_files = set([f.name for f in manifest.files])
    new_files = [f for f in files if f.name not in existing_files]
    for file in new_files:
        manifest.files.append(ManifestFile(name=file.name))

    with open(manifest_path, "w") as outfile:
        yaml.safe_dump(data=Manifest.Schema().dump(manifest), stream=outfile)

    return "updated", list(new_files)


def update_manifests(config: Config, directory: Path | None = None) -> bool:
    """Recursively update data directory manifest files.

    This function iterates recursively within a target directory, updating manifest
    files. If no manifest file is found, one is created. Otherwise, files in each
    directory are added to the existing manifest file.

    Args:
        config: A config object
        directory: A directory within which to carry out script validation.

    """

    # Default is to search the data directory
    if directory is None:
        directory = Path(config.repository_path) / "data"

    LOGGER.info(f"Checking all data directories within : {directory}")

    # Walk the directories. Future note pathlib.Path.walk() in 3.12+
    directories = [path for path in directory.rglob("*") if path.is_dir()]
    directories.insert(0, directory)

    for each_dir in directories:
        directory_relative = each_dir.resolve().relative_to(config.repository_path)
        LOGGER.info(f"  Directory: {directory_relative}")

        try:
            status, files = populate_manifest(config=config, directory=each_dir)
        except yaml.error.YAMLError as excep:
            LOGGER.error(f"   \u2717 Existing manifest not valid YAML: {excep!s}")
            continue
        except ValidationError as excep:
            LOGGER.error("   \u2717 Existing manifest contains metadata errors:")
            LOGGER.error(
                indent(pformat(excep.messages, indent=1, compact=True), " " * 5)
            )
            continue

        match status:
            case "empty":
                LOGGER.info("   \u2713 Empty directory")
            case "created":
                LOGGER.info(f"   \u2713 Manifest created: {len(files)} added")
            case "updated":
                if files:
                    LOGGER.info(f"   \u2713 Manifest updated: {len(files)} added")
                else:
                    LOGGER.info("   \u2713 Manifest up to date")
    return True
