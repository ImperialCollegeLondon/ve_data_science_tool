"""Test the scripts module."""

from importlib import resources

import pytest


def test_read_r_script_metadata():
    """Test read_r_script_metadata."""
    from ve_data_science_tool.scripts import read_r_script_metadata

    path = resources.files("tests.script_files")
    metadata = read_r_script_metadata(path / "script.R")

    assert isinstance(metadata, dict)


def test_read_py_script_metadata():
    """Test read_py_script_metadata."""
    from ve_data_science_tool.scripts import read_py_script_metadata

    path = resources.files("tests.script_files")
    metadata = read_py_script_metadata(path / "script.py")

    assert isinstance(metadata, dict)


@pytest.mark.parametrize(argnames="file_name", argvalues=("script.Rmd", "script.md"))
def test_read_markdown_notebook_metadata(file_name):
    """Test read_rmd_script_metadata."""
    from ve_data_science_tool.scripts import read_markdown_notebook_metadata

    path = resources.files("tests.script_files")
    metadata = read_markdown_notebook_metadata(path / file_name)

    assert isinstance(metadata, dict)


@pytest.mark.parametrize(
    argnames="filename", argvalues=("script.R", "script.py", "script.Rmd", "script.md")
)
def test_validate_script_metadata(filename):
    """Test the validation function."""
    from ve_data_science_tool.scripts import (
        ScriptMetadata,
        validate_script_metadata,
    )

    path = resources.files("tests.script_files")
    metadata = validate_script_metadata(path / filename)

    assert isinstance(metadata, ScriptMetadata)


def test_check_scripts(fixture_config):
    """Test the validation function."""
    from ve_data_science_tool.scripts import check_scripts

    path = resources.files("tests.script_files")
    success = check_scripts(config=fixture_config, directory=path)

    assert success
