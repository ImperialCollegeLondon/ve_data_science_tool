"""Testing the ve_data_science_tool CLI."""


def test_ve_data_science_tool_scripts():
    """Test the maintenance tool endpoints."""
    from ve_data_science_tool.entry_points import ve_data_science_tool_cli

    val = ve_data_science_tool_cli(["scripts", "tests/script_files"])

    assert val
