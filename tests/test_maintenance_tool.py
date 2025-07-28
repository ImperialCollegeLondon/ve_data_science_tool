"""Testing the maintenance tool CLI."""


def test_ve_data_science_tool():
    """Test the maintenance tool endpoints."""
    from ve_data_science_tool.entry_points import ve_data_science_tool_cli

    val = ve_data_science_tool_cli(
        ["check_data_directory", "data/primary/soil/carbon_use_efficiency"]
    )

    assert val
