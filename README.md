# The ve_data_science_tool package

This is a python package to help maintain the Virtual Ecosystem data science repository.
The modules provide functionality in two main areas:

1. Both script files in the repository and data files require additional metadata to help
   keep track of where data comes from and how it is processed.

2. The data files are not stored within the repo but are gathered under the `data`
   directory on a GLOBUS share. The directory structure of the files is managed within
   the repo, using data manifest files to record the directories and their contents.

The package module roles are:

* `config`: provides a `Config` dataclass that stores the local path of a
  `ve_data_science` repository on a computer along with GLOBUS IDs for the remote data
  endpoint and the local endpoint provided by GLOBUS Personal Connect. The module can
  create the configuration and store it in a standard location (`configure()`) and then
  load the config (`load_config()`).

* `scripts`: provides dataclasses to store script and validate script metadata
  (`ScriptMetadata` and `ScriptFileDetails`). It provides functions to load and validate
  metadata from individual script files: the `validate_script_metadata()` function
  supports R, Python, R Markdown and Myst Markdown file formats. The
  high-level `check_scripts()` function runs validationto recursively within a top level
  directory, defaulting to the `analysis` directory.

* `data`: provides dataclasses to store and validate data manifest metadata (`Manifest`
  and `ManifestFile`). The `check_data_directory()` function checks that the manifest
  metadata in data directory is valid and matches the directory contents. The high_level
  `check_data()` function runs validation recursively within a top level directory,
  defaulting to the `data` directory.

* `globus`: provides functionality to connect to the GLOBUS API and then perform two
  high-level operations. The `globus_status()` function check the status of the files in
  the remote and local endpoints are report which files are up to date, missing or
  outdated across the two locations. The `globus_sync()` function automates the process
  of synchronising the most recent versions of files between the two endpoints.

* `entry_points`: provides a command-line interface to the high level functions in the
  other modules.

## Installing and setting up the tool

1. Install the package:

```sh
pip install git+https://github.com/ImperialCollegeLondon/ve_data_science_tool@main
```

   You should now be able to run the command `ve_data_science_tool -h`.

1. Make sure you have installed [Globus Connect
   Personal](https://www.globus.org/globus-connect-personal ), created a
   personal endpoint and that the Globus Connect Personal app is running.

1. At the command line, change directory to the root of your clone of the
   `ve_data_science` repo and run the `configure` subcommand. You will need two
   security ID tokens that identify the tool to GLOBUS and identify the remote endpoint
   for the `ve_data_science` data and which replace `UVW` and `XYZ` in the code below

```sh
python -m ve_data_science_tool configure --client-uuid UVW --remote-uuid XYZ
```

   You should now be able to use the command lines tools below.

## Command line usage

The command line tool `ve_data_science_tool` provides the following subcommands:

```sh
ve_data_science_tool configure
ve_data_science_tool scripts
ve_data_science_tool data
ve_data_science_tool globus_status
ve_data_science_tool globus_sync
```

Each of those subcommands runs the high level functionality from the submodules.

## Testing

There is currently rather minimal testing to check that correctly formatted metadata
passes validation. There is no testing of the GLOBUS systems, which would require
extensive mocking of the GLOBUS API.
