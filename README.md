# The ve_data_science_tool package

This is a python package to help maintain the Virtual Ecosystem data science repository.
There are two main areas:

## Script metadata

Both script files in the repository and data files require additional metadata to help
keep track of where data comes from and how it is processed. This tool checks that
metadata is provided and is consistent across the repository.

## Data files

The data files are not stored within the repo but are gathered under the `data`
directory on a GLOBUS share. The directory structure of the files is managed within the
repo, using data manifest files to record the directories and their contents. The tool
provides a synchronisation command to populate the directories with actual data from the
GLOBUS repo and upload any new data.
