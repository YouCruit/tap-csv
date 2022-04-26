"""CSV tap class."""

import json
import os
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty

from .client import CSVStream
from . import get_file_paths


class TapCSV(Tap):
    """CSV tap class."""

    name = "tap-csv"

    config_jsonschema = th.PropertiesList(
        th.Property("path", th.StringType, required=False),
        th.Property(
            "files",
            th.ArrayType(
                th.ObjectType(
                    th.Property("entity", th.StringType, required=True),
                    th.Property("path", th.StringType, required=False),
                    th.Property("keys", th.ArrayType(th.StringType), required=True),
                    th.Property("prefix", th.StringType, required=False),
                    th.Property("delimiter", th.StringType, required=False),
                    th.Property("dialect", th.StringType, required=False),
                    th.Property("encoding", th.StringType, required=False),
                    th.Property("quotechar", th.StringType, required=False),
                    th.Property("start_from", th.StringType, required=False),
                )
            ),
            description="An array of csv file stream settings.",
        ),
        th.Property(
            "csv_files_definition",
            th.StringType,
            description="A path to the JSON file holding an array of file settings.",
        ),
    ).to_dict()

    @classproperty
    def capabilities(self) -> List[str]:
        """Get tap capabilites."""
        return ["state", "sync", "catalog", "discover"]

    def get_file_configs(self) -> List[dict]:
        """Return a list of file configs.

        Either directly from the config.json or in an external file
        defined by csv_files_definition.
        """
        for key in self.config:
            if key not in self.config_jsonschema["properties"]:
                self.logger.error(f"Unknown config variable: '{key}'")
                raise ValueError(f"Unknown config variable: '{key}'")

        default_path = self.config.get("path")
        csv_files = self.config.get("files")
        csv_files_definition = self.config.get("csv_files_definition")
        if csv_files_definition:
            if os.path.isfile(csv_files_definition):
                with open(csv_files_definition, "r") as f:
                    csv_files = json.load(f)
            else:
                self.logger.error(f"tap-csv: '{csv_files_definition}' file not found")
                raise ValueError(f"tap-csv: '{csv_files_definition}' file not found")

        if not csv_files:
            return []

        # Iterate through and set path if needed
        for f in csv_files:
            for key in f:
                if (
                    key
                    not in self.config_jsonschema["properties"]["files"]["items"][
                        "properties"
                    ]
                ):
                    self.logger.error(f"Unknown config variable: '{key}'")
                    raise ValueError(f"Unknown config variable: '{key}'")
            if "path" not in f:
                if not default_path:
                    self.logger.error("No global path and no file path defined")
                    raise ValueError("No global path and no file path defined")
                f["path"] = default_path
        return csv_files

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [
            CSVStream(
                tap=self,
                name=file_config.get("entity"),
                file_config=file_config,
            )
            for file_config in self.get_file_configs()
            if get_file_paths(file_config)
        ]


if __name__ == "__main__":
    TapCSV.cli()
