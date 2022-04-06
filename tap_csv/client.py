"""Custom client handling, including CSVStream base class."""

import csv
import os
from typing import Iterable, List, Optional, Any

from singer_sdk import typing as th
from singer_sdk.streams import Stream


class CSVStream(Stream):
    """Stream class for CSV streams."""

    def __init__(self, *args, **kwargs):
        """Init CSVStram."""
        self.file_paths: List[str] = []
        # cache file_config so we dont need to go iterating the config list again later
        self.file_config = kwargs.pop("file_config")
        super().__init__(*args, **kwargs)

    @property
    def replication_key(self) -> Optional[str]:
        return "replication_key"

    def get_starting_replication_key_value(
        self, context: Optional[dict]
    ) -> Optional[Any]:
        """Get starting replication key.

        Will return the value of the stream's replication key when `--state` is passed.
        If no prior state exists, will return `None`.

        If config specifies a starting value, then the max of that compared to the state is returned.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Starting replication value.
        """
        stream_state = super().get_starting_replication_key_value(context)

        config_state = self.file_config.get('start_from')

        if config_state and stream_state:
            # Return max
            if config_state > stream_state:
                return config_state
            else:
                return stream_state
        if config_state:
            # There is no stream_state
            return config_state
        # There is no config state
        return stream_state


    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        starting_replication_value = self.get_starting_replication_key_value(context)
        if starting_replication_value:
            splitted = starting_replication_value.split(":")
            starting_replication_file = ":".join(splitted[:-1])
            starting_replication_line = int(splitted[-1])
        else:
            starting_replication_file = None
            starting_replication_line = 0

        # Important that iteration order of file paths remain stable for incremental to work
        for file_path in self.get_file_paths():
            filename = os.path.basename(file_path).lower()
            is_starting_file = False

            if starting_replication_file and starting_replication_file > filename:
                # Skip file since we've already synced it
                continue
            elif starting_replication_file == filename:
                is_starting_file = True

            headers: List[str] = []
            for rowindex, row in enumerate(self.get_rows(file_path)):
                replication_value = f"{filename}:{rowindex}"
                if not headers:
                    headers = row + [self.replication_key]
                    continue
                # This will output the last replicated row again, but that is how other taps do it too
                if is_starting_file and rowindex < starting_replication_line:
                    continue

                yield dict(zip(headers, row + [f"{filename}:{rowindex}"]))

    def get_file_paths(self) -> list:
        """Return a list of file paths to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside.
        """
        # Cache file paths so we dont have to iterate multiple times
        if self.file_paths:
            return self.file_paths

        file_path = self.file_config["path"]
        if not os.path.exists(file_path):
            raise Exception(f"File path does not exist {file_path}")

        prefix = (self.file_config.get('prefix') or '').lower()

        file_paths = []
        if os.path.isdir(file_path):
            clean_file_path = os.path.normpath(file_path)
            # listdir returns an arbitrary order, but we care about the order for incremental support
            listed_files = sorted(
                os.listdir(clean_file_path),
                lambda f: f.lower()
            )

            for filename in listed_files:
                if filename.lower().startswith(prefix):
                    file_path = os.path.join(clean_file_path, filename)
                    if os.path.isfile(file_path):
                        file_paths.append(file_path)
        else:
            file_paths.append(file_path)

        if not file_paths:
            raise Exception(
                f"Stream '{self.name}' has no acceptable files. \
                    See warning for more detail."
            )
        self.file_paths = file_paths
        return file_paths

    def get_rows(self, file_path: str) -> Iterable[list]:
        """Return a generator of the rows in a particular CSV file."""
        with open(file_path, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                yield row

    @property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[th.Property] = []
        self.primary_keys = self.file_config.get("keys", [])

        for file_path in self.get_file_paths():
            for header in self.get_rows(file_path):
                break
            break

        for column in header:
            # Set all types to string
            # TODO: Try to be smarter about inferring types.
            properties.append(th.Property(column, th.StringType()))
        # Add row based replication key
        properties.append(th.Property(self.replication_key, th.StringType()))
        return th.PropertiesList(*properties).to_dict()

    @property
    def is_sorted(self) -> bool:
        """Check if stream is sorted.

        When `True`, incremental streams will attempt to resume if unexpectedly
        interrupted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.

        Returns:
            `True` if stream is sorted. Defaults to `False`.
        """
        return True

    @property
    def is_timestamp_replication_key(self) -> bool:
        return False
