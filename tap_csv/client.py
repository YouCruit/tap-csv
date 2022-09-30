"""Custom client handling, including CSVStream base class."""

import csv
import os
from typing import Any, Iterable, List, Optional

from singer_sdk import typing as th
from singer_sdk.streams import Stream
from . import get_file_paths


class CSVStream(Stream):
    """Stream class for CSV streams."""

    def __init__(self, *args, **kwargs):
        """Init CSVStram."""
        self.file_paths: List[str] = []
        # cache file_config so we dont need to go iterating the config list again later
        self.file_config = kwargs.pop("file_config")
        super().__init__(*args, **kwargs)

    @property
    def replication_key(self) -> str:
        """Get replication key.

        Returns:
            Replication key for the stream.
        """
        return "replication_key"

    @replication_key.setter
    def replication_key(self, new_value: str) -> None:
        """Doesn't do anything. Only exists to silence warnings
        """
        # To silence warnings
        pass

    def get_starting_replication_key_value(
        self, context: Optional[dict]
    ) -> Optional[Any]:
        """Get starting replication key.

        Will return the value of the stream's replication key when `--state` is passed.
        If no prior state exists, will return `None`.

        If config specifies a starting value, then the max of that compared
        to the state is returned.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            Starting replication value.
        """
        stream_state = super().get_starting_replication_key_value(context)

        config_state = self.file_config.get("start_from")
        if config_state:
            # state is in lowercase
            config_state = config_state.lower()

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

        # Important that iteration order of file paths remains stable
        for file_path in self.get_file_paths():
            filename = os.path.basename(file_path).lower()
            is_starting_file = False

            if starting_replication_file and starting_replication_file > filename:
                # Skip file since we've already synced it
                continue
            elif starting_replication_file == filename:
                is_starting_file = True

            headers: List[str] = self.file_config.get("header", [])
            if headers:
                headers += [self.replication_key]

            for rowindex, row in enumerate(self.get_rows(file_path)):
                if not headers:
                    headers = row + [self.replication_key]
                    continue
                if headers and rowindex == 0:
                    if list_equals(headers[:-1], row):
                        # Header line
                        continue
                # This will output the last replicated row again
                # but that is how other taps do it too
                if is_starting_file and rowindex < starting_replication_line:
                    continue

                # Padding with zeroes so lexicographic sorting matches numeric
                yield dict(zip(headers, row + [f"{filename}:{rowindex:09}"]))

    def get_file_paths(self) -> list:
        """Return a list of file paths to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside.
        """
        # Cache file paths so we dont have to iterate multiple times
        if not self.file_paths:
            self.file_paths = get_file_paths(self.file_config)
        # Check again
        if not self.file_paths:
            self.logger.warning(f"Stream '{self.name}' has no acceptable files")

        return self.file_paths

    def get_rows(self, file_path: str) -> Iterable[list]:
        """Return a generator of the rows in a particular CSV file."""
        params = {}
        if self.file_config.get("delimiter", None):
            params["delimiter"] = self.file_config["delimiter"]
        if self.file_config.get("quotechar", None):
            params["quotechar"] = self.file_config["quotechar"]
        if self.file_config.get("dialect", None):
            params["dialect"] = self.file_config["dialect"]
        with open(file_path, "r", encoding=self.file_config.get("encoding")) as f:
            reader = csv.reader(f, **params)
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

        header: List[str] = self.file_config.get("header", [])

        if not header:
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
        """
        Replication key is not a timestamp
        """
        return False


def list_equals(lhs: List, rhs: List):
    """Returns True IFF all objects in lhs are equal to all objects in rhs"""
    if len(lhs) != len(rhs):
        return False

    for l, r in zip(lhs, rhs):
        if l != r:
            return False

    return True
