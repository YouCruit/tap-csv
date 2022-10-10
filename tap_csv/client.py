"""Custom client handling, including CSVStream base class."""

import csv
import gzip
import json
import os
from typing import IO, Any, Iterable, List, Optional
from uuid import uuid4

from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.streams import Stream

from . import get_file_paths

MAX_BATCH_SIZE: int = 9223372036854775807


class CSVStream(Stream):
    """Stream class for CSV streams."""

    # Set this value to force an early batch message
    _force_batch_message: bool = False

    def __init__(self, *args, **kwargs):
        """Init CSVStram."""
        self.file_paths: List[str] = []
        # cache file_config so we dont need to go iterating the config list again later
        self.file_config = kwargs.pop("file_config")
        super().__init__(*args, **kwargs)

    @property
    def batch_size(self) -> int:
        return self.config.get("batch_size", 10_000_000)

    @property
    def replication_key(self) -> str:
        """Get replication key.

        Returns:
            Replication key for the stream.
        """
        return "replication_key"

    @replication_key.setter
    def replication_key(self, new_value: str) -> None:
        """Doesn't do anything. Only exists to silence warnings"""
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
                self.logger.info(f"Skipping [{filename}] because it's already synced")
                continue
            elif starting_replication_file == filename:
                self.logger.info(
                    f"Starting sync of [{filename}] from line [{starting_replication_line}]"
                )
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

                if rowindex % 100000 == 0:
                    self.logger.debug(f"Syncing [{filename}] line [{rowindex:09}]")

                # Padding with zeroes so lexicographic sorting matches numeric
                yield dict(zip(headers, row + [f"{filename}:{rowindex:09}"]))

            # Force a batch message to be sent - mixing data from different files is bad
            # since unique ids may become non-unique
            self._force_batch_message = True

            self.logger.info(f"Synced [{filename}] with [{rowindex:09}] lines")

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

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: Optional[dict] = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        i = 1
        chunk_size = 0
        filename: Optional[str] = None
        f: Optional[IO] = None
        gz: Optional[gzip.GzipFile] = None

        with batch_config.storage.fs() as fs:
            for record in self._sync_records(context, write_messages=False):
                # Why do this first? Because get_records can change the batch size
                # but that won't be visible until the NEXT record
                if self._force_batch_message or chunk_size >= self.batch_size:
                    gz.close()
                    gz = None
                    f.close()
                    f = None
                    file_url = fs.geturl(filename)
                    yield batch_config.encoding, [file_url]

                    filename = None

                    i += 1
                    chunk_size = 0

                    # Reset force flag
                    self._force_batch_message = False

                if filename is None:
                    filename = f"{prefix}{sync_id}-{i}.json.gz"
                    f = fs.open(filename, "wb")
                    gz = gzip.GzipFile(fileobj=f, mode="wb")

                gz.write((json.dumps(record) + "\n").encode())
                chunk_size += 1

            if chunk_size > 0:
                gz.close()
                f.close()
                file_url = fs.geturl(filename)
                yield batch_config.encoding, [file_url]

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
