# `tap-csv`
A Singer Tap for extracting data from CSV files built using the Meltano SDK.

CSV tap class.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

## Capabilities

* `state`
* `sync`
* `catalog`
* `discover`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| files               | False    | None    | An array of csv file stream settings. |
| csv_files_definition| False    | None    | A path to the JSON file holding an array of file settings. |

A full list of supported settings and capabilities is available by running: `tap-csv --about`

The `config.json` contains an array called `files` that consists of dictionary objects detailing each destination table to be passed to Singer. Each of those entries contains:
* `entity`: The entity name to be passed to singer (i.e. the table)
* `path`: Local path to the file to be ingested. Note that this may be a directory, in which case all files in that directory be processed if they also match the `prefix`.
* `prefix`: The required (case-insensitive) prefix of the filename for it to be processed.
* `keys`: The names of the columns that constitute the unique keys for that entity
* `start_from`: The replication key to start from. The format is `filename:rownumber` in lowercase.
* `delimiter`: What separator is used in the CSV files.
* `dialect`: What dialect is used in the CSV files. See <https://docs.python.org/3/library/csv.html#csv.list_dialects>
* `encoding`: What encoding is used in the CSV files.
* `quotechar`: What character is used for quoting in the CSV files.
* `header`: In case some or all files are missing a header, it can be specified here. Rows which match the specified header are skipped. So files with or without headers can be included in the same batch.

It is also possible to define a global `path` value which will be used as a fallback in case `path` is not specified for
the individual files object.

Example:

```json
{
  "path": "/path/to/default/",
  "files": [
    {
      "entity": "leads",
      "path": "/path/to/leads/",
      "prefix": "lead_con",
      "delimiter": ",",
      "dialect": "excel",
      "encoding": "utf-8",
      "quotechar": "\"",
      "header": [
        "Id",
        "Column1",
        "Column2"
      ],
      "keys": [
        "Id"
      ],
      "start_from": "lead_contracts.csv:8"
    },
    {
      "entity": "opportunities",
      "keys": [
        "Id"
      ]
    }
  ]
}
```

Optionally, the files definition can be provided by an external json file:

**config.json**
```json
{
  "path": "/path/to/default/",
	"csv_files_definition": "files_def.json"
}
```

**files_def.json**
```json
[
  {
    "entity": "leads",
    "path": "/path/to/leads.csv",
    "keys": [
      "Id"
    ]
  },
  {
    "entity": "opportunities",
    "path": "opportunities.csv",
    "keys": [
      "Id"
    ]
  }
]
```

## Installation

```bash
pipx install git+https://github.com/youcruit/tap-csv.git
```

## Usage

You can easily run `tap-csv` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-csv --version
tap-csv --help
tap-csv --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_csv/tests` subfolder and
  then run:

```bash
poetry run tox
poetry run tox -e pytest
poetry run tox -e format
poetry run tox -e lint
```

You can also test the `tap-csv` CLI interface directly using `poetry run`:

```bash
poetry run tap-csv --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-csv
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-csv --version
# OR run a test `elt` pipeline:
meltano elt tap-csv target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
