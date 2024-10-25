# Running the project

## Prerequisites

- Python 3.11
- [pyenv](https://github.com/pyenv/pyenv) (optional, for managing Python versions)
- [miniconda](https://docs.conda.io/en/latest/miniconda.html) (optional, for managing Python version and packages)

## Setup Instructions

Set up the Python environment:

- If using miniconda:

```bash
conda create -n tick_data_analytics python=3.11
conda activate tick_data_analytics
```

Set up the dbt project:

```bash
make setup
```

> This will install the Python dependencies, configure dbt, install dbt packages, and check the connection to Snowflake.

## Running the dbt pipeline

> Note the data has already been parsed into parquet files and uploaded to S3, therefore Snowflake has access to it.
> More info on the data flow can be found in the [architecture](./architecture.md) documentation.

Therefore we just need to run the transformations in snowflake by triggering the dbt pipeline with

```bash
make run
```

dbt models can be found in the [models](../models) directory.

Running the pipeline end to end takes approximately **35 minutes**. The majority of this time is spent on constructing the continuous price series.

To access Snowflake and execute SQL queries on the tables, navigate to https://ud78363.eu-west-1.snowflakecomputing.com and use the credentials in the `.env` file.

Normally, these should not be disclosed, but for this project it's acceptable and comes with no risk.

## Running the analysis scripts and generate plots

```bash
make analysis
```

For more commands, check the Makefile.
