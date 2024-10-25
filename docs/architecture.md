# Architecture

## Overview

For this project I decided to use snowflake and dbt given the amount of data that needs to be processed (856m rows).

Python is used for statistical analysis and plotting, where the data is retrieved by the snowpark python connector.

The overall architecture diagram can be seen below:
![architecture](.github/images/architecture.png)

## Snowflake

Snowflake's Free tier comes with $400 worth of credits, which is more than enough for this project.

## ELT Process

### Extraction

The first step is to parse the data from the `ES.h5` file into multiple smaller parquet files which is required by Snowflake.

This is done by the [scripts/convert_h5_to_parquet_chunks.py](scripts/convert_h5_to_parquet_chunks.py) script which takes about 1 hour to run.

The data is then uploaded to an S3 bucket by the [scripts/upload_parquet_chunks_to_s3.py](scripts/upload_parquet_chunks_to_s3.py) script.

In practice, these functions could run in AWS Batch / ECS given the size of the data.

> Note: Although two datasets were found in the `ES.h5` file, only the `tick/trades_filter0vol` dataset was loaded into s3.
> The reasoning for this is that the `tick/trades` dataset contains 0 volume trades, which are not needed for this analysis.
> But it's debatable whether we may still want to load all the data in the data warehouse for further analysis.
> Note2: There may be a simpler way of parsing the data, the currently script seems overly complex.

### Load

Next step is to load the data into Snowflake, which is done through an s3 integration object and external stage (see [infra](infra) DDL statements).

The external table, which serves as a reference to the S3 location, is created during the `make setup` process, and can be found in the SOURCE schema.

The raw data is then loaded into the [stg**mock_provider**equity_index_future\_\_tick_data](models/equity_index_future/staging/stg__mock_provider__equity_index_future__tick_data.sql) staging table as part of running the dbt pipeline.

### Transformation

This is the hierarchy of the tables, where the data flows from source -> staging -> transform -> fact schemas.

```
models/
└── equity_index_future
    ├── fact
    │   ├── timeseries__es_equity_index_future__bar_returns_correlation.sql
    │   ├── timeseries__es_equity_index_future__monthly_bar_variance.sql
    │   └── timeseries__es_equity_index_future__weekly_bar_counts.sql
    ├── staging
    │   ├── config.yml
    │   ├── mock_provider
    │   │   └── sources.yml
    │   └── stg__mock_provider__equity_index_future__tick_data.sql
    └── transform
        ├── int__es_equity_index_future__continuous.sql
        ├── int__es_equity_index_future__dollars_traded_bars.sql
        ├── int__es_equity_index_future__tick_bars.sql
        └── int__es_equity_index_future__volume_bars.sql
```

The model lineage can be seen in the diagram below:

![dbt lineage](.github/images/dbt_model_lineage.png)

This diagram is a screenshot from the dbt docs site, which can be viewed by running `make serve-docs`.

## Analysis

The analysis is done in the [analysis](analysis) directory, where the data is retrieved by the snowpark python connector and plots are generated.

These should answer the questions in [docs/analysis.md](docs/analysis.md) documentation.
