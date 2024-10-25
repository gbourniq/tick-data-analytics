USE ACCOUNTADMIN;

-- https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
CREATE STORAGE INTEGRATION IF NOT EXISTS dev_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::148761645097:role/TickDataDemoSnowflakeRole'
    STORAGE_ALLOWED_LOCATIONS = ('s3://dev.data-staging.eu-west-1/')
    COMMENT = 'dev s3 integration';

-- Then copy the secret to the AWS externalId trust policy field
DESC INTEGRATION dev_s3_integration;
