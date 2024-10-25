USE ROLE SYSADMIN;

-- Create the DATA_PLATFORM database
CREATE DATABASE IF NOT EXISTS DATA_PLATFORM;

-- Set the owner of the database to SYSADMIN
ALTER DATABASE DATA_PLATFORM SET OWNER = SYSADMIN;

-- Use the newly created database
USE DATABASE DATA_PLATFORM;

-- Optional: Create a comment for the database
COMMENT ON DATABASE DATA_PLATFORM IS 'Database for data platform and analytics';
