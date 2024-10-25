import os
from pathlib import Path

from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Get the current file's directory and construct the path to the .env file
current_dir = Path(__file__).resolve().parent
env_path = current_dir.parent.parent / ".env"

# Load the .env file
load_dotenv(dotenv_path=env_path)


def get_snowflake_connection() -> Session:
    try:
        session = Session.builder.configs(
            {
                "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
                "user": os.environ.get("SNOWFLAKE_USER"),
                "password": os.environ.get("SNOWFLAKE_PASSWORD"),
                "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
                "database": os.environ.get("SNOWFLAKE_DATABASE"),
                "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
            }
        ).create()
        # print("Successfully connected to Snowflake")
        return session
    except SnowparkSQLException as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        raise
