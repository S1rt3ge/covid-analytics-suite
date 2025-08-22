import os
from dotenv import load_dotenv

load_dotenv()

APP_NAME = "Snowflake+Mongo API"

SF_CFG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_DB = os.getenv("MONGODB_DB", "covid_meta")
MONGO_COUNTRY_COL = os.getenv("MONGODB_COUNTRY_COL", "country_stats")

REQUIRED_SF = ["account", "user", "password", "warehouse", "database", "schema"]