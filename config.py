import os
from dotenv import load_dotenv

load_dotenv()

START_YEAR = 2024
START_MONTH = 1
DATA_DIRECTORY = "."
FILE_PREFIX = "yellow_tripdata"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")