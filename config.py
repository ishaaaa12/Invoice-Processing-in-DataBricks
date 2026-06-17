from dotenv import load_dotenv
from fastapi import FastAPI
import os

load_dotenv()

app = FastAPI()

HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID = int(os.getenv("JOB_ID"))
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}
