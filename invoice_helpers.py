import requests
import time

from config import HEADERS, HOST, JOB_ID
from chatbot_helpers import execute_sql


def upload_to_volume(file_bytes, filename):
    volume_path = f"/Volumes/workspace/default/raw_invoices/{filename}"
    url = f"{HOST}/api/2.0/fs/files{volume_path}"

    response = requests.put(
        url,
        headers={
            **HEADERS,
            "Content-Type": "application/octet-stream"
        },
        data=file_bytes
    )

    response.raise_for_status()

    return volume_path


def start_job():
    response = requests.post(
        f"{HOST}/api/2.1/jobs/run-now",
        headers=HEADERS,
        json={
            "job_id": JOB_ID
        }
    )

    response.raise_for_status()

    return response.json()["run_id"]


def wait_for_job(run_id):
    while True:
        response = requests.get(
            f"{HOST}/api/2.1/jobs/runs/get",
            headers=HEADERS,
            params={
                "run_id": run_id
            }
        )

        response.raise_for_status()

        state = response.json()["state"]
        lifecycle = state["life_cycle_state"]

        if lifecycle == "TERMINATED":
            if state.get("result_state") == "SUCCESS":
                return
            raise Exception("Workflow failed")

        time.sleep(5)


def get_invoice_result(filename):
    sql = f"""
    SELECT *
    FROM gs_invoices.invoice_gold
    WHERE path LIKE '%{filename}'
    LIMIT 1
    """

    rows = execute_sql(sql)

    if not rows:
        raise Exception("Invoice processed but row not found")

    return rows[0]
