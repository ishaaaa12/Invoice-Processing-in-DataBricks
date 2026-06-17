import json
import time

import requests
from pydantic import BaseModel

from config import HEADERS, HOST, WAREHOUSE_ID


def execute_sql(statement):
    response = requests.post(
        f"{HOST}/api/2.0/sql/statements",
        headers=HEADERS,
        json={
            "warehouse_id": WAREHOUSE_ID,
            "statement": statement
        }
    )

    response.raise_for_status()

    statement_id = response.json()["statement_id"]

    while True:
        result = requests.get(
            f"{HOST}/api/2.0/sql/statements/{statement_id}",
            headers=HEADERS
        )

        result.raise_for_status()

        data = result.json()
        state = data["status"]["state"]

        if state == "SUCCEEDED":
            return data["result"]["data_array"]

        if state == "FAILED":
            raise Exception(data)

        time.sleep(1)


def escape_sql_string(text):
    return text.replace("'", "''")


def clean_sql(sql_query):
    if isinstance(sql_query, list):
        sql_query = sql_query[0][0]

    sql_query = sql_query.strip()
    sql_query = sql_query.replace("```sql", "")
    sql_query = sql_query.replace("```", "")

    return sql_query.strip()


def generate_sql(question):
    prompt = f"""
You are an expert SQL generator.

Generate ONLY executable Databricks SQL.

Table:
gs_invoices.invoice_gold

Schema:

path STRING
subtotal DOUBLE
shipping DOUBLE
tax DOUBLE
total DOUBLE
calculated_total DOUBLE
diff DOUBLE
is_consistent BOOLEAN
status STRING

Business Logic:

status is computed as:

MISSING_TOTAL
    if total is null

MISSING_SUBTOTAL
    if subtotal is null

MISSING_SHIPPING
    if shipping is null

MISSING_TAX
    if tax is null

TOTAL_MISMATCH
    if diff > 0.05

OK
    otherwise

Additional Rules:

is_consistent = true
    means diff <= 0.05

is_consistent = false
    means diff > 0.05

A flagged invoice means:
    status != 'OK'

A valid invoice means:
    status = 'OK'

A mismatched invoice means:
    status = 'TOTAL_MISMATCH'

Examples:

Question:
How many invoices were flagged?

SQL:
SELECT COUNT(*)
FROM gs_invoices.invoice_gold
WHERE status != 'OK';

Question:
How many invoices passed validation?

SQL:
SELECT COUNT(*)
FROM gs_invoices.invoice_gold
WHERE status = 'OK';

Question:
How many invoices have total mismatches?

SQL:
SELECT COUNT(*)
FROM gs_invoices.invoice_gold
WHERE status = 'TOTAL_MISMATCH';

Question:
How many invoices are inconsistent?

SQL:
SELECT COUNT(*)
FROM gs_invoices.invoice_gold
WHERE is_consistent = false;

Question:
Show invoices with missing shipping.

SQL:
SELECT *
FROM gs_invoices.invoice_gold
WHERE status = 'MISSING_SHIPPING';

Question:
What is the average tax?

SQL:
SELECT AVG(tax) AS avg_tax
FROM gs_invoices.invoice_gold;

Question:
Which invoice has the highest total?

SQL:
SELECT path, total
FROM gs_invoices.invoice_gold
ORDER BY total DESC
LIMIT 1;

Important Instructions:

1. Return ONLY SQL.
2. Do NOT explain anything.
3. Do NOT use markdown.
4. Do NOT wrap SQL in ```sql.
5. Use COUNT(*) for "how many", "count", or "number of" questions.
6. Use AVG() for average questions.
7. Use MAX() or ORDER BY DESC LIMIT 1 for highest questions.
8. Use MIN() or ORDER BY ASC LIMIT 1 for lowest questions.
9. Use only the table gs_invoices.invoice_gold.

Question:
{question}

SQL:
"""

    prompt = escape_sql_string(prompt)

    sql = f"""
    SELECT ai_query(
      'databricks-gemini-3-5-flash',
      '{prompt}'
    ) AS generated_sql
    """

    result = execute_sql(sql)
    generated_sql = result[0][0]
    generated_sql = clean_sql(generated_sql)

    print("Generated SQL:", generated_sql)

    return generated_sql


def validate_sql(sql_query):
    sql_query = clean_sql(sql_query)
    sql_upper = sql_query.upper()

    if not sql_upper.startswith("SELECT"):
        raise Exception(
            f"Only SELECT statements allowed. Got: {sql_query}"
        )

    return True


def run_generated_sql(sql_query):
    validate_sql(sql_query)
    return execute_sql(sql_query)


def generate_answer(question, sql_result):
    sql_result = str(sql_result)
    sql_result = sql_result.replace("'", "")
    sql_result = sql_result.replace("\n", " ")

    prompt = f"""
    Question:{question}

    Result:{sql_result}

    Answer in plain English.
    """
    prompt = escape_sql_string(prompt)
    sql = f"""
    SELECT ai_query(
      'databricks-gemini-3-5-flash',
      '{prompt}'
    ) AS answer
    """

    answer = execute_sql(sql)

    return answer[0][0]


def chat(question):
    sql_query = generate_sql(question)
    result = run_generated_sql(sql_query)
    answer = generate_answer(
        question,
        json.dumps(result)
    )

    return {
        "question": question,
        "generated_sql": sql_query,
        "result": result,
        "answer": answer
    }


class ChatRequest(BaseModel):
    question: str
