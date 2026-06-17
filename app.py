from fastapi import File, UploadFile
import uuid

from chatbot_helpers import ChatRequest, chat
from config import app
from invoice_helpers import get_invoice_result, start_job, upload_to_volume, wait_for_job


@app.post("/validate-invoice")
async def validate_invoice(file: UploadFile = File(...)):
    unique_filename = f"{uuid.uuid4()}_{file.filename}"
    content = await file.read()

    upload_to_volume(content, unique_filename)

    run_id = start_job()
    wait_for_job(run_id)

    row = get_invoice_result(unique_filename)

    return {
        "path": row[0],
        "shipping": row[1],
        "tax": row[2],
        "subtotal": row[3],
        "total": row[4],
        "calculated_total": row[5],
        "diff": row[6],
        "status": row[7]
    }


@app.post("/chat")
def chat_api(req: ChatRequest):
    return chat(req.question)
