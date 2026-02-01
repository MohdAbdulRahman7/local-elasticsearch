from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
from .database import search_documents, get_all_documents, get_raw_documents, get_raw_extracted_text, get_raw_inverted_index, insert_document, get_document_status
from .rabbitmq import publish_to_queue, get_queue_stats, TEXT_EXTRACT_QUEUE
import json
import os

router = APIRouter()

class Document(BaseModel):
    id: str
    title: str
    content: str

@router.post("/documents")
async def add_document(doc: Document):
    message = json.dumps({"id": doc.id, "title": doc.title, "content": doc.content})
    await publish_to_queue(TEXT_EXTRACT_QUEUE, message)
    return {"message": "Document queued for processing"}

@router.post("/upload")
async def upload_document(file: UploadFile = File(...)):
    content = await file.read()
    text_content = content.decode("utf-8")
    doc_id = file.filename
    title = file.filename
    file_path = f"app/uploads/{doc_id}"
    with open(file_path, "w") as f:
        f.write(text_content)
    insert_document(doc_id, title, text_content, file_path)
    message = json.dumps({"id": doc_id})
    await publish_to_queue(TEXT_EXTRACT_QUEUE, message)
    return {"message": f"Document '{file.filename}' uploaded and indexed", "doc_id": doc_id}

@router.get("/documents")
async def get_documents(q: str = None, limit: int = 10):
    if q:
        return {"results": search_documents(q, limit)}
    else:
        return {"results": get_all_documents(limit)}

@router.get("/search")
async def search(q: str, limit: int = 10):
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    return {"results": search_documents(q, limit)}

@router.get("/raw/documents")
async def raw_documents(limit: int = 100):
    return {"data": get_raw_documents(limit)}

@router.get("/raw/extracted_text")
async def raw_extracted_text(limit: int = 100):
    return {"data": get_raw_extracted_text(limit)}

@router.get("/raw/inverted_index")
async def raw_inverted_index(limit: int = 100):
    return {"data": get_raw_inverted_index(limit)}

@router.get("/queue/stats")
async def queue_stats():
    return await get_queue_stats()

@router.get("/documents/{doc_id}/status")
async def document_status(doc_id: str):
    status = get_document_status(doc_id)
    if not status:
        raise HTTPException(status_code=404, detail="Document not found")
    return status