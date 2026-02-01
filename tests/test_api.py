import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_get_documents_empty():
    response = client.get("/documents")
    assert response.status_code == 200
    assert "results" in response.json()

def test_add_document():
    doc = {"id": "test1", "title": "Test Doc", "content": "This is a test document."}
    response = client.post("/documents", json=doc)
    assert response.status_code == 200
    assert "message" in response.json()

def test_search_documents():
    response = client.get("/search?q=test")
    assert response.status_code == 200
    assert "results" in response.json()

def test_search_empty_query():
    response = client.get("/search?q=")
    assert response.status_code == 400
    assert "required" in response.json()["detail"]

def test_raw_documents():
    response = client.get("/raw/documents")
    assert response.status_code == 200
    assert "data" in response.json()

def test_raw_extracted_text():
    response = client.get("/raw/extracted_text")
    assert response.status_code == 200
    assert "data" in response.json()

def test_raw_inverted_index():
    response = client.get("/raw/inverted_index")
    assert response.status_code == 200
    assert "data" in response.json()

def test_upload_document():
    # Mock file upload
    files = {"file": ("test.txt", "Test content", "text/plain")}
    response = client.post("/upload", files=files)
    assert response.status_code == 200
    assert "uploaded" in response.json()["message"]
    doc_id = response.json()["doc_id"]
    # Test status (may be processed quickly)
    import time
    time.sleep(1)  # Wait for processing
    status_response = client.get(f"/documents/{doc_id}/status")
    assert status_response.status_code == 200
    status = status_response.json()
    assert status["status"] in ["uploaded", "extracting", "indexing", "indexed"]

def test_queue_stats():
    response = client.get("/queue/stats")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]