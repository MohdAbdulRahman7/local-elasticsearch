# local-elasticsearch
A single-node mini Elasticsearch built with FastAPI, RabbitMQ, and SQLite.

## Features
- Async document processing via RabbitMQ
- Full-text search using SQLite FTS
- Simple web UI for searching and adding documents

## Setup
1. Create a virtual environment:
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Ensure RabbitMQ is running locally (default: amqp://guest:guest@localhost/). If not, install and start RabbitMQ.

3. Run the application:
   ```
   uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
   ```

4. Open http://localhost:8080 in your browser to access the UI.

5. (Optional) Run tests:
   ```
   pytest tests/
   ```

## API Endpoints

### Add Document
- `POST /documents`: Queue a document for async processing
  ```
  curl -X POST "http://localhost:8080/documents" \
       -H "Content-Type: application/json" \
       -d '{"id": "1", "title": "Sample Title", "content": "Sample content here"}'
  ```

### Upload Document
- `POST /upload`: Upload a text file for processing
  ```
  curl -X POST "http://localhost:8080/upload" \
       -F "file=@sample.txt"
  ```

### Get Documents
- `GET /documents`: Retrieve all documents (or search with `?q=query&limit=10`)
  ```
  curl "http://localhost:8080/documents"
  curl "http://localhost:8080/documents?q=hello&limit=5"
  ```

### Search Documents
- `GET /search?q=query`: Full-text search with AND semantics using inverted index, returns relevance scores
  ```
  curl "http://localhost:8080/search?q=hello world"
  ```

### Raw Data (Debugging)
- `GET /raw/documents`: Raw documents table
  ```
  curl "http://localhost:8080/raw/documents"
  ```
- `GET /raw/extracted_text`: Raw extracted text table
  ```
  curl "http://localhost:8080/raw/extracted_text"
  ```
- `GET /raw/inverted_index`: Raw inverted index table
  ```
  curl "http://localhost:8080/raw/inverted_index"
  ```

### Queue Stats
- `GET /queue/stats`: Get message counts for queues and DLQs
  ```
  curl "http://localhost:8080/queue/stats"
  ```

### Document Status
- `GET /documents/{doc_id}/status`: Get document processing status
  ```
  curl "http://localhost:8080/documents/sample.txt/status"
  ```

### Health Check
- `GET /health`: Service health
  ```
  curl "http://localhost:8080/health"
  ```

## Notes
- Documents are processed asynchronously.
- Search uses SQLite's full-text search with ranking.
