import asyncio
import json
import aio_pika
from .rabbitmq import get_connection, TEXT_EXTRACT_QUEUE, INDEX_QUEUE, publish_to_queue
from .database import add_document, update_document_status

async def process_text_extract(message: aio_pika.IncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        doc_id = data["id"]
        # Get file_path from DB
        from .database import get_document_status
        doc = get_document_status(doc_id)
        if not doc or 'file_path' not in doc:
            print(f"No file path for {doc_id}")
            return
        file_path = doc['file_path']
        try:
            with open(file_path, 'r') as f:
                content = f.read()
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            update_document_status(doc_id, 'failed')
            return
        update_document_status(doc_id, 'extracting')
        # Extract plain text: simple lowercase
        extracted = content.lower()
        # Store in SQLite
        from .database import get_db
        db = get_db()
        db.execute("INSERT INTO extracted_text (doc_id, text, version) VALUES (?, ?, ?)",
                   (doc_id, extracted, doc['version']))
        db.commit()
        db.close()
        # Publish to index queue
        index_data = {"id": doc_id, "extracted": extracted}
        await publish_to_queue(INDEX_QUEUE, json.dumps(index_data))
        print(f"Text extracted for document: {doc_id}")

async def process_index(message: aio_pika.IncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        doc_id = data["id"]
        extracted = data["extracted"]
        # Get document from DB
        from .database import get_document_status
        doc = get_document_status(doc_id)
        if not doc:
            print(f"Document not found: {doc_id}")
            return
        update_document_status(doc_id, 'indexing')
        # Tokenize and normalize: split by space, lowercase already done
        tokens = extracted.split()
        term_positions = {}
        for pos, token in enumerate(tokens):
            if token not in term_positions:
                term_positions[token] = []
            term_positions[token].append(pos)
        # Store in inverted index with transaction
        from .database import get_db
        db = get_db()
        try:
            db.execute("BEGIN TRANSACTION")
            for term, positions in term_positions.items():
                db.execute("INSERT OR REPLACE INTO inverted_index (term, doc_id, positions, version) VALUES (?, ?, ?, ?)",
                           (term, doc_id, json.dumps(positions), doc['version']))
            # Update FTS
            db.execute("INSERT OR REPLACE INTO fts_documents (id, title, content) VALUES (?, ?, ?)",
                       (doc_id, doc['title'], doc['content']))
            db.execute("COMMIT")
            update_document_status(doc_id, 'indexed')
            print(f"Indexed document: {doc_id} with {len(term_positions)} terms")
        except Exception as e:
            db.execute("ROLLBACK")
            update_document_status(doc_id, 'failed')
            print(f"Indexing failed for {doc_id}: {e}")
        finally:
            db.close()

async def start_consumers():
    try:
        connection = await get_connection()
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            # Consumer for text_extract_queue
            text_queue = await channel.get_queue(TEXT_EXTRACT_QUEUE)
            await text_queue.consume(process_text_extract)
            # Consumer for index_queue
            index_queue = await channel.get_queue(INDEX_QUEUE)
            await index_queue.consume(process_index)
            print("Consumers started. Waiting for messages...")
            await asyncio.Future()  # Keep running
    except Exception as e:
        print(f"Failed to start consumers: {e}. Processing will not happen.")