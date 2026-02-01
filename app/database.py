import sqlite3
import os
import json
from datetime import datetime

DATABASE_PATH = "elasticsearch.db"
UPLOAD_DIR = "app/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def get_db():
    db = sqlite3.connect(DATABASE_PATH)
    db.execute("PRAGMA foreign_keys = ON")
    db.row_factory = sqlite3.Row  # For dict-like access
    return db

def init_db():
    db = get_db()
    # Create documents table
    db.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            title TEXT,
            content TEXT,
            version INTEGER DEFAULT 1,
            status TEXT DEFAULT 'uploaded',
            file_path TEXT,
            created_at TEXT,
            updated_at TEXT
        );
    """)
    # Add columns if not exist (for migration)
    try:
        db.execute("ALTER TABLE documents ADD COLUMN status TEXT DEFAULT 'uploaded'")
    except sqlite3.OperationalError:
        pass
    try:
        db.execute("ALTER TABLE documents ADD COLUMN file_path TEXT")
    except sqlite3.OperationalError:
        pass
    # Create extracted_text table
    db.execute("""
        CREATE TABLE IF NOT EXISTS extracted_text (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id TEXT,
            text TEXT,
            version INTEGER,
            FOREIGN KEY (doc_id) REFERENCES documents(id)
        );
    """)
    # Create inverted_index table
    db.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term TEXT,
            doc_id TEXT,
            positions TEXT,  -- JSON array of positions
            version INTEGER,
            PRIMARY KEY (term, doc_id, version),
            FOREIGN KEY (doc_id) REFERENCES documents(id)
        );
    """)
    # Create FTS virtual table for search
    db.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS fts_documents USING fts5(
            id UNINDEXED,
            title,
            content,
            tokenize = 'porter unicode61'
        );
    """)
    db.commit()
    db.close()

# Call init_db on import
init_db()

def insert_document(doc_id, title, content, file_path=None):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("""
        INSERT OR REPLACE INTO documents (id, title, content, status, file_path, created_at, updated_at)
        VALUES (?, ?, ?, 'uploaded', ?, ?, ?)
    """, (doc_id, title, content, file_path, now, now))
    db.commit()
    db.close()

def update_document_status(doc_id, status):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("""
        UPDATE documents SET status = ?, updated_at = ? WHERE id = ?
    """, (status, now, doc_id))
    db.commit()
    db.close()

def add_document(doc_id, title, content):
    db = get_db()
    now = datetime.now().isoformat()
    # Update status to indexing
    update_document_status(doc_id, 'indexing')
    # Insert or update document
    db.execute("""
        UPDATE documents SET title = ?, content = ?, version = version + 1, updated_at = ?, status = 'indexed'
        WHERE id = ?
    """, (title, content, now, doc_id))
    # Get current version
    cursor = db.execute("SELECT version FROM documents WHERE id = ?", (doc_id,))
    version = cursor.fetchone()[0]
    # Extract text (simple: just content lowercased)
    extracted = content.lower()
    db.execute("INSERT INTO extracted_text (doc_id, text, version) VALUES (?, ?, ?)", (doc_id, extracted, version))
    # Build inverted index (simple tokenization)
    tokens = extracted.split()
    term_positions = {}
    for pos, token in enumerate(tokens):
        if token not in term_positions:
            term_positions[token] = []
        term_positions[token].append(pos)
    for term, positions in term_positions.items():
        db.execute("INSERT OR REPLACE INTO inverted_index (term, doc_id, positions, version) VALUES (?, ?, ?, ?)",
                   (term, doc_id, json.dumps(positions), version))
    # Update FTS
    db.execute("INSERT OR REPLACE INTO fts_documents (id, title, content) VALUES (?, ?, ?)", (doc_id, title, content))
    db.commit()
    db.close()

def get_document_status(doc_id):
    db = get_db()
    cursor = db.execute("SELECT id, title, status, version, created_at, updated_at FROM documents WHERE id = ?", (doc_id,))
    row = cursor.fetchone()
    db.close()
    if row:
        status = dict(row)
        # Add terms count
        db = get_db()
        cursor = db.execute("SELECT COUNT(*) FROM inverted_index WHERE doc_id = ?", (doc_id,))
        status['terms_count'] = cursor.fetchone()[0]
        db.close()
        return status
    return None

def search_documents(query, limit=10):
    terms = query.lower().split()
    if not terms:
        return []
    db = get_db()
    # Get docs for each term
    term_docs = {}
    for term in terms:
        cursor = db.execute("SELECT doc_id, positions FROM inverted_index WHERE term = ?", (term,))
        docs = cursor.fetchall()
        term_docs[term] = {doc[0]: len(json.loads(doc[1])) for doc in docs}  # doc_id: term_freq
    # Find docs that have all terms (AND)
    if not term_docs:
        db.close()
        return []
    common_docs = set(term_docs[terms[0]].keys())
    for term in terms[1:]:
        common_docs &= set(term_docs[term].keys())
    # Calculate scores: sum of term frequencies
    scores = {}
    for doc_id in common_docs:
        score = sum(term_docs[term][doc_id] for term in terms if doc_id in term_docs[term])
        scores[doc_id] = score
    # Sort by score desc
    sorted_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:limit]
    # Get document details
    results = []
    for doc_id, score in sorted_docs:
        cursor = db.execute("SELECT title, content FROM documents WHERE id = ?", (doc_id,))
        row = cursor.fetchone()
        if row:
            results.append({"id": doc_id, "title": row[0], "content": row[1], "score": score})
    db.close()
    return results

def get_all_documents(limit=100):
    db = get_db()
    cursor = db.execute("SELECT id, title, content, status, version, created_at, updated_at FROM documents LIMIT ?", (limit,))
    results = cursor.fetchall()
    db.close()
    return [{"id": r["id"], "title": r["title"], "content": r["content"], "status": r["status"], "version": r["version"], "created_at": r["created_at"], "updated_at": r["updated_at"]} for r in results]

def get_raw_documents(limit=100):
    # For debugging: raw rows from documents table
    db = get_db()
    cursor = db.execute("SELECT * FROM documents LIMIT ?", (limit,))
    results = cursor.fetchall()
    db.close()
    return [dict(r) for r in results]

def get_raw_extracted_text(limit=100):
    db = get_db()
    cursor = db.execute("SELECT * FROM extracted_text LIMIT ?", (limit,))
    results = cursor.fetchall()
    db.close()
    return [dict(r) for r in results]

def get_raw_inverted_index(limit=100):
    db = get_db()
    cursor = db.execute("SELECT * FROM inverted_index LIMIT ?", (limit,))
    results = cursor.fetchall()
    db.close()
    return [dict(r) for r in results]