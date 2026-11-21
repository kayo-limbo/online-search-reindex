BEGIN;

CREATE TABLE index_control (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    active text NOT NULL,
    committed_seq bigint NOT NULL DEFAULT 0 CHECK (committed_seq >= 0)
);
INSERT INTO index_control (active) VALUES ('docs_v1');

CREATE TABLE documents (
    id text PRIMARY KEY,
    title text NOT NULL,
    body text NOT NULL,
    category text NOT NULL
);

CREATE TABLE document_changes (
    seq bigint PRIMARY KEY,
    document_id text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('upsert', 'delete')),
    payload jsonb,
    CHECK ((operation = 'delete') = (payload IS NULL))
);

CREATE FUNCTION put_document(doc_id text, doc_title text, doc_body text, doc_category text)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    next_seq bigint;
    saved documents;
BEGIN
    UPDATE index_control SET committed_seq = committed_seq + 1 WHERE singleton
        RETURNING committed_seq INTO STRICT next_seq;
    INSERT INTO documents VALUES (doc_id, doc_title, doc_body, doc_category)
        ON CONFLICT (id) DO UPDATE SET title = excluded.title, body = excluded.body,
                                       category = excluded.category
        RETURNING * INTO saved;
    INSERT INTO document_changes VALUES (next_seq, doc_id, 'upsert', to_jsonb(saved));
    RETURN next_seq;
END;
$$;

CREATE FUNCTION delete_document(doc_id text) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    next_seq bigint;
BEGIN
    PERFORM 1 FROM index_control WHERE singleton FOR UPDATE;
    DELETE FROM documents WHERE id = doc_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    UPDATE index_control SET committed_seq = committed_seq + 1 WHERE singleton
        RETURNING committed_seq INTO STRICT next_seq;
    INSERT INTO document_changes VALUES (next_seq, doc_id, 'delete', NULL);
    RETURN next_seq;
END;
$$;

COMMIT;
