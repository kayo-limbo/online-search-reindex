DO $$
BEGIN
    IF put_document('42', 'before', 'text', 'news') IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'wrong insert sequence';
    END IF;
    IF put_document('42', 'after', 'updated', 'news') IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION 'wrong update sequence';
    END IF;
    IF (SELECT payload FROM document_changes WHERE seq = 1) IS DISTINCT FROM
       '{"id":"42","title":"before","body":"text","category":"news"}'::jsonb OR
       (SELECT payload FROM document_changes WHERE seq = 2) IS DISTINCT FROM
       (SELECT to_jsonb(documents) FROM documents WHERE id = '42') THEN
        RAISE EXCEPTION 'incorrect event snapshots';
    END IF;
END;
$$;

ALTER TABLE document_changes ADD CONSTRAINT inject_log_failure CHECK (seq <> 3);
DO $$
DECLARE
    failed_constraint text;
BEGIN
    BEGIN
        PERFORM put_document('42', 'must roll back', 'updated', 'news');
        RAISE EXCEPTION 'expected log failure';
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS failed_constraint = CONSTRAINT_NAME;
        IF failed_constraint <> 'inject_log_failure' THEN RAISE; END IF;
    END;
    IF (SELECT title FROM documents WHERE id = '42') IS DISTINCT FROM 'after' OR
       (SELECT committed_seq FROM index_control) <> 2 OR
       (SELECT count(*) FROM document_changes) <> 2 THEN
        RAISE EXCEPTION 'partial write after log failure';
    END IF;
END;
$$;
ALTER TABLE document_changes DROP CONSTRAINT inject_log_failure;

DO $$
BEGIN
    IF delete_document('42') IS DISTINCT FROM 3 THEN RAISE EXCEPTION 'wrong delete sequence'; END IF;
    IF delete_document('42') IS NOT NULL THEN RAISE EXCEPTION 'unexpected second delete'; END IF;
    IF EXISTS (SELECT FROM documents) OR (SELECT committed_seq FROM index_control) <> 3 OR
       NOT EXISTS (SELECT FROM document_changes WHERE seq = 3 AND document_id = '42'
                   AND operation = 'delete' AND payload IS NULL) THEN
        RAISE EXCEPTION 'invalid deletion state';
    END IF;
END;
$$;
