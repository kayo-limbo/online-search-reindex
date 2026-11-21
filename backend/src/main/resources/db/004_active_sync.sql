CREATE TABLE active_sync (
    target text PRIMARY KEY,
    confirmed_seq bigint NOT NULL,
    pending_seq bigint CHECK (pending_seq = confirmed_seq + 1),
    failure text
);
INSERT INTO active_sync(target, confirmed_seq)
SELECT active, COALESCE((SELECT confirmed_seq FROM replay_job WHERE target=active AND status='complete'), 0)
FROM index_control;
