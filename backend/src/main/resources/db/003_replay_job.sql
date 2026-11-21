CREATE TABLE replay_job (
    target text PRIMARY KEY,
    source text NOT NULL CHECK (source <> target),
    confirmed_seq bigint NOT NULL CHECK (confirmed_seq >= 0),
    status text NOT NULL DEFAULT 'ready'
        CHECK (status IN ('ready', 'running', 'complete', 'failed')),
    failure text
);
CREATE UNIQUE INDEX one_pending_replay ON replay_job ((true))
    WHERE status IN ('ready', 'running');
