CREATE TABLE IF NOT EXISTS documents(
    id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP PUBLICATION IF EXISTS syncguard_pub;
CREATE PUBLICATION syncguard_pub FOR TABLE documents;