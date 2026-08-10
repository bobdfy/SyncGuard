--V1
CREATE TABLE IF NOT EXISTS
users(
    id  SERIAL PRIMARY KEY,
    username VARCHAR UNIQUE NOT NULL,
    password_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS
connections(
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id INT REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS
sync_jobs(
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    connection_id INT REFERENCES connections(id),
    task_name VARCHAR NOT NULL,
    status VARCHAR DEFAULT 'pending',
    cursor TEXT DEFAULT '',
    total_count INT DEFAULT 0,
    error_msg TEXT DEFAULT '',
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);
