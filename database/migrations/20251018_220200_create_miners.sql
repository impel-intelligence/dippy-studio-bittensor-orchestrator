-- Create miners table to store serialized miner state payloads
CREATE TABLE miners (
    hotkey TEXT PRIMARY KEY,
    value  JSONB NOT NULL
);
