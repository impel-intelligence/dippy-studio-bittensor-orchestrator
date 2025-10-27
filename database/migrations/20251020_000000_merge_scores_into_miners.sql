-- Add a JSONB column to store score payloads alongside miner state.
ALTER TABLE miners
    ADD COLUMN scores JSONB;

-- Migrate existing records from the legacy scores table when present.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'scores'
    ) THEN
        INSERT INTO miners (hotkey, scores)
        SELECT hotkey, value::jsonb
        FROM scores
        ON CONFLICT (hotkey) DO UPDATE
        SET scores = EXCLUDED.scores;

        DROP TABLE scores;
    END IF;
END $$;
