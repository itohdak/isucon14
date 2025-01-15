ALTER TABLE rides ADD sales INTEGER DEFAULT 0;
ALTER TABLE chairs ADD is_available TINYINT(1) INVISIBLE NOT NULL DEFAULT 1;
CREATE INDEX idx_is_active_is_available_id ON chairs (is_active, is_available, id);
CREATE INDEX idx_id_is_active_is_available ON chairs (id, is_active, is_available);