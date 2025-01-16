ALTER TABLE rides ADD sales INTEGER DEFAULT 0;
ALTER TABLE chairs ADD is_available TINYINT(1) INVISIBLE NOT NULL DEFAULT 1;
CREATE INDEX idx_is_active_is_available_id ON chairs (is_active, is_available, id);
CREATE INDEX idx_id_is_active_is_available ON chairs (id, is_active, is_available);
ALTER TABLE coupons ADD id VARCHAR(255) NOT NULL;
UPDATE coupons SET id = CONCAT(user_id, '_', code);
ALTER TABLE coupons DROP PRIMARY KEY, ADD PRIMARY KEY (id);
CREATE INDEX idx_user_id_code ON coupons (user_id, code);