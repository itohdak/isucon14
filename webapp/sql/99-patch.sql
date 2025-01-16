ALTER TABLE rides ADD sales INTEGER DEFAULT 0;
ALTER TABLE coupons ADD id VARCHAR(255) NOT NULL;
UPDATE coupons SET id = CONCAT(user_id, '_', code);
ALTER TABLE coupons DROP PRIMARY KEY, ADD PRIMARY KEY (id);
CREATE INDEX idx_user_id_code ON coupons (user_id, code);