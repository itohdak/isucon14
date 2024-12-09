DROP TABLE IF EXISTS ride_statuses_org;
ALTER TABLE ride_statuses RENAME ride_statuses_org;

DROP TABLE IF EXISTS ride_statuses;
CREATE TABLE ride_statuses
(
  id              VARCHAR(26)                                                                NOT NULL,
  ride_id VARCHAR(26)                                                                        NOT NULL COMMENT 'ライドID',
  status          ENUM ('MATCHING', 'ENROUTE', 'PICKUP', 'CARRYING', 'ARRIVED', 'COMPLETED') NOT NULL COMMENT '状態',
  created_at      DATETIME(6)                                                                NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '状態変更日時',
  app_sent_at     DATETIME(6)                                                                NULL COMMENT 'ユーザーへの状態通知日時',
  chair_sent_at   DATETIME(6)                                                                NULL COMMENT '椅子への状態通知日時',
  PRIMARY KEY (ride_id),
  INDEX idx_ride_id_created_at (ride_id, created_at DESC)
);

INSERT INTO
    ride_statuses (
        id,
        ride_id,
        status,
        created_at,
        app_sent_at,
        chair_sent_at
    )
SELECT
    r1.*
FROM
    ride_statuses_org r1
    INNER JOIN (
        SELECT
            r2.ride_id as ride_id,
            MAX(r2.created_at) as created_at
        FROM
            ride_statuses_org r2
        GROUP BY
            r2.ride_id
    ) AS r3 ON r1.ride_id = r3.ride_id
    AND r1.created_at = r3.created_at;