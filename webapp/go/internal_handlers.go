package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to begin in internal matching: %v", err))
		return
	}
	defer tx.Rollback()
	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matched := &Chair{}
	if err := tx.GetContext(
		ctx,
		matched,
		`SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE (SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = chairs.id) GROUP BY ride_id) is_completed WHERE completed = FALSE) AND is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id FOR UPDATE SKIP LOCKED`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)

	}

	if _, err := tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to commit in internal matching: %v", err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
