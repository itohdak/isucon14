package main

import (
	"database/sql"
	"errors"
	"math"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 最も待たせているリクエストを取得
	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 空いている椅子とその最新の位置情報を一度に取得
	type ChairWithLocation struct {
		Chair
		Latitude  int `db:"latitude"`
		Longitude int `db:"longitude"`
	}
	chairsWithLocations := []ChairWithLocation{}
	query := `
			SELECT c.id, cl.latitude, cl.longitude
			FROM chairs c
			JOIN (
					SELECT chair_id, latitude, longitude
					FROM chair_locations
					WHERE (chair_id, created_at) IN (
							SELECT chair_id, MAX(created_at)
							FROM chair_locations
							GROUP BY chair_id
					)
			) cl ON c.id = cl.chair_id
			WHERE c.is_active = TRUE
	`
	if err := db.SelectContext(ctx, &chairsWithLocations, query); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairsWithLocations) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 最も近い椅子を見つける
	var nearestChair *ChairWithLocation
	minDistance := math.MaxInt

	for _, chair := range chairsWithLocations {
		distance := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, chair.Latitude, chair.Longitude)
		if distance < minDistance {
			minDistance = distance
			nearestChair = &chair
		}
	}

	if nearestChair == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// ライドに椅子を割り当てる
	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", nearestChair.ID, ride.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
