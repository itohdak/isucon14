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

	// 最も待たせているリクエストを最大1件取得
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 5`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 空いている椅子とその最新の位置情報を一度に取得
	type ChairWithLocation struct {
		Chair
		Latitude  int `db:"latitude"`
		Longitude int `db:"longitude"`
		Speed     int `db:"speed"`
	}
	chairsWithLocations := []ChairWithLocation{}
	query := `
			SELECT c.id, cl.latitude, cl.longitude, cm.speed
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
			JOIN chair_models cm ON c.model = cm.name
			WHERE c.is_active = TRUE
			AND c.id NOT IN (
					SELECT r.chair_id
					FROM rides r
					JOIN ride_statuses rs ON r.id = rs.ride_id
					WHERE chair_id IS NOT NULL
					AND rs.chair_sent_at IS NOT NULL
					AND rs.app_sent_at IS NOT NULL
					GROUP BY r.chair_id, rs.ride_id
					HAVING COUNT(rs.id) < 6
			)
	`
	if err := db.SelectContext(ctx, &chairsWithLocations, query); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(chairsWithLocations) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 割り当て済みの椅子を追跡するためのマップ
	assignedChairs := make(map[string]bool)

	// 各リクエストに対して最も近い椅子を見つけて割り当てる
	for _, ride := range rides {
		var bestChair *ChairWithLocation
		minScore := math.MaxInt

		for _, chair := range chairsWithLocations {
			if assignedChairs[chair.ID] {
				continue
			}

			pickupDistance := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, chair.Latitude, chair.Longitude)
			destDistance := calculateDistance(ride.DestinationLatitude, ride.DestinationLongitude, ride.PickupLatitude, ride.PickupLongitude)
			rideFee := 500 + destDistance*100
			// 最も時給のいいものを選択
			score := rideFee / ((pickupDistance + destDistance) / chair.Speed)
			if score < minScore {
				minScore = score
				bestChair = &chair
			}
		}

		if bestChair != nil {
			// ライドに椅子を割り当てる
			if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", bestChair.ID, ride.ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			// 割り当て済みの椅子としてマーク
			assignedChairs[bestChair.ID] = true
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
