package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const costReductionSec float64 = 30

type MatchingResult struct {
	Chair ChairWithLatLon
	Ride  Ride
}

func cube(f float64) float64 {
	return f * f * f
}
func execMatching(rides []Ride, chairs []ChairWithLatLon) []MatchingResult {
	n, m := len(rides), len(chairs)
	g := newMinCostFlow(n + m + 2)
	s, t := n+m, n+m+1
	for i, _ := range rides {
		g.AddEdge(s, i, 1, 0)
	}
	for i, _ := range chairs {
		g.AddEdge(n+i, t, 1, 0)
	}
	for i, ride := range rides {
		for j, chair := range chairs {
			var model = ChairModel{
				Speed: 1,
			}
			if modelCached, found := chairModelCache.Load(chair.Model); found {
				model = modelCached.(ChairModel)
			} else {
				log.Printf("[WARN] chair model not found: model name: %s", chair.Model)
			}
			cost := float64(
				abs(ride.PickupLatitude-chair.Latitude)+
					abs(ride.PickupLongitude-chair.Longitude)+
					abs(ride.DestinationLatitude-ride.PickupLatitude)+
					abs(ride.DestinationLongitude-ride.PickupLongitude)) /
				float64(model.Speed) *
				cube(max(0, (costReductionSec-time.Since(ride.CreatedAt).Seconds()))/costReductionSec)
			g.AddEdge(i, n+j, 1, int(cost))
		}
	}
	g.FlowL(s, t, n)
	edges := g.Edges()
	matches := make([]MatchingResult, 0, len(edges))
	for _, e := range edges {
		if e.from == s || e.to == t || e.flow == 0 {
			continue
		}
		matches = append(matches, MatchingResult{
			Chair: chairs[e.to-n],
			Ride:  rides[e.from],
		})
	}
	return matches
}

func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chairs := []ChairWithLatLon{}
	if err := db.Select(&chairs, `
	WITH chair_latest_location AS (
		SELECT
			*
		FROM
			(
				SELECT
					chair_locations.*,
					ROW_NUMBER() OVER (
						PARTITION BY chair_id
						ORDER BY
							created_at DESC
					) AS rn
				FROM
					chair_locations
			) c
		WHERE
			c.rn = 1
	),
	chair_latest_status AS (
		SELECT
			*
		FROM
			(
				SELECT
					rides.*,
					ride_statuses.status AS ride_status,
					ROW_NUMBER() OVER (
						PARTITION BY chair_id
						ORDER BY
							ride_statuses.created_at DESC
					) AS rn
				FROM
					rides
					INNER JOIN ride_statuses ON rides.id = ride_statuses.ride_id
					AND ride_statuses.chair_sent_at IS NOT NULL -- この条件は椅子の通知エンドポイントの実装で、未送信の状態がある2つ以上の異なるライドが割り当てられていても正しく順番に送るように修正していれば不要
			) r
		WHERE
			r.rn = 1
	)
	SELECT
		chairs.*,
		chair_latest_location.latitude,
		chair_latest_location.longitude
	FROM
		chairs
		LEFT JOIN chair_latest_status ON chairs.id = chair_latest_status.chair_id
		LEFT JOIN chair_latest_location ON chairs.id = chair_latest_location.chair_id
	WHERE
		(
			chair_latest_status.ride_status = 'COMPLETED'
			OR chair_latest_status.ride_status IS NULL
		)
		AND chairs.is_active
		AND chair_latest_location.latitude IS NOT NULL`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// there're 2 areas
	// matching heuristically
	var chairsA, chairsB []ChairWithLatLon
	for _, chair := range chairs {
		if chair.Latitude < 150 {
			chairsA = append(chairsA, chair)
		} else {
			chairsB = append(chairsB, chair)
		}
	}
	var ridesA, ridesB []Ride
	if err := db.SelectContext(ctx, &ridesA, `SELECT * FROM rides WHERE chair_id IS NULL AND pickup_latitude < 150 ORDER BY created_at LIMIT ?`, len(chairsA)); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := db.SelectContext(ctx, &ridesB, `SELECT * FROM rides WHERE chair_id IS NULL AND pickup_latitude >= 150 ORDER BY created_at LIMIT ?`, len(chairsB)); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(ridesA) == 0 && len(ridesB) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var wg sync.WaitGroup
	var matchesA, matchesB []MatchingResult
	wg.Add(1)
	go func() {
		matchesA = execMatching(ridesA, chairsA)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		matchesB = execMatching(ridesB, chairsB)
		wg.Done()
	}()
	wg.Wait()
	matches := append(matchesA, matchesB...)

	var chairIDs = make([]string, 0, len(matches))
	var rideIDs = make([]string, 0, len(matches))
	for _, match := range matches {
		chairIDs = append(chairIDs, match.Chair.ID)
		rideIDs = append(rideIDs, match.Ride.ID)
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	query := "UPDATE rides SET chair_id = ELT(FIELD(id, :rideIDs), :chairIDs), updated_at = :updatedAt WHERE id IN (:rideIDs)"
	query, params, err := sqlx.Named(query, map[string]interface{}{
		"rideIDs":   rideIDs,
		"chairIDs":  chairIDs,
		"updatedAt": time.Now(),
	})
	query, params, err = sqlx.In(query, params...)
	if _, err := tx.ExecContext(ctx, query, params...); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to bulk update rides: rideIDs: %s chairIDs: %s: %w", rideIDs, chairIDs, err))
		return
	}

	query = "UPDATE chairs SET is_available = :isAvailable WHERE id IN (:chairIDs)"
	query, params, err = sqlx.Named(query, map[string]interface{}{
		"isAvailable": true,
		"chairIDs":    chairIDs,
	})
	query, params, err = sqlx.In(query, params...)
	if _, err := tx.ExecContext(ctx, query, params...); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to bulk update chairs: chairIDs: %s: %w", chairIDs, err))
		return
	}

	query = "SELECT id, ride_id FROM ride_statuses WHERE ride_id IN (?) AND status = 'MATCHING' FOR SHARE"
	query, params, err = sqlx.In(query, rideIDs)
	var rideStatuses []RideStatus
	if err := tx.SelectContext(ctx, &rideStatuses, query, params...); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to select ride_statuses: rideIDs: %s: %w", rideIDs, err))
		return
	}
	var rideStatusMap map[string]string
	for _, rideStatus := range rideStatuses {
		rideStatusMap[rideStatus.RideID] = rideStatus.ID
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matchedString := "[internal_matcher] chair_id,ride_id,pck_lat,pck_lon,dst_lat,dst_lon,curr_lat,curr_lon\n"
	for _, match := range matches {
		matchedRideID := match.Ride.ID
		matchedUserID := match.Ride.UserID
		matchedChairID := match.Chair.ID

		// commit locals
		userRideCache.Delete(matchedUserID)
		chairRideCache.Delete(matchedChairID)
		rideCache.Delete(matchedRideID)
		notifyToChannel("", matchedChairID, rideStatusMap[matchedRideID], matchedRideID, "MATCHING")

		matchedString += fmt.Sprintf("[internal_matcher] %s,%s,%d,%d,%d,%d,%d,%d\n", matchedChairID, matchedRideID, match.Ride.PickupLatitude, match.Ride.PickupLongitude, match.Ride.DestinationLatitude, match.Ride.DestinationLongitude, match.Chair.Latitude, match.Chair.Longitude)
	}
	log.Printf("[internal_matcher] internalGetMatching: matches: %d, chairs: %d, rides: %d", len(matches), len(chairs), len(ridesA)+len(ridesB))
	log.Printf(matchedString)

	w.WriteHeader(http.StatusNoContent)
}
