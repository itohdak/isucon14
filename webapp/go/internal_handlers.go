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
	validChairIDs, err := getValidChairIDsCache(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	for _, validChairID := range validChairIDs {
		chair, err := getChairCache(ctx, db, validChairID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		chairLocation, err := getChairLatestLocationCache(ctx, chair.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		chairs = append(chairs, ChairWithLatLon{
			ID:        chair.ID,
			Latitude:  chairLocation.Latitude,
			Longitude: chairLocation.Longitude,
		})
	}
	// if err := db.Select(&chairs, `
	// SELECT
	// 	chairs.*,
	// 	chair_latest_location.latest_latitude AS latitude,
	// 	chair_latest_location.latest_longitude AS longitude
	// FROM
	// 	chairs
	// 	INNER JOIN chair_latest_location ON chairs.id = chair_latest_location.chair_id
	// WHERE
	// 	chairs.is_available = TRUE
	// 	AND chairs.is_active = TRUE`); err != nil {
	// 	writeError(w, http.StatusInternalServerError, err)
	// 	return
	// }
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

	// query := "UPDATE chairs SET is_available = :isAvailable WHERE id IN (:chairIDs)"
	// query, params, _ := sqlx.Named(query, map[string]interface{}{
	// 	"isAvailable": false,
	// 	"chairIDs":    chairIDs,
	// })
	// query, params, _ = sqlx.In(query, params...)
	// if _, err := db.ExecContext(ctx, query, params...); err != nil {
	// 	writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to bulk update chairs: chairIDs: %s: %w", chairIDs, err))
	// 	return
	// }

	query := "UPDATE rides SET chair_id = ELT(FIELD(id, :rideIDs), :chairIDs), updated_at = :updatedAt WHERE id IN (:rideIDs)"
	query, params, _ := sqlx.Named(query, map[string]interface{}{
		"rideIDs":   rideIDs,
		"chairIDs":  chairIDs,
		"updatedAt": time.Now(),
	})
	query, params, _ = sqlx.In(query, params...)
	if _, err := db.ExecContext(ctx, query, params...); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to bulk update rides: rideIDs: %s chairIDs: %s: %w", rideIDs, chairIDs, err))
		return
	}

	query = "SELECT id, ride_id FROM ride_statuses WHERE ride_id IN (?) AND status = 'MATCHING' FOR SHARE"
	query, params, _ = sqlx.In(query, rideIDs)
	var rideStatuses []RideStatus
	if err := db.SelectContext(ctx, &rideStatuses, query, params...); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to select ride_statuses: rideIDs: %s: %w", rideIDs, err))
		return
	}
	var rideStatusMap = make(map[string]string, len(rideStatuses))
	for _, rideStatus := range rideStatuses {
		rideStatusMap[rideStatus.RideID] = rideStatus.ID
	}

	// matchedString := "[internal_matcher] chair_id,ride_id,pck_lat,pck_lon,dst_lat,dst_lon,curr_lat,curr_lon\n"
	for _, match := range matches {
		matchedRideID := match.Ride.ID
		matchedUserID := match.Ride.UserID
		matchedChairID := match.Chair.ID

		// commit locals
		userRideCache.Delete(matchedUserID)
		chairRideCache.Delete(matchedChairID)
		rideCache.Delete(matchedRideID)
		notifyToChannel("", matchedChairID, rideStatusMap[matchedRideID], matchedRideID, "MATCHING")
		validChairsCache.Delete(matchedChairID)

		// matchedString += fmt.Sprintf("[internal_matcher] %s,%s,%d,%d,%d,%d,%d,%d\n", matchedChairID, matchedRideID, match.Ride.PickupLatitude, match.Ride.PickupLongitude, match.Ride.DestinationLatitude, match.Ride.DestinationLongitude, match.Chair.Latitude, match.Chair.Longitude)
	}
	// log.Printf("[internal_matcher] internalGetMatching: matches: %d, chairs: %d, rides: %d", len(matches), len(chairs), len(ridesA)+len(ridesB))
	// log.Printf(matchedString)

	w.WriteHeader(http.StatusNoContent)
}
