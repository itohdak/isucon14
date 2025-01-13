package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	chairs := []Chair{}
	if err := db.SelectContext(
		ctx,
		&chairs,
		`SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE (SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = chairs.id) GROUP BY ride_id) is_completed WHERE completed = FALSE) AND is_active = TRUE ORDER BY RAND()) AS tmp ON chairs.id = tmp.id FOR UPDATE SKIP LOCKED`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
	}
	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	chairIDs := make([]string, len(chairs))
	chairMap := map[string]Chair{}
	for i, chair := range chairs {
		chairIDs[i] = chair.ID
		chairMap[chair.ID] = chair
	}

	locations := []ChairLocation{}
	query := `SELECT l1.* FROM chair_locations l1 JOIN (SELECT chair_id, MAX(created_at) AS created_at FROM chair_locations l2 GROUP BY chair_id) AS tmp ON l1.chair_id = tmp.chair_id AND l1.created_at = tmp.created_at WHERE l1.chair_id IN (?)`
	query, param, err := sqlx.In(query, chairIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to prepare in query: %v", err))
		return
	}
	err = db.SelectContext(ctx, &locations, query, param...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to exec in query: %v", err))
		return
	}

	n, m := len(rides), len(locations)
	g := newMinCostFlow(n + m + 2)
	s, t := n+m, n+m+1
	for i, _ := range rides {
		g.AddEdge(s, i, 1, 0)
	}
	for i, _ := range locations {
		g.AddEdge(n+i, t, 1, 0)
	}
	for i, ride := range rides {
		for j, location := range locations {
			var model = ChairModel{
				Speed: 1,
			}
			if modelCached, found := chairModelCache.Load(chairMap[location.ChairID].Model); found {
				model = modelCached.(ChairModel)
			} else {
				log.Printf("chair model not found: model name: %s", chairMap[location.ChairID].Model)
			}
			cost := max((abs(ride.PickupLatitude-location.Latitude)+
				abs(ride.PickupLongitude-location.Longitude)+
				abs(ride.DestinationLatitude-ride.PickupLatitude)+
				abs(ride.DestinationLongitude-ride.PickupLongitude))/model.Speed-int(time.Now().Sub(ride.CreatedAt).Seconds())*10, 0)
			g.AddEdge(i, n+j, 1, cost)
		}
	}
	g.FlowL(s, t, n)
	edges := g.Edges()
	for _, e := range edges {
		if e.from == s || e.to == t || e.flow == 0 {
			continue
		}
		matchedRideID := rides[e.from].ID
		matchedUserID := rides[e.from].UserID
		matchedChairID := locations[e.to-n].ChairID
		log.Printf("matched ride %s with chair %s\n", matchedChairID, matchedRideID)
		db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matchedChairID, matchedRideID)
		userRideCache.Delete(matchedUserID)
		chairRideCache.Delete(matchedChairID)
		rideCache.Delete(matchedRideID)
	}

	w.WriteHeader(http.StatusNoContent)
}
