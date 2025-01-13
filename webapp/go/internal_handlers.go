package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

type ChairWithLatLon struct {
	ID          string    `db:"id"`
	OwnerID     string    `db:"owner_id"`
	Name        string    `db:"name"`
	Model       string    `db:"model"`
	IsActive    bool      `db:"is_active"`
	AccessToken string    `db:"access_token"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`

	Latitude  int `db:"latitude"`
	Longitude int `db:"longitude"`
}

type MatchingResult struct {
	Chair ChairWithLatLon
	Ride  Ride
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
				log.Printf("chair model not found: model name: %s", chair.Model)
			}
			cost := max((abs(ride.PickupLatitude-chair.Latitude)+
				abs(ride.PickupLongitude-chair.Longitude)+
				abs(ride.DestinationLatitude-ride.PickupLatitude)+
				abs(ride.DestinationLongitude-ride.PickupLongitude))/model.Speed-int(time.Now().Sub(ride.CreatedAt).Seconds())*10, 0)
			g.AddEdge(i, n+j, 1, cost)
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

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chairs := []ChairWithLatLon{}
	if err := db.Select(&chairs, `
 WITH chair_latest_location AS (
 	SELECT *
 	FROM (
 		SELECT chair_locations.*, ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY created_at DESC) AS rn
 		FROM chair_locations
 	) c
 	WHERE c.rn = 1
 ),
 chair_latest_status AS (
 	SELECT *
 	FROM (
 		SELECT rides.*, ride_statuses.status AS ride_status, ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY ride_statuses.created_at DESC) AS rn
 		FROM rides INNER JOIN ride_statuses ON rides.id = ride_statuses.ride_id AND ride_statuses.chair_sent_at IS NOT NULL -- この条件は椅子の通知エンドポイントの実装で、未送信の状態がある2つ以上の異なるライドが割り当てられていても正しく順番に送るように修正していれば不要
 	) r
 	WHERE r.rn = 1
 )
 SELECT
 	chairs.*, chair_latest_location.latitude, chair_latest_location.longitude
 FROM chairs
 LEFT JOIN chair_latest_status ON chairs.id = chair_latest_status.chair_id
 LEFT JOIN chair_latest_location ON chairs.id = chair_latest_location.chair_id
 WHERE
 	(chair_latest_status.ride_status = 'COMPLETED' OR chair_latest_status.ride_status IS NULL) AND chairs.is_active AND chair_latest_location.latitude IS NOT NULL`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(chairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
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
	matchesA := execMatching(ridesA, chairsA)
	matchesB := execMatching(ridesB, chairsB)
	matches := append(matchesA, matchesB...)

	matchedString := "===========chair_id,ride_id,pck_lat,pck_lon,dst_lat,dst_lon,curr_lat,curr_lon\n"
	matchedCount := 0
	for _, match := range matches {
		matchedRideID := match.Ride.ID
		matchedUserID := match.Ride.UserID
		matchedChairID := match.Chair.ID
		// log.Printf("matched ride %s with chair %s\n", matchedChairID, matchedRideID)
		db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matchedChairID, matchedRideID)
		userRideCache.Delete(matchedUserID)
		chairRideCache.Delete(matchedChairID)
		rideCache.Delete(matchedRideID)
		var rideStatusID string
		if err := db.GetContext(ctx, &rideStatusID, "SELECT id FROM ride_statuses WHERE ride_id = ? AND status = ?", matchedRideID, "MATCHING"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		notifyToChannel("", matchedChairID, rideStatusID, matchedRideID, "MATCHING")
		matchedString += fmt.Sprintf("===========%s,%s,%d,%d,%d,%d,%d,%d\n", matchedChairID, matchedRideID, match.Ride.PickupLatitude, match.Ride.PickupLongitude, match.Ride.DestinationLatitude, match.Ride.DestinationLongitude, match.Chair.Latitude, match.Chair.Longitude)
		matchedCount += 1
	}
	log.Printf("===========internalGetMatching: matches: %d, chairs: %d, rides: %d", matchedCount, len(chairs), len(ridesA)+len(ridesB))
	log.Printf(matchedString)

	w.WriteHeader(http.StatusNoContent)
}
