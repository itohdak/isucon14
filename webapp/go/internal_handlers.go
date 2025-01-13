package main

import (
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

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	// 決まっていないライドと空いている椅子を全て取得
	rides := []Ride{}
	if err := db.Select(&rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at ASC`); err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(rides) <= 0 {
		return
	}

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
		 (chair_latest_status.ride_status = 'COMPLETED' OR chair_latest_status.ride_status IS NULL) AND chairs.is_active`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matchedUserIDs := []string{}
	matchedChairIDs := []string{}
	matchedRideIDs := []string{}
	for _, ride := range rides {
		minDistance := 400
		var minChair *ChairWithLatLon
		var minChairIdx int
		for idx, chair := range chairs {
			distance := calculateDistance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
			if distance < minDistance {
				minDistance = distance
				minChair = &chair
				minChairIdx = idx
			}
		}
		if minChair != nil {
			// 複数のmatcherが動く場合には、複数の椅子が同じライドに割り当てられないようトランザクションなどで排他制御を行う必要がある
			if _, err := db.Exec("UPDATE rides SET chair_id = ? WHERE id = ?", minChair.ID, ride.ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			chairs = append(chairs[:minChairIdx], chairs[minChairIdx+1:]...)
			matchedUserIDs = append(matchedUserIDs, ride.UserID)
			matchedChairIDs = append(matchedChairIDs, minChair.ID)
			matchedRideIDs = append(matchedRideIDs, ride.ID)
		}
	}
	// ctx := r.Context()
	// // MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	// tx, err := db.Beginx()
	// if err != nil {
	// 	writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to begin in internal matching: %v", err))
	// 	return
	// }
	// defer tx.Rollback()
	// rides := []Ride{}
	// numPerBatch := 200
	// if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED`, numPerBatch); err != nil {
	// 	writeError(w, http.StatusInternalServerError, err)
	// 	return
	// }
	// if len(rides) == 0 {
	// 	w.WriteHeader(http.StatusNoContent)
	// 	return
	// }

	// chairs := []Chair{}
	// if err := tx.SelectContext(
	// 	ctx,
	// 	&chairs,
	// 	`SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE (SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = chairs.id) GROUP BY ride_id) is_completed WHERE completed = FALSE) AND is_active = TRUE ORDER BY RAND()) AS tmp ON chairs.id = tmp.id FOR UPDATE SKIP LOCKED`); err != nil {
	// 	writeError(w, http.StatusInternalServerError, err)
	// }
	// if len(chairs) == 0 {
	// 	w.WriteHeader(http.StatusNoContent)
	// 	return
	// }
	// chairIDs := make([]string, len(chairs))
	// chairMap := map[string]Chair{}
	// for i, chair := range chairs {
	// 	chairIDs[i] = chair.ID
	// 	chairMap[chair.ID] = chair
	// }

	// locations := []ChairLocation{}
	// query := `SELECT l1.* FROM chair_locations l1 JOIN (SELECT chair_id, MAX(created_at) AS created_at FROM chair_locations l2 GROUP BY chair_id) AS tmp ON l1.chair_id = tmp.chair_id AND l1.created_at = tmp.created_at WHERE l1.chair_id IN (?)`
	// query, param, err := sqlx.In(query, chairIDs)
	// if err != nil {
	// 	writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to prepare in query: %v", err))
	// 	return
	// }
	// err = tx.SelectContext(ctx, &locations, query, param...)
	// if err != nil {
	// 	writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to exec in query: %v", err))
	// 	return
	// }

	// n, m := len(rides), len(locations)
	// g := newMinCostFlow(n + m + 2)
	// s, t := n+m, n+m+1
	// for i, _ := range rides {
	// 	g.AddEdge(s, i, 1, 0)
	// }
	// for i, _ := range locations {
	// 	g.AddEdge(n+i, t, 1, 0)
	// }
	// for i, ride := range rides {
	// 	for j, location := range locations {
	// 		var model = ChairModel{
	// 			Speed: 1,
	// 		}
	// 		if modelCached, found := chairModelCache.Load(chairMap[location.ChairID].Model); found {
	// 			model = modelCached.(ChairModel)
	// 		} else {
	// 			log.Printf("chair model not found: model name: %s", chairMap[location.ChairID].Model)
	// 		}
	// 		cost := max((abs(ride.PickupLatitude-location.Latitude)+
	// 			abs(ride.PickupLongitude-location.Longitude)+
	// 			abs(ride.DestinationLatitude-ride.PickupLatitude)+
	// 			abs(ride.DestinationLongitude-ride.PickupLongitude))/model.Speed-int(time.Now().Sub(ride.CreatedAt).Seconds()), 0)
	// 		g.AddEdge(i, n+j, 1, cost)
	// 	}
	// }
	// g.FlowL(s, t, n)
	// edges := g.Edges()
	// matchedUserIDs := []string{}
	// matchedChairIDs := []string{}
	// matchedRideIDs := []string{}
	// for _, e := range edges {
	// 	if e.from == s || e.to == t || e.flow == 0 {
	// 		continue
	// 	}
	// 	matchedRideID := rides[e.from].ID
	// 	matchedUserID := rides[e.from].UserID
	// 	matchedChairID := locations[e.to-n].ChairID
	// 	log.Printf("matched ride %s with chair %s\n", matchedChairID, matchedRideID)
	// 	tx.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matchedChairID, matchedRideID)
	// 	matchedUserIDs = append(matchedUserIDs, matchedUserID)
	// 	matchedChairIDs = append(matchedChairIDs, matchedChairID)
	// 	matchedRideIDs = append(matchedRideIDs, matchedRideID)
	// }

	// if err := tx.Commit(); err != nil {
	// 	writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to commit in internal matching: %v", err))
	// 	return
	// }
	for _, matchedUserID := range matchedUserIDs {
		userRideCache.Delete(matchedUserID)
	}
	for _, matchedChairID := range matchedChairIDs {
		chairRideCache.Delete(matchedChairID)
	}
	for _, matchedRideID := range matchedRideIDs {
		rideCache.Delete(matchedRideID)
	}

	w.WriteHeader(http.StatusNoContent)
}
