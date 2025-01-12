package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	owner := &Owner{}
	if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE chair_register_token = ?", req.ChairRegisterToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := db.ExecContext(ctx, "UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	accessToken, _ := chairIDAccessTokenMap.Load(chair.ID)
	chairAccessTokenCache.Delete(accessToken)
	chairCache.Delete(chair.ID)

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*Chair)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	createdAt := time.Now()
	updateCoordinateQueue <- CoordinateToUpdate{
		ChairLocationID: ulid.Make().String(),
		ChairID:         chair.ID,
		Latitude:        req.Latitude,
		Longitude:       req.Longitude,
		CreatedAt:       createdAt,
	}

	commitCache := func() {}
	ride := &Ride{}
	rideExists := false
	if chairRideCached, found := chairRideCache.Load(chair.ID); found {
		ride = chairRideCached.(*Ride)
		rideExists = true
	} else {
		if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			rideExists = true
			chairRideCache.Store(chair.ID, ride)
		}
	}
	if rideExists {
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				rideStatusID := ulid.Make().String()
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", rideStatusID, ride.ID, "PICKUP"); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				commitCache = func() {
					latestRideStatusCacheByRideID.Store(ride.ID, "PICKUP")
					notifyToChannel(ride.UserID, rideStatusID, ride.ID, "PICKUP", false)
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				rideStatusID := ulid.Make().String()
				if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", rideStatusID, ride.ID, "ARRIVED"); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				commitCache = func() {
					latestRideStatusCacheByRideID.Store(ride.ID, "ARRIVED")
					notifyToChannel(ride.UserID, rideStatusID, ride.ID, "ARRIVED", false)
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	commitCache()

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: createdAt.UnixMilli(),
	})
}

func updateCoordinates() {
	length := len(updateCoordinateQueue)
	if length == 0 {
		return
	}
	log.Printf("queue length: %d", length)

	var coordinates []CoordinateToUpdate
	// var count = map[string][]time.Time{}
	var maxLength = 200
	for i := 0; i < maxLength; i++ {
		select {
		case coordinate := <-updateCoordinateQueue:
			coordinates = append(coordinates, coordinate)
			// count[coordinate.ChairID] = append(count[coordinate.ChairID], coordinate.CreatedAt)
		case <-time.After(1 * time.Microsecond):
			break
		}
	}
	log.Printf("dequeued length: %d", len(coordinates))
	// type duplicates struct {
	// 	chairID    string
	// 	timestamps []time.Time
	// }
	// var chairIDsWithMoreThanOne = make([]duplicates, 0, len(count))
	// for chairID, timestamps := range count {
	// 	if len(timestamps) > 1 {
	// 		chairIDsWithMoreThanOne = append(chairIDsWithMoreThanOne, duplicates{
	// 			chairID:    chairID,
	// 			timestamps: timestamps,
	// 		})
	// 	}
	// }
	// if len(chairIDsWithMoreThanOne) > 0 {
	// 	log.Printf("multiple coordinates for single chairID: %v", chairIDsWithMoreThanOne)
	// }

	tx, err := db.Beginx()
	if err != nil {
		log.Printf("failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	for _, coordinate := range coordinates {
		coordinate.ChairLocationID = ulid.Make().String()
	}
	if _, err := tx.NamedExec(
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES (:chair_location_id, :chair_id, :latitude, :longitude, :created_at)`,
		coordinates,
	); err != nil {
		log.Printf("failed to insert chair_locations: %v", err)
		return
	}

	if _, err := tx.NamedExec(
		`INSERT INTO
			chair_total_distance (chair_id, total_distance, latest_timestamp, latest_latitude, latest_longitude)
		VALUES (:chair_id, 0, :created_at, :latitude, :longitude)
		ON DUPLICATE KEY UPDATE
			total_distance = total_distance + ABS(latest_latitude - VALUES(latest_latitude)) + ABS(latest_longitude - VALUES(latest_longitude)),
			latest_timestamp = VALUES(latest_timestamp),
			latest_latitude = VALUES(latest_latitude),
			latest_longitude = VALUES(latest_longitude)`,
		coordinates,
	); err != nil {
		log.Printf("failed to insert chair_total_distance: %v: coordinates: %v", err, coordinates)
		return
	}

	if err := tx.Commit(); err != nil {
		log.Printf("failed to commit: %v", err)
		return
	}
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponse struct {
	Data         *chairGetNotificationResponseData `json:"data"`
	RetryAfterMs int                               `json:"retry_after_ms"`
}

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()
	ride := &Ride{}
	yetSentRideStatus := RideStatus{}
	status := ""

	if chairRideCached, found := chairRideCache.Load(chair.ID); found {
		ride = chairRideCached.(*Ride)
	} else {
		if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusOK, &chairGetNotificationResponse{
					RetryAfterMs: RetryAfterMs,
				})
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		chairRideCache.Store(chair.ID, ride)
	}

	chairChan, found := chairNotifications.Load(ride.ID)
	if !found {
		log.Printf("notification channel for chair not found, regarding as completed: rideID: %s", ride.ID)
		status, err = getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	} else {
		chairChannel := chairChan.(chan RideStatus)
		select {
		case newStatus := <-chairChannel:
			yetSentRideStatus = newStatus
			status = yetSentRideStatus.Status
		case <-time.After(time.Duration(PollingSec) * time.Second):
			status, err = getLatestRideStatus(ctx, tx, ride.ID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}

	user := &User{}
	if userCached, found := userCache.Load(ride.UserID); found {
		user = userCached.(*User)
	} else {
		err = tx.GetContext(ctx, user, "SELECT * FROM users WHERE id = ? FOR SHARE", ride.UserID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		userCache.Store(ride.UserID, user)
	}

	if yetSentRideStatus.ID != "" {
		_, err := tx.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, yetSentRideStatus.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if yetSentRideStatus.Status == "COMPLETED" {
		chairNotifications.Delete(ride.ID)
	}

	writeJSON(w, http.StatusOK, &chairGetNotificationResponse{
		Data: &chairGetNotificationResponseData{
			RideID: ride.ID,
			User: simpleUser{
				ID:   user.ID,
				Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
			},
			PickupCoordinate: Coordinate{
				Latitude:  ride.PickupLatitude,
				Longitude: ride.PickupLongitude,
			},
			DestinationCoordinate: Coordinate{
				Latitude:  ride.DestinationLatitude,
				Longitude: ride.DestinationLongitude,
			},
			Status: status,
		},
		RetryAfterMs: RetryAfterMs,
	})
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*Chair)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, "SELECT * FROM rides WHERE id = ? FOR UPDATE", rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if ride.ChairID.String != chair.ID {
		writeError(w, http.StatusBadRequest, errors.New("not assigned to this ride"))
		return
	}

	commitCache := func() {}
	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		rideStatusID := ulid.Make().String()
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", rideStatusID, ride.ID, "ENROUTE"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		commitCache = func() {
			latestRideStatusCacheByRideID.Store(ride.ID, "ENROUTE")
			notifyToChannel(ride.UserID, rideStatusID, ride.ID, "ENROUTE", false)
		}
	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		rideStatusID := ulid.Make().String()
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", rideStatusID, ride.ID, "CARRYING"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		commitCache = func() {
			latestRideStatusCacheByRideID.Store(ride.ID, "CARRYING")
			notifyToChannel(ride.UserID, rideStatusID, ride.ID, "CARRYING", false)
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	commitCache()

	w.WriteHeader(http.StatusNoContent)
}
