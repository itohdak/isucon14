package main

import (
	"context"
	"database/sql"
	"encoding/json"
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

	// commit locals
	createNewChannelForChair(chairID)
	chairStatsCache.Store(chairID, ChairStats{
		TotalRideCount:  0,
		TotalEvaluation: 0,
	})

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
	if req.IsActive {
		validChairsCache.Store(chair.ID, struct{}{})
	} else {
		validChairsCache.Delete(chair.ID)
	}

	// commit locals
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
	// enqueue coordinates for bulk insert/update
	updateCoordinateQueue <- CoordinateToUpdate{
		ChairLocationID: ulid.Make().String(),
		ChairID:         chair.ID,
		Latitude:        req.Latitude,
		Longitude:       req.Longitude,
		CreatedAt:       createdAt,
	}
	updateChairLatestLocationCache(ctx, chair.ID, &ChairLatestLocation{
		ChairID:   chair.ID,
		Latitude:  req.Latitude,
		Longitude: req.Longitude,
		UpdatedAt: sql.NullTime{Time: createdAt, Valid: true},
	})

	commitCache := func() {}
	ride := &Ride{}
	if ride, err = getChairRideCache(ctx, tx, chair.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	} else { // ride exists
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "COMPLETED" && status != "CANCELED" {
			if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && status == "ENROUTE" {
				rideStatusID := ulid.Make().String()
				commitCache = func() {
					latestRideStatusCacheByRideID.Store(ride.ID, "PICKUP")
					if ride.ChairID.Valid {
						notifyToChannel(ride.UserID, ride.ChairID.String, rideStatusID, ride.ID, "PICKUP")
					} else {
						log.Printf("chairID is NULL: ride: %v", ride)
					}
					insertRideStatusQueue <- RideStatus{
						ID:     rideStatusID,
						RideID: ride.ID,
						Status: "PICKUP",
					}
				}
			}

			if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && status == "CARRYING" {
				rideStatusID := ulid.Make().String()
				commitCache = func() {
					latestRideStatusCacheByRideID.Store(ride.ID, "ARRIVED")
					if ride.ChairID.Valid {
						notifyToChannel(ride.UserID, ride.ChairID.String, rideStatusID, ride.ID, "ARRIVED")
					} else {
						log.Printf("chairID is NULL: ride: %v", ride)
					}
					insertRideStatusQueue <- RideStatus{
						ID:     rideStatusID,
						RideID: ride.ID,
						Status: "ARRIVED",
					}
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// commit locals
	commitCache()

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: createdAt.UnixMilli(),
	})
}

func updateCoordinates() {
	var maxLength = 2000
	var coordinates = make([]CoordinateToUpdate, 0, maxLength)
	var timeout = 500 * time.Millisecond
	now := time.Now()
	for {
		select {
		case coordinate := <-updateCoordinateQueue:
			coordinates = append(coordinates, coordinate)
		case <-time.After(1 * time.Microsecond):
			break
		}
		if len(coordinates) == maxLength {
			break
		}
		if time.Since(now) > timeout {
			break
		}
	}
	if len(coordinates) == 0 {
		return
	}

	if _, err := db.NamedExec(
		`INSERT INTO
			chair_latest_location (chair_id, total_distance, latest_timestamp, latest_latitude, latest_longitude)
		VALUES (:chair_id, 0, :created_at, :latitude, :longitude)
		ON DUPLICATE KEY UPDATE
			total_distance = total_distance + ABS(latest_latitude - VALUES(latest_latitude)) + ABS(latest_longitude - VALUES(latest_longitude)),
			latest_timestamp = VALUES(latest_timestamp),
			latest_latitude = VALUES(latest_latitude),
			latest_longitude = VALUES(latest_longitude)`,
		coordinates,
	); err != nil {
		log.Printf("[ERROR] failed to insert into chair_latest_location: %w: coordinates: %v", err, coordinates)
		return
	}
	log.Printf("[INFO] queue length: %d, dequeued length: %d, oldest timestamp duration: %s", len(updateCoordinateQueue), len(coordinates), time.Since(coordinates[0].CreatedAt))
}

func insertRideStatuses() {
	var maxLength = 2000
	var rideStatuses = make([]RideStatus, 0, maxLength)
	var timeout = 500 * time.Millisecond
	now := time.Now()
	for {
		select {
		case rideStatus := <-insertRideStatusQueue:
			rideStatuses = append(rideStatuses, rideStatus)
		case <-time.After(1 * time.Microsecond):
			break
		}
		if len(rideStatuses) == maxLength {
			break
		}
		if time.Since(now) > timeout {
			break
		}
	}
	if len(rideStatuses) == 0 {
		return
	}

	if _, err := db.NamedExec(
		`INSERT INTO ride_statuses (id, ride_id, status) VALUES (:id, :ride_id, :status)`,
		rideStatuses,
	); err != nil {
		log.Printf("[ERROR] failed to bulk insert into ride_statuses: %w: rideStatuses: %v", err, rideStatuses)
		return
	}
	log.Printf("[INFO] rideStatus queue length: %d, dequeued length: %d", len(insertRideStatusQueue), len(rideStatuses))
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

func chairGetNotificationSSE(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	// ref: https://packagemain.tech/p/implementing-server-sent-events-in-go

	// Set http headers required for SSE
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Create a channel for client disconnection
	clientGone := r.Context().Done()

	rc := http.NewResponseController(w)
	chairChan, found := chairNotifications.Load(chair.ID)
	if !found {
		log.Printf("[ERROR] notification channel for user not found: chairID: %s", chair.ID)
		return
	}
	chairChannel := chairChan.(chan RideStatus)

	for {
		select {
		case <-clientGone:
			fmt.Println("Client disconnected")
			return
		case newStatus := <-chairChannel:
			// Send an event to the client
			data, err := chairGetNotificationData(ctx, chair, &newStatus)
			if err != nil {
				log.Printf("failed to get chair notification data: %w", err)
				return
			}
			dataMarshal, err := json.Marshal(data)
			if err != nil {
				log.Printf("failed to json marshal: %w", err)
				return
			}
			// log.Printf("data: %s\n\n", string(dataMarshal))
			if _, err = fmt.Fprintf(w, "data: %s\n\n", string(dataMarshal)); err != nil {
				log.Printf("failed to write data: %w", err)
				return
			}
			err = rc.Flush()
			if err != nil {
				log.Printf("failed to flush: %w", err)
				return
			}
		}
	}
}

func chairGetNotificationData(ctx context.Context, chair *Chair, newRideStatus *RideStatus) (*chairGetNotificationResponseData, error) {
	tx, err := db.Beginx()
	if err != nil {
		return &chairGetNotificationResponseData{}, err
	}
	defer tx.Rollback()
	ride := &Ride{}
	yetSentRideStatus := &RideStatus{}
	status := ""

	yetSentRideStatus = newRideStatus
	status = yetSentRideStatus.Status
	if ride, err = getRideCache(ctx, tx, newRideStatus.RideID); err != nil {
		return &chairGetNotificationResponseData{}, fmt.Errorf("notification channel for chair not found: chairID: %s", chair.ID)
	}

	user := &User{}
	if user, err = getUserCache(ctx, tx, ride.UserID); err != nil {
		return &chairGetNotificationResponseData{}, fmt.Errorf("failed to get user in chairGetNotification: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return &chairGetNotificationResponseData{}, err
	}

	return &chairGetNotificationResponseData{
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
	}, nil
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
		writeError(w, http.StatusBadRequest, fmt.Errorf("not assigned to this ride: ride.ChairID: %s, current chair.ID: %s", ride.ChairID.String, chair.ID))
		return
	}

	commitCache := func() {}
	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		rideStatusID := ulid.Make().String()
		commitCache = func() {
			latestRideStatusCacheByRideID.Store(ride.ID, "ENROUTE")
			notifyToChannel(ride.UserID, ride.ChairID.String, rideStatusID, ride.ID, "ENROUTE")
			insertRideStatusQueue <- RideStatus{
				ID:     rideStatusID,
				RideID: ride.ID,
				Status: "ENROUTE",
			}
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
		commitCache = func() {
			latestRideStatusCacheByRideID.Store(ride.ID, "CARRYING")
			notifyToChannel(ride.UserID, ride.ChairID.String, rideStatusID, ride.ID, "CARRYING")
			insertRideStatusQueue <- RideStatus{
				ID:     rideStatusID,
				RideID: ride.ID,
				Status: "CARRYING",
			}
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// commit locals
	commitCache()

	w.WriteHeader(http.StatusNoContent)
}
