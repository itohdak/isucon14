package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/kaz/pprotein/integration/standalone"
)

var db *sqlx.DB

// common variables
var (
	// notification interval
	RetryAfterMs int = 30

	// long polling timeout
	PollingSec int = 2
)

// chaches
var (
	// access tokens
	userAccessTokenCache  sync.Map
	ownerAccessTokenCache sync.Map
	chairAccessTokenCache sync.Map
	chairIDAccessTokenMap sync.Map

	// rides
	rideCache                     sync.Map
	userRideCache                 sync.Map
	chairRideCache                sync.Map
	latestRideStatusCacheByRideID sync.Map

	// users
	userCache sync.Map

	// chairs
	chairCache sync.Map
	// chair models
	chairModelCache sync.Map
	// chair stats
	chairStatsCache sync.Map

	// coupons
	rideCouponCache sync.Map
)

// notification channels
var (
	appNotifications   sync.Map
	chairNotifications sync.Map

	channelSize int = 10
)

func createNewChannelForUser(userID string) {
	appNotifications.Store(userID, make(chan RideStatus, channelSize))
}
func createNewChannelForChair(chairID string) {
	chairNotifications.Store(chairID, make(chan RideStatus, channelSize))
}

type CoordinateToUpdate struct {
	ChairLocationID string    `db:"chair_location_id"`
	ChairID         string    `db:"chair_id"`
	Latitude        int       `db:"latitude"`
	Longitude       int       `db:"longitude"`
	CreatedAt       time.Time `db:"created_at"`
}

// channel to enqueue chair latest locations
var updateCoordinateQueue chan CoordinateToUpdate

func main() {
	go standalone.Integrate(":8888")

	updateCoordinateQueue = make(chan CoordinateToUpdate, 100000)
	go func() {
		log.Println("start listening for updateCoordinateQueue")
		for {
			updateCoordinates()
		}
	}()

	mux := setup()
	slog.Info("Listening on :8080")
	http.ListenAndServe(":8080", mux)
}

func setup() http.Handler {
	host := os.Getenv("ISUCON_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("ISUCON_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("failed to convert DB port number from ISUCON_DB_PORT environment variable into int: %v", err))
	}
	user := os.Getenv("ISUCON_DB_USER")
	if user == "" {
		user = "isucon"
	}
	password := os.Getenv("ISUCON_DB_PASSWORD")
	if password == "" {
		password = "isucon"
	}
	dbname := os.Getenv("ISUCON_DB_NAME")
	if dbname == "" {
		dbname = "isuride"
	}

	dbConfig := mysql.NewConfig()
	dbConfig.User = user
	dbConfig.Passwd = password
	dbConfig.Addr = net.JoinHostPort(host, port)
	dbConfig.Net = "tcp"
	dbConfig.DBName = dbname
	dbConfig.ParseTime = true
	dbConfig.InterpolateParams = true

	_db, err := sqlx.Connect("mysql", dbConfig.FormatDSN())
	if err != nil {
		panic(err)
	}
	db = _db
	db.SetConnMaxLifetime(10 * time.Second)
	db.SetMaxIdleConns(1024)
	db.SetMaxOpenConns(2048)

	mux := chi.NewRouter()
	mux.Use(middleware.Logger)
	mux.Use(middleware.Recoverer)
	mux.HandleFunc("POST /api/initialize", postInitialize)

	// app handlers
	{
		mux.HandleFunc("POST /api/app/users", appPostUsers)

		authedMux := mux.With(appAuthMiddleware)
		authedMux.HandleFunc("POST /api/app/payment-methods", appPostPaymentMethods)
		authedMux.HandleFunc("GET /api/app/rides", appGetRides)
		authedMux.HandleFunc("POST /api/app/rides", appPostRides)
		authedMux.HandleFunc("POST /api/app/rides/estimated-fare", appPostRidesEstimatedFare)
		authedMux.HandleFunc("POST /api/app/rides/{ride_id}/evaluation", appPostRideEvaluatation)
		authedMux.HandleFunc("GET /api/app/notification", appGetNotification)
		authedMux.HandleFunc("GET /api/app/nearby-chairs", appGetNearbyChairs)
	}

	// owner handlers
	{
		mux.HandleFunc("POST /api/owner/owners", ownerPostOwners)

		authedMux := mux.With(ownerAuthMiddleware)
		authedMux.HandleFunc("GET /api/owner/sales", ownerGetSales)
		authedMux.HandleFunc("GET /api/owner/chairs", ownerGetChairs)
	}

	// chair handlers
	{
		mux.HandleFunc("POST /api/chair/chairs", chairPostChairs)

		authedMux := mux.With(chairAuthMiddleware)
		authedMux.HandleFunc("POST /api/chair/activity", chairPostActivity)
		authedMux.HandleFunc("POST /api/chair/coordinate", chairPostCoordinate)
		authedMux.HandleFunc("GET /api/chair/notification", chairGetNotification)
		authedMux.HandleFunc("POST /api/chair/rides/{ride_id}/status", chairPostRideStatus)
	}

	// internal handlers
	{
		mux.HandleFunc("GET /api/internal/matching", internalGetMatching)
	}

	return mux
}

type postInitializeRequest struct {
	PaymentServer string `json:"payment_server"`
}

type postInitializeResponse struct {
	Language string `json:"language"`
}

func postInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &postInitializeRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize: %s: %w", string(out), err))
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'", req.PaymentServer); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := prepare(ctx); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to prepare: %w", err))
		return
	}

	go func() {
		if _, err := http.Get("http://pprotein.maca.jp:9000/api/group/collect"); err != nil {
			log.Printf("failed to communicate with pprotein: %v", err)
		}
	}()

	writeJSON(w, http.StatusOK, postInitializeResponse{Language: "go"})
}

func prepare(ctx context.Context) error {
	// load cache
	if err := loadCache(ctx); err != nil {
		return fmt.Errorf("failed to load cache: %w", err)
	}

	// prepare for notification
	if err := prepareNotification(ctx); err != nil {
		return fmt.Errorf("failed to prepare notification: %w", err)
	}

	// update sales for already completed rides
	if _, err := db.ExecContext(
		ctx,
		`UPDATE rides
		 SET sales = ? + ? * (ABS(pickup_latitude - destination_latitude) + ABS(pickup_longitude - destination_longitude))
		 WHERE (SELECT COUNT(*) FROM ride_statuses WHERE ride_id = rides.id AND status = 'COMPLETED')`,
		initialFare, farePerDistance,
	); err != nil {
		return fmt.Errorf("failed to update sales in rides: %w", err)
	}

	// update is_available for free chairs
	if _, err := db.ExecContext(
		ctx,
		`UPDATE chairs SET is_available = 0 WHERE (SELECT COUNT(*) FROM rides WHERE chair_id = chairs.id AND evaluation IS NULL)`,
	); err != nil {
		return fmt.Errorf("failed to update is_available in chairs: %w", err)
	}

	// store chair latest distance/location into chair_total_distance
	if _, err := db.ExecContext(ctx, `
	INSERT INTO
		chair_total_distance(
			chair_id,
			total_distance,
			latest_timestamp,
			latest_latitude,
			latest_longitude
		)(
			select
				distance.chair_id,
				distance.total_distance,
				distance.latest_timestamp,
				chair_locations.latitude,
				chair_locations.longitude
			from
				chair_locations
				INNER JOIN (
					SELECT
						chair_id,
						SUM(IFNULL(distance, 0)) AS total_distance,
						MAX(created_at) AS latest_timestamp,
						0 AS latest_latitude,
						0 AS latest_longitude
					FROM
						(
							SELECT
								chair_id,
								created_at,
								ABS(
									latitude - LAG(latitude) OVER(
										PARTITION BY chair_id
										ORDER BY
											created_at
									)
								) + ABS(
									longitude - LAG(longitude) OVER(
										PARTITION BY chair_id
										ORDER BY
											created_at
									)
								) AS distance
							FROM
								chair_locations
						) AS tmp
					GROUP BY
						chair_id
				) as distance ON chair_locations.created_at = distance.latest_timestamp
		) ON DUPLICATE KEY
	UPDATE
		total_distance = VALUES(total_distance),
		latest_timestamp = VALUES(latest_timestamp),
		latest_latitude = 0,
		latest_longitude = 0
	`); err != nil {
		return fmt.Errorf("failed to insert into chair_latest_distance: %w", err)
	}

	return nil
}

func loadCache(ctx context.Context) error {
	// cache chairModels
	var chairModels []ChairModel
	if err := db.SelectContext(ctx, &chairModels, `SELECT * FROM chair_models`); err != nil {
		return fmt.Errorf("failed to get chair models: %w", err)
	}
	for _, chairModel := range chairModels {
		chairModelCache.Store(chairModel.Name, chairModel)
	}

	// cache chair stats
	chairStats := []struct {
		ChairID string `db:"chair_id"`
		ChairStats
	}{}
	if err := db.SelectContext(
		ctx,
		&chairStats,
		`SELECT r1.chair_id AS chair_id, IFNULL(COUNT(1), 0) AS total_ride_count, IFNULL(SUM(r1.evaluation), 0) AS total_evaluation FROM rides r1, ride_statuses r2 WHERE r1.id = r2.ride_id AND r2.status = 'COMPLETED' GROUP BY r1.chair_id`,
	); err != nil {
		return fmt.Errorf("failed to get chair stats: %w", err)
	}
	for _, chairStat := range chairStats {
		chairStatsCache.Store(chairStat.ChairID, chairStat.ChairStats)
	}

	return nil
}

func prepareNotification(ctx context.Context) error {
	// create channel for user notification
	var userIDs []string
	if err := db.SelectContext(ctx, &userIDs, `SELECT id FROM users`); err != nil {
		return fmt.Errorf("failed to get user IDs: %w", err)
	}
	for _, userID := range userIDs {
		createNewChannelForUser(userID)
	}

	// create channel for chair notification
	var chairIDs []string
	if err := db.SelectContext(ctx, &chairIDs, `SELECT id FROM chairs`); err != nil {
		return fmt.Errorf("failed to get ride IDs: %w", err)
	}
	for _, chairID := range chairIDs {
		createNewChannelForChair(chairID)
	}

	return nil
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

func bindJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	buf, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	w.Write(buf)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
	buf, marshalError := json.Marshal(map[string]string{"message": err.Error()})
	if marshalError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"marshaling error failed"}`))
		return
	}
	w.Write(buf)

	slog.Error("error response wrote", err)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}

func notifyToChannel(userID string, chairID string, rideStatusID string, rideID string, status string) (err error) {
	// notification for app
	if userID != "" {
		appChan, found := appNotifications.Load(userID)
		if !found {
			log.Printf("notification channel for app not found: userID: %s", userID)
			return fmt.Errorf("notification channel for app not found: userID: %s", userID)
		}
		appChannel := appChan.(chan RideStatus)
		appChannel <- RideStatus{
			ID:     rideStatusID,
			RideID: rideID,
			Status: status,
		}
	}
	// notification for chair
	if chairID != "" {
		chairChan, found := chairNotifications.Load(chairID)
		if !found {
			log.Printf("notification channel for chair not found: chairID: %s", chairID)
			return fmt.Errorf("notification channel for chair not found: chairID: %s", chairID)
		}
		chairChannel := chairChan.(chan RideStatus)
		chairChannel <- RideStatus{
			ID:     rideStatusID,
			RideID: rideID,
			Status: status,
		}
	}
	return nil
}
