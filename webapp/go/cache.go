package main

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func getChairCache(ctx context.Context, tx *sqlx.Tx, chairID string) (chair *Chair, err error) {
	if chairCached, found := chairCache.Load(chairID); found {
		chair = chairCached.(*Chair)
		return chair, nil
	}
	if err = tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, chairID); err != nil {
		return chair, fmt.Errorf("failed to get chair in getChairCache: chairID: %s: %w", chairID, err)
	}
	chairCache.Store(chairID, chair)
	return chair, nil
}

func getLatestRideStatusCache(ctx context.Context, tx executableGet, rideID string) (rideStatus string, err error) {
	if rideStatusCached, found := latestRideStatusCacheByRideID.Load(rideID); found {
		rideStatus = rideStatusCached.(string)
		return rideStatus, nil
	}
	if err = tx.GetContext(ctx, &rideStatus, `SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1`, rideID); err != nil {
		return "", err
	}
	latestRideStatusCacheByRideID.Store(rideID, rideStatus)
	return rideStatus, nil
}

func getRideCache(ctx context.Context, tx *sqlx.Tx, rideID string) (ride *Ride, err error) {
	if rideCached, found := rideCache.Load(rideID); found {
		ride = rideCached.(*Ride)
		return ride, nil
	}
	if err = tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, rideID); err != nil {
		return ride, err
		// return ride, fmt.Errorf("failed to get ride in getRideCache: rideID: %s: %w", rideID, err)
	}
	rideCache.Store(rideID, ride)
	return ride, nil
}

func getUserRideCache(ctx context.Context, tx *sqlx.Tx, userID string) (ride *Ride, err error) {
	if userRideCached, found := userRideCache.Load(userID); found {
		ride = userRideCached.(*Ride)
		return ride, nil
	}
	if err = tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC LIMIT 1`, userID); err != nil {
		return ride, err
	}
	userRideCache.Store(userID, ride)
	return ride, nil
}

func getChairStatsCache(ctx context.Context, tx *sqlx.Tx, chairID string) (stats ChairStats, err error) {
	if statsCached, found := chairStatsCache.Load(chairID); found {
		stats = statsCached.(ChairStats)
		return stats, nil
	}
	if err = tx.GetContext(
		ctx,
		&stats,
		`SELECT IFNULL(COUNT(1), 0) AS total_ride_count, IFNULL(SUM(r1.evaluation), 0) AS total_evaluation FROM rides r1, ride_statuses r2 WHERE r1.id = r2.ride_id AND r2.status = 'COMPLETED' AND r1.chair_id = ?`,
		chairID,
	); err != nil {
		return stats, err
	}
	chairStatsCache.Store(chairID, stats)
	return stats, nil
}

func getRideCouponCache(ctx context.Context, tx *sqlx.Tx, rideID string) (coupon Coupon, err error) {
	if rideCouponCached, found := rideCouponCache.Load(rideID); found {
		coupon = rideCouponCached.(Coupon)
		return coupon, nil
	}
	if err = tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", rideID); err != nil {
		return coupon, err
	}
	rideCouponCache.Store(rideID, coupon)
	return coupon, nil
}
