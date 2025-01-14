package main

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
)

func getUserCache(ctx context.Context, tx *sqlx.Tx, userID string) (user *User, err error) {
	user = &User{}
	if userCached, found := userCache.Load(userID); found {
		user = userCached.(*User)
		return user, nil
	}
	if err = tx.GetContext(ctx, user, "SELECT * FROM users WHERE id = ? FOR SHARE", userID); err != nil {
		return user, fmt.Errorf("failed to get user in getUserCache: userID: %s: %w", userID, err)
	}
	userCache.Store(userID, user)
	return user, nil
}

func getChairCache(ctx context.Context, tx *sqlx.Tx, chairID string) (chair *Chair, err error) {
	chair = &Chair{}
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
	rideStatus = ""
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
	ride = &Ride{}
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
	ride = &Ride{}
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

func getChairRideCache(ctx context.Context, tx *sqlx.Tx, chairID string) (ride *Ride, err error) {
	ride = &Ride{}
	if chairRideCached, found := chairRideCache.Load(chairID); found {
		ride = chairRideCached.(*Ride)
		return ride, nil
	}
	if err = tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chairID); err != nil {
		return ride, err
	}
	chairRideCache.Store(chairID, ride)
	return ride, nil
}

func getChairStatsCache(ctx context.Context, tx *sqlx.Tx, chairID string) (stats ChairStats, err error) {
	stats = ChairStats{}
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
	coupon = Coupon{}
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

func getUserCacheByAccessToken(ctx context.Context, accessToken string) (user *User, err error) {
	user = &User{}
	if userCached, found := userAccessTokenCache.Load(accessToken); found {
		user = userCached.(*User)
		return user, nil
	}
	err = db.GetContext(ctx, user, "SELECT * FROM users WHERE access_token = ?", accessToken)
	if err != nil {
		return user, err
	}
	userAccessTokenCache.Store(accessToken, user)
	return user, nil
}

func getOwnerCacheByAccessToken(ctx context.Context, accessToken string) (owner *Owner, err error) {
	owner = &Owner{}
	if ownerCached, found := ownerAccessTokenCache.Load(accessToken); found {
		owner = ownerCached.(*Owner)
		return owner, nil
	}
	err = db.GetContext(ctx, owner, "SELECT * FROM owners WHERE access_token = ?", accessToken)
	if err != nil {
		return owner, err
	}
	ownerAccessTokenCache.Store(accessToken, owner)
	return owner, nil
}

func getChairCacheByAccessToken(ctx context.Context, accessToken string) (chair *Chair, err error) {
	chair = &Chair{}
	if chairCached, found := chairAccessTokenCache.Load(accessToken); found {
		chair = chairCached.(*Chair)
		return chair, nil
	}
	err = db.GetContext(ctx, chair, "SELECT * FROM chairs WHERE access_token = ?", accessToken)
	if err != nil {
		return chair, err
	}
	chairAccessTokenCache.Store(accessToken, chair)
	return chair, nil
}
