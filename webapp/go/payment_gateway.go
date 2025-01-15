package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

var erroredUpstream = errors.New("errored upstream")

type paymentGatewayPostPaymentRequest struct {
	Amount int    `json:"amount"`
	RideID string `json:"ride_id"`
}

type paymentGatewayGetPaymentsResponseOne struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
}

func requestPaymentGatewayPostPayment(ctx context.Context, paymentGatewayURL string, token string, param *paymentGatewayPostPaymentRequest) error {
	b, err := json.Marshal(param)
	if err != nil {
		return err
	}

	// 失敗したらとりあえずリトライ
	retry := 0
	for {
		err := func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", param.RideID)

			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()

			if res.StatusCode != http.StatusNoContent {
				return fmt.Errorf("got unexpected status code: expected: %s, got: %s", http.StatusNoContent, res.StatusCode)
			}
			return nil
		}()
		if err != nil {
			if retry < 5 {
				retry++
				time.Sleep(50 * time.Millisecond)
				continue
			} else {
				return err
			}
		}
		break
	}

	return nil
}
