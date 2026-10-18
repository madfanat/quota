package quota

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrInvalidQuery = errors.New("use region codes MOW, LED or KZN and weight_grams between 1 and 30000")
	ErrUnavailable  = errors.New("all providers unavailable")
	ErrBusy         = errors.New("quote capacity exhausted")
)

type Query struct {
	Origin      string
	Destination string
	WeightGrams int
}

func NewQuery(origin, destination string, weightGrams int) (Query, error) {
	q := Query{
		Origin:      strings.ToUpper(strings.TrimSpace(origin)),
		Destination: strings.ToUpper(strings.TrimSpace(destination)),
		WeightGrams: weightGrams,
	}
	if !validRegion(q.Origin) || !validRegion(q.Destination) || weightGrams < 1 || weightGrams > 30000 {
		return Query{}, ErrInvalidQuery
	}
	return q, nil
}

func validRegion(region string) bool {
	return region == "MOW" || region == "LED" || region == "KZN"
}

type Quote struct {
	Provider     string `json:"provider"`
	PriceMinor   int64  `json:"price_minor"`
	Currency     string `json:"currency"`
	EstimateDays int    `json:"estimate_days"`
}

type Result struct {
	Quotes          []Quote   `json:"quotes"`
	Complete        bool      `json:"complete"`
	FailedProviders []string  `json:"failed_providers"`
	GeneratedAt     time.Time `json:"generated_at"`
}
