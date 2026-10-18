package carrierhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/madfanat/quota/internal/quota"
)

type Provider struct {
	name   string
	url    string
	client *http.Client
}

func New(name, endpoint string, client *http.Client) *Provider {
	return &Provider{name: name, url: endpoint, client: client}
}

func (p *Provider) Name() string { return p.name }

func (p *Provider) GetQuote(ctx context.Context, query quota.Query) (quota.Quote, error) {
	params := url.Values{
		"origin":       {query.Origin},
		"destination":  {query.Destination},
		"weight_grams": {strconv.Itoa(query.WeightGrams)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"?"+params.Encode(), nil)
	if err != nil {
		return quota.Quote{}, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return quota.Quote{}, err
	}
	defer resp.Body.Close()
	const maxBody = 64 << 10
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return quota.Quote{}, err
	}
	if len(body) > maxBody {
		return quota.Quote{}, fmt.Errorf("%s: response exceeds %d bytes", p.name, maxBody)
	}
	if resp.StatusCode != http.StatusOK {
		return quota.Quote{}, fmt.Errorf("%s: HTTP %d", p.name, resp.StatusCode)
	}
	var dto struct {
		PriceMinor   int64  `json:"price_minor"`
		Currency     string `json:"currency"`
		EstimateDays int    `json:"estimate_days"`
	}
	if err := json.Unmarshal(body, &dto); err != nil {
		return quota.Quote{}, fmt.Errorf("%s: invalid JSON: %w", p.name, err)
	}
	if dto.PriceMinor <= 0 || dto.Currency != "RUB" || dto.EstimateDays <= 0 {
		return quota.Quote{}, fmt.Errorf("%s: invalid quote", p.name)
	}
	return quota.Quote{
		Provider: p.name, PriceMinor: dto.PriceMinor,
		Currency: dto.Currency, EstimateDays: dto.EstimateDays,
	}, nil
}
