package recallify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

var (
	ErrInvalidRecallifyBaseURL  = errors.New("invalid_recallify_base_url")
	ErrInvalidRecallifyResponse = errors.New("invalid_recallify_response")
)

type RecallifyClient struct {
	BaseURL     string
	BearerToken string
	HTTPClient  *http.Client
}

type RecallifyClientError struct {
	StatusCode int
	Retryable  bool
	Err        error
}

func (e RecallifyClientError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("recallify request failed with status %d", e.StatusCode)
	}
	return e.Err.Error()
}

func (e RecallifyClientError) Unwrap() error {
	return e.Err
}

func IsRetryableRecallifyError(err error) bool {
	var clientErr RecallifyClientError
	return errors.As(err, &clientErr) && clientErr.Retryable
}

func (c RecallifyClient) GenerateMCQs(ctx context.Context, text string, count int, level string) (string, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return "", ErrInvalidRecallifyBaseURL
	}

	body, err := json.Marshal(map[string]string{
		"text":  text,
		"count": strconv.Itoa(count),
		"level": level,
	})
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/ai/generateMcqs", bytes.NewReader(body))
	if err != nil {
		return "", ErrInvalidRecallifyBaseURL
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(c.BearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.BearerToken))
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", RecallifyClientError{Retryable: retryableRecallifyRequestError(err), Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", RecallifyClientError{StatusCode: resp.StatusCode, Retryable: retryableRecallifyStatus(resp.StatusCode)}
	}

	var payload struct {
		MCQs string `json:"mcqs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil || strings.TrimSpace(payload.MCQs) == "" {
		return "", RecallifyClientError{Err: ErrInvalidRecallifyResponse}
	}
	return payload.MCQs, nil
}

func retryableRecallifyRequestError(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr) && netErr.Timeout()
}

func retryableRecallifyStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
