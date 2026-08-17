package recallify

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
)

const (
	DefaultRecallifyTitle = "Untitled Study Set"
	DefaultRecallifyLevel = "medium"
	DefaultRecallifyCount = 10
)

var (
	ErrMissingRecallifyDocumentText = errors.New("missing_recallify_document_text")
	ErrInvalidRecallifyLevel        = errors.New("invalid_recallify_level")
	ErrInvalidRecallifyCount        = errors.New("invalid_recallify_count")
	ErrInvalidRecallifyField        = errors.New("invalid_recallify_field")
)

type RecallifyRunInput struct {
	DocumentText      string
	Title             string
	Level             string
	MCQCount          int
	CallbackURL       string
	ExternalRequestID string
}

func NormalizeRecallifyRunInput(input map[string]any) (RecallifyRunInput, error) {
	documentText, ok := input["document_text"].(string)
	if !ok || strings.TrimSpace(documentText) == "" {
		return RecallifyRunInput{}, ErrMissingRecallifyDocumentText
	}

	normalized := RecallifyRunInput{
		DocumentText: strings.TrimSpace(documentText),
		Title:        DefaultRecallifyTitle,
		Level:        DefaultRecallifyLevel,
		MCQCount:     DefaultRecallifyCount,
	}

	if value, ok := input["title"]; ok {
		title, ok := value.(string)
		if !ok {
			return RecallifyRunInput{}, ErrInvalidRecallifyField
		}
		normalized.Title = title
	}
	normalized.Title = strings.TrimSpace(normalized.Title)
	if normalized.Title == "" {
		normalized.Title = DefaultRecallifyTitle
	}

	if value, ok := input["level"]; ok && value != nil {
		level, ok := value.(string)
		if !ok {
			return RecallifyRunInput{}, ErrInvalidRecallifyField
		}
		normalized.Level = strings.TrimSpace(level)
	}
	if normalized.Level != "easy" && normalized.Level != "medium" && normalized.Level != "hard" {
		return RecallifyRunInput{}, ErrInvalidRecallifyLevel
	}

	if err := optionalCount(input, "mcq_count", &normalized.MCQCount); err != nil {
		return RecallifyRunInput{}, err
	}
	if normalized.MCQCount == 0 {
		return RecallifyRunInput{}, ErrInvalidRecallifyCount
	}

	if err := optionalString(input, "callback_url", &normalized.CallbackURL); err != nil {
		return RecallifyRunInput{}, err
	}
	normalized.CallbackURL = strings.TrimSpace(normalized.CallbackURL)

	if err := optionalString(input, "external_request_id", &normalized.ExternalRequestID); err != nil {
		return RecallifyRunInput{}, err
	}
	normalized.ExternalRequestID = strings.TrimSpace(normalized.ExternalRequestID)

	return normalized, nil
}

func optionalString(input map[string]any, key string, target *string) error {
	value, ok := input[key]
	if !ok || value == nil {
		return nil
	}

	text, ok := value.(string)
	if !ok {
		return ErrInvalidRecallifyField
	}
	*target = text
	return nil
}

func optionalCount(input map[string]any, key string, target *int) error {
	value, ok := input[key]
	if !ok || value == nil {
		return nil
	}

	count, ok := intValue(value)
	if !ok || count < 0 {
		return ErrInvalidRecallifyCount
	}
	*target = count
	return nil
}

func intValue(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		if int64(int(v)) != v {
			return 0, false
		}
		return int(v), true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v || int64(int(v)) != int64(v) {
			return 0, false
		}
		return int(v), true
	case json.Number:
		i, err := v.Int64()
		if err != nil || int64(int(i)) != i {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}
