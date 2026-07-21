package httpserver

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/google/uuid"
)

const requestIDHeader = "X-Request-ID"

type errorResponse struct {
	Error     string `json:"error"`
	Message   string `json:"message,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

var (
	errMalformedJSON          = errors.New("malformed_json")
	errEmptyBody              = errors.New("empty_body")
	errUnsupportedContentType = errors.New("unsupported_content_type")
)

func decodeJSON(r *http.Request, dst any) error {
	if r.Body == nil {
		return errEmptyBody
	}
	defer r.Body.Close()

	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil || mediaType != "application/json" {
			return errUnsupportedContentType
		}
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return errEmptyBody
		}
		return errMalformedJSON
	}

	if decoder.Decode(&struct{}{}) != io.EOF {
		return errMalformedJSON
	}

	return nil
}

func requestID(r *http.Request) string {
	if id := r.Header.Get(requestIDHeader); id != "" {
		return id
	}

	return uuid.NewString()
}

func writeError(w http.ResponseWriter, status int, code, message, requestID string) {
	if requestID != "" {
		w.Header().Set(requestIDHeader, requestID)
	}

	writeJSON(w, status, errorResponse{
		Error:     code,
		Message:   message,
		RequestID: requestID,
	})
}

func writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	id := requestIDForRequest(r)

	switch {
	case errors.Is(err, errUnsupportedContentType):
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_content_type", "Content-Type must be application/json", id)
	case errors.Is(err, errEmptyBody):
		writeError(w, http.StatusBadRequest, "empty_body", "Request body is required", id)
	default:
		writeError(w, http.StatusBadRequest, "malformed_json", "Request body must be valid JSON", id)
	}
}
