package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type requestIDContextKey struct{}

func (s *Server) useMiddleware() {
	s.router.Use(s.requestIDMiddleware)
	s.router.Use(s.loggingMiddleware)
}

func (s *Server) requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestID(r)
		w.Header().Set(requestIDHeader, id)

		ctx := context.WithValue(r.Context(), requestIDContextKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDContextKey{}).(string)
	return id
}

func requestIDForRequest(r *http.Request) string {
	if id := requestIDFromContext(r.Context()); id != "" {
		return id
	}

	return requestID(r)
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &responseStatusRecorder{ResponseWriter: w}

		next.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}

		s.logger.InfoContext(r.Context(), "http_request",
			slog.String("request_id", requestIDForRequest(r)),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.Int64("duration_ms", time.Since(startedAt).Milliseconds()),
		)
	})
}

type responseStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseStatusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}

	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseStatusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}

	return r.ResponseWriter.Write(body)
}
