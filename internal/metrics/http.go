package metrics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var out bytes.Buffer
		if err := r.Render(req.Context(), &out); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(out.Bytes())
	})
}

func (r *Registry) Render(ctx context.Context, w io.Writer) error {
	for _, definition := range Definitions {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", definition.Name, definition.Help, definition.Name, definition.Type); err != nil {
			return err
		}

		switch definition.Type {
		case Counter:
			if _, err := fmt.Fprintf(w, "%s %d\n", definition.Name, r.counterValue(definition.Name)); err != nil {
				return err
			}
		case Gauge:
			value, err := r.gaugeValue(ctx, definition.Name)
			if err != nil {
				return fmt.Errorf("render gauge %s: %w", definition.Name, err)
			}
			if _, err := fmt.Fprintf(w, "%s %d\n", definition.Name, value); err != nil {
				return err
			}
		}
	}
	return nil
}
