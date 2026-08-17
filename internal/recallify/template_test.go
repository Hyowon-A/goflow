package recallify

import (
	"reflect"
	"testing"
)

func TestTemplateShapeAndGenerationConfig(t *testing.T) {
	template := NewTemplate("http://recallify.test", " token ")
	if len(template.Tasks) != 6 {
		t.Fatalf("expected 6 tasks, got %d", len(template.Tasks))
	}
	wantEdges := [][2]string{
		{"validate_request", "clean_text"},
		{"clean_text", "generate_mcqs"},
		{"generate_mcqs", "validate_mcqs"},
		{"validate_mcqs", "merge_study_set"},
		{"merge_study_set", "notify_callback"},
	}
	if !reflect.DeepEqual(template.Dependencies, wantEdges) {
		t.Fatalf("dependency mismatch: got %#v, want %#v", template.Dependencies, wantEdges)
	}
	if got := template.Tasks[2].Config; got["base_url"] != "http://recallify.test" || got["bearer_token"] != "token" {
		t.Fatalf("unexpected generation config: %#v", got)
	}
}
