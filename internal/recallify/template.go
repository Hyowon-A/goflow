package recallify

import (
	"strings"

	"github.com/Hyowon-A/goflow/internal/worker"
)

const (
	ExecutorTypeValidateRequest = "recallify_validate_request"
	ExecutorTypeCleanText       = "recallify_clean_text"
	ExecutorTypeGenerateMCQs    = "recallify_generate_mcqs"
	ExecutorTypeValidateMCQs    = "recallify_validate_mcqs"
	ExecutorTypeMergeStudySet   = "recallify_merge_study_set"
	ExecutorTypeNotifyCallback  = "recallify_notify_callback"
)

type ExecutionInput = worker.ExecutionInput
type ExecutionResult = worker.ExecutionResult

type TaskSpec struct {
	Name         string
	ExecutorType string
	Config       map[string]any
}

type Template struct {
	Tasks        []TaskSpec
	Dependencies [][2]string
}

func NewTemplate(baseURL, bearerToken string) Template {
	generateConfig := map[string]any{"base_url": baseURL}
	if token := strings.TrimSpace(bearerToken); token != "" {
		generateConfig["bearer_token"] = token
	}

	return Template{
		Tasks: []TaskSpec{
			{Name: "validate_request", ExecutorType: ExecutorTypeValidateRequest},
			{Name: "clean_text", ExecutorType: ExecutorTypeCleanText},
			{Name: "generate_mcqs", ExecutorType: ExecutorTypeGenerateMCQs, Config: generateConfig},
			{Name: "validate_mcqs", ExecutorType: ExecutorTypeValidateMCQs},
			{Name: "merge_study_set", ExecutorType: ExecutorTypeMergeStudySet},
			{Name: "notify_callback", ExecutorType: ExecutorTypeNotifyCallback},
		},
		Dependencies: [][2]string{
			{"validate_request", "clean_text"},
			{"clean_text", "generate_mcqs"},
			{"generate_mcqs", "validate_mcqs"},
			{"validate_mcqs", "merge_study_set"},
			{"merge_study_set", "notify_callback"},
		},
	}
}
