package worker

import (
	"context"
	"fmt"
	"strings"

	"github.com/Hyowon-A/goflow/internal/queue"
	"github.com/Hyowon-A/goflow/internal/workflow"
)

type ServiceConfig struct {
	WorkerID string
}

type TaskRunClaimer interface {
	ClaimTaskRun(ctx context.Context, input workflow.ClaimTaskRunInput) (workflow.TaskRun, error)
}

type Service struct {
	config   ServiceConfig
	consumer queue.TaskConsumer
	claimer  TaskRunClaimer
}

func NewService(config ServiceConfig, consumer queue.TaskConsumer, claimer TaskRunClaimer) *Service {
	return &Service{
		config:   config,
		consumer: consumer,
		claimer:  claimer,
	}
}

func (s *Service) ProcessOne(ctx context.Context) error {
	if s == nil || s.consumer == nil || s.claimer == nil || strings.TrimSpace(s.config.WorkerID) == "" {
		return fmt.Errorf("invalid worker service config")
	}

	message, err := s.consumer.ReceiveTask(ctx)
	if err != nil {
		return err
	}

	_, err = s.claimer.ClaimTaskRun(ctx, workflow.ClaimTaskRunInput{
		TaskRunID: message.TaskRunID,
		WorkerID:  s.config.WorkerID,
	})
	if err != nil {
		return err
	}

	if err := s.consumer.AckTask(ctx, message.MessageID); err != nil {
		return err
	}

	return nil
}
