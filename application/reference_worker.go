package application

import (
	"context"
	"time"
)

type ReferenceWorker struct {
	service  *WagerService
	interval time.Duration
	batch    int32
	onError  func(error)
}

func NewReferenceWorker(service *WagerService, interval time.Duration, batch int32, onError func(error)) *ReferenceWorker {
	return &ReferenceWorker{service: service, interval: interval, batch: batch, onError: onError}
}

func (worker *ReferenceWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(worker.interval)
	defer ticker.Stop()
	for {
		if err := worker.service.RetryPendingReferences(ctx, worker.batch); err != nil && ctx.Err() == nil && worker.onError != nil {
			worker.onError(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
