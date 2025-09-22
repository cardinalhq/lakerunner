// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package metricsprocessing

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// BoxerConsumer represents a generic boxer consumer interface
type BoxerConsumer interface {
	Run(ctx context.Context) error
	Close() error
}

// BoxerManager coordinates multiple boxer consumers running concurrently
type BoxerManager struct {
	consumers []BoxerConsumer
	tasks     []string
	mu        sync.RWMutex
}

// ConsumerFactory creates boxer consumers for different task types
type ConsumerFactory interface {
	CreateLogCompactionConsumer(ctx context.Context) (BoxerConsumer, error)
	CreateMetricCompactionConsumer(ctx context.Context) (BoxerConsumer, error)
	CreateTraceCompactionConsumer(ctx context.Context) (BoxerConsumer, error)
	CreateMetricRollupConsumer(ctx context.Context) (BoxerConsumer, error)
}

// DefaultConsumerFactory implements ConsumerFactory using existing consumer constructors
type DefaultConsumerFactory struct {
	factory    *fly.Factory
	boxerStore BoxerStore
	cfg        *config.Config
}

// NewDefaultConsumerFactory creates a new DefaultConsumerFactory
func NewDefaultConsumerFactory(cfg *config.Config, boxerStore BoxerStore) (*DefaultConsumerFactory, error) {
	kafkaFactory := fly.NewFactory(&cfg.Kafka)

	return &DefaultConsumerFactory{
		factory:    kafkaFactory,
		boxerStore: boxerStore,
		cfg:        cfg,
	}, nil
}

func (f *DefaultConsumerFactory) CreateLogCompactionConsumer(ctx context.Context) (BoxerConsumer, error) {
	return NewLogCompactionBoxerConsumer(ctx, f.cfg, f.boxerStore, f.factory)
}

func (f *DefaultConsumerFactory) CreateMetricCompactionConsumer(ctx context.Context) (BoxerConsumer, error) {
	return NewMetricCompactionBoxerConsumer(ctx, f.cfg, f.boxerStore, f.factory)
}

func (f *DefaultConsumerFactory) CreateTraceCompactionConsumer(ctx context.Context) (BoxerConsumer, error) {
	return NewTraceCompactionBoxerConsumer(ctx, f.cfg, f.boxerStore, f.factory)
}

func (f *DefaultConsumerFactory) CreateMetricRollupConsumer(ctx context.Context) (BoxerConsumer, error) {
	return NewMetricBoxerConsumer(ctx, f.cfg, f.boxerStore, f.factory)
}

// NewBoxerManager creates a new BoxerManager with the specified tasks
func NewBoxerManager(ctx context.Context, cfg *config.Config, store BoxerStore, tasks []string) (*BoxerManager, error) {
	factory, err := NewDefaultConsumerFactory(cfg, store)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer factory: %w", err)
	}

	return NewBoxerManagerWithFactory(ctx, factory, tasks)
}

// NewBoxerManagerWithFactory creates a new BoxerManager with a custom factory (for testing)
func NewBoxerManagerWithFactory(ctx context.Context, factory ConsumerFactory, tasks []string) (*BoxerManager, error) {
	ll := logctx.FromContext(ctx)

	if len(tasks) == 0 {
		return nil, fmt.Errorf("at least one task must be specified")
	}

	manager := &BoxerManager{
		tasks: tasks,
	}

	// Create consumers for each task
	for _, task := range tasks {
		consumer, err := manager.createConsumerForTask(ctx, factory, task)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer for task %s: %w", task, err)
		}
		manager.consumers = append(manager.consumers, consumer)
	}

	ll.Info("Created boxer manager", "tasks", tasks, "consumers", len(manager.consumers))
	return manager, nil
}

func (m *BoxerManager) createConsumerForTask(ctx context.Context, factory ConsumerFactory, task string) (BoxerConsumer, error) {
	switch task {
	case config.TaskCompactLogs:
		return factory.CreateLogCompactionConsumer(ctx)
	case config.TaskCompactMetrics:
		return factory.CreateMetricCompactionConsumer(ctx)
	case config.TaskCompactTraces:
		return factory.CreateTraceCompactionConsumer(ctx)
	case config.TaskRollupMetrics:
		return factory.CreateMetricRollupConsumer(ctx)
	default:
		return nil, fmt.Errorf("unknown task: %s", task)
	}
}

// Run starts all consumers concurrently and waits for them to complete
func (m *BoxerManager) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx)

	if len(m.consumers) == 0 {
		return fmt.Errorf("no consumers to run")
	}

	g, gCtx := errgroup.WithContext(ctx)

	m.mu.RLock()
	consumers := make([]BoxerConsumer, len(m.consumers))
	copy(consumers, m.consumers)
	tasks := make([]string, len(m.tasks))
	copy(tasks, m.tasks)
	m.mu.RUnlock()

	// Start each consumer in its own goroutine
	for i, consumer := range consumers {
		taskName := tasks[i]
		c := consumer // Capture for closure
		g.Go(func() error {
			gll := logctx.FromContext(gCtx).With("task", taskName)
			gCtx = logctx.WithLogger(gCtx, gll)
			gll.Info("Starting boxer consumer")
			if err := c.Run(gCtx); err != nil {
				gll.Error("Boxer consumer failed", "error", err)
				return fmt.Errorf("task %s failed: %w", taskName, err)
			}
			gll.Info("Boxer consumer stopped")
			return nil
		})
	}

	// Wait for all consumers to complete or for one to fail
	if err := g.Wait(); err != nil {
		return fmt.Errorf("boxer failed: %w", err)
	}

	ll.Info("All boxer consumers completed successfully")
	return nil
}

// Close stops all consumers
func (m *BoxerManager) Close() error {
	m.mu.RLock()
	consumers := make([]BoxerConsumer, len(m.consumers))
	copy(consumers, m.consumers)
	m.mu.RUnlock()

	var errors []error
	for i, consumer := range consumers {
		if err := consumer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close consumer %d: %w", i, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors closing consumers: %v", errors)
	}

	return nil
}

// getTasks returns the list of tasks this manager is running
func (m *BoxerManager) getTasks() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tasks := make([]string, len(m.tasks))
	copy(tasks, m.tasks)
	return tasks
}
