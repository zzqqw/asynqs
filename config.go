package asynqs

import (
	"context"
	"github.com/hibiken/asynq"
	"time"
)

var DefaultASYNQConfig = asynq.Config{Concurrency: 10}

type ASYNQConfig struct {
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the current process.
	Concurrency int

	// BaseContext optionally specifies a function that returns the base context for Handler invocations on this server.
	//
	// If BaseContext is nil, the default is context.Background().
	// If this is defined, then it MUST return a non-nil context
	BaseContext func() context.Context

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	RetryDelayFunc asynq.RetryDelayFunc

	// Predicate function to determine whether the error returned from Handler is a failure.
	// If the function returns false, Server will not increment the retried counter for the task,
	// and Server won't record the queue stats (processed and failed stats) to avoid skewing the error
	// rate of the queue.
	//
	// By default, if the given error is non-nil the function returns true.
	IsFailure func(error) bool

	// List of queues to process with given priority value. Keys are the names of the
	// queues and values are associated priority value.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	//
	//     Queues: map[string]int{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     }
	//
	// With the above config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	//
	// If a queue has a zero or negative priority value, the queue will be ignored.
	Queues map[string]int

	// StrictPriority indicates whether the queue priority should be treated strictly.
	//
	// If set to true, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	StrictPriority bool

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	//
	// Example:
	//
	//     func reportError(ctx context, task *asynq.Task, err error) {
	//         retried, _ := asynq.GetRetryCount(ctx)
	//         maxRetry, _ := asynq.GetMaxRetry(ctx)
	//     	   if retried >= maxRetry {
	//             err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	//     	   }
	//         errorReportingService.Notify(err)
	//     })
	//
	//     ErrorHandler: asynq.ErrorHandlerFunc(reportError)
	ErrorHandler asynq.ErrorHandler

	// Logger specifies the logger used by the server instance.
	//
	// If unset, default logger is used.
	Logger asynq.Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel asynq.LogLevel

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	ShutdownTimeout time.Duration

	// HealthCheckFunc is called periodically with any errors encountered during ping to the
	// connected redis server.
	HealthCheckFunc func(error)

	// HealthCheckInterval specifies the interval between healthchecks.
	//
	// If unset or zero, the interval is set to 15 seconds.
	HealthCheckInterval time.Duration

	// DelayedTaskCheckInterval specifies the interval between checks run on 'scheduled' and 'retry'
	// tasks, and forwarding them to 'pending' state if they are ready to be processed.
	//
	// If unset or zero, the interval is set to 5 seconds.
	DelayedTaskCheckInterval time.Duration

	// GroupGracePeriod specifies the amount of time the server will wait for an incoming task before aggregating
	// the tasks in a group. If an incoming task is received within this period, the server will wait for another
	// period of the same length, up to GroupMaxDelay if specified.
	//
	// If unset or zero, the grace period is set to 1 minute.
	// Minimum duration for GroupGracePeriod is 1 second. If value specified is less than a second, the call to
	// NewServer will panic.
	GroupGracePeriod time.Duration

	// GroupMaxDelay specifies the maximum amount of time the server will wait for incoming tasks before aggregating
	// the tasks in a group.
	//
	// If unset or zero, no delay limit is used.
	GroupMaxDelay time.Duration

	// GroupMaxSize specifies the maximum number of tasks that can be aggregated into a single task within a group.
	// If GroupMaxSize is reached, the server will aggregate the tasks into one immediately.
	//
	// If unset or zero, no size limit is used.
	GroupMaxSize int

	// GroupAggregator specifies the aggregation function used to aggregate multiple tasks in a group into one task.
	//
	// If unset or nil, the group aggregation feature will be disabled on the server.
	GroupAggregator asynq.GroupAggregator
}

func (a *ASYNQConfig) ASYNQConfig() asynq.Config {
	if a != nil {
		return asynq.Config{
			Concurrency:              a.Concurrency,
			BaseContext:              a.BaseContext,
			RetryDelayFunc:           a.RetryDelayFunc,
			IsFailure:                a.IsFailure,
			Queues:                   a.Queues,
			StrictPriority:           a.StrictPriority,
			ErrorHandler:             a.ErrorHandler,
			Logger:                   a.Logger,
			LogLevel:                 a.LogLevel,
			ShutdownTimeout:          a.ShutdownTimeout,
			HealthCheckFunc:          a.HealthCheckFunc,
			HealthCheckInterval:      a.HealthCheckInterval,
			DelayedTaskCheckInterval: a.DelayedTaskCheckInterval,
			GroupGracePeriod:         a.GroupGracePeriod,
			GroupMaxDelay:            a.GroupMaxDelay,
			GroupMaxSize:             a.GroupMaxSize,
			GroupAggregator:          a.GroupAggregator,
		}
	}
	return DefaultASYNQConfig
}

type RedisConfig struct {
	Redis  asynq.RedisClientOpt
	Config *ASYNQConfig
}

func (r *RedisConfig) Asynqs() *Asynqs {
	return New(r.Redis, r.Config.ASYNQConfig())
}

type RedisFailoverConfig struct {
	Redis  asynq.RedisFailoverClientOpt
	Config *ASYNQConfig
}

func (r *RedisFailoverConfig) Asynqs() *Asynqs {
	return New(r.Redis, r.Config.ASYNQConfig())
}

type RedisClusterConfig struct {
	Redis  asynq.RedisClusterClientOpt
	Config *ASYNQConfig
}

func (r *RedisClusterConfig) Asynqs() *Asynqs {
	return New(r.Redis, r.Config.ASYNQConfig())
}
