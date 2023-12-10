package asynqs

import (
	"context"
	"github.com/hibiken/asynq"
	"time"
)

type HandleFunc struct {
	Pattern    string
	HandleFunc func(context.Context, *asynq.Task) error
}

type Asynqs struct {
	client     *asynq.Client
	srv        *asynq.Server
	mux        *asynq.ServeMux
	handleFunc []*HandleFunc
	option     []asynq.Option
}

func New(redis asynq.RedisConnOpt, config asynq.Config) *Asynqs {
	return &Asynqs{
		client:     asynq.NewClient(redis),
		srv:        asynq.NewServer(redis, config),
		mux:        asynq.NewServeMux(),
		handleFunc: []*HandleFunc{},
		option:     []asynq.Option{},
	}
}

// WithOption
//asynq.ProcessIn(delay) 延迟队列
//asynq.MaxRetry(5) 失败重试次数
//asynq.TaskID("uuid") 任务ID
func (a *Asynqs) WithOption(opt asynq.Option) *Asynqs {
	a.option = append(a.option, opt)
	return a
}

// PushByte 队列
func (a *Asynqs) PushByte(typename string, payload []byte) (*asynq.TaskInfo, error) {
	return a.Push(asynq.NewTask(typename, payload))
}

// DelayPushByte 延迟队列
func (a *Asynqs) DelayPushByte(typename string, payload []byte, delay time.Duration) (*asynq.TaskInfo, error) {
	a.WithOption(asynq.ProcessIn(delay))
	return a.Push(asynq.NewTask(typename, payload))
}

// Push 加入队列
func (a *Asynqs) Push(task *asynq.Task, opt ...asynq.Option) (*asynq.TaskInfo, error) {
	defer a.client.Close()
	a.option = append(a.option, opt...)
	return a.client.Enqueue(task, a.option...)
}

func (a *Asynqs) AddHandleFunc(pattern string, handler func(context.Context, *asynq.Task) error) *Asynqs {
	a.handleFunc = append(a.handleFunc, &HandleFunc{Pattern: pattern, HandleFunc: handler})
	a.mux.HandleFunc(pattern, handler)
	return a
}

func (a *Asynqs) Run() error {
	return a.srv.Run(a.mux)
}

func (a *Asynqs) Shutdown() {
	a.srv.Shutdown()
}

func (a *Asynqs) Stop() {
	a.srv.Stop()
}

func (a *Asynqs) Start() error {
	return a.srv.Start(a.mux)
}
