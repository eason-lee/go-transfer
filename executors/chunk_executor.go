package executors

import "time"

const defaultChunkSize = 1024 * 1024 // 1M
const defaultFlushInterval = time.Second

type (
	chunk struct {
		val  interface{}
		size int
	}

	// ChunkOption ...
	ChunkOption func(options *chunkOptions)

	// ChunkExecutor 任务执行者
	ChunkExecutor struct {
		executor  *PeriodicalExecutor
		container *chunkContainer
	}

	// chunkContainer 任务块的容器
	chunkContainer struct {
		tasks        []interface{}
		execute      Execute
		size         int
		maxChunkSize int
	}

	chunkOptions struct {
		chunkSize     int
		flushInterval time.Duration
	}
	// Execute 任务处理函数
	Execute func(tasks []interface{})
)

// NewChunkExecutor ...
func NewChunkExecutor(execute Execute, opts ...ChunkOption) *ChunkExecutor {
	options := newChunkOptions()
	for _, opt := range opts {
		opt(&options)
	}

	container := &chunkContainer{
		execute:      execute,
		maxChunkSize: options.chunkSize,
	}
	executor := &ChunkExecutor{
		executor:  NewPeriodicalExecutor(options.flushInterval, container),
		container: container,
	}

	return executor
}

// Add ...
func (ce *ChunkExecutor) Add(task interface{}, size int) error {
	ce.executor.Add(chunk{
		val:  task,
		size: size,
	})
	return nil
}

// Flush ...
func (ce *ChunkExecutor) Flush() {
	ce.executor.Flush()
}

// Wait ...
func (ce *ChunkExecutor) Wait() {
	ce.executor.Wait()
}

// WithChunkBytes 设置任务块的大小
func WithChunkBytes(size int) ChunkOption {
	return func(options *chunkOptions) {
		options.chunkSize = size
	}
}

// WithFlushInterval 设置 flush 任务的时间间隔
func WithFlushInterval(duration time.Duration) ChunkOption {
	return func(options *chunkOptions) {
		options.flushInterval = duration
	}
}

func newChunkOptions() chunkOptions {
	return chunkOptions{
		chunkSize:     defaultChunkSize,
		flushInterval: defaultFlushInterval,
	}
}

// AddTask 添加任务，并返回目前的任务大小是否大于设置的最大任务块
func (bc *chunkContainer) AddTask(task interface{}) bool {
	ck := task.(chunk)
	bc.tasks = append(bc.tasks, ck.val)
	bc.size += ck.size
	return bc.size >= bc.maxChunkSize
}

// Execute 执行任务处理函数
func (bc *chunkContainer) Execute(tasks interface{}) {
	vals := tasks.([]interface{})
	bc.execute(vals)
}

// RemoveAll remove 所有的 tasks 并返回 tasks
func (bc *chunkContainer) RemoveAll() interface{} {
	tasks := bc.tasks
	bc.tasks = nil
	bc.size = 0
	return tasks
}
