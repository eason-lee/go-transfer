package handler

import (
	"go-transfer/config"

	"go-transfer/es"
	"go-transfer/filter"

	jsoniter "github.com/json-iterator/go"
)

// MessageHandler ...
type MessageHandler struct {
	writer  *es.Writer
	filters []filter.FilterFunc
	index   string
}

// NewHandler ...
func NewHandler(conf *config.EsConf, writer *es.Writer) *MessageHandler {
	return &MessageHandler{
		writer: writer,
		index:  conf.Index,
	}
}

// AddFilters 添加过滤器
func (mh *MessageHandler) AddFilters(filters ...filter.FilterFunc) {
	for _, f := range filters {
		mh.filters = append(mh.filters, f)
	}
}

// Consume 消费数据
func (mh *MessageHandler) Consume(_, val string) error {
	var m map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(val), &m); err != nil {
		return err
	}

	for _, proc := range mh.filters {
		if m = proc(m); m == nil {
			return nil
		}
	}

	bs, err := jsoniter.Marshal(m)
	if err != nil {
		return err
	}

	return mh.writer.Write(mh.index, string(bs))
}
