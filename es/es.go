package es

import (
	"context"
	"go-transfer/config"
	"go-transfer/executors"
	"log"

	"github.com/olivere/elastic"
)

type (
	// Writer ...
	Writer struct {
		docType  string
		client   *elastic.Client
		inserter *executors.ChunkExecutor
	}

	valueWithIndex struct {
		index string
		val   string
	}
)

// NewWriter ...
func NewWriter(conf *config.EsConf) (*Writer, error) {
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(conf.Hosts...),
		elastic.SetGzip(conf.Compress),
	)
	if err != nil {
		return nil, err
	}

	writer := Writer{
		docType: conf.DocType,
		client:  client,
	}

	writer.inserter = executors.NewChunkExecutor(writer.execute,
		executors.WithChunkBytes(conf.MaxChunkBytes))
	return &writer, nil
}

func (w *Writer) Write(index, val string) error {
	return w.inserter.Add(valueWithIndex{
		index: index,
		val:   val,
	}, len(val))
}

func (w *Writer) execute(vals []interface{}) {
	var bulk = w.client.Bulk()
	for _, val := range vals {
		pair := val.(valueWithIndex)
		// log.Printf("发送数据 %v", pair.val)
		req := elastic.NewBulkIndexRequest().Index(pair.index).Type(w.docType).Doc(pair.val)
		bulk.Add(req)
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		log.Fatalf("execute es data err %v", err)
	}
}
