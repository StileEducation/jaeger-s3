package s3spanstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/hashicorp/go-hclog"
)

const (
	PARQUET_CONCURRENCY = 1
	PARTION_FORMAT      = "2006/01/02/15"
)

type ParquetWriter struct {
	deliveryStreamName string
	logger             hclog.Logger
	svc                FirehoseAPI
	rowType            interface{}
}

type IParquetWriter interface {
	Write(ctx context.Context, row interface{}) error
}

func NewParquetWriter(ctx context.Context, logger hclog.Logger, svc FirehoseAPI, deliveryStreamName string, rowType interface{}) *ParquetWriter {
	return &ParquetWriter{
		svc:                svc,
		logger:             logger,
		deliveryStreamName: deliveryStreamName,
		rowType:            rowType,
	}
}

func (w *ParquetWriter) Write(ctx context.Context, row interface{}) error {
	data, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("failed to convert row to JSON: %w", err)
	}

	output, err := w.svc.PutRecordWithContext(ctx, &firehose.PutRecordInput{
		DeliveryStreamName: &w.deliveryStreamName,
		Record: &firehose.Record{
			Data: data,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to write record to delivery stream %s: %v",
			w.deliveryStreamName,
			err,
		)
	}
	w.logger.Debug(
		"Successfully wrote record",
		"deliveryStream", w.deliveryStreamName,
		"record", *output.RecordId,
	)

	return nil
}
