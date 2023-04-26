package s3spanstore

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"golang.org/x/sync/errgroup"
)

type FirehoseAPI interface {
	PutRecordWithContext(ctx context.Context, input *firehose.PutRecordInput, opts ...request.Option) (*firehose.PutRecordOutput, error)
}

type Writer struct {
	logger hclog.Logger

	spanWriter      *ParquetWriter
	operationWriter *ParquetWriter
}

func NewWriter(ctx context.Context, logger hclog.Logger, svc FirehoseAPI, firehoseConfig config.Firehose) (*Writer, error) {
	rand.Seed(time.Now().UnixNano())

	operationWriter := NewParquetWriter(
		ctx,
		logger,
		svc,
		firehoseConfig.OperationsDeliveryStreamName,
		new(OperationRecord),
	)
	spanWriter := NewParquetWriter(
		ctx,
		logger,
		svc,
		firehoseConfig.SpansDeliveryStreamName,
		new(SpanRecord),
	)

	w := &Writer{
		logger:          logger,
		operationWriter: operationWriter,
		spanWriter:      spanWriter,
	}

	return w, nil
}

func (w *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		operationRecord, err := NewOperationRecordFromSpan(span)
		if err != nil {
			return fmt.Errorf("failed to create operation record: %w", err)
		}

		w.logger.Debug("Writing operation record", "operation", operationRecord)
		if err := w.operationWriter.Write(gCtx, operationRecord); err != nil {
			return fmt.Errorf("failed to write operation item: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		w.logger.Debug("Writing span record", "span", span)
		spanRecord, err := NewSpanRecordFromSpan(span)
		if err != nil {
			return fmt.Errorf("failed to create span record: %w", err)
		}

		if err := w.spanWriter.Write(gCtx, spanRecord); err != nil {
			return fmt.Errorf("failed to write span item: %w", err)
		}

		return nil
	})

	return g.Wait()
}
