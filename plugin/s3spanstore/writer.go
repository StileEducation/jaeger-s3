package s3spanstore

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// mockgen -destination=./plugin/s3spanstore/mocks/mock_s3.go -package=mocks github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore S3API

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

const (
	PARQUET_CONCURRENCY = 1
	PARTION_FORMAT      = "2006/01/02/15"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func S3ParquetKey(prefix, suffix string, datehour string) string {
	return prefix + datehour + "/" + suffix + ".parquet"
}

func S3PartitionKey(t time.Time) string {
	return t.Format(PARTION_FORMAT)
}

type ParquetRef struct {
	parquetWriteFile source.ParquetFile
	parquetWriter    *writer.ParquetWriter
}

type Writer struct {
	logger     hclog.Logger
	svc        S3API
	bucketName string
	prefix     string
	ticker     *time.Ticker
	done       chan bool

	parquetWriterRefs map[string]*ParquetRef
	bufferMutex       sync.Mutex
	ctx               context.Context
}

func EmptyBucket(ctx context.Context, svc S3API, bucketName string) error {
	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	paginator := s3.NewListObjectsV2Paginator(svc, params)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed fetch page: %w", err)
		}
		for _, value := range output.Contents {
			if _, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    value.Key,
			}); err != nil {
				return fmt.Errorf("failed to delete object: %w", err)
			}
		}
	}

	return nil
}

func NewWriter(logger hclog.Logger, svc S3API, s3Config config.S3) (*Writer, error) {
	rand.Seed(time.Now().UnixNano())

	bufferDuration := time.Second * 60
	if s3Config.BufferDuration != "" {
		duration, err := time.ParseDuration(s3Config.BufferDuration)
		if err != nil {
			return nil, fmt.Errorf("failed to parse buffer duration: %w", err)
		}
		bufferDuration = duration
	}

	ctx := context.Background()

	if s3Config.EmptyBucket {
		if err := EmptyBucket(ctx, svc, s3Config.BucketName); err != nil {
			return nil, fmt.Errorf("failed to empty s3 bucket: %w", err)
		}
	}

	w := &Writer{
		svc:               svc,
		bucketName:        s3Config.BucketName,
		prefix:            s3Config.Prefix,
		logger:            logger,
		ticker:            time.NewTicker(bufferDuration),
		done:              make(chan bool),
		parquetWriterRefs: map[string]*ParquetRef{},
		ctx:               ctx,
	}

	go func() {
		for {
			select {
			case <-w.done:
				return
			case <-w.ticker.C:
				if err := w.rotateParquetWriters(); err != nil {
					w.logger.Error("failed to rotate parquet writer", err)
				}
			}
		}
	}()

	return w, nil
}

func (w *Writer) getParquetWriter(datehour string) (*writer.ParquetWriter, error) {
	if w.parquetWriterRefs[datehour] != nil {
		return w.parquetWriterRefs[datehour].parquetWriter, nil
	}

	writeFile, err := s3v2.NewS3FileWriterWithClient(w.ctx, w.svc, w.bucketName, w.parquetKey(datehour), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet s3 client: %w", err)
	}

	parquetWriter, err := writer.NewParquetWriter(writeFile, new(SpanRecord), PARQUET_CONCURRENCY)
	if err != nil {
		writeFile.Close()
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	w.parquetWriterRefs[datehour] = &ParquetRef{
		parquetWriteFile: writeFile,
		parquetWriter:    parquetWriter,
	}

	return parquetWriter, nil
}

func (w *Writer) parquetKey(datehour string) string {
	return S3ParquetKey(w.prefix, RandStringBytes(32), datehour)
}

func (w *Writer) closeParquetWriter(parquetRef *ParquetRef) error {
	if parquetRef.parquetWriter != nil {
		if err := parquetRef.parquetWriter.WriteStop(); err != nil {
			return fmt.Errorf("parquet write stop error: %w", err)
		}
	}

	if parquetRef.parquetWriteFile != nil {
		if err := parquetRef.parquetWriteFile.Close(); err != nil {
			return fmt.Errorf("parquet file write close error: %w", err)
		}
	}

	return nil
}

func (w *Writer) rotateParquetWriters() error {
	w.bufferMutex.Lock()

	writerRefs := w.parquetWriterRefs
	w.parquetWriterRefs = map[string]*ParquetRef{}

	w.bufferMutex.Unlock()

	return w.closeParquetWriters(writerRefs)
}

func (w *Writer) closeParquetWriters(parquetWriterRefs map[string]*ParquetRef) error {
	for _, writerRef := range parquetWriterRefs {
		if err := w.closeParquetWriter(writerRef); err != nil {
			return fmt.Errorf("failed to close previous parquet writer: %w", err)
		}
	}

	return nil
}

func (w *Writer) WriteSpan(ctx context.Context, span *model.Span) error {
	// s.logger.Debug("WriteSpan", span)

	spanRecord, err := NewSpanRecordFromSpan(span)
	if err != nil {
		return fmt.Errorf("failed to create span record: %w", err)
	}

	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	spanDatehour := S3PartitionKey(span.StartTime)

	parquetWriter, err := w.getParquetWriter(spanDatehour)
	if err != nil {
		return fmt.Errorf("failed to get parquet writer: %w", err)
	}

	if err := parquetWriter.Write(spanRecord); err != nil {
		return fmt.Errorf("failed to write span item: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	w.ticker.Stop()
	w.done <- true

	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	return w.closeParquetWriters(w.parquetWriterRefs)
}
