package s3spanstore

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore/mocks"
	"github.com/stretchr/testify/assert"
)

func TestWriteSpan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSvc := mocks.NewMockS3API(ctrl)
	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.PutObjectOutput{}, nil)

	assert := assert.New(t)
	loggerName := "jaeger-s3"

	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Debug.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	ctx := context.TODO()

	writer, err := NewWriter(logger, mockSvc, config.S3{
		BucketName: "jaeger-spans",
		Prefix:     "/spans/",
	})
	assert.NoError(err)

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(`{
		"traceId": "AAAAAAAAAAAAAAAAAAAAEQ==",
		"spanId": "AAAAAAAAAAM=",
		"operationName": "example-operation-1",
		"references": [],
		"startTime": "2017-01-26T16:46:31.639875Z",
		"duration": "100000ns",
		"tags": [],
		"process": {
			"serviceName": "example-service-1",
			"tags": []
		},
		"logs": [
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			},
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			}
		]
	}`), &span))

	assert.NoError(writer.WriteSpan(ctx, &span))

	assert.NoError(writer.Close())

	// assert.Equal(stripFormatting(`{
	// 	"traceid":"0000000000000011",
	// 	"spanid":"0000000000000003",
	// 	"operationname":"example-operation-1",
	// 	"spankind":"",
	// 	"starttime":1485449191639,
	// 	"duration":100000,
	// 	"tags":{},
	// 	"servicename":"example-service-1",
	// 	"spanpayload":"/wYAAHNOYVBwWQBZAAB5D7oLeggKEAA2AQAIERIIDRGwAxoTZXhhbXBsZS1vcGVyYXRpb24tMTIMCOfPqMQFELjvjrECOgQQoI0GSg4KMhYAAEo6EAAMUhMKERFLIHNlcnZpY2UtMQ==",
	// 	"references":[]
	// }`), string(writtenRecord.Data))
}

func stripFormatting(json string) string {
	return strings.ReplaceAll(strings.ReplaceAll(json, "\n", ""), "\t", "")
}

func BenchmarkWriteSpan(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	assert := assert.New(b)
	loggerName := "jaeger-s3"

	logLevel := os.Getenv("GRPC_STORAGE_PLUGIN_LOG_LEVEL")
	if logLevel == "" {
		logLevel = hclog.Warn.String()
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.LevelFromString(logLevel),
		Name:       loggerName,
		JSONFormat: true,
	})

	ctx := context.TODO()

	mockSvc := mocks.NewMockS3API(ctrl)
	mockSvc.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&s3.PutObjectOutput{}, nil)

	writer, err := NewWriter(logger, mockSvc, config.S3{
		BucketName: "jaeger-spans",
		Prefix:     "/spans/",
	})
	assert.NoError(err)

	var span model.Span
	assert.NoError(jsonpb.Unmarshal(strings.NewReader(`{
		"traceId": "AAAAAAAAAAAAAAAAAAAAEQ==",
		"spanId": "AAAAAAAAAAM=",
		"operationName": "example-operation-1",
		"references": [],
		"startTime": "2017-01-26T16:46:31.639875Z",
		"duration": "100000ns",
		"tags": [],
		"process": {
			"serviceName": "example-service-1",
			"tags": []
		},
		"logs": [
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			},
			{
				"timestamp": "2017-01-26T16:46:31.639875Z",
				"fields": []
			}
		]
	}`), &span))

	// run the WriteSpan function b.N times
	for n := 0; n < b.N; n++ {
		assert.NoError(writer.WriteSpan(ctx, &span))
	}
	assert.NoError(writer.Close())
}
