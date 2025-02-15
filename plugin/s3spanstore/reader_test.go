package s3spanstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/johanneswuerbach/jaeger-s3/plugin/config"
	"github.com/johanneswuerbach/jaeger-s3/plugin/s3spanstore/mocks"
	"github.com/stretchr/testify/assert"
)

func NewTestReader(ctx context.Context, assert *assert.Assertions, mockSvc *mocks.MockAthenaAPI) *Reader {
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

	reader, err := NewReader(ctx, logger, mockSvc, config.Athena{
		DatabaseName:         "default",
		SpansTableName:       "jaeger_spans",
		OperationsTableName:  "jaeger_operations",
		OutputLocation:       "s3://jaeger-s3-test-results/",
		WorkGroup:            "jaeger",
		MaxSpanAge:           "336h",
		DependenciesQueryTTL: "6h",
		ServicesQueryTTL:     "10s",
	})

	assert.NoError(err)

	return reader
}

func toAthenaResultSet(results [][]string) *types.ResultSet {
	resultsRows := make([]types.Row, len(results)+1)
	resultsRows[0] = types.Row{} // Header row, usually ignored by the reader
	for i, row := range results {
		row := row

		rowValues := make([]types.Datum, len(row))
		for i, columnValue := range row {
			columnValue := columnValue
			rowValues[i] = types.Datum{VarCharValue: &columnValue}
		}

		resultsRows[i+1] = types.Row{Data: rowValues}
	}

	return &types.ResultSet{Rows: resultsRows}
}

func mockQueryRunAndResult(mockSvc *mocks.MockAthenaAPI, result [][]string) {
	queryID := "queryId"
	now := time.Now()

	mockSvc.EXPECT().StartQueryExecution(gomock.Any(), gomock.Any()).
		Return(&athena.StartQueryExecutionOutput{
			QueryExecutionId: &queryID,
		}, nil)
	mockSvc.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).
		Return(&athena.GetQueryExecutionOutput{
			QueryExecution: &types.QueryExecution{
				Status: &types.QueryExecutionStatus{
					CompletionDateTime: &now,
				},
			},
		}, nil)
	mockSvc.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).
		Return(&athena.GetQueryResultsOutput{
			ResultSet: toAthenaResultSet(result),
		}, nil)
}

func TestGetServices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	serviceName := "test"

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
    mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{}, nil)

	mockQueryRunAndResult(mockSvc, [][]string{{serviceName}})

	assert := assert.New(t)
	ctx := context.TODO()

	reader := NewTestReader(ctx, assert, mockSvc)

	services, err := reader.GetServices(ctx)

	assert.NoError(err)
	assert.Equal([]string{serviceName}, services)
}

func TestGetOperations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	results := [][]string{
		{
			"server-op",
			"server",
		},
		{
			"client-op",
			"client",
		},
	}

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)
    mockSvc.EXPECT().ListQueryExecutions(gomock.Any(), gomock.Any()).
		Return(&athena.ListQueryExecutionsOutput{}, nil)

	mockQueryRunAndResult(mockSvc, results)

	reader := NewTestReader(ctx, assert, mockSvc)

	operations, err := reader.GetOperations(ctx, spanstore.OperationQueryParameters{ServiceName: "test", SpanKind: ""})

	assert.NoError(err)
	assert.Equal([]spanstore.Operation{
		{
			Name:     "server-op",
			SpanKind: "server",
		},
		{
			Name:     "client-op",
			SpanKind: "client",
		},
	}, operations)
}
