package s3spanstore

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
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

// mockQueryRunAndResultCapturing behaves like mockQueryRunAndResult but records the
// SQL of every issued query into the supplied slice in call order.
func mockQueryRunAndResultCapturing(mockSvc *mocks.MockAthenaAPI, capture *[]string, result [][]string) {
	queryID := "queryId"
	now := time.Now()

	mockSvc.EXPECT().StartQueryExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error) {
			*capture = append(*capture, *in.QueryString)
			return &athena.StartQueryExecutionOutput{QueryExecutionId: &queryID}, nil
		})
	mockSvc.EXPECT().GetQueryExecution(gomock.Any(), gomock.Any()).
		Return(&athena.GetQueryExecutionOutput{
			QueryExecution: &types.QueryExecution{
				Status: &types.QueryExecutionStatus{CompletionDateTime: &now},
			},
		}, nil)
	mockSvc.EXPECT().GetQueryResults(gomock.Any(), gomock.Any()).
		Return(&athena.GetQueryResultsOutput{ResultSet: toAthenaResultSet(result)}, nil)
}

func encodedTestSpan(t *testing.T) (model.TraceID, string) {
	t.Helper()
	span := &model.Span{
		TraceID:       model.NewTraceID(0, 0xaf0d7a36b1a8eff0),
		SpanID:        model.NewSpanID(1),
		OperationName: "test-op",
		StartTime:     time.Unix(0, 0).UTC(),
	}
	payload, err := EncodeSpanPayload(span)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return span.TraceID, payload
}

func TestGetTrace_FirstTierHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	assert := assert.New(t)
	ctx := context.TODO()

	traceID, payload := encodedTestSpan(t)

	mockSvc := mocks.NewMockAthenaAPI(ctrl)

	captured := []string{}
	mockQueryRunAndResultCapturing(mockSvc, &captured, [][]string{{payload}})

	reader := NewTestReader(ctx, assert, mockSvc)
	trace, err := reader.GetTrace(ctx, traceID)

	assert.NoError(err)
	assert.Len(trace.Spans, 1)
	assert.Len(captured, 1, "should stop after the first tier hits")
}

func TestGetTrace_FallsThroughTiers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	assert := assert.New(t)
	ctx := context.TODO()

	traceID, payload := encodedTestSpan(t)

	mockSvc := mocks.NewMockAthenaAPI(ctrl)

	captured := []string{}
	// Empty result at 6h, empty at 48h, hit at maxSpanAge.
	// gomock plays sequential EXPECTs in call order.
	mockQueryRunAndResultCapturing(mockSvc, &captured, nil)
	mockQueryRunAndResultCapturing(mockSvc, &captured, nil)
	mockQueryRunAndResultCapturing(mockSvc, &captured, [][]string{{payload}})

	reader := NewTestReader(ctx, assert, mockSvc)
	trace, err := reader.GetTrace(ctx, traceID)

	assert.NoError(err)
	assert.Len(trace.Spans, 1)
	assert.Len(captured, 3, "should try each tier in turn")

	// Successive tiers should scan strictly wider datehour ranges. The
	// lower bound moves earlier each time; upper bound stays roughly now.
	extractLower := func(sql string) string {
		i := strings.Index(sql, "BETWEEN '")
		if i < 0 {
			t.Fatalf("no BETWEEN clause in %q", sql)
		}
		rest := sql[i+len("BETWEEN '"):]
		j := strings.Index(rest, "'")
		return rest[:j]
	}
	l0, l1, l2 := extractLower(captured[0]), extractLower(captured[1]), extractLower(captured[2])
	assert.True(l0 > l1, "6h lower bound (%s) should be later than 48h lower bound (%s)", l0, l1)
	assert.True(l1 > l2, "48h lower bound (%s) should be later than maxSpanAge lower bound (%s)", l1, l2)
}

func TestGetTrace_MissAllTiers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	assert := assert.New(t)
	ctx := context.TODO()

	mockSvc := mocks.NewMockAthenaAPI(ctrl)

	captured := []string{}
	mockQueryRunAndResultCapturing(mockSvc, &captured, nil)
	mockQueryRunAndResultCapturing(mockSvc, &captured, nil)
	mockQueryRunAndResultCapturing(mockSvc, &captured, nil)

	reader := NewTestReader(ctx, assert, mockSvc)
	_, err := reader.GetTrace(ctx, model.NewTraceID(0, 0xdeadbeef))

	assert.ErrorIs(err, spanstore.ErrTraceNotFound)
	assert.Len(captured, 3)
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
