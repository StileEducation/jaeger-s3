package config

type Firehose struct {
	SpansDeliveryStreamName      string
	OperationsDeliveryStreamName string
}

type Athena struct {
	DatabaseName         string
	SpansTableName       string
	OperationsTableName  string
	WorkGroup            string
	OutputLocation       string
	MaxSpanAge           string
	DependenciesQueryTTL string
	ServicesQueryTTL     string
	MaxTraceDuration     string
	DependenciesPrefetch bool
}

type Configuration struct {
	Firehose Firehose
	Athena   Athena
}
