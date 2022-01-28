package config

type S3 struct {
	BucketName string
	Prefix     string
}

type Athena struct {
	DatabaseName   string
	TableName      string
	OutputLocation string
}

type Configuration struct {
	S3     S3
	Athena Athena
}
