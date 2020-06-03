package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	bucket   = "<your_s3_bucket>"
	prefix   = "<your_s3_bucket_prefix>"
	querySQL = "<your_query_sql_condition>"
)

type S3SelectUtil struct {
	token  string
	appId  string
	secret string
	region string

	service *s3.S3
}

func BuildS3SelectUtil() *S3SelectUtil {
	return &S3SelectUtil{
		token:  "<your_aws_token>",
		region: "<your_s3_region>",
		appId:  "<your_aws_appId>",
		secret: "<your_aws_secret>",
	}
}

func (util *S3SelectUtil) init() {
	cred := credentials.NewStaticCredentials(util.appId, util.secret, util.token)

	awsSession, err := session.NewSession(&aws.Config{Credentials: cred, Region: aws.String(util.region)})
	if err != nil {
		fmt.Println("new session cause some thing wrong", err.Error())
		return
	}

	util.service = s3.New(awsSession)
}

func (util *S3SelectUtil) queryObjectList(bucket, prefix string) *s3.ListObjectsOutput {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(30)*time.Second)
	defer cancel()

	output, err := util.service.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		fmt.Println("new session cause some thing wrong", err.Error())
		return nil
	}

	return output
}

func (util *S3SelectUtil) buildSelectParams(bucket, key, sql string) *s3.SelectObjectContentInput {
	return &s3.SelectObjectContentInput{
		Bucket:              aws.String(bucket),
		Key:                 aws.String(key),
		Expression:          aws.String(fmt.Sprintf(sql)),
		ExpressionType:      aws.String("SQL"),
		OutputSerialization: &s3.OutputSerialization{JSON: &s3.JSONOutput{RecordDelimiter: aws.String("\n")}},
		InputSerialization:  &s3.InputSerialization{CompressionType: aws.String("GZIP"), JSON: &s3.JSONInput{Type: aws.String("LINES")}},
	}
}

func (util *S3SelectUtil) count(params *s3.SelectObjectContentInput) int {
	output, err := util.service.SelectObjectContent(params)
	if err != nil {
		fmt.Println("select object cause some thing wrong", err.Error())
		return 0
	}

	defer func() {
		if err := output.EventStream.Close(); err != nil {
			fmt.Println("close output event stream cause some thing wrong", err.Error())
			return
		}
	}()

	reader, writer := io.Pipe()
	go func() {
		defer func() {
			if err := writer.Close(); err != nil {
				fmt.Println("close writer cause some thing wrong", err.Error())
				return
			}
		}()
		for event := range output.EventStream.Events() {
			switch e := event.(type) {
			case *s3.RecordsEvent:
				_, err := writer.Write(e.Payload)
				if err != nil {
					fmt.Println("event write cause some thing wrong", err.Error())
					return
				}
			}
		}
	}()

	var record map[string]interface{}
	var records []map[string]interface{}
	resReader := json.NewDecoder(reader)
	for {
		err := resReader.Decode(&record)
		if err == io.EOF {
			break
		}
		records = append(records, record)
	}

	return len(records)
}

func (util *S3SelectUtil) Execute() {
	util.init()

	total := 0
	for _, content := range util.queryObjectList(bucket, prefix).Contents {
		param := util.buildSelectParams(bucket, *content.Key, querySQL)
		count := util.count(param)
		total += count
		fmt.Printf("file: %s contains %d target\n", *content.Key, count)
	}
	fmt.Println("total:", total)
}

func main() {
	BuildS3SelectUtil().Execute()
}
