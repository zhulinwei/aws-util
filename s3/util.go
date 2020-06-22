package s3

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	emptyString = ""
)

type S3Util struct {
	token  string
	appId  string
	secret string
	region string

	service *s3.S3
}

type S3Config struct {
	token  string
	appId  string
	secret string
	region string
}

func BuildS3Util(s3Config S3Config) *S3Util {
	s3Util := &S3Util{
		token:  s3Config.token,
		appId:  s3Config.appId,
		secret: s3Config.secret,
		region: s3Config.region,
	}
	s3Util.init()

	return s3Util
}

func (util *S3Util) init() {
	awsConfig := &aws.Config{Region: aws.String(util.region)}

	if util.appId != emptyString && util.secret != emptyString {
		awsConfig.Credentials = credentials.NewStaticCredentials(util.appId, util.secret, util.token)
	}

	awsSession, err := session.NewSession(awsConfig)
	if err != nil {
		fmt.Println("new session cause some thing wrong", err.Error())
		return
	}

	util.service = s3.New(awsSession)
}

func (util *S3Util) queryObjectList(bucket, prefix string) *s3.ListObjectsOutput {
	inputParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	outputResult := &s3.ListObjectsOutput{}
	turnPageFn := func(output *s3.ListObjectsV2Output, lastPage bool) bool {
		outputResult.Contents = append(outputResult.Contents, output.Contents...)
		return !lastPage
	}
	if err := util.service.ListObjectsV2Pages(inputParams, turnPageFn); err != nil {
		fmt.Println("query object list cause some thing wrong", err.Error())
		return nil
	}

	return outputResult
}

func (util *S3Util) buildSelectParams(bucket, key, sql string) *s3.SelectObjectContentInput {
	return &s3.SelectObjectContentInput{
		Bucket:              aws.String(bucket),
		Key:                 aws.String(key),
		Expression:          aws.String(fmt.Sprintf(sql)),
		ExpressionType:      aws.String("SQL"),
		OutputSerialization: &s3.OutputSerialization{JSON: &s3.JSONOutput{RecordDelimiter: aws.String("\n")}},
		InputSerialization:  &s3.InputSerialization{CompressionType: aws.String("GZIP"), JSON: &s3.JSONInput{Type: aws.String("LINES")}},
	}
}

func (util *S3Util) parseRecordsFromContent(params *s3.SelectObjectContentInput) *[]map[string]interface{} {
	output, err := util.service.SelectObjectContent(params)
	if err != nil {
		fmt.Println("select object cause some thing wrong", err.Error())
		return nil
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

	return &records
}

func (util *S3Util) deleteFile(bucket, path string) {
	_, err := util.service.DeleteObject(&s3.DeleteObjectInput{
		Key:    aws.String(path),
		Bucket: aws.String(bucket),
	})
	if err != nil {
		fmt.Println("delte file cause some thing wrong", err.Error())
		return
	}
}

func (util *S3Util) Records(bucket, prefix, querySQL string) []map[string]interface{} {
	list := util.queryObjectList(bucket, prefix)

	records := make([]map[string]interface{}, 0, 1000)
	for _, content := range list.Contents {

		result := util.parseRecordsFromContent(util.buildSelectParams(bucket, *content.Key, querySQL))
		if *result != nil {
			records = append(records, *result...)
		}
	}

	return records
}

func (util *S3Util) Count(bucket, prefix, querySQL string) {
	var count int32
	var wg sync.WaitGroup

	list := util.queryObjectList(bucket, prefix)
	wg.Add(len(list.Contents))

	for _, content := range list.Contents {
		go func(content *s3.Object, wg *sync.WaitGroup) {
			defer wg.Done()

			records := util.parseRecordsFromContent(util.buildSelectParams(bucket, *content.Key, querySQL))
			if records != nil {
				atomic.AddInt32(&count, int32(len(*records)))
				fmt.Printf("file: %s contains %d target\n", *content.Key, len(*records))
			} else {
				fmt.Printf("file: %s contains 0 target\n", *content.Key)
			}
		}(content, &wg)
	}

	wg.Wait()
	fmt.Printf("sql: [%s] total [%d] files and contains [%d] target\n", querySQL, len(list.Contents), int(count))
}

func (util *S3Util) Remove(bucket, prefix, querySQL string) {
	var wg sync.WaitGroup

	list := util.queryObjectList(bucket, prefix)
	wg.Add(len(list.Contents))

	for _, content := range list.Contents {
		go func(content *s3.Object, wg *sync.WaitGroup) {
			defer wg.Done()

			records := util.parseRecordsFromContent(util.buildSelectParams(bucket, *content.Key, querySQL))
			if records != nil && len(*records) > 0 {
				fmt.Printf("start remove %s", *content.Key)
				util.deleteFile(bucket, *content.Key)
			}
		}(content, &wg)
	}

	wg.Wait()
}
