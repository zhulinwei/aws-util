# aws-util

aws相关的工具脚本

## s3 

### 场景一

输出包含name为tony的记录
```$xslt
const (
	token  = ""
	region = "us-west-2"
	appId  = "123456789"
	secret = "123456789"
	bucket = "tony_bucket"
	prefix = "year=2020/month=01"
	querySQL = "select * from S3Object where S3Object.name='tony'"
)

util := BuildS3Util(S3Config{
	token:  token,
	appId:  appId,
	secret: secret,
	region: region,
})

util.Records(bucket, prefix, querySQL)
```

### 场景二

统计包含name为tony的数量
```$xslt
const (
	token  = ""
	region = "us-west-2"
	appId  = "123456789"
	secret = "123456789"
	bucket = "tony_bucket"
	prefix = "year=2020/month=01"
	querySQL = "select * from S3Object where S3Object.name='tony'"
)

util := BuildS3Util(S3Config{
	token:  token,
	appId:  appId,
	secret: secret,
	region: region,
})

util.Count(bucket, prefix, querySQL)
```

### 场景三

删除包含name为tony的文件
```$xslt
const (
	token  = ""
	region = "us-west-2"
	appId  = "123456789"
	secret = "123456789"
	bucket = "tony_bucket"
	prefix = "year=2020/month=01"
	querySQL = "select * from S3Object where S3Object.name='tony'"
)

util := BuildS3Util(S3Config{
	token:  token,
	appId:  appId,
	secret: secret,
	region: region,
})

util.Remove(bucket, prefix, querySQL)
```

## 更新日志

### 20200603
新增 s3 关于统计条数功能

### 20200604
改良 s3 关于统计条数功能为并发统计

### 20200605
修复 s3 数据获取不全问题
