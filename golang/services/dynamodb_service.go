package ddbsvc

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

var dynamo *dynamodb.DynamoDB

const TABLE_NAME = "deali_schedules"
const QUERY_LIMIT = 5

type ScheduleQueryResponse struct {
	Schedules                   []Schedule
	ShouldImmediatelyQueryAgian bool
}

type Schedule struct {
	ShardId   string `json:"shard_id"`
	DateToken string `json:"date_token"`
	JobStatus string `json:"job_status"`
	JobSpec   string `json:"job_spec"`
}

// func decodeSchedule(ddbItem map[string]*dynamodb.AttributeValue) {
// 	dynamo.Handlers.UnmarshalMeta.Swap(ddbItem)
// 	shard_id := ddbItem.S
// 	fmt.Println(shard_id)

// 	// return schedule{shardId: shard_id}
// }

func connectDynamo() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:   aws.String("ap-northeast-2"),
		Endpoint: aws.String("http://localhost:8000"),
	}))
	svc := dynamodb.New(sess)
	return svc
}

func overdueQueryExpression(partition int, timestamp int64, jobStatus string) expression.Expression {
	keyCond1 := expression.Key("shard_id").Equal(expression.Value(strconv.Itoa(partition)))
	keyCond2 := expression.Key("date_token").LessThan(expression.Value(strconv.FormatInt(timestamp, 10)))
	filt := expression.Name("job_status").Equal(expression.Value(jobStatus))
	expr, _ := expression.NewBuilder().
		WithKeyCondition(keyCond1.And(keyCond2)).
		WithFilter(filt).
		Build()
	return expr
}

func GetOverdueJobs(partition int, timestamp int64, jobStatus string) ScheduleQueryResponse {
	expr := overdueQueryExpression(partition, timestamp, jobStatus)
	res, err := dynamo.Query(&dynamodb.QueryInput{
		TableName:                 aws.String(TABLE_NAME),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Limit:                     aws.Int64(QUERY_LIMIT),
	})
	if err != nil {
		fmt.Println(err)
	}

	var schedules []Schedule
	dynamodbattribute.UnmarshalListOfMaps(res.Items, &schedules)
	return ScheduleQueryResponse{
		Schedules:                   schedules,
		ShouldImmediatelyQueryAgian: *res.Count == QUERY_LIMIT || res.LastEvaluatedKey != nil,
	}
}

// These init() functions can be used within a package block
// and regardless of how many times that package is imported,
// the init() function will only be called once.
func init() {
	dynamo = connectDynamo()
}
