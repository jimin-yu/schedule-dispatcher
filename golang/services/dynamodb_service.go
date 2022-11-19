package ddbsvc

import (
	"fmt"
	"strconv"
	"time"

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

// These init() functions can be used within a package block
// and regardless of how many times that package is imported,
// the init() function will only be called once.
func init() {
	fmt.Println("!!!! init ddb service !!!!!")
	dynamo = connectDynamo()
}

func connectDynamo() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:   aws.String("ap-northeast-2"),
		Endpoint: aws.String("http://localhost:8000"),
	}))
	svc := dynamodb.New(sess)
	return svc
}

func makeOverdueQueryExpression(partition int, timestamp int64, jobStatus string) *dynamodb.QueryInput {
	keyCond1 := expression.Key("shard_id").Equal(expression.Value(strconv.Itoa(partition)))
	keyCond2 := expression.Key("date_token").LessThan(expression.Value(strconv.FormatInt(timestamp, 10)))
	filt := expression.Name("job_status").Equal(expression.Value(jobStatus))
	expr, _ := expression.NewBuilder().
		WithKeyCondition(keyCond1.And(keyCond2)).
		WithFilter(filt).
		Build()

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(TABLE_NAME),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Limit:                     aws.Int64(QUERY_LIMIT),
	}
	return input
}

func makeUpdateItemInput(schedule Schedule, oldStatus string, newStatus string) *dynamodb.UpdateItemInput {
	update := expression.Set(expression.Name("job_status"), expression.Value(newStatus))
	cond := expression.Name("job_status").Equal(expression.Value(oldStatus))
	expr, _ := expression.NewBuilder().WithCondition(cond).WithUpdate(update).Build()

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(TABLE_NAME),
		Key: map[string]*dynamodb.AttributeValue{
			"shard_id": {
				S: aws.String(schedule.ShardId),
			},
			"date_token": {
				S: aws.String(schedule.DateToken),
			},
		},
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              aws.String("ALL_NEW"),
	}
	return input
}

func makeDeleteItemInput(schedule Schedule) *dynamodb.DeleteItemInput {
	cond := expression.Name("job_status").Equal(expression.Value("ACQUIRED"))
	expr, _ := expression.NewBuilder().WithCondition(cond).Build()
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(TABLE_NAME),
		Key: map[string]*dynamodb.AttributeValue{
			"shard_id": {
				S: aws.String(schedule.ShardId),
			},
			"date_token": {
				S: aws.String(schedule.DateToken),
			},
		},
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConditionExpression:       expr.Condition(),
	}
	return input
}

func GetOverdueJobs(partition int) ScheduleQueryResponse {
	now := time.Now().UnixMilli()
	input := makeOverdueQueryExpression(partition, now, "SCHEDULED")
	res, err := dynamo.Query(input)
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

func UpdateStatus(schedule Schedule, oldStatus string, newStatus string) Schedule {
	input := makeUpdateItemInput(schedule, oldStatus, newStatus)
	res, err := dynamo.UpdateItem(input)
	if err != nil {
		fmt.Println(err)
	}
	dynamodbattribute.UnmarshalMap(res.Attributes, &schedule)
	return schedule
}

func DeleteDispatchedJob(schedule Schedule) {
	input := makeDeleteItemInput(schedule)
	_, err := dynamo.DeleteItem(input)
	if err != nil {
		fmt.Println(err)
	}
}
