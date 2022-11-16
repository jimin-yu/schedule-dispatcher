import { 
  DynamoDBClient, 
  PutItemCommand, 
  UpdateItemCommand,
  DeleteItemCommand, 
  QueryCommand,
  ListTablesCommand,
  DeleteTableCommand,
  CreateTableCommand 
} from "@aws-sdk/client-dynamodb";
import { getRandomInt, makeSampleJobPayload } from '../common/utils.js'
import Schedule from '../common/schedule.js'
import { v4 as uuidv4 } from 'uuid';

class DynamoDBService {
  constructor(metrics){
    this.maxShardId = 10
    this.queryLimit = 5
    this.tableName = 'deali_schedules'
    this.client = new DynamoDBClient({
      endpoint: 'http://localhost:8000'
    });
    this.metrics = metrics
  }

  makePutItemCommand(tableName, schedule){
    const putItemRequest = {
      TableName: tableName,
      Item: {
        shard_id: {S: schedule.shardId},
        date_token: {S: schedule.dateToken},
        job_status: {S: schedule.jobStatus},
        job_spec: {S: JSON.stringify(schedule.jobSpec)}
      }
    }
    return new PutItemCommand(putItemRequest)
  }

  makeOverdueJobsQueryCommand(partition, timestamp, jobStatus){
    const input = {
      TableName: this.tableName,
      KeyConditionExpression: 'shard_id = :shardId and date_token < :dateToken',
      FilterExpression: 'job_status = :jobStatus',
      ExpressionAttributeValues: {
        ':shardId': {S: partition.toString()},
        ':dateToken' : {S: timestamp.toString()},
        ':jobStatus' : {S: jobStatus}
      },
      Limit: this.queryLimit
    }
    return new QueryCommand(input)
  }

  makeUpdateItemCommand(schedule, oldStatus, newStatus){
    const updateItemRequest = {
      TableName: this.tableName,
      Key: {
        shard_id: {S: schedule.shardId},
        date_token: {S: schedule.dateToken}
      },
      ConditionExpression: 'job_status = :oldStatus',
      ExpressionAttributeValues: {
        ':oldStatus': {S: oldStatus},
        ':newStatus': {S: newStatus},
      },
      UpdateExpression: 'SET job_status = :newStatus',
      ReturnValues: 'UPDATED_NEW'
    }
    return new UpdateItemCommand(updateItemRequest);
  }

  makeDeleteItemCommand(schedule){
    const deleteItemRequest = {
      TableName: this.tableName,
      Key: {
        shard_id: {S: schedule.shardId},
        date_token: {S: schedule.dateToken}
      },
      ConditionExpression: 'job_status = :acquired',
      ExpressionAttributeValues: {
        ':acquired': {S: 'ACQUIRED'}
      }
    }
    return new DeleteItemCommand(deleteItemRequest)
  }

  decodeSchedule(ddbItem){
    const shardId = ddbItem.shard_id.S
    const dateToken = ddbItem.date_token.S
    const jobStatus = ddbItem.job_status.S
    const jobSpec = JSON.parse(ddbItem.job_spec.S)
    return new Schedule(shardId, dateToken, jobStatus, jobSpec)
  }

  // 임시 - 테스트를 위한 random item put
  async addJob(tableName){
    const sampleSchedule = new Schedule(
      getRandomInt(this.maxShardId),
      `${Date.now()}#${uuidv4()}`,
      'SCHEDULED',
      makeSampleJobPayload()
    )
    const command = this.makePutItemCommand(tableName, sampleSchedule);
    return this.client
    .send(command)
    .then(()=>sampleSchedule);
  }

  async getOverdueJobs(partition){
    const start = Date.now()
    const command = this.makeOverdueJobsQueryCommand(partition, Date.now(), 'SCHEDULED');
    return this.client
    .send(command)
    .then((response)=>{
      this.metrics.getOverdueJobs(Date.now()-start, partition)
      return {
        schedules: response.Items.map(this.decodeSchedule),
        shouldImmediatelyQueryAgain: response.Count == this.queryLimit || !!response.LastEvaluatedKey
      }
    })
  }

  async updateStatus(schedule, oldStatus, newStatus){
    const command = this.makeUpdateItemCommand(schedule, oldStatus, newStatus)
    return this.client
    .send(command)
    .then((response)=>{
      const updatedStatus = response.Attributes.job_status.S
      schedule.jobStatus = updatedStatus
      return schedule
    })
  }

  async deleteDispatchedJob(schedule){
    const command = this.makeDeleteItemCommand(schedule)
    return this.client
    .send(command)
  }

  async isTableExists(tableName){
    const command = new ListTablesCommand({})
    const response = await this.client.send(command)
    return response['TableNames'].includes(tableName)
  }

  async deleteTable(tableName){
    const command = new DeleteTableCommand({TableName: tableName})
    return this.client.send(command)
  }

  async createTable(tableName){
    const command = new CreateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [
        {
          AttributeName: 'shard_id',
          AttributeType: 'S'
        },
        {
          AttributeName: 'date_token',
          AttributeType: 'S'
        }
      ],
      KeySchema: [
        {
          AttributeName: 'shard_id',
          KeyType: 'HASH'
        },
        {
          AttributeName: 'date_token',
          KeyType: 'RANGE'
        }
      ],
      BillingMode: 'PAY_PER_REQUEST'
    })
    return this.client.send(command)
  }
}

export default DynamoDBService