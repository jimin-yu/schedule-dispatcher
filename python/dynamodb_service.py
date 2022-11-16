from utils import *
from common import Schedule, ScheduleQueryResponse
import json
import asyncio
import aioboto3
from boto3.dynamodb.conditions import Key

class DynamoDBService:
  def __init__(self):
    self.shard_number = 10
    self.query_limit = 5
    self.table_name = 'deali_schedules_python'
  
  def _get_dynamodb_session(self):
    session = aioboto3.Session()
    return session.resource('dynamodb', endpoint_url='http://localhost:8000')

  async def add_job(self) -> None:
    schedule = make_sample_schedule(self.shard_number-1)

    async with self._get_dynamodb_session() as ddb_client:
      table = await ddb_client.Table(self.table_name)

      await table.put_item(
        Item={
          'shard_id': str(schedule.shard_id),
          'date_token': schedule.date_token,
          'job_status': schedule.job_status,
          'job_spec': json.dumps(schedule.job_spec)
        }
      )

  async def get_overdue_jobs(self, partition: int) -> ScheduleQueryResponse:
    now = str(time.time() * 1000)
    async with self._get_dynamodb_session() as ddb_client:
      table = await ddb_client.Table(self.table_name)

      response = await table.query(
        KeyConditionExpression=Key('shard_id').eq(str(partition)) & Key('date_token').lt(now),
        FilterExpression=Key('job_status').eq('SCHEDULED'),
        Limit=self.query_limit
      )
    
      return ScheduleQueryResponse(
        [Schedule.decode_to_schedule(ddb_item) for ddb_item in response['Items']],
        response['Count'] == self.query_limit or 'LastEvaluatedKey' in response
      )

  async def update_status(self, schedule, old_status, new_status):
    async with self._get_dynamodb_session() as ddb_client:
      table = await ddb_client.Table(self.table_name)

      response = await table.update_item(
        Key={
          'shard_id': str(schedule.shard_id), 
          'date_token': schedule.date_token
        },
        ConditionExpression='job_status = :oldStatus',
        UpdateExpression='SET job_status = :newStatus',
        ExpressionAttributeValues={
          ':oldStatus': old_status,
          ':newStatus': new_status
        }
      )
      print(response)

  async def delete_dispatched_job(self, schedulde):
    pass





async def test():
  ddb_service = DynamoDBService()
  result = await ddb_service.get_overdue_jobs(8)
  schedule = result.schedules[0]
  await ddb_service.update_status(schedule, 'SCHEDULED', 'ACQUIRED')


if __name__ == "__main__":
  asyncio.run(test())
