from utils import *
from common import Schedule, ScheduleQueryResponse
from metrics import Metrics
import json
import asyncio
import aioboto3
from boto3.dynamodb.conditions import Key

class DynamoDBService:
  def __init__(self):
    self.metrics = Metrics()
    self.shard_number = 10
    self.query_limit = 5
    self.table_name = 'deali_schedules'
  
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
    start = time.time() * 1000
    now = str(start)
    async with self._get_dynamodb_session() as ddb_client:
      table = await ddb_client.Table(self.table_name)

      response = await table.query(
        KeyConditionExpression=Key('shard_id').eq(str(partition)) & Key('date_token').lt(now),
        FilterExpression=Key('job_status').eq('SCHEDULED'),
        Limit=self.query_limit
      )

      self.metrics.get_overdue_jobs(time.time() * 1000 - start, partition)
      return ScheduleQueryResponse(
        [Schedule.decode_to_schedule(ddb_item) for ddb_item in response['Items']],
        response['Count'] == self.query_limit or 'LastEvaluatedKey' in response
      )

  async def update_status(self, schedule: Schedule, old_status: str, new_status: str) -> Schedule:
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
        },
        ReturnValues='UPDATED_NEW'
      )
      updated_status = response['Attributes']['job_status']
      schedule.job_status = updated_status
      return schedule

  async def delete_dispatched_job(self, schedule: Schedule) -> None:
    async with self._get_dynamodb_session() as ddb_client:
      table = await ddb_client.Table(self.table_name)
      
      await table.delete_item(
        Key={
          'shard_id': str(schedule.shard_id), 
          'date_token': schedule.date_token
        },
        ConditionExpression='job_status = :acquired',
        ExpressionAttributeValues={
          ':acquired': 'ACQUIRED'
        }
      )


async def test():
  ddb_service = DynamoDBService()
  item_count = 50
  async with asyncio.TaskGroup() as tg:
    for i in range(item_count):
      tg.create_task(ddb_service.add_job())
  # schedule = make_sample_schedule(5)
  # schedule.job_status = 'ACQUIRED'
  # print(str(schedule))


  # await ddb_service.add_job()
  # result = await ddb_service.get_overdue_jobs(4)
  # schedule = result.schedules[0]
  # await ddb_service.delete_dispatched_job(schedule)


if __name__ == "__main__":
  asyncio.run(test())
