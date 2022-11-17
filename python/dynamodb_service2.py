# boto3
from metrics import Metrics
from common import Schedule, ScheduleQueryResponse
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, wait
import boto3
from boto3.dynamodb.conditions import Key

class DynamoDBService2:
  def __init__(self):
    self.executor = ThreadPoolExecutor(
        max_workers=10,
    )
    self.metrics = Metrics()
    self.shard_number = 10
    self.query_limit = 5
    self.table_name = 'deali_schedules'
    self.client = boto3.client('dynamodb', endpoint_url='http://localhost:8000')

  def _query_args(self, partition, timestamp, job_status):
    return {
      'TableName': self.table_name,
      'KeyConditionExpression': 'shard_id = :shardId and date_token < :dateToken',
      'FilterExpression': 'job_status = :jobStatus',
      'ExpressionAttributeValues': {
        ':shardId': {'S': str(partition)},
        ':dateToken' : {'S': str(timestamp)},
        ':jobStatus' : {'S': job_status}
      },
      'Limit': self.query_limit
    }


  def get_overdue_jobs(self, partition):
    def callback(_fut):
      response = _fut.result()
      return ScheduleQueryResponse(
        [Schedule.decode_to_schedule(ddb_item) for ddb_item in response['Items']],
        response['Count'] == self.query_limit or 'LastEvaluatedKey' in response
      )

    now = time.time() * 1000
    # response = self.client.query(**self._query_args(partition, now, 'SCHEDULED'))
    future = self.executor.submit(self.client.query, **self._query_args(partition, now, 'SCHEDULED'))
    future.add_done_callback(callback)
    return future

  def update_status(self, schedule, old_status, new_status):
    pass

  def delete_dispatched_job(self, schedule):
    pass
  
async def test():
  ddb_service = DynamoDBService2()
  f = ddb_service.get_overdue_jobs(1)
  r = wait([f])
  print('--', r, '--')

if __name__ == "__main__":
  asyncio.run(test())