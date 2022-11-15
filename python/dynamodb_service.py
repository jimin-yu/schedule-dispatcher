from utils import *
from schedule import Schedule
import json
import asyncio
import aioboto3

# from boto3.dynamodb.conditions import Key


async def main():
    session = aioboto3.Session()
    async with session.resource('dynamodb', region_name='eu-central-1') as dynamo_resource:
        table = await dynamo_resource.Table('test_table')

        await table.put_item(
            Item={'pk': 'test1', 'col1': 'some_data'}
        )

        result = await table.query(
            KeyConditionExpression=Key('pk').eq('test1')
        )

        # Example batch write
        more_items = [{'pk': 't2', 'col1': 'c1'}, \
                      {'pk': 't3', 'col1': 'c3'}]
        async with table.batch_writer() as batch:
            for item_ in more_items:
                await batch.put_item(Item=item_)

# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())

# Outputs:
#  [{'col1': 'some_data', 'pk': 'test1'}]


class DynamoDBService:
	def __init__(self):
		self.shard_number = 10
		self.query_limit = 5
		self.table_name = 'deali_schedules_python'
		self.session = aioboto3.Session()
		self.ddb_client = self.session.resource('dynamodb', endpoint_url='http://localhost:8000')

	def _make_put_item_args(self, schedule: Schedule) -> dict:
		return {
			'TableName': self.table_name,
			'Item': {
				'shard_id': {'S': schedule.shard_id},
				'date_token': {'S': schedule.date_token},
				'job_status': {'S': schedule.job_status},
				'job_spec': {'S':schedule.job_spec}
			}
		}

	async def add_job(self):
		schedule = make_sample_schedule(self.shard_number-1)

		async with self.ddb_client as ddb_client:
			table = await ddb_client.Table(self.table_name)

			await table.put_item(
				Item={
					'shard_id': str(schedule.shard_id),
					'date_token': schedule.date_token,
					'job_status': schedule.job_status,
					'job_spec': json.dumps(schedule.job_spec)
				}
			)
	
if __name__ == "__main__":
	ddb_service = DynamoDBService()
	asyncio.run(ddb_service.add_job())
