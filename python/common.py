import json
class Schedule:
	def __init__(self, **kwargs):
		self.shard_id = kwargs['shard_id']
		self.date_token = kwargs['date_token']
		self.job_status = kwargs['job_status']
		self.job_spec = kwargs['job_spec']
	
	def __str__(self):
		return f'''
shard_id={self.shard_id}
date_token={self.date_token}
job_status={self.job_status}
job_spec={self.job_spec}
		'''

	@classmethod
	def decode_to_schedule(cls, ddb_item):
		return cls(
			shard_id=int(ddb_item['shard_id']),
			date_token=ddb_item['date_token'],
			job_status=ddb_item['job_status'],
			job_spec=json.loads(ddb_item['job_spec'])
		)


class ScheduleQueryResponse:
	def __init__(self, schedules: list[Schedule], should_immediately_query_again: bool):
		self.schedules = schedules
		self.should_immediately_query_again = should_immediately_query_again
