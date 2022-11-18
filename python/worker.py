from utils import *
import asyncio
from metrics import Metrics
from dynamodb_service import DynamoDBService
import time


class Worker:
  def __init__(self, partitions):
    self.partitions = partitions
    self.metrics = Metrics(time.time()*1000, partitions)
    self.ddb_service = DynamoDBService(self.metrics)
    self.scan_times = dict()
  
  async def start(self):
    # sys.setrecursionlimit(10000)
    now = time.time() * 1000
    for partition in self.partitions:
      self.scan_times[partition] = now

    async with self.ddb_service.ddb_client as ddb_client:
      self.table = await ddb_client.Table(self.ddb_service.table_name)
      await self.scan_group(self.partitions)
  
  async def scan_group(self, partitions):
    start = time.time() * 1000
    no_delay = False
    
    # iterate partitions
    for partition in partitions:
      now = time.time() * 1000
      if(self.scan_times[partition] <= now):
        schedule_immediate = await self.dispatch_overdue(partition)
        next_scan = time.time() * 1000 if schedule_immediate else (time.time() + 1) * 1000
        self.scan_times[partition] = next_scan
        no_delay = no_delay or schedule_immediate
      else:
        no_delay = False
    self.metrics.scan_group(time.time()*1000-start, partitions, no_delay)

    # next scan
    if not no_delay:
      await asyncio.sleep(1)
    else:
      await asyncio.sleep(0)     # maximum recursion depth exceeded 에러 때문에 추가
    await self.scan_group(partitions)

    
  async def dispatch_overdue(self, partition) -> bool:
    start = time.time() * 1000
    try:
      schedule_query_response = await self.ddb_service.get_overdue_jobs(self.table, partition)
      schedules = schedule_query_response.schedules
      async with asyncio.TaskGroup() as tg:
        for schedule in schedules:
          tg.create_task(self.after_dispatch(schedule))
      print(f'fetch done from partition {partition}')
      self.metrics.dispatch_overdue(time.time()*1000-start, partition, len(schedules))
      return schedule_query_response.should_immediately_query_again
    except Exception as err:
      print(err)
      return True


  async def after_dispatch(self, schedule):
    schedule = await self.ddb_service.update_status(self.table, schedule, 'SCHEDULED', 'ACQUIRED')
    await self.dispatch_to_destination(schedule)


  async def dispatch_to_destination(self, schedule):
    message = f'''
== PUSH JOB TO DESTINATION QUEUE ==
QUEUE: {schedule.job_spec['queueName']}
JOB: {schedule.job_spec['jobClass']}
PARAMS: {schedule.job_spec['jobParams']}
'''
    print(message)
    await self.ddb_service.delete_dispatched_job(self.table, schedule)


async def test():
  await Worker([1,2,3,4,5,6,7,8]).start()


if __name__ == "__main__":
  asyncio.run(test())