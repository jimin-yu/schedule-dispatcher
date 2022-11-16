from utils import *
import asyncio
import sys

from dynamodb_service import DynamoDBService
import time


class Worker:
  def __init__(self):
    self.ddb_service = DynamoDBService()
    self.scan_times = dict()
  
  async def start(self, partitions):
    # sys.setrecursionlimit(10000)
    now = time.time() * 1000
    for partition in partitions:
      self.scan_times[partition] = now
    await self.scan_group(partitions)
  
  async def scan_group(self, partitions):
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
        no_delay = True
    
    # next scan
    if not no_delay:
      await asyncio.sleep(1)
    else:
      await asyncio.sleep(0)     # maximum recursion depth exceeded 에러 때문에 추가
    await self.scan_group(partitions)

    
  async def dispatch_overdue(self, partition) -> bool:
    try:
      schedule_query_response = await self.ddb_service.get_overdue_jobs(partition)
      schedules = schedule_query_response.schedules
      async with asyncio.TaskGroup() as tg:
        for schedule in schedules:
          tg.create_task(self.after_dispatch(schedule))
      print(f'fetch done from partition {partition}')
      return schedule_query_response.should_immediately_query_again
    except Exception as err:
      print(err)
      return True


  async def after_dispatch(self, schedule):
    schedule = await self.ddb_service.update_status(schedule, 'SCHEDULED', 'ACQUIRED')
    await self.dispatch_to_destination(schedule)


  async def dispatch_to_destination(self, schedule):
    message = f'''
== PUSH JOB TO DESTINATION QUEUE ==
QUEUE: {schedule.job_spec['queueName']}
JOB: {schedule.job_spec['jobClass']}
PARAMS: {schedule.job_spec['jobParams']}
'''
    print(message)
    await self.ddb_service.delete_dispatched_job(schedule)


async def test():
  worker = Worker()
  await worker.start([1,2,3,4,5,6,7,8])


if __name__ == "__main__":
  asyncio.run(test())