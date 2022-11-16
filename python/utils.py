import random
import time
import uuid
from common import Schedule

def make_sample_schedule(max_shard_id):
  sample = Schedule(
    shard_id = random.randint(0, max_shard_id-1),
    date_token = f'{time.time() * 1000}#{uuid.uuid4()}',
    job_status = 'SCHEDULED',
    job_spec = make_sample_job_payload()
  )
  return sample

def make_sample_job_payload():
  queues = ['dealibird-retail-bill', 'abara-join', 'wms-stock-reconciliation', 'send-email']
  jobs = ['createRetailBill', 'joinUsers', 'stockCheck', 'sendEmail']
  params = [['2022-10', 'jjmmyyou111'], [], ['nosnos'], ['matrix'], ['2022-11', 'emily'], ['2022-11', 'james']]
  return {
    'queueName': random.sample(queues, 1),
    'jobClass': random.sample(jobs, 1),
    'jobParams': random.sample(params, 1)
  }

if __name__ == "__main__":
    sample_schedule = make_sample_schedule(5)
    print(str(sample_schedule))