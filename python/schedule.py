class Schedule:
  def __init__(self, **kwargs):
    self.shard_id = kwargs['shard_id']
    self.date_token = kwargs['date_token']
    self.job_status = kwargs['job_status']
    self.job_spec = kwargs['job_spec']
