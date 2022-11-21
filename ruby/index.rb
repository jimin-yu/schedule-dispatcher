# require_relative './dynamodb_service'


# ddbsvc = DynamoDBService.new
# result = ddbsvc.get_overdue_jobs(1)
# s = result[:schedules].first
# s = ddbsvc.update_status(s, 'SCHEDULED', 'ACQUIRED')
# p s.job_spec['queueName']

# require_relative './worker'

# worker = Worker.new([0,1,2,3,4,5,6,7,8,9])
# worker.scan_group


require_relative './worker'
require_relative './dynamodb_service'
require_relative './metrics'

partitions = [0,1,2,3,4,5,6,7,8,9]
worker = Worker.new(partitions)
worker.scan_group







