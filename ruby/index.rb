require_relative './dynamodb_service'

ddbsvc = DynamoDBService.new
result = ddbsvc.get_overdue_jobs(9)
s = result[:schedules].first
s = ddbsvc.update_status(s, 'SCHEDULED', 'ACQUIRED')
ddbsvc.delete_dispatched_jobs(s)

