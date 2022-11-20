require_relative './dynamodb_service'

class Worker
  def initialize(partitions)
    @partitions = partitions
    @ddbsvc = DynamoDBService.new
    @scan_times = Hash.new
    current_time = Time.now
    @partitions.each do |partition|
      @scan_times[partition] = current_time
    end
  end

  def dispatch_to_destination(schedule)
    msg = 
    p "== PUSH JOB TO DESTINATION QUEUE =="
    p "QUEUE: #{schedule.job_spec['queueName']}"
    p "JOB: #{schedule.job_spec['jobClass']}"
    p "QUEUE: #{schedule.job_spec['jobParams']}"
    @ddbsvc.delete_dispatched_jobs(schedule)
  end

  def dispatch_overdue(partition)
    query_result = @ddbsvc.get_overdue_jobs(partition)
    query_result[:schedules].each do |schedule|
      schedule = @ddbsvc.update_status(schedule, 'SCHEDULED', 'ACQUIRED')
      dispatch_to_destination(schedule)
    end
    query_result[:should_immediately_query_again]
  end

  def scan_group
    p 'start scanning...'
    no_delay = false
    @partitions.each do |partition|
      now = Time.now
      if @scan_times[partition] <= now
        this_no_delay = dispatch_overdue(partition)
        next_scan = this_no_delay ? Time.now : Time.now + 1
        @scan_times[partition] = next_scan
      else
        no_delay = false
      end
    end

    sleep(1) unless no_delay
    scan_group
  end
end