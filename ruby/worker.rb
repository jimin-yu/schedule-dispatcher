require_relative './dynamodb_service'
require_relative './metrics'

class Worker
  include Concurrent::Promises::FactoryMethods

  def initialize(partitions)
    @metrics = Metrics.new(partitions)
    @partitions = partitions
    @ddbsvc = DynamoDBService.new(@metrics)
    @scan_times = Hash.new
    current_time = Time.now
    @partitions.each do |partition|
      @scan_times[partition] = current_time
    end
  end

  # return value : future<nil>
  def dispatch_to_destination(schedule)
    msg = 
    puts "== PUSH JOB TO DESTINATION QUEUE =="
    puts "QUEUE: #{schedule.job_spec['queueName']}"
    puts "JOB: #{schedule.job_spec['jobClass']}"
    puts "QUEUE: #{schedule.job_spec['jobParams']}"
    puts ''
    @ddbsvc.delete_dispatched_jobs(schedule)
  end

  # return value : future<bool>
  def dispatch_overdue(partition)
    start = Time.now
    done = resolvable_future
    @ddbsvc.get_overdue_jobs(partition)
    .then (done) do |query_result, done|
      tasks = query_result[:schedules].map do |schedule|
        @ddbsvc.update_status(schedule, 'SCHEDULED', 'ACQUIRED')
        .then {|schedule| dispatch_to_destination(schedule)}
      end
      zip(*tasks).value!
      @metrics.dispatch_overdue(Time.now-start, partition, query_result[:schedules].count)
      done.fulfill query_result[:should_immediately_query_again]
    end
    return done
  end

  def scan_group
    start = Time.now
    puts 'start scanning...'
    no_delay = false
    @partitions.each do |partition|
      now = Time.now
      if @scan_times[partition] <= now
        this_no_delay = dispatch_overdue(partition)
        .then do |scanImmediate|
          next_scan = scanImmediate ? Time.now : Time.now + 1
          @scan_times[partition] = next_scan
          scanImmediate
        end.value!
        no_delay ||= this_no_delay
      else
        no_delay = false
      end
    end

    @metrics.scan_group(Time.now-start, no_delay)
    sleep(1) unless no_delay
    scan_group
  end
end