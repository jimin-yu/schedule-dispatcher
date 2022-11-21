require 'concurrent'
require 'aws-sdk-dynamodb'
require 'attr_extras'

class Schedule
  aattr_initialize [:shard_id!, :date_token!, :job_status!, :job_spec!]

  def self.decode_to_schedule(ddb_item)
    return self.new(
      shard_id: ddb_item['shard_id'],
      date_token: ddb_item['date_token'],
      job_status: ddb_item['job_status'],
      job_spec: JSON.parse(ddb_item['job_spec'])
    )
  end
end

class DynamoDBService
  include Concurrent::Promises::FactoryMethods

  def initialize(metrics)
    @metrics = metrics
    @dynamodb = Aws::DynamoDB::Client.new(endpoint: 'http://localhost:8000')
    @table_name = 'deali_schedules'
    @query_limit = 5
  end
  
  # return value : future<hash>
  def get_overdue_jobs(partition)
    start = Time.now
    now = (start.to_f * 1000).to_i
    return future (partition) do |partition|
      res = @dynamodb.query({
        table_name: @table_name,
        key_condition_expression: 'shard_id = :shardId AND date_token < :dateToken',
        filter_expression: 'job_status = :jobStatus',
        expression_attribute_values: {
          ':shardId': partition.to_s,
          ':dateToken': now.to_s,
          ':jobStatus': 'SCHEDULED'
        },
        limit: @query_limit
      })
      @metrics.get_overdue_jobs(Time.now-start, partition)
      {
        schedules: res.items.map { |item| Schedule.decode_to_schedule(item) },
        should_immediately_query_again: res.count == @query_limit || !!res.last_evaluated_key
      }
    end
  end

  
  # return value : future<Schedule>
  def update_status(schedule, old_status, new_status)
    return future(schedule, old_status, new_status) do |schedule, old_status, new_status|
      res = @dynamodb.update_item({
        table_name: @table_name,
        key: {
          shard_id: schedule.shard_id,
          date_token: schedule.date_token
        },
        condition_expression: 'job_status = :oldStatus',
        expression_attribute_values: {
          ':oldStatus': old_status,
          ':newStatus': new_status,
        },
        update_expression: 'SET job_status = :newStatus',
        return_values: 'UPDATED_NEW'
      })
      res
    end
    .then do |res|
      schedule.job_status = res.attributes['job_status']
      schedule
    end
  end

  # return value : future<nil>
  def delete_dispatched_jobs(schedule)
    return future(schedule) do |schedule|
      @dynamodb.delete_item({
        table_name: @table_name,
        key: {
          shard_id: schedule.shard_id,
          date_token: schedule.date_token
        },
        condition_expression: 'job_status = :acquired',
        expression_attribute_values: {
          ':acquired': 'ACQUIRED'
        }
      })
    end
  end
end
