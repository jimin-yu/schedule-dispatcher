require 'colorize'

require 'set'

class Metrics
  def initialize(partitions)
    @empty = false
    @start_time = Time.now
    @partition_set = partitions.to_set
  end

  def time_to_str(elapsed_time)
    "#{(elapsed_time * 1000).to_i}ms"
  end

  def get_overdue_jobs(elapsed_time, partition)
    puts "#{"[get overdue jobs]".blue} #{time_to_str(elapsed_time).on_green} partition = #{partition}"
  end

  def dispatch_overdue(elapsed_time, partition, count)
    table_empty(partition) if !@empty && count == 0
    puts "#{"[dispatch overdue]".red} #{time_to_str(elapsed_time).on_green} get #{count} schedule(s) from partition #{partition}"
  end

  def scan_group(elapsed_time, scan_immediately)
    puts "#{"[scan group]".yellow} #{time_to_str(elapsed_time).on_green} scan_immediate = #{scan_immediately}"
  end

  def table_empty(partition)
    puts "#{partition} EMPTY!!!! #{@partition_set.size}".on_blue
    @partition_set.delete(partition)
    if @partition_set.size == 0
      @empty = true
      puts "#{"[empty table]".light_magenta.on_white} #{(time_to_str(Time.now - @start_time)).on_green} table is empty"
    end
  end
end

