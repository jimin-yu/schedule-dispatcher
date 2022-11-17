from print_color import print
import time

class Metrics:
  def __init__(self, start_time, partitions):
    self.empty = False
    self.start_time = start_time
    self.partition_set = set(partitions)

  def get_overdue_jobs(self, elapsed_time, partition):
    message = f'{int(elapsed_time)}ms : retrieve overdue schedules from partition {partition}'
    print(message, tag='get overdue jobs', tag_color='red', color='white')

  def dispatch_overdue(self, elapsed_time, partition, count):
    if(not self.empty and count == 0):
      self.table_empty(partition)
    message = f'{int(elapsed_time)}ms : fetch {count} schedules(s) and dispatch to destination from partition {partition}'
    print(message, tag=f'dispatch overdue', tag_color='magenta', color='white')

  def scan_group(self, elapsed_time, partitions, scan_immediately):
    message = f'{int(elapsed_time)}ms : iterate all {len(partitions)} partitions once. start next scan immediately = {scan_immediately}'
    print(message, tag='scan group', tag_color='blue', color='white')
  
  def table_empty(self, partition):
    self.partition_set.discard(partition)
    if(len(self.partition_set)==0):
      self.empty = True
      message = f'{int(time.time()*1000-self.start_time)}ms : dispatch done'
      print(message, tag='table empty', tag_color='yellow', color='white')
  
