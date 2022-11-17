from print_color import print

class Metrics:
  def get_overdue_jobs(self, elapsed_time, partition):
    message = f'{int(elapsed_time)}ms : retrieve overdue schedules from partition {partition}'
    print(message, tag='get_overdue_jobs', tag_color='red', color='white')

  def dispatch_overdue(self, elapsed_time, partition, count):
    message = f'{int(elapsed_time)}ms : fetch {count} schedules(s) and dispatch to destination from partition {partition}'
    print(message, tag=f'dispatch_overdue', tag_color='blue', color='white')

  def scan_group(self, elapsed_time, partitions, scan_immediately):
    message = f'{int(elapsed_time)}ms : iterate all {len(partitions)} partitions once. start next scan immediately = {scan_immediately}'
    print(message, tag='scan_group', tag_color='yellow', color='white')