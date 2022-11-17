from print_color import print

class Metrics:
  def get_overdue_jobs(self, elapsed_time, partition):
    message = f'{int(elapsed_time)}ms : retrieve overdue schedules from partition {partition}'
    print(message, tag='get overdue jobs', tag_color='red', color='white')

  def dispatch_overdue(self, elapsed_time, partition, count):
    message = f'{int(elapsed_time)}ms : fetch {count} schedules(s) and dispatch to destination from partition {partition}'
    print(message, tag=f'dispatch overdue', tag_color='blue', color='white')

  def scan_group(self, elapsed_time, partitions, scan_immediately):
    message = f'{int(elapsed_time)}ms : iterate all {len(partitions)} partitions once. start next scan immediately = {scan_immediately}'
    print(message, tag='scan group', tag_color='yellow', color='white')
  
  def opening_session(self, elapsed_time):
    message = f'{int(elapsed_time)}ms : ddb session open'
    print(message, tag='open session', tag_color='magenta', color='white')