import asyncio
import concurrent.futures
from worker import Worker

def cpu_bound():
    # CPU-bound operations will block the event loop:
    # in general it is preferable to run them in a
    # process pool.
  return sum(i * i for i in range(10 ** 7))

class WorkerManager:
  def __init__(self, partitions):
    self.partitions = partitions
    
  async def start(self):
    self.loop = asyncio.get_running_loop()
    print('---')
    print(self.loop)
    # self.loop.run_forever()
    await self.recreate_workers()

  async def recreate_workers(self):
    with concurrent.futures.ThreadPoolExecutor() as executor:
      worker = Worker(self.partitions)
      self.loop.run_until_complete(executor, worker.start)

      # executor.submit(worker.start)


if __name__ == "__main__":
  wm = WorkerManager([0,1,2,3,4,5,6,7,8,9])
  # wm.recreate_workers()
  asyncio.run(wm.start())

# import asyncio
# import concurrent.futures

# def blocking_io():
#   pass
#     # File operations (such as logging) can block the
#     # event loop: run them in a thread pool.
#     # with open('/dev/urandom', 'rb') as f:
#     #     return f.read(100)



# async def main():
#   loop = asyncio.get_running_loop()

#   ## Options:

#   # 1. Run in the default loop's executor:
#   result = await loop.run_in_executor(
#     None, blocking_io)
#   print('default thread pool', result)

#   # 2. Run in a custom thread pool:
#   with concurrent.futures.ThreadPoolExecutor() as pool:
#     result = await loop.run_in_executor(
#       pool, blocking_io)
#     print('custom thread pool', result)

#   # 3. Run in a custom process pool:
#   with concurrent.futures.ProcessPoolExecutor() as pool:
#     result = await loop.run_in_executor(
#       pool, cpu_bound)
#     print('custom process pool', result)

# if __name__ == '__main__':
#   asyncio.run(main())