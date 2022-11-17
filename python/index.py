
from worker import Worker
import asyncio

async def main():
  worker = Worker()
  await worker.start([1,2,3,4,5,6,7,8])

asyncio.run(main())
  