
from worker import Worker
import asyncio

async def main():
  await Worker([0,1,2,3,4,5,6,7,8,9]).start()

asyncio.run(main())
  