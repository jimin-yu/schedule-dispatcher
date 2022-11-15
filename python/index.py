
from dynamodb_service import DynamoDBService
import asyncio

ddb_service = DynamoDBService()
loop = asyncio.get_event_loop()
loop.run_until_complete(ddb_service.add_job())

# def main():
#   print("entry point")

# main()
