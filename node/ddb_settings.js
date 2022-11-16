import DynamoDBService from "./services/dynamodb_service.js";

const ddbService = new DynamoDBService()

function createSampleSchedules(tableName, itemCount){
  Array(itemCount)
  .fill(0)
  .forEach(async ()=>await ddbService.addJob(tableName))
}

async function clearTableAndSeed(tableName, itemCount){
  if (await ddbService.isTableExists(tableName)){
    await ddbService.deleteTable(tableName)
  }
  await ddbService.createTable(tableName)
  createSampleSchedules(tableName, itemCount)
}

clearTableAndSeed('deali_schedules', 50)



