import DynamoDBService from "./services/dynamodb_service.js";

const ddbService = DynamoDBService()

function createSampleSchedules(itemCount){
  Array(itemCount)
  .fill(0)
  .forEach(async ()=>await ddbService.addJob())
}


// clear table


// create sample schedules
createSampleSchedules(50)

