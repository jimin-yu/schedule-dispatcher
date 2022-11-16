import DynamoDBService from "../services/dynamodb_service.js"
import Metrics from "../common/metrics.js";

class Worker{
  constructor(){
    this.metrics = new Metrics
    this.ddbService = new DynamoDBService(this.metrics);
    this.scanTimes = new Map;
  }
  // 서비스 SQS에 넣기
  async dispatchToDestination(schedule){
    const message = `
    == PUSH JOB TO DESTINATION QUEUE ==
    QUEUE: ${schedule.jobSpec.queueName}
    JOB: ${schedule.jobSpec.jobClass}
    PARAMS: ${schedule.jobSpec.jobParams}

    `
    console.log(message);
    return this.ddbService.deleteDispatchedJob(schedule);
  }

  async dispatchOverdue(partition){
    const start = Date.now()
    return new Promise((resolve, reject)=>{
      this.ddbService
      .getOverdueJobs(partition)
      .then(({schedules, shouldImmediatelyQueryAgain})=>{
        const promises = schedules.map(async (schedule)=>{
          return this.ddbService
          .updateStatus(schedule, 'SCHEDULED', 'ACQUIRED')
          .then(schedule => this.dispatchToDestination.call(this, schedule))
        })
        Promise.all(promises)
        .then(()=>{
          console.log("all schedule dispatch promise done..")
          this.metrics.dispatchOverdue(Date.now()-start, partition, schedules.length)
          resolve(shouldImmediatelyQueryAgain);
        })
      })
      .catch((err)=>{
        console.log(err);
        resolve(true);
      })
    })
  }

  async scanGroup(partitions){
    const start = Date.now()
    let noDelay = false;

    for(const partition of partitions){
      const now = Date.now()

      if(this.scanTimes.get(partition) <= now){
        const thisNoDelay = 
        await this.dispatchOverdue(partition)
        .then(scheduleImmediate=>{
          const nextScan = scheduleImmediate ? Date.now() : Date.now() + 1000
          this.scanTimes.set(partition, nextScan)
          return scheduleImmediate
        })
        noDelay = noDelay || thisNoDelay
      }else{
        noDelay = true;
      }
    }
    // TODO : post PartitionWorkerIterationEvent event
    this.metrics.scanGroup(Date.now()-start, partitions, noDelay)
    if(noDelay){
      // this.scanGroup(partitions) => stack overflow...
      setTimeout(()=>this.scanGroup(partitions), 0)
    }else{
      setTimeout(()=>this.scanGroup(partitions), 1000)
    }
  }

  // start worker thread (entry point)
  start(partitions){
    const currentTimestamp = Date.now()
    partitions.forEach(partition=>{
      this.scanTimes.set(partition, currentTimestamp)
    })
    this.scanGroup(partitions)
  }
}

export default Worker
