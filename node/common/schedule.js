export default class Schedule{
  constructor(shardId, dateToken, jobStatus, jobSpec){
    this.shardId = shardId
    this.dateToken = dateToken
    this.jobStatus = jobStatus,
    this.jobSpec = jobSpec
  }
}