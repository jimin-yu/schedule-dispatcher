import chalk from 'chalk'

const logFormat = (funcName, elapsedTime, message) => {
  const colors = {
    getOverdueJobs: chalk.bold.red,
    dispatchOverdue: chalk.bold.blue,
    scanGroup: chalk.bold.yellow,
    tableEmpty: chalk.bold.magenta
  }
  return `${colors[funcName](funcName)} `+ 
  chalk.bgGreen.bold(`${elapsedTime}ms`) +
  `: ${message}`
}

export default class Metrics{
  constructor(startTime, partitions){
    this.empty = false
    this.startTime = startTime
    this.partitionSet = new Set(partitions)
  }
  // ddb_service metric
  getOverdueJobs(elapsedTime, partition){
    const message = logFormat('getOverdueJobs', elapsedTime, `retrieve overdue schedules from partition ${partition}`)
    console.log(message)
  }

  // worker metric
  dispatchOverdue(elapsedTime, partition, count){
    if(!this.empty && count==0){
      this.tableEmpty(partition)
    }
    const message = logFormat('dispatchOverdue', elapsedTime, `fetch ${count} schedule(s) and dispatch to destination from partition ${partition}`)
    console.log(message)
  }

  scanGroup(elapsedTime, partitions, scanImmediately){
    const message = logFormat('scanGroup', elapsedTime, `iterate all ${partitions.length} partitions once. start next scan immediately = ${scanImmediately}`)
    console.log(message)
  }

  // 최초로 테이블이 빌 때까지의 시간 측정
  tableEmpty(partition){
    this.partitionSet.delete(partition)
    if (this.partitionSet.size==0){
      this.empty = true
      const message = logFormat('tableEmpty', Date.now()-this.startTime, 'dispatch done')
      console.log(message)
    }
  }
}