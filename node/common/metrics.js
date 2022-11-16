import chalk from 'chalk'

const logFormat = (funcName, elapsedTime, message) => {
  const colors = {
    getOverdueJobs: chalk.bold.red,
    dispatchOverdue: chalk.bold.blue,
    scanGroup: chalk.bold.yellow
  }
  return `${colors[funcName](funcName)} `+ 
  chalk.bgGreen.bold(`${elapsedTime}ms`) +
  `: ${message}`
}

export default class Metrics{
  // ddb_service metric
  getOverdueJobs(elapsedTime, partition){
    const message = logFormat('getOverdueJobs', elapsedTime, `retrieve overdue schedules from partition ${partition}`)
    console.log(message)
  }

  // worker metric
  dispatchOverdue(elapsedTime, partition, count){
    const message = logFormat('dispatchOverdue', elapsedTime, `fetch ${count} schedule(s) and dispatch to destination from partition ${partition}`)
    console.log(message)
  }

  scanGroup(elapsedTime, partitions, scanImmediately){
    const message = logFormat('scanGroup', elapsedTime, `iterate all ${partitions.length} partitions once. start next scan immediately = ${scanImmediately}`)
    console.log(message)
  }
}