function getRandomInt(max) {
  return Math.floor(Math.random() * max);
}

function sample(array){
  return array[Math.floor(Math.random() * array.length)]
}

function makeSampleJobPayload(){
  const queues = ['dealibird-retail-bill', 'abara-join', 'wms-stock-reconciliation', 'send-email']
  const jobs = ['createRetailBill', 'joinUsers', 'stockCheck', 'sendEmail']
  const params = [['2022-10', 'jjmmyyou111'], [], ['nosnos'], ['matrix'], ['2022-11', 'emily'], ['2022-11', 'james']]
  return {
    queueName: sample(queues),
    jobClass: sample(jobs),
    jobParams: sample(params)
  }
}

function sleep(delay) {
  var start = new Date().getTime();
  while (new Date().getTime() < start + delay);
}

function createArrayMap(size){
  return new Map([...Array(size).keys()].map(x => [x, []]));
}

export { getRandomInt, makeSampleJobPayload, sleep, createArrayMap };