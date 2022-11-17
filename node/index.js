import Worker from  "./job_dispatcher/worker.js";

async function main(){
  const partitions = [0,1,2,3,4,5,6,7,8,9] 
  const worker = new Worker(partitions).start();
}

main()
