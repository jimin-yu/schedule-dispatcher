import Worker from  "./job_dispatcher/worker.js";

async function main(){
  const partitions = [1,2,3]
  const worker = new Worker;
  worker.start(partitions)
}

main()
