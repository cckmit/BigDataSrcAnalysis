
*. 调度线程: 
    - "flink-scheduler-1"
    - "FlinkCompletableFutureDelayScheduler-thread-1"
    
1. netty 通信框架: SelectorImpl.select(), NioEventLoop.run()
    - "flink-rest-server-netty-worker-thread-n":  SingleThreadEventExecutor.run() -> SelectedSelectionKeySetSelector.select();
    - "flink-rest-server-netty-boss-thread-1": 
    - "flink-rest-client-netty-thread-n"
    - "Flink Netty Server (0) Thread 0": 通信启动服务?

    

2. dispatcher: 消息转发: Dispatcher, 
	- "flink-akka.actor.default-dispatcher-15"
 	- "Flink-DispatcherRestEndpoint-thread-n"

    

3. metric监控:
    - "flink-metrics-akka.remote.default-remote-dispatcher-13"线程:
    - "flink-metrics-akka.remote.default-remote-dispatcher-4" 
    - "flink-metrics-scheduler-n": ThreadRenamingRunnable.run() -> NioEventLoop.select() -> SelectedSelectionKeySetSelector.select()
    - "flink-metrics-13"
    - "Flink-MetricRegistry-thread-1" : 注册Metric?


"Reduce (SUM(1)) (1/1)" #534 prio=5 os_prio=0 tid=0x00007f02e4370800 nid=0xdf1 runnable [0x00007f02ac7f1000]
   java.lang.Thread.State: RUNNABLE    
   
// Client?
- "Flink-RestClusterClient-IO-thread-n"

4. 其他?
- "jobmanager-future-thread-2"
- "IOManager reader thread #1"
- "IOManager writer thread #1"
- "Timer-n" ?



Flink的组件和概念:

TaskSlot: TaskSlot是Flink中最小的资源管理单元，它仅均分TaskManager上的内存，但不隔离CPU
    - 每个TaskManager上面有3个TaskSlot，那么意味着每个TaskSlot有三分之一TaskManage内存
    - Flink的Job逻辑会被分为一个一个的Task，在资源分配上，Flink保证Task的每个并行度一定有个TaskSlot使用


//main线程 -> flink-akka.actor.default-dispatcher-3 线程
env.executor() -> MiniCluster.submitJob(): 远程Rpc调用,并返回结果 
    // flink-akka.actor.default-dispatcher-3 线程 -> flink-akka.actor.default-dispatcher-2 线程
    Dispatcher.submitJob() -> Dispatcher.runJob(jobGraph): 
    
        // flink-akka.actor.default-dispatcher-2 线程 ->  mini-cluster-io-thread-2 线程: 
        runJob(jobGraph) -> JobManagerRunnerImpl.verifyJobSchedulingStatusAndStartJobManager()
            
            // mini-cluster-io-thread-2 线程:  -> flink-akka.actor.default-dispatcher-3
            verifyJobSchedulingStatusAndStartJobManager -> JobMaster.notifyOfNewResourceManagerLeader
            
                // flink-akka.actor.default-dispatcher-3线程  -> "flink-akka.actor.default-dispatcher-5"
                notifyOfNewResourceManagerLeader() -> ResourceManager.requestSlot()
                
                    
                    // "flink-akka.actor.default-dispatcher-5" -> 远程线程;Rpc调用 TaskExecutor.requestSlot()
                    resourceManagerGateway.requestSlot() -> TaskExecutor.requestSlot() 
                    
                    //? 如何 从 TaskExecutor.requestSlot 到Task.run() ?
                    
                        // TaskManager进程中单个Task的执行;
                        Task.run() -> StreamTask.invoke(){
                            beforeInvoke();
                            runMailboxLoop();
                            afterInvoke();
                        }
                    


//main线程 -> flink-akka.actor.default-dispatcher-3 线程
env.executor() -> MiniCluster.submitJob(): 远程Rpc调用,并返回结果 
    // flink-akka.actor.default-dispatcher-3 线程 -> flink-akka.actor.default-dispatcher-2 线程
    Dispatcher.submitJob() -> Dispatcher.runJob(jobGraph): 
        // flink-akka.actor.default-dispatcher-2 线程 ->  mini-cluster-io-thread-2 线程: 
        runJob(jobGraph) -> JobManagerRunnerImpl.verifyJobSchedulingStatusAndStartJobManager()
            // mini-cluster-io-thread-2 线程:  -> flink-akka.actor.default-dispatcher-3
            verifyJobSchedulingStatusAndStartJobManager -> JobMaster.notifyOfNewResourceManagerLeader
                // flink-akka.actor.default-dispatcher-3线程  -> "flink-akka.actor.default-dispatcher-5"
                notifyOfNewResourceManagerLeader() -> ResourceManager.requestSlot()
                    // "flink-akka.actor.default-dispatcher-5" -> 远程线程;Rpc调用 TaskExecutor.requestSlot()
                    resourceManagerGateway.requestSlot() -> TaskExecutor.requestSlot() 
                    //? 如何 从 TaskExecutor.requestSlot 到Task.run() ?
                        // TaskManager进程中单个Task的执行;
                        Task.run() -> StreamTask.invoke(){
                            beforeInvoke();
                            runMailboxLoop();
                            afterInvoke();
                        }
                    

                    
    

"main"线程重要方法:

// 1. 根据env 判断并判断是创建Local, StreamPlan还是StreamContext的执行环境;
StreamExecutionEnvironment.createStreamExecutionEnvironment(){ 
    if (env instanceof ContextEnvironment) {
        return new StreamContextEnvironment((ContextEnvironment) env);
    } else if (env instanceof OptimizerPlanEnvironment) {
        return new StreamPlanEnvironment(env);
    } else {
        return createLocalEnvironment();
    }
}









FlinkStreaming 执行入口: StreamExecutionEnvironment.execute(jobName) 

StreamExecutionEnvironment.execute(jobName){
    StreamGraph streamGraph= getStreamGraph(jobName)
    
    return execute(streamGraph);{
        // 主要步骤1: 获取或创建?执行环境
        final JobClient jobClient = executeAsync(streamGraph);
        
        // 重要步骤2: 执行job并等待结果?
        if (configuration.getBoolean(DeploymentOptions.ATTACHED)) { //local模式, 这里等于true, LocalStreamEnvironment.validateAndGetConfiguration()中设定的;
            jobExecutionResult = jobClient.getJobExecutionResult(userClassloader).get();
            
        } else {
            jobExecutionResult = new DetachedJobExecutionResult(jobClient.getJobID());
        }

        jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
        return jobExecutionResult;
    }
}




















# 1.2 Flink Local 源码详解: 向前追溯;



// "flink-akka.actor.default-dispatcher-2" 线程:  这段代码的功能,好像就是创建一个 JobManagerLeaderListener,并添加到 listeners的Set中;

    - 触发线程和方法: "flink-akka.actor.default-dispatcher-5"  线程;
        - ResourceManager.requestSlot() -> 
            -> SlotManagerImpl.internalRequestSlot() -> allocateSlot()
                -> gateway.requestSlot() : 远程Rpc调用 TaskExecutor.requestSlot()

TaskExecutor.requestSlot(){
    if (jobManagerTable.contains(jobId)) {
        offerSlotsToJobManager(jobId);
    }else{ //第一次进入这里: 
        jobLeaderService.addJob(jobId, targetAddress);{
            JobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = new JobManagerLeaderListener(jobId);
            leaderRetrievalService.start(jobManagerLeaderListener);{//EmbeddedLeaderService.start()
                // Set<EmbeddedLeaderRetrievalService> listeners: 该Set()中存多个 Service? 
                if (!listeners.add(service)) throw new IllegalStateException
                
            }
        }
    }
    return CompletableFuture.completedFuture(Acknowledge.get());
}















    
