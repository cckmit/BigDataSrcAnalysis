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

"FIFOScheduler-inter": Flink程序执行入口;
Job.run()-> interpreter.interpret(script, context); -> FlinkInterpreter.interpret()



    "flink-rest-server-netty-worker-thread-n":  NioEventLoop.processSelectedKey() -> AbstractChannelHandlerContext.fireChannelRead()-> LeaderRetrievalHandler.channelRead0() -> 
        -> JobSubmitHandler.handleRequest() -> gateway.submitJob() -> 进入另一线程
            => "flink-akka.actor.default-dispatcher-15"线程:  Dispatcher.submitJob() -> internalSubmitJob()
                Dispatcher.waitForTerminatingJobManager()
                => "flink-akka.actor.default-dispatcher-642"线程: action.apply(jobGraph) -> Dispatcher.persistAndRunJob() -> runJob()

   
 -----------------



//线程: flink-akka.actor.default-dispatcher-15: 
JobMaster.createScheduler() -> SerializedValue.deserializeValue()
    
        deserializeObject:569, InstantiationUtil (org.apache.flink.util)
        deserializeObject:562, InstantiationUtil (org.apache.flink.util)
        deserializeObject:550, InstantiationUtil (org.apache.flink.util)
        deserializeValue:58, SerializedValue (org.apache.flink.util)
        createInstance:75, DefaultSchedulerFactory (org.apache.flink.runtime.scheduler)
        createScheduler:278, JobMaster (org.apache.flink.runtime.jobmaster)
        <init>:266, JobMaster (org.apache.flink.runtime.jobmaster)
        createJobMasterService:98, DefaultJobMasterServiceFactory (org.apache.flink.runtime.jobmaster.factories)
        createJobMasterService:40, DefaultJobMasterServiceFactory (org.apache.flink.runtime.jobmaster.factories)
        <init>:146, JobManagerRunnerImpl (org.apache.flink.runtime.jobmaster)
        createJobManagerRunner:84, DefaultJobManagerRunnerFactory (org.apache.flink.runtime.dispatcher)
        lambda$createJobManagerRunner$6:381, Dispatcher (org.apache.flink.runtime.dispatcher)
        get:-1, 99880939 (org.apache.flink.runtime.dispatcher.Dispatcher$$Lambda$421)
        lambda$unchecked$0:34, CheckedSupplier (org.apache.flink.util.function)
        get:-1, 1200094012 (org.apache.flink.util.function.CheckedSupplier$$Lambda$391)
        run:1604, CompletableFuture$AsyncSupply (java.util.concurrent)
        run:40, TaskInvocation (akka.dispatch)
        exec:44, ForkJoinExecutorConfigurator$AkkaForkJoinTask (akka.dispatch)
        doExec:260, ForkJoinTask (akka.dispatch.forkjoin)
        runTask:1339, ForkJoinPool$WorkQueue (akka.dispatch.forkjoin)
        runWorker:1979, ForkJoinPool (akka.dispatch.forkjoin)
        run:107, ForkJoinWorkerThread (akka.dispatch.forkjoin)

        // 2. TaskExecutor.submitTask()
        deserializeObject:569, InstantiationUtil (org.apache.flink.util)
        deserializeObject:562, InstantiationUtil (org.apache.flink.util)
        deserializeObject:550, InstantiationUtil (org.apache.flink.util)
        deserializeValue:58, SerializedValue (org.apache.flink.util)
        submitTask:506, TaskExecutor (org.apache.flink.runtime.taskexecutor)
        invoke0:-1, NativeMethodAccessorImpl (sun.reflect)
        invoke:62, NativeMethodAccessorImpl (sun.reflect)
        invoke:43, DelegatingMethodAccessorImpl (sun.reflect)
        invoke:498, Method (java.lang.reflect)
        handleRpcInvocation:284, AkkaRpcActor (org.apache.flink.runtime.rpc.akka)
        handleRpcMessage:199, AkkaRpcActor (org.apache.flink.runtime.rpc.akka)
        handleMessage:152, AkkaRpcActor (org.apache.flink.runtime.rpc.akka)

        
// Reduce(sum()** in group Flink Task Thread
	at org.apache.flink.util.InstantiationUtil.deserializeObject(InstantiationUtil.java:576)
	at org.apache.flink.util.InstantiationUtil.deserializeObject(InstantiationUtil.java:562)
	at org.apache.flink.util.InstantiationUtil.deserializeObject(InstantiationUtil.java:550)
	at org.apache.flink.util.InstantiationUtil.readObjectFromConfig(InstantiationUtil.java:511)
	at org.apache.flink.runtime.operators.util.TaskConfig.getStubWrapper(TaskConfig.java:288)
	at org.apache.flink.runtime.operators.BatchTask.initStub(BatchTask.java:646)
	at org.apache.flink.runtime.operators.BatchTask.initInputLocalStrategy(BatchTask.java:966)
	at org.apache.flink.runtime.operators.BatchTask.initLocalStrategies(BatchTask.java:804)
	at org.apache.flink.runtime.operators.BatchTask.invoke(BatchTask.java:345)
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:708)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:533)   
   
   
   
  "FlinkCompletableFutureDelayScheduler-thread-1" #103 daemon prio=5 os_prio=0 tid=0x00007f02f00ca800 nid=0xba9 waiting on condition [0x00007f02ac2f2000]
   java.lang.Thread.State: TIMED_WAITING (parking)
	at sun.misc.Unsafe.park(Native Method)
 
Task.run()
- : Thread[CHAIN DataSource (
- : Reduce(sum()** in group Flink Task Thread
- : DataSink (collect()) (1/1)