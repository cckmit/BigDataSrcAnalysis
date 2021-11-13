
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



// "main"线程: 
StreamExecutionEnvironment.execute(jobName){
    StreamGraph streamGraph= getStreamGraph(jobName)
    
    return execute(streamGraph);{
        // 主要步骤1: 获取或创建?执行环境
        final JobClient jobClient = executeAsync(streamGraph);{
            final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);
            CompletableFuture<? extends JobClient> jobClientFuture = executorFactory
                .getExecutor(configuration)
                .execute(streamGraph, configuration);{//LocalExecutor.execute()
                    final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig);
                    return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory).submitJob(jobGraph);{// PerJobMiniClusterFactory.submitJob()
                        MiniCluster miniCluster = miniClusterFactory.apply(miniClusterConfig);
                        miniCluster.start();
                        
                        return miniCluster
                            .submitJob(jobGraph){//MiniCluster.submitJob()
                                final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = getDispatcherGatewayFuture();
                                final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);
                                final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture
                                .thenCombine(dispatcherGatewayFuture,(Void ack, DispatcherGateway dispatcherGateway) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout)){
                                    dispatcherGateway.submitJob(): 发起远程Rpc请求: 实际执行 Dispatcher.submitJob()
                                    Dispatcher.submitJob(){ //远程Rpc调用,并返回结果;
                                        //代码详情如下:
                                    }
                                }
                                .thenCompose(Function.identity());
                                return acknowledgeCompletableFuture.thenApply((Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
                                
                            }
                            .thenApply(result -> new PerJobMiniClusterJobClient(result.getJobID(), miniCluster))
                            .whenComplete((ignored, throwable) -> {
                                if (throwable != null) {
                                    // We failed to create the JobClient and must shutdown to ensure cleanup.
                                    shutDownCluster(miniCluster);
                                }
                            });
                            
                    }
                }
            jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
        }
        
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
       

        
// flink-akka.actor.default-dispatcher-3 线程:
submitJob(){//Dispatcher.
    if (isDuplicateJob(jobGraph.getJobID())) {
        return FutureUtils.completedExceptionally(new DuplicateJobSubmissionException(jobGraph.getJobID()));
    }else if (isPartialResourceConfigured(jobGraph)) { //偏爱/倾向于 资源配置?
    }else{ //不是重复的: 正常进入这里;
        return internalSubmitJob(jobGraph);{//Dispatcher.
            final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob).thenApply(ignored -> Acknowledge.get());{
                Dispatcher.waitForTerminatingJobManager(){
                    jobManagerTerminationFuture.thenComposeAsync((ignored) -> {
                        jobManagerTerminationFutures.remove(jobId);
                        return action.apply(jobGraph);{//这里的action 即 persistAndRunJob()方法:{
                            persistAndRunJob(){// action = persistAndRunJob()
                                runJob(jobGraph); //代码细节详见下一条:
                                
                            }
                        }
                    }):
                }
            }
    
            return persistAndRunFuture.handleAsync((acknowledge, throwable) -> {});
        }
    }
}



// flink-akka.actor.default-dispatcher-2 线程: 
persistAndRunJob(){//Dispatcher.
    final CompletableFuture<Void> runJobFuture = runJob(jobGraph);{//Dispatcher
        final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);{
            final RpcService rpcService = getRpcService();
            return CompletableFuture.supplyAsync();
        }
        
        return jobManagerRunnerFuture
            .thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner(){
                // 上面的 CompletableFuture.supplyAsync(); 执行完后, 就触发该startJobManagerRunner()执行;
                Dispatcher.startJobManagerRunner();{
                    jobManagerRunner.getResultFuture().handleAsync(()->{});
                    
                    jobManagerRunner.start();{// JobManagerRunnerImpl.start()
                        leaderElectionService.start(this);{//EmbeddedLeaderService.EmbeddedLeaderElectionService
                            addContender(this, contender);{
                                if (!allLeaderContenders.add(service)) throw new IllegalStateException();
                                
                                updateLeader().whenComplete((aVoid, throwable) -> {fatalError(throwable);});{
                                    EmbeddedLeaderService.updateLeader(){//
                                        EmbeddedLeaderElectionService leaderService = allLeaderContenders.iterator().next();
                                        
                                        return execute(new GrantLeadershipCall(leaderService.contender, leaderSessionId, LOG));{
                                            return CompletableFuture.runAsync(runnable, notificationExecutor);{
                                                GrantLeadershipCall.run(){
                                                    contender.grantLeadership(leaderSessionId);{//JobManagerRunnerImpl.
                                                        leadershipOperation = leadershipOperation.thenCompose((ignored) -> {
                                                            return verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);{
                                                                //代码如下
                                                                final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();
                                                                return jobSchedulingStatusFuture.thenCompose(()->{
                                                                    return startJobMaster(leaderSessionId);
                                                                })
                                                            }
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }))
            .thenApply(FunctionUtils.nullFn())
            .whenCompleteAsync(
                (ignored, throwable) -> {
                    if (throwable != null) jobManagerRunnerFutures.remove(jobGraph.getJobID());
                },
                getMainThreadExecutor());
    
    }
    
    return runJobFuture.whenComplete(()->{jobGraphWriter.removeJobGraph(jobGraph.getJobID());});
}
        
        
// mini-cluster-io-thread-2 线程: 
verifyJobSchedulingStatusAndStartJobManager(){//JobManagerRunnerImpl.verifyJobSchedulingStatusAndStartJobManager()
    final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();
    return jobSchedulingStatusFuture.thenCompose(jobSchedulingStatus -> {
        if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
            return jobAlreadyDone();
        } else {
            return startJobMaster(leaderSessionId);{//JobManagerRunnerImpl.startJobMaster()
                runningJobsRegistry.setJobRunning(jobGraph.getJobID());
                startFuture = jobMasterService.start(new JobMasterId(leaderSessionId));{//JobMaster.start()
                    start();
                    
                    return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);{
                        // 中间一堆的装换;
                        
                        JobMaster.startJobExecution(){
                            startJobMasterServices();{
                                startHeartbeatServices();
                                slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
                                scheduler.start(getMainThreadExecutor());
                                reconnectToResourceManager(new FlinkException("Starting JobMaster component."));
                                
                                resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());{//
                                    EmbeddedLeaderService.EmbeddedLeaderRetrievalService.start(){
                                        addListener(this, listener);{//EmbeddedLeaderService.addListener()
                                            notifyListener(currentLeaderAddress, currentLeaderSessionId, listener);{//EmbeddedLeaderService.notifyListener()
                                                return CompletableFuture.runAsync(new NotifyOfLeaderCall(address, leaderSessionId, listener, LOG), notificationExecutor);{
                                                    NotifyOfLeaderCall.run(){
                                                        listener.notifyLeaderAddress(address, leaderSessionId);{
                                                            runAsync(() -> notifyOfNewResourceManagerLeader(){// 异步执行该 notifyOfNewResourceManagerLeader()方法;
                                                                ResourceManagerLeaderListener.notifyOfNewResourceManagerLeader(){
                                                                    resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
                                                                    reconnectToResourceManager(); // 源码详解下面;
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            resetAndStartScheduler();
                        }
                        
                    }
                    
                }
                
                return startFuture.thenAcceptAsync((Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader());
            }
        }
    });
}



// flink-akka.actor.default-dispatcher-3 线程:  这里的作用是 连接 RM 资源管理器?
JobMaster.ResourceManagerLeaderListener.notifyLeaderAddress(){
    runAsync(() -> notifyOfNewResourceManagerLeader(){//JobMaster.notifyOfNewResourceManagerLeader
        resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        reconnectToResourceManager(new FlinkException(String.format( resourceManagerAddress)));{
            closeResourceManagerConnection(cause);
            tryConnectToResourceManager();{//JobMaster.tryConnectToResourceManager()
                if (resourceManagerAddress != null) connectToResourceManager();{
                    resourceManagerConnection = new ResourceManagerConnection()
                    resourceManagerConnection.start();{//ResourceManagerConnection.start()
                        final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
                        
                        newRegistration.startRegistration();{
                            CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync((G rpcGateway) -> {
                                
                                // 这里异步线程,执行 register()方法, 并在 thenAcceptAsync()方法中定义注册成功后的动作: 
                                register(rpcGateway, 1);{//RetryingRegistration.register()
                                    CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);
                                    
                                    CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync(
                                        (RegistrationResponse result) -> {
                                            if (result instanceof RegistrationResponse.Success) {
                                                completionFuture.complete(Tuple2.of(gateway, success));{//CompletableFuture.complete()
                                                    // 下面调用框架来异步执行;
                                                    CompletableFuture.postComplete() -> tryFire() -> uniWhenComplete(){//CompletableFuture.uniWhenComplete()
                                                        c.claim(){
                                                            e.execute(this);{//ScheduledThreadPoolExecutor.execute
                                                                ScheduledThreadPoolExecutor.schedule();{
                                                                    new FutureTask().run() -> Executors.RunnableAdapter.call() -> Completion.run() -> UniWhenComplete.tryFire() -> CompletableFuture.uniWhenComplete(){
                                                                        // 这里执行 上面 RetryingRegistration.createNewRegistration() 方法中 future.whenCompleteAsync()中的方法体:
                                                                        RetryingRegistration.createNewRegistration() -> future.whenCompleteAsync(()->{
                                                                            
                                                                            AkkaRpcActor.handleMessage -> handleRpcMessage() -> handleRunAsync() => runAsync.getRunnable().run(){
                                                                                // 这里触发 ResourceManagerConnection.onRegistrationSuccess()
                                                                                onRegistrationSuccess(result.f1); //具体代码详解下面;
                                                                            }
                                                                        })
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            
                                        }
                                    );
                                }
                                
                            })
                        }
                    }
                }
            }
        }
    });
}




// flink-akka.actor.default-dispatcher-3 线程: RM注册成功后的操作: onRegistrationSuccess(), 即 requestSlot()申请Task插槽以运行Task ? 
AkkaRpcActor.handleMessage()-> handleRpcMessage(message);{
    if (expectedFencingToken == null){
        
    }else{
        super.handleRpcMessage(fencedMessage.getPayload());{//AkkaRpcActor.handleRpcMessage()
            if (message instanceof RunAsync) {
                handleRunAsync((RunAsync) message);{//AkkaRpcActor.handleRunAsync()
                    runAsync.getRunnable().run();{
                        
                        ResourceManagerConnection.onRegistrationSuccess(){
                            runAsync(() -> {
                                if (this == resourceManagerConnection) {
                                    JobMaster.establishResourceManagerConnection(success);{//
                                        final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();
                                        establishedResourceManagerConnection = new EstablishedResourceManagerConnection();
                                        slotPool.connectToResourceManager(resourceManagerGateway);{//SlotPoolImpl.
                                            for (PendingRequest pendingRequest : waitingForResourceManager.values()) {
                                                requestSlotFromResourceManager(resourceManagerGateway, pendingRequest);{
                                                    final AllocationID allocationId = new AllocationID();
                                                    CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot();{ //远程调用;
                                                        // 发起Akka的Rpc请求, 远程 执行 ResourceManager.requestSlot()方法并返回结果;
                                                    }
                                                }
                                            }
                                        }
                                        
                                    }
                                }
                            });    
                        }
                    }
                    
                }
            }
        }
    }
}




// "flink-akka.actor.default-dispatcher-5" : 在TaskManager端发起 申请Slot请求 ?
ResourceManager.requestSlot(){
    checkInit();
    internalRequestSlot(pendingSlotRequest);{//SlotManagerImpl.internalRequestSlot()
        OptionalConsumer.of(findMatchingSlot(resourceProfile))
            .ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))
            .ifNotPresent(() -> fulfillPendingSlotRequestWithPendingTaskManagerSlot(pendingSlotRequest));{
                SlotManagerImpl.allocateSlot(){
                    TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();
                    //gateway 是什么? TaskExecutorGateway => AkkaInvocationHandler => TaskExecutor.requestSlot(), 应该是调远程Rpc传输数据服务;
                    CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot();{
                        // 该Ack发起Rpc请求,并最终调用 TaskExecutor的  方法完成执行;
                        
                        { //线程: flink-akka.actor.default-dispatcher-3
                            TaskExecutor.requestSlot()
                                ->jobLeaderService.addJob(jobId, targetAddress) -> leaderRetrievalService.start(jobManagerLeaderListener);
                                
                        }
                    }
                    
                    requestFuture.whenComplete();
                    
                    completableFuture.whenCompleteAsync();
                }
            }
    }
}



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




















# 3.1 TaskExecutor上的 



// Window(TumblingEventTimeWindows(3000), EventTimeTrigger, CoGroupWindowFunction) -> Map -> Filter -> Sink: Print to Std. Err (2/4) 线程: 
Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                
                // 真正消费数据前,先进行初始化
                beforeInvoke();
                
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}



Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();{//MailboxProcessor.runMailboxLoop()
                        final MailboxController defaultActionContext = new MailboxController(this);
                        while (processMail(localMailbox)) {
                            mailboxDefaultAction.runDefaultAction(defaultActionContext); {// 实现类: StreamTask.runDefaultAction()
                                // 这个mailboxDefaultAction()函数,即 new MailboxProcessor(this::processInput, mailbox, actionExecutor) 方法的 this::processInput 方法;
                                StreamTask.processInput(){
                                    InputStatus status = input.emitNext(output);{//StreamTaskNetworkInput.
                                        while (true) {
                                            DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                                            if (result.isFullRecord()) {//isFullRecord字段为true;
                                                processElement(deserializationDelegate.getInstance(), output);{//StreamTaskNetworkInput.
                                                    if (recordOrMark.isRecord()){ //return getClass() == StreamRecord.class;
                                                        output.emitRecord(recordOrMark.asRecord());
                                                    } else if (recordOrMark.isWatermark()) { // return getClass() == Watermark.class;
                                                        statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);{//StatusWatermarkValue.inputWatermark()
                                                            // 重要逻辑: 基于水位的过滤?
                                                            if (watermark.getTimestamp() > channelStatuses[channelIndex].watermark) {
                                                                channelStatuses[channelIndex].watermark = watermarkMillis;
                                                                findAndOutputNewMinWatermarkAcrossAlignedChannels();{
                                                                    if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
                                                                        lastOutputWatermark = newMinWatermark;
                                                                        output.emitWatermark(new Watermark(lastOutputWatermark));{//OneInputStreamTask.StreamTaskNetworkOutput 
                                                                            operator.processWatermark(watermark){//AbstractStreamOperator
                                                                                if (timeServiceManager != null) {
                                                                                    timeServiceManager.advanceWatermark(mark);{//InternalTimeServiceManager
                                                                                        for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
                                                                                            service.advanceWatermark(watermark.getTimestamp());{//InternalTimeServiceManager
                                                                                                while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
                                                                                                    eventTimeTimersQueue.poll();
                                                                                                    triggerTarget.onEventTime(timer);{// WindowOperator.onEventTime
                                                                                                        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
                                                                                                        if (triggerResult.isFire()) {
                                                                                                            emitWindowContents(triggerContext.window, contents);{//WindowOperator.
                                                                                                                timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
                                                                                                                userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);{//InternalIterableWindowFunction.
                                                                                                                    wrappedFunction.apply(key, window, input, out);
                                                                                                                }
                                                                                                            }
                                                                                                        }
                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }
                                                                                output.emitWatermark(mark);
                                                                                
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    } else if (recordOrMark.isLatencyMarker()) { // return getClass() == LatencyMarker.class;
                                                        output.emitLatencyMarker(recordOrMark.asLatencyMarker());
                                                    } else if (recordOrMark.isStreamStatus()) {
                                                        statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
                                                    } else {
                                                        throw new UnsupportedOperationException("Unknown type of StreamElement");
                                                    } 
                                                }
                                            }
                                        }
                                    }
                                }
                                
                            }
                        }
                    }
                }
            }
        }
    }
}

// 对于 直接从Kafka消费的数据,或者直接fromElements 生成的数据: 

// 共同的函数逻辑:  LegacySourceFunctionThread.run() -> StreamSource.run()-> userFunction.run(ctx);

// Legacy Source: 遗留的Source: 遗留的数据源?
SourceStreamTask.LegacySourceFunctionThread.run(){
    headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);{//StreamSource
        run(lockingObject, streamStatusMaintainer, output, operatorChain);{
            this.ctx = StreamSourceContexts.getSourceContext();
            userFunction.run(ctx);{//FlinkKafkaConsumerBase.run()
                
                FlinkKafkaConsumerBase.run(){
                    this.kafkaFetcher = createFetcher();
                    kafkaFetcher.runFetchLoop();{
                        final Handover handover = this.handover;
                        // 启动Kafka消费线程, 持续从Kafka消费数据;
                        consumerThread.start();
                        
                        while (running) {
                            //consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
                            final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
                            for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
                                List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
                                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                    
                                    // 这里是处理业务逻辑的核心方法: 
                                    emitRecord(value, partition, record.offset(), record);{//KafkaFetcher.
                                        emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());{//AbstractFetcher.
                                            sourceContext.collectWithTimestamp(record, timestamp);{//StreamSourceContexts.
                                                collect(element);{//StreamSourceContexts.NonTimestampContext
                                                    output.collect(reuse.replace(element));{//AbstractStreamOperator.CountingOutput
                                                        numRecordsOut.inc();
                                                        output.collect(record);{// Operator.CopyingChainingOutput.
                                                            pushToOperator(record);{//
                                                                StreamRecord<T> castRecord = (StreamRecord<T>) record;
                                                                StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                operator.processElement(copy);{//StreamMap.
                                                                    // 这个element:StreamRecord(value:T,hasTimestamp:Boolean,timestamp:Long);
                                                                    // 这个 userFunction 即用户定义的函数体: 相当于Java中的 @Interface 接口的函数实现; 
                                                                    // 这个userFunction 也可以是MapFunction的实现类: MyMapFuncImpl, 则直接调用 MyMapFuncImpl.map()进行处理;
                                                                    X element = userFunction.map(element.getValue());{// 
                                                                        
                                                                    }
                                                                    
                                                                    output.collect(element.replace(element){//StreamRecord.replace() 将原来.value字段替换成新的值,避免新new StreamRecord 的转换成本;
                                                                        this.value = (T) element; 
                                                                        return (StreamRecord<X>) this; //这里就是类型装换了, 因为原来可能是: value:String 类型 => value:(String,String) 
                                                                    });{//AbstractStreamOperator.CountingOutput.collect()
                                                                        numRecordsOut.inc(); //所谓的CountingOutput,就是这里简单对StreamRecord技术+1;
                                                                        output.collect(record);{//OperatorChain.WatermarkGaugeExposingOutput 的几个接口
                                                                            // outputs: 即addSink()所添加的输出; 
                                                                            OperatorChain.BroadcastingOutputCollector.collect(record){
                                                                                for (Output<StreamRecord<T>> output : outputs) {
                                                                                    output.collect(record);
                                                                                }    
                                                                            }
                                                                            
                                                                            // 当该算子后面还接另一个filter/map()/等算子时,
                                                                            OperatorChain.CopyingChainingOutput.collect(){
                                                                                if (this.outputTag != null) return;
                                                                                //将该Record发给 Operator操作器;
                                                                                pushToOperator(record);{//CopyingChainingOutput.
                                                                                    StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                                    operator.processElement(copy);{ //对于FilterStream的苏州尼
                                                                                        // 这里的userFunction: FilterFunction<T>的实现类 MyFilterFuncImpl的实例对象;
                                                                                        if (userFunction.filter(element.getValue())) {
                                                                                            output.collect(element);
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                            
                                                                            // Shuffle的写出的,就用这个Output;
                                                                            RecordWriterOutput[implements WatermarkGaugeExposingOutput].collect(){
                                                                                
                                                                            }
                                                                            
                                                                            // DirectedOutput[implements WatermarkGaugeExposingOutput]: 
                                                                            
                                                                            // ChiningOutput [implements WatermarkGaugeExposingOutput]: 
                                                                            
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            partitionState.setOffset(offset);
                                        }
                                    }
                                }
                            }
                            
                        }
                    }
                    
                }
                
                FromElementsFunction.run(){ // 
                    ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
                    // 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
                    while (isRunning && numElementsEmitted < numElements) {
                        T next = serializer.deserialize(input);
                        // 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
                        ctx.collect(next);{//StreamSourceContexts.NonTimestampContext.collect()
                            //中间标准的装换, 最后还是调: element.replace(userFunction.map()); 或者 userFunction.xxx(): map(),filter(),sum(),reduce()..
                            output.collect(reuse.replace(element));-> output.collect(record);->pushToOperator(record);-> operator.processElement(copy);{
                                output.collect(element.replace(userFunction.map(element.getValue())));
                            }
                        }
                        numElementsEmitted++;
                    }
                }
                
            }
        }
    }
    completionFuture.complete(null);
}




// 3.2 对于KafkaConsumer的逻辑: 

FlinkKafkaConsumerBase.run(){
    this.kafkaFetcher = createFetcher();
    kafkaFetcher.runFetchLoop();{
        final Handover handover = this.handover;
        // 启动Kafka消费线程, 持续从Kafka消费数据;
        consumerThread.start();
        
        while (running) {
            //consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
            final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
            for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
                List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                    
                    // 这里是处理业务逻辑的核心方法: 
                    emitRecord(value, partition, record.offset(), record);{//KafkaFetcher.
                        emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());{//AbstractFetcher.
                            sourceContext.collectWithTimestamp(record, timestamp);{//StreamSourceContexts.
                                collect(element);{//StreamSourceContexts.NonTimestampContext
                                    output.collect(reuse.replace(element));{//AbstractStreamOperator.CountingOutput
                                        numRecordsOut.inc();
                                        output.collect(record);{// Operator.CopyingChainingOutput.
                                            pushToOperator(record);{//
                                                StreamRecord<T> castRecord = (StreamRecord<T>) record;
                                                StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                operator.processElement(copy);{//StreamMap.
                                                    // 这个element:StreamRecord(value:T,hasTimestamp:Boolean,timestamp:Long);
                                                    // 这个 userFunction 即用户定义的函数体: 相当于Java中的 @Interface 接口的函数实现; 
                                                    // 这个userFunction 也可以是MapFunction的实现类: MyMapFuncImpl, 则直接调用 MyMapFuncImpl.map()进行处理;
                                                    X element = userFunction.map(element.getValue());{// 
                                                        
                                                    }
                                                    
                                                    output.collect(element.replace(element){//StreamRecord.replace() 将原来.value字段替换成新的值,避免新new StreamRecord 的转换成本;
                                                        this.value = (T) element; 
                                                        return (StreamRecord<X>) this; //这里就是类型装换了, 因为原来可能是: value:String 类型 => value:(String,String) 
                                                    });{//AbstractStreamOperator.CountingOutput.collect()
                                                        numRecordsOut.inc(); //所谓的CountingOutput,就是这里简单对StreamRecord技术+1;
                                                        output.collect(record);{//OperatorChain.WatermarkGaugeExposingOutput 的几个接口
                                                            // outputs: 即addSink()所添加的输出; 
                                                            OperatorChain.BroadcastingOutputCollector.collect(record){
                                                                for (Output<StreamRecord<T>> output : outputs) {
                                                                    output.collect(record);
                                                                }    
                                                            }
                                                            
                                                            // 当该算子后面还接另一个filter/map()/等算子时,
                                                            OperatorChain.CopyingChainingOutput.collect(){
                                                                if (this.outputTag != null) return;
                                                                //将该Record发给 Operator操作器;
                                                                pushToOperator(record);{//CopyingChainingOutput.
                                                                    StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
                                                                    operator.processElement(copy);{ //对于FilterStream的苏州尼
                                                                        // 这里的userFunction: FilterFunction<T>的实现类 MyFilterFuncImpl的实例对象;
                                                                        if (userFunction.filter(element.getValue())) {
                                                                            output.collect(element);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            
                                                            // Shuffle的写出的,就用这个Output;
                                                            RecordWriterOutput[implements WatermarkGaugeExposingOutput].collect(){
                                                                
                                                            }
                                                            
                                                            // DirectedOutput[implements WatermarkGaugeExposingOutput]: 
                                                            
                                                            // ChiningOutput [implements WatermarkGaugeExposingOutput]: 
                                                            
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            partitionState.setOffset(offset);
                        }
                    }
                }
            }
            
        }
    }
    
}



// 3.3 对于 FromElements的计算逻辑: 
FromElementsFunction.run(){ // 
    ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
    // 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
    while (isRunning && numElementsEmitted < numElements) {
        T next = serializer.deserialize(input);
        // 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
        ctx.collect(next);{//StreamSourceContexts.NonTimestampContext.collect()
            //中间标准的装换, 最后还是调: element.replace(userFunction.map()); 或者 userFunction.xxx(): map(),filter(),sum(),reduce()..
            output.collect(reuse.replace(element));-> output.collect(record);{//RecordWriterOutput.collect()
                if (this.outputTag != null) return;
                pushToRecordWriter(record);{//RecordWriterOutput.
                    serializationDelegate.setInstance(record);
                    recordWriter.emit(serializationDelegate);{
                        int nextChannelToSendTo = channelSelector.selectChannel(record);{// RebalancePartitioner.selectChannel() 决定分区?
                            nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
                            return nextChannelToSendTo;
                        }
                        emit(record, nextChannelToSendTo);{//ChannelSelectorRecordWriter.emit()
                            serializer.serializeRecord(record);
                            
                            boolean pruneTriggered = copyFromSerializerToTargetChannel(targetChannel);{// RecordWriter.copyFromSerializerToTargetChannel()
                                SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
                                while (result.isFullBuffer()) {
                                    finishBufferBuilder(bufferBuilder);
                                }
                                if (flushAlways) flushTargetPartition(targetChannel);
                            }
                            
                            if (pruneTriggered) { //
                                serializer.prune();
                            }
                        }
                    }
                }
            }
        }
        numElementsEmitted++;
    }
}

    // 下游子线程: Map -> Sink: Print to Std. Out

// Window(TumblingEventTimeWindows(3000), EventTimeTrigger, CoGroupWindowFunction) -> Map -> Filter -> Sink: Print to Std. Err (2/4) 线程: 
Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                beforeInvoke(); // 真正消费数据前,先进行初始化
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();{
                        boolean hasAction = processMail(localMailbox);{//源码中while(processMail(localMailbox)); 没有Mail时会一直阻塞在此,有消息才
                            if (!mailbox.createBatch(){// TaskMailboxImpl.createBatch()
                                if (!hasNewMail) {
                                    return !batch.isEmpty();
                                }
                                
                            }) {
                                return true;
                            }
                            
                            while (isDefaultActionUnavailable() && isMailboxLoopRunning()) { //循环阻塞在此,等到Mail消息; 有新消息才会进入while()循环中的 runDefaultAction();
                                // 阻塞方法, 一直等到直到 queue不为空,取出了一个 headMail:Mail
                                mailbox.take(MIN_PRIORITY).run();{//TaskMailboxImpl.take(int priority)
                                    Mail head = takeOrNull(batch, priority);
                                    while ((headMail = takeOrNull(queue, priority)) == null) {
                                        // 接受线程信号; 阻塞在此, 一旦("OutputFlusher for Source")线程发出 notEmpty.signal()信号,就结束等待,处理消息;
                                        notEmpty.await();
                                    }
                                    hasNewMail = !queue.isEmpty(); // 用于 createBatch()中判断;
                                    return headMail;
                                }
                            }
                            return isMailboxLoopRunning();// return mailboxLoopRunning;
                        }
                        
                        while (hasAction = processMail(localMailbox)) {//阻塞在条件判断的方法中, 判断还处于Running状态时,会进入下面的 runDefaultAction()
                            mailboxDefaultAction.runDefaultAction(defaultActionContext); {
                                this.processInput();{
                                    StreamTask.processInput();{
                                        // 这里不同的 inputProcessor:StreamInputProcessor 实现类,进行不同处理;
                                        InputStatus status = inputProcessor.processInput();{
                                            StreamoneInputProcessor.processInput();{}
                                        }
                                    }
                                
                                    SourceStreamTask.processInput(controller);{
                                        
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}
 
Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();{//MailboxProcessor.runMailboxLoop()
                        final MailboxController defaultActionContext = new MailboxController(this);
                        while (processMail(localMailbox)) {
                            mailboxDefaultAction.runDefaultAction(defaultActionContext); {// 实现类: StreamTask.runDefaultAction()
                                // 这个mailboxDefaultAction()函数,即 new MailboxProcessor(this::processInput, mailbox, actionExecutor) 方法的 this::processInput 方法;
                                StreamTask.processInput(){
                                    InputStatus status = input.emitNext(output);{//StreamTaskNetworkInput.
                                        // 这个地方循环遍历, 从缓存总依次读取每个record的 字节数组,反序列化后交给后面operator取处理;
                                        while (true) {
                                            DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                                            if (result.isFullRecord()) {//isFullRecord字段为true;
                                                processElement(deserializationDelegate.getInstance(), output);{//StreamTaskNetworkInput.
                                                    if (recordOrMark.isRecord()){ //return getClass() == StreamRecord.class;
                                                        output.emitRecord(recordOrMark.asRecord());{//OneInputStreamTask.StreamTaskNetworkOutput.emitRecord()
                                                            operator.processElement(record);{ //StreamMap.
                                                                //这里执行用户定义 逻辑: userFunction.map();
                                                                output.collect(element.replace( userFunction.map(element.getValue())));
                                                            }
                                                        }
                                                    } else if (recordOrMark.isWatermark()) { // return getClass() == Watermark.class;
                                                        statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
                                                    } else if (recordOrMark.isLatencyMarker()) { // return getClass() == LatencyMarker.class;
                                                        output.emitLatencyMarker(recordOrMark.asLatencyMarker());
                                                    } else if (recordOrMark.isStreamStatus()) {
                                                        statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
                                                    } else {
                                                        throw new UnsupportedOperationException("Unknown type of StreamElement");
                                                    } 
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}



StreamTask MailboxController  InputStatus StreamRecord
AbstractStreamOperator 
// 学习目的 
    - 了解 1个作业/算子 从数据read -> writer写出, 的完整流程和主要耗时;
    

//














    
