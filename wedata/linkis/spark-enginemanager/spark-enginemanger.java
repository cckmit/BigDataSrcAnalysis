

EngineManagerReceiver.receiveAndReply(){
    case request: RequestEngine =>
      val engine = if(duration != null) engineManager.requestEngine(request, duration.toMillis) {//AbstractEngineManager.requestEngine()
			getEngineManagerContext.getOrCreateEngineHook.foreach(hook => realRequest = hook.beforeCreateSession(realRequest)){
				ConsoleConfigurationEngineHook.beforeCreateSession(){
					val globalConfig = ConfigurationMapCache.globalMapCache.getCacheMap(requestEngine){//RPCMapCache.getCacheMap()
						val result = sender.ask(createRequest(key));// sender: publicserver, 
						createMap(result) // result: Map<String,String> 来自 页面的 常用功能-设置-通用设置?
					}
					properties.putAll(globalConfig)
					// 通过cloud-publicservice 服务查询 engine的各配置属性?
					val engineConfig = ConfigurationMapCache.engineMapCache.getCacheMap(requestEngine) 
					properties.putAll(engineConfig)
					requestEngine.properties.putAll(properties)
				}
			}
			
			val resource = Utils.tryThrow(getEngineManagerContext.getOrCreateEngineResourceFactory.createEngineResource(realRequest))
			val usedResource = getEngineManagerContext.getOrCreateEngineFactory.getUsedResources.getOrElse(Resource.getZeroResource(resource.getResource));{
				EngineManagerContextImpl.getOrCreateEngineFactory(){return SparkEngineResourceFactory}
				SparkEngineResourceFactory.getUsedResources()
				SparkEngineResourceFactory.AbstractEngineResourceFactory.createEngineResource(){
					val user = if(StringUtils.isEmpty(request.user)) request.creator else else request.user // "bigdata"
					val engineResource == new UserTimeoutEngineResource
					engineResource.setResource(getRequestResource(request.properties)){//SparkEngineResourceFactory.getRequestResource()
						
						val executorNum = DWC_SPARK_EXECUTOR_INSTANCES.getValue(properties) // spark.executor.instances, 默认=3
						
						// YarnResource.queueMemory= spark.executor.memory(默认4) * executorNum(spark.executor.instances,默认3) = 12G; 
						// YarnResource.queueCores = spark.executor.cores (默认2) * executorNum(spark.executor.instances,默认3) = 6 ; 
						new DriverAndYarnResource(
						  new LoadInstanceResource(ByteTimeUtils.byteStringAsBytes(DWC_SPARK_DRIVER_MEMORY.getValue(properties) + "G"),
							DWC_SPARK_DRIVER_CORES,1),
						  new YarnResource(ByteTimeUtils.byteStringAsBytes(DWC_SPARK_EXECUTOR_MEMORY.getValue(properties) * executorNum + "G"),
							DWC_SPARK_EXECUTOR_CORES.getValue(properties) * executorNum,
							0,DWC_QUEUE_NAME.getValue(properties))
						)
					}
				}
			}
			if(nodeResourceInfo.totalResource - nodeResourceInfo.protectedResource - usedResource <= resource.getResource) {
				throw new EngineManagerWarnException(31000, "远程服务器资源已被用光，请切换远程服务器再试！")
			}
			val resultResource: ResultResource = getEngineManagerContext.getOrCreateResourceRequester.request(resource);{//ResourceRequesterImpl.request()
				case time: UserTimeoutEngineResource => // 正是从这里 发出了 DriverAndYarnResource 的请求对象
					val timeout = math.max(5000, math.min(time.getTimeout / 5, 30000))
					rmClient.requestResource(time.getUser, time.getCreator, resourceRequest.getResource, timeout)
				case user: UserEngineResource =>
					rmClient.requestResource(user.getUser, user.getCreator, resourceRequest.getResource)
			}
			resultResource match {
				case NotEnoughResource(reason) =>
					throw new EngineManagerWarnException(30001, LogUtils.generateWarn(reason))
				case AvailableResource(ticketId) =>
					//The first step: get the creation request(第一步：拿到创建请求)
					val engine = getEngineManagerContext.getOrCreateEngineCreator.create(ticketId, resource, realRequest)
					engine.setResource(resource.getResource)	
					engine.init()
					Future {
						engine.init()
						getEngineManagerContext.getOrCreateEngineHook.foreach(hook => hook.afterCreatedSession(engine, realRequest))
					}
						.onComplete ()
					Some(engine)
			}
			
	  }else engineManager.requestEngine(request)
      engine.map {
        case p: ProcessEngine =>
          portToSenders.put(p.getPort, sender)
          getMsg(p.getState, ResponseEngineStatusCallback(p.getPort, p.getState.id, p.getInitErrorMsg))
      }.get
    case request: RequestUserEngineKill =>
      engineManager.getEngineManagerContext.getOrCreateEngineFactory.list().find(_.getTicketId == request.ticketId)
        .foreach(engineManager.getEngineManagerContext.getOrCreateEngineFactory.delete)
      ResponseUserEngineKill(request.ticketId, ResponseUserEngineKill.Success, "")
    case _ => warn(s"cannot recognize the message $message.")
	
}






Spark-Entrance

@Path("/execute")
EntranceRestfulApi.execute(Map<String, Object> json){
	String execID = entranceServer.execute(json);
    Job job = entranceServer.getJob(execID).get();
	Task task = ((EntranceJob)job).getTask();
	message = Message.ok();
	return Message.messageToResponse(message);
}


@Path("/{id}/status")
public Response status(@PathParam("id") String id, @QueryParam("taskID")String taskID) {
	String realId = ZuulEntranceUtils.parseExecID(id)[3];
	try{
		job = entranceServer.getJob(realId);{// EntranceServer.getJob
			var scheduler = getEntranceContext.getOrCreateScheduler();{
				
			}
			scheduler.get(execId).map(_.asInstanceOf[Job]);{
				AbstractScheduler.get(){
					val (index, groupName) = getIndexAndGroupName(eventId)
					val consumer = getSchedulerContext.getOrCreateConsumerManager.getOrCreateConsumer(groupName)
					consumer.getRunningEvents.find(_.getId == eventId).orElse(consumer.getConsumeQueue.get(index))
				}
			}
		}
	}catch(Exception e){
		
	}	
	return Message.messageToResponse(message);
}


// 该线程 askExecutor()获取Spark引擎,并利用该引擎执行 作业: executeService.submit(job)
//	- executor: Option[Executor] = askExecutor(job);
//	- executeService.submit(job)

FIFOUserConsumer.run(){
	
	info(s"$toString thread started!")
	while (!terminate) {
		Utils.tryAndError(loop()){//FIFOUserConsumer.loop() 
			val completedNums = runningJobs.filter(e => e == null || e.isCompleted)
			while(event.isEmpty) {
				event = if (takeEvent.exists(e => Utils.tryCatch(e.turnToScheduled())))
						takeEvent
					else 
						getWaitForRetryEvent
			}
		}
		Utils.tryAndError(Thread.sleep(10))
	}

	event.foreach { case job: Job => 
		Utils.tryCatch (){// try do... 
			val (totalDuration, askDuration) = (fifoGroup.getMaxAskExecutorDuration, fifoGroup.getAskExecutorInterval)
			
			// 1. 获取或生成 相应的执行引擎 Executor 
			var executor: Option[Executor] = None // 下面代码就是生产相应的 Executor 
			Utils.waitUntil(() => {
				executor = schedulerContext.getOrCreateExecutorManager.askExecutor(job, askDuration);{//EntranceExecutorManager.askExecutor()
					val startTime = System.currentTimeMillis()
					while(System.currentTimeMillis - startTime < wait.toMillis && executor.isEmpty){
						askExecutor(job);{
							findUsefulExecutor(job){ // EntranceExecutorManager.findUsefulExecutor
								val engines = findExecutors(job).toBuffer
								while(lock.isEmpty && engines.nonEmpty) {
									engine = getOrCreateEngineSelector().chooseEngine(engines.toArray)
									var ruleEngines = engine.map(Array(_)).getOrElse(Array.empty)
									ruleEngines.foreach(e => lock = getOrCreateEngineSelector().lockEngine(e));{
										// 这里就是匹配和获取 引擎的执行锁?  如获取spark?
										SingleEngineSelector.lockEngine(){
											case s: SingleEntranceEngine =>{
												s.tryLock(sender => Utils.tryThrow {
													sender.ask(RequestEngineLock(engine.getModuleInstance.getInstance, ENGINE_LOCK_MAX_HOLDER_TIME.getValue.toLong)) match {
														case ResponseEngineLock(l) => lock
														case ResponseEngineStatus(instance, state, overload, concurrent, _) => None
														case warn: WarnException => None
													}
												}
											}
										}
									}
									engine.foreach(engines -= _)
								}
								orElse {
									val executor = createExecutor(job)
								};
							}
						}
					}}}
					
					if(warnException != null && executor.isEmpty) throw warnException
					executor : Option[Executor] // spark引擎对应: Some( SparkSingleEntranceEngine)
				}
				Utils.tryQuietly(askExecutorGap())
				executor.isDefined
			});
			
			// 2. 利用该executor 执行该job 
			executor.foreach { executor =>
				job.setExecutor(executor)
				job.future = executeService.submit(job)
				job.getJobDaemon.foreach(jobDaemon => jobDaemon.future = executeService.submit(jobDaemon))
			}
			
		} catch {// catch()异常处理
			case _: TimeoutException =>
				job.onFailure("The request engine times out and the cluster cannot provide enough resources(请求引擎超时，集群不能提供足够的资源).",
			case error: Throwable => // 其他失败,其他超时, 就报这个错误: 说 请求引擎失败
				job.onFailure("Request engine failed, possibly due to insufficient resources or background process error(请求引擎失败，可能是由于资源不足或后台进程错误)!", error)
				
		}
		
		val (totalDuration, askDuration) = (fifoGroup.getMaxAskExecutorDuration, fifoGroup.getAskExecutorInterval)
		job.consumerFuture = new BDPFutureTask(this.future)
		
	}
}



EntranceReceiver.receive(){
	case res: ResponseTaskStatus =>{
		val state = SchedulerEventState(res.state)
		findEngineExecuteAsynReturn(res.execId, sender, s"ResponseTaskStatus(${res.execId}, ${res.state})")
			.foreach(_.notifyStatus(res)){
				EngineExecuteAsynReturn.notifyStatus(){
					val response = if(responseEngineStatus.state > SchedulerEventState.maxId){ Some(IncompleteExecuteResponse(errorMsg))
						}else SchedulerEventState(responseEngineStatus.state) match {
							case Succeed => Some(SuccessExecuteResponse())
							case Failed | Cancelled | Timeout => Some(ErrorExecuteResponse(errorMsg, error))
							case _ => None
						}
					response.foreach{ r =>
						callback(this)
						if(notifyJob == null){
							this synchronized(while(notifyJob == null) this.wait(1000))
						}
						notifyJob(r);{// Job.run()中代码
							transitionCompleted(realRS){// EntranceJob.transitionCompleted()
								super.transitionCompleted(executeCompleted);{
									val state = getState
									executeCompleted match {
										case _: SuccessExecuteResponse => {
											
										}
										
									}
									IOUtils.closeQuietly(this){
										EntranceExecutionJob.close(){
											logger.info("job:" + getId() + " is closing");
										}
									}
								}
							}
						}
					}
				}
			}
	}
	case ResponseTaskError(execId, errorMsg) => 
		findEngineExecuteAsynReturn(execId, sender, "ResponseTaskError").foreach(_.notifyError(errorMsg))
		
	case ResponseTaskProgress(execId, progress, progressInfo) =>{
		
	}
}



EntranceReceiver.receiveAndReply(){
	case res: ResponseTaskStatus =>{
		val state = SchedulerEventState(res.state)
		askRetryWhenExecuteAsynReturnIsEmpty(res.execId, sender, s"ResponseTaskStatus(${res.execId}, ${res.state})")
			.notifyStatus(res);{//
				EngineExecuteAsynReturn.notifyStatus(){ // 代码同上 
					val response 
					response.foreach{ r =>
						callback(this)
						if(notifyJob == null){
							this synchronized(while(notifyJob == null) this.wait(1000))
						}
						notifyJob(r);
					}
				}
		
			}
	}
		
	case ResponseTaskResultSize(execId, resultSize) =>
}





// Spark-SQL 执行完整源码









