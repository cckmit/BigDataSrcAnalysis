

// 启动脚本: EngineManagerReceiver.receiveAndReply(){case request: RequestEngine} -> engineManager.requestEngine() -> AbstractEngineManager.requestEngine()-> Future.onComplete(){ engine.init() };

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
			resultResource match { // 分辨和处理返回的 ResultResource结果; 如果有资源就进一步创建执行引擎
				case NotEnoughResource(reason) =>
					throw new EngineManagerWarnException(30001, LogUtils.generateWarn(reason))
				case AvailableResource(ticketId) =>
					//The first step: get the creation request(第一步：拿到创建请求)
					val engine = getEngineManagerContext.getOrCreateEngineCreator.create(ticketId, resource, realRequest)
					engine.setResource(resource.getResource)	
					getEngineManagerContext.getOrCreateEngineFactory.addEngine(engine);
					Future { // 异步执行该 engine.init() 即初始化/启动SparkSubmit脚本的方法; 
						engine.init();{//ProcessEngine.init()
							process = processBuilder.start();{// SparkSubmitProcessBuilder.start
								var command = args_.mkString(" ")
								val pb = new ProcessBuilder(sudoCommand: _*)
								pb.start();
							}
							Utils.waitUntil(() => _state != Starting, Duration(timeout, TimeUnit.MILLISECONDS))
						}
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





// 线程Engine-Manager-Thread-1 : 执行spark-submit脚本:  带RM资源分配下来后, request match AvailableResource 就调用  进程构建的start() 构建spark-submit脚本,并调用pd.start()其新进程; 

// 启动脚本: EngineManagerReceiver.receiveAndReply(){case request: RequestEngine} -> engineManager.requestEngine() -> AbstractEngineManager.requestEngine()-> Future.onComplete(){ engine.init() };
EngineManagerReceiver.receiveAndReply(){
	case request: RequestEngine => engineManager.requestEngine(request, duration.toMillis){resultResource match {
		case AvailableResource(ticketId) => Future { // 启动异步线程:  Engine-Manager-Thread-1
			
			engine.init();{//ProcessEngine.init()
				process = processBuilder.start();{// SparkSubmitProcessBuilder.start(); 更多细节在如下 代码中; 
					var command = args_.mkString(" ")
					info(s"Running ${command}")
					val sudoCommand = Array(JavaProcessEngineBuilder.sudoUserScript.getValue, request.user, command)
						- linkis/linkis-ujes-spark-enginemanager/bin/rootScript.sh 
						- bigdata 
						- command="spark-submit --master yarn --driver-memory 1G --class DataWorkCloudEngineApplication"
					val pb = new ProcessBuilder(sudoCommand: _*)
					
					pb.start();
				}
				Utils.waitUntil(() => _state != Starting, Duration(timeout, TimeUnit.MILLISECONDS))
			}
			getEngineManagerContext.getOrCreateEngineHook.foreach(hook => hook.afterCreatedSession(engine, realRequest))
		}.onComplete()
	}}
}

{ // SparkSubmitProcessBuilder.start() 方法的详解

	SparkSubmitProcessBuilder.start(){
		var args_ = ArrayBuffer(fromPath(_executable))
		addOpt("--master", _master) addOpt("--deploy-mode", _deployMode) addOpt("--name", _name)
		addList("--jars", _jars.map(fromPath))
		addOpt("--driver-memory", _driverMemory)	addClasspath("--driver-class-path", _driverClassPath)
		addOpt("--executor-memory", _executorMemory)
		addOpt("--num-executors", _numExecutors)
		addOpt("--class", _className)
		addOpt("", Some(ENGINE_JAR.getValue)) // ../lib/linkis-ujes-spark-engine-0.11.0.jar
		
		args_ ++= args		// 多个--dwc-conf 参数;
		var command = args_.mkString(" ")
		info(s"Running ${command}") // 这就是 Spark-EM的out日志中答应的那一行日志; 
		
		val sudoCommand = Array(JavaProcessEngineBuilder.sudoUserScript.getValue, request.user, command)
		val pb = new ProcessBuilder(sudoCommand: _*)
		val env = pb.environment() // 来自哪里?
		for ((key, value) <- _env) {
			env.put(key, value)
		}
		
		pb.start();// Linux 脚本?
	}

	/* 拼接出的最终spark-submit脚本
	spark-submit 
	--master yarn --deploy-mode client --name linkis 
	--conf spark.driver.extraJavaOptions="-Dwds.linkis.configuration=linkis-engine.properties -Duser.timezone=Asia/Shanghai " --conf spark.driver.cores=1 
	--driver-memory 1G --driver-class-path "/opt/spark/conf:/opt/hadoop/data/conf:/opt/dss_linkis/linkis_dss_release/linkis/linkis-ujes-spark-enginemanager/conf:/opt/dss_linkis/linkis_dss_release/linkis/linkis-ujes-spark-enginemanager/lib/*" 
	--driver-cores 1 --executor-memory 3G --executor-cores 1 --num-executors 1 --queue default 
	--class com.webank.wedatasphere.linkis.engine.DataWorkCloudEngineApplication  /opt/dss_linkis/dss_linkis-0.9.1/linkis/linkis-ujes-spark-enginemanager/lib/linkis-ujes-spark-engine-0.11.0.jar 
		--dwc-conf _req_entrance_instance=sparkEntrance,192.168.51.111:9106 --dwc-conf wds.linkis.yarnqueue.memory.max=300G 
		--dwc-conf wds.linkis.preheating.time=9:00 --dwc-conf wds.linkis.instance=1 	
	*/
	
	SparkSubmitProcessBuilder.build(engineRequest: EngineResource, request: RequestEngine){
		this.master("yarn")
		this.conf(SPARK_DRIVER_EXTRA_JAVA_OPTIONS.key, SPARK_DRIVER_EXTRA_JAVA_OPTIONS.getValue)
		properties.getOrDefault("jars", "").split(",").map(RelativePath).foreach(jar)
		getValueAndRemove(properties, SPARK_APPLICATION_JARS).split(",").map(RelativePath).foreach(jar)
	}
	
}









