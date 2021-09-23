

// 线程: WebSocket-Server-Event-ListenerBus-Thread-0


// 消息发送源码: linkis-module 公共模块; 
//ListenerEventBus.ListenerEventThread.run()-> postToAll()-> doPostEvent()

ListenerEventBus.ListenerEventThread.run(){
	val currentThreadName = s"$name-Thread-$index"
	while(continue) {
		while(event.isEmpty){
			wait();//Object.wait()
		}
		
		event.foreach(postToAll());{//ListenerEventBus.postToAll()
			val iter = listeners.iterator
			while (iter.hasNext) {
				doPostEvent(iter.next(), event);{// 由其不同得实现类 重写实现
					ServerListenerEventBus.doPostEvent(){
						if(StringUtils.isEmpty(serverEvent.getMethod)){
							info("ignore empty method with " + serverEvent.getData)
						} else if(serverEvent.getMethod.startsWith(listener.serviceName)){
							val response = listener.onEvent(serverEvent);{// listener:ServerEventService 接口定义服务Event消息
								EntranceWebSocketService.onEvent();// 只有这1个实现类?
								// 
							}
							event.socket.sendMessage(response)
						}
					}
					SingleThreadListenerBus.doPostEvent()
					
					AsynRPCMessageBus.doPostEvent(){
						listener.onEvent(event);// 一样,出发Lisstener.onEvent()
					}
				}
			}
		}
	}
}

// 线程: WebSocket-Server-Event-ListenerBus-Thread-0
// ListenerEventBus.ListenerEventThread.run()->postToAll()->EntranceWebSocketService.onEvent()


EntranceWebSocketService.onEvent();{
	case `executePattern` => dealExecute(event);{// EntranceWebSocketService.dealExecute
		// 执行1个脚本命令,并返回JobId;
		val jobId = entranceServer.execute(params);{//EntranceServer.execute()
			var task = getEntranceContext.getOrCreateEntranceParser().parseToTask(params)
			//将map parse 成 task 之后，我们需要将它存储到数据库中，task可以获得唯一的taskID
			getEntranceContext.getOrCreatePersistenceManager().createPersistenceEngine().persist(task)
			getEntranceContext.getOrCreateEntranceInterceptors().foreach(int => task = int.apply(task, logAppender)));{
				CSEntranceInterceptor.apply(){
					task match {case requestPersistTask: RequestPersistTask =>{
						Utils.tryAndWarn(CSEntranceHelper.addCSVariable(requestPersistTask))
						Utils.tryAndWarn(CSEntranceHelper.resetCreator(requestPersistTask))
						Utils.tryAndWarn(CSEntranceHelper.initNodeCSInfo(requestPersistTask))
					}}
				}
			}
			
			val job = getEntranceContext.getOrCreateEntranceParser().parseToJob(task)
			job.init()
			getEntranceContext.getOrCreateScheduler().submit(job);{//Scheduler.submit()提交Job任务的抽象类;
				AbstractScheduler.submit(){ // Scheduler唯一的子类, 最终还有个实现类: EventSchedulerImpl
					val groupName = getSchedulerContext.getOrCreateGroupFactory.getGroupNameByEvent(event)
					val consumer = getSchedulerContext.getOrCreateConsumerManager.getOrCreateConsumer(groupName)
					index.map(getEventId(_, groupName)).foreach(event.setId)
				}
			}
			job.getId
		}
		val task = entranceServer.getJob(jobId).get.asInstanceOf[EntranceJob].getTask.asInstanceOf[RequestPersistTask]
		val execID = ZuulEntranceUtils.generateExecID(jobId, executeApplicationName, Sender.getThisInstance, creator)
		
		val code = task.asInstanceOf[RequestPersistTask].getCode
		
	}
	case logUrlPattern(id) => dealLog(event, id)
	case progressUrlPattern(id) => dealProgress(event, id)
	case pauseUrlPattern(id) => dealPause(event, id)
}





// IDE_bigdataConsumerThread 线程: 

// 该线程 askExecutor()获取Spark引擎,并利用该引擎执行 作业: executeService.submit(job)
//	- executor: Option[Executor] = askExecutor(job);
//	- executeService.submit(job)

/*	IDE_bigdataConsumerThread 线程: 
	
	Debug关键类: SchedulerEvent.transition()
*/

FIFOUserConsumer.run(){
	
	info(s"$toString thread started!")
	while (!terminate) {
		Utils.tryAndError(loop()){//FIFOUserConsumer.loop() 
			val completedNums = runningJobs.filter(e => e == null || e.isCompleted)
			while(event.isEmpty) {
				val scheduledEventExist = takeEvent.exists(e => Utils.tryCatch(e.turnToScheduled()));{//Job.turnToScheduled
					SchedulerEvent.turnToScheduled(){
						if(!isWaiting) false else this synchronized {
							scheduledTime = System.currentTimeMillis
							while(id == null) wait(100)
							transition(Scheduled);{//SchedulerEvent.transition()  SchedulerEvent是接口类,
								if(state.id < this.state.id && state != WaitForRetry){
									throw new SchedulerErrorException(12000, s"Task status flip error! Cause: Failed to flip from ${this.state} to $state.（任务状态翻转出错！原因：不允许从${this.state} 翻转为$state.）")//抛异常
								}
								info(s"$toString change state ${this.state} => $state.") // 这里是
								afterStateChanged(oldState, state);{// 抽象类,这里由子类 EntranceJob实现: EntranceJob.afterStateChanged()
									EntranceJob.afterStateChanged(){
										// case Scheduled => getLogListener.foreach(_.onLogUpdate(this,  LogUtils.generateInfo( "Your job is Scheduled. Please wait it to run.")))
									}
								}
							}
							true
						}
					}
				}
				event = if (scheduledEventExist)
						takeEvent
					else 
						getWaitForRetryEvent
			}
			
			event.foreach { case job: Job => 
				Utils.tryCatch{
					val (totalDuration, askDuration) = (fifoGroup.getMaxAskExecutorDuration, fifoGroup.getAskExecutorInterval)
					var executor: Option[Executor] = None // 下面代码就是生产相应的 Executor 
					Utils.waitUntil(() => {
						executor = schedulerContext.getOrCreateExecutorManager.askExecutor(job, askDuration);{//EntranceExecutorManager.askExecutor()
							schedulerEvent match {case job: Job => {
								val startTime = System.currentTimeMillis()
								while(System.currentTimeMillis - startTime < wait.toMillis && executor.isEmpty){
									askExecutor(job);{
										findUsefulExecutor(job){ // EntranceExecutorManager.findUsefulExecutor
											val engines = findExecutors(job).toBuffer
											while(lock.isEmpty && engines.nonEmpty) {
												engine = getOrCreateEngineSelector().chooseEngine(engines.toArray)
												ruleEngines.foreach(e => lock = getOrCreateEngineSelector().lockEngine(e));{// SingleEngineSelector.lockEngine(){
													case s: SingleEntranceEngine =>{
														s.tryLock(sender => Utils.tryThrow {
															sender.ask(RequestEngineLock(engine.getModuleInstance.getInstance, ENGINE_LOCK_MAX_HOLDER_TIME.getValue.toLong)) match {
																case ResponseEngineLock(l) => lock
																case ResponseEngineStatus(instance, state, overload, concurrent, _) => None
																case warn: WarnException => None
															}
														})
													}
													case _ => None
												}
											}
											setLock(lock, job)
										}
									}
								}
								if(warnException != null && executor.isEmpty) throw warnException
								executor
							}}
						}
						Utils.tryQuietly(askExecutorGap())
						executor.isDefined
					});
				}
			
			}
			
		}
		Utils.tryAndError(Thread.sleep(10))
	}
}




// Engine-Scheduler-ThreadPool 线程

Job.run(){
	Utils.tryAndWarn(transition(Running));{//Job 集成自 SchedulerEvent.transition()	[EntranceExecutionJob]
		info(s"$toString change state ${this.state} => $state.")
		afterStateChanged(oldState, state);{//EntranceJob.afterStateChanged() [EntranceExecutionJob]
			// case Running => getLogListener.foreach(_.onLogUpdate(this,  LogUtils.generateInfo( "Your job is Running now. Please wait it to complete.")))
		}
	}
}





// RPC-Receiver-Asyn-Thread-Thread-0

//ListenerEventBus.ListenerEventThread.run()-> postToAll()-> doPostEvent()
ListenerEventBus.ListenerEventThread.run(){
	postToAll(){doPostEvent(){//AsynRPCMessageBus.doPostEvent()
		listener.onEvent(event);{//RPCReceiveRestful.addBroadcastListener()中匿名函数.RPCMessageEventListener.onEvent()
			new RPCMessageEventListener().onEvent(){
				event.message match {
					case broadcastProtocol: BroadcastProtocol => broadcastListener.onBroadcastEvent(broadcastProtocol, event);{
						ResponseEngineStatusChangedBroadcastListener.onBroadcastEvent(){
							case ResponseEngineStatusChanged(instance, fromState, toState, overload, concurrent) => {
								val from = ExecutorState(fromState)
								val to = ExecutorState(toState)
								getEntranceExecutorManager.getOrCreateEngineManager().get(instance).foreach { engine =>
									engine.updateState(from, to, overload, concurrent);{//EntranceEngine.updateState() [SparkSingleEntranceEngine] 
										toState match {
											case ExecutorState.ShuttingDown => transition(toState)
											case _ if ExecutorState.isCompleted(toState) => transition(toState)
											case _ => changeState(fromState, toState)
										}
									}
								}
							}
						}
					}
					case _ =>
				}
			}
		}
	}}
}
















