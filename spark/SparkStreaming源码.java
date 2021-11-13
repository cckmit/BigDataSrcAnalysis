Streaming相关线程和作用:
- Application.main所在的main线程?

- userDriver启动线程?

- "streaming-start" 线程: 仅启动其他线程用, 启动后就停了?

- "JobScheduler" 线程: 循环生成调度任务的线程? 

- "JobGenerator" 线程: 根据batchDuration生成一个个的Job

- "RecurringTimer - JobGenerator" 线程: 按batchDuration微批间隔 定时发送GenerateJob(time)消息;
	* RecurringTimer 类是spark的(自定义)定时任务执行类, 用于spark中 BlockGenerator(定时更新缓存), JobGenerator(定时触发微批), ExecutorAllocationManger(定时动态扩缩容)3个组件的定时任务;
	* 该线程"RecurringTimer-JobGenerator": 作用就是按batchDuration微批间隔执行 post(GenerateJobs()),向"JobGenerator"线程的eventLoop.eventQueue队列添加 GenerateJobs消息;

- "streaming-job-executor-0" 线程: 将一个Job交由DAGScheduler划分Stage/Tasks并提交执行, 同时给sparkListenerBus发监控消息;

- dispatcher-event-loop-n 线程	: 将一个Job切分成n个Stages,并将每个Stage拆分成partition个Tasks以提交的核心调度;





# Shuffle相关线程
- shuffle-server-6-1 线程: Stage6-Task1的shuffle线程?

- map-output-dispatcher-n 线程(0-7个):



# BlockManager相关
- block-manager-slave-async-thread-pool-14 线程(1-14): 

- block-manager-ask-thread-pool-11 线程(1-22): 


# Spark 辅助线程:
- Spark Context Cleaner: 每30分钟的GC清理线程;

- SparkUI-34 线程(34- 44): 

- heartbeat-receiver-event-loop-thread 线程: 心跳检测;

- context-cleaner-periodic-gc线程: 

- Yarn application state monitor: Yarn任务状态监控线程;


# 通信相关线程
- netty-rpc-env-timeout 线程: 

- rpc-server-3-1 线程(3-1, 3-2,3-3):		?

- FileSystem$Statistics$StatisticsDataReferenceCleaner: 

- IPC Client (1685134322) connection to ldsver51/192.168.51.151:8032 from app

- IPC Parameter Sending Thread #0 线程: 


# 其他
- Timer-0 线程: 

- driver-revive-thread 线程: 

- threadDeathWatcher-4-1

- SparkListenerBus 线程: Spark的事件状态和Mestics监控;

- ForkJoinPool-1-worker-5 线程: ?

- task-result-getter-n: driver对于完成的Task 获取其结果的线程;



	
JVM相关线程: 
	* Attach Listener: 负责接收到外部的命令, 会将命令转到"signal dispather"线程取中转处理;
	* Signal Dispatcher	: JVM线程: 接收外部jvm命令后，会交给signal dispather线程去进行分发到各个不同的模块处理命令，并且返回处理结果。也是在第一次接收外部jvm命令时进行初始化工作


其他线程:
	* Reference Handler: spark 源码中没有, 是jconsole的线程?
	* Finalizer 线程: JVM的GC线程
	


//3. Streaming 最核心的流启动入口: 主要步骤: 1. 创建相关RDD和JobSet, 2. 提交Job到DAGScheduler; 3. 循环以上2步;

StreamingContext.start(): 	流处理的启动方法, 会启动各[Job生成和调度] n个线程
	- "streaming-start" 线程: 仅启动其他线程用, 启动后就停了?
	- "JobScheduler" 线程: 循环生成调度任务的线程? 			在ssc.start()-> JobScheduler.start()中启动
	- "JobGenerator" 线程: 根据batchDuration生成一个个的Job
	
	- "RecurringTimer - JobGenerator" 线程: 按batchDuration微批间隔 定时发送GenerateJob(time)消息;
		* RecurringTimer 类是spark的(自定义)定时任务执行类, 用于spark中 BlockGenerator(定时更新缓存), JobGenerator(定时触发微批), ExecutorAllocationManger(定时动态扩缩容)3个组件的定时任务;
		* 该线程"RecurringTimer-JobGenerator": 作用就是按batchDuration微批间隔执行 post(GenerateJobs()),向"JobGenerator"线程的eventLoop.eventQueue队列添加 GenerateJobs消息;
	
	- "streaming-job-executor-0" 线程: 将一个Job交由DAGScheduler划分Stage/Tasks并提交执行, 同时给sparkListenerBus发监控消息;
	
	
	问题:
		1. ReceiverTracker 的作用?
			* 网上: ReceiverTracker 启动后首先在Spark Cluster中启动Receiver
		2. 按固定间隔生成微批任务的代码和线程是? 
			- 答: 线程名:"RecurringTimer - JobGenerator", 在JobGenerator.startFirstTime()中启动该线程; 线程核心逻辑代码:RecurringTimer.loop(); 
		3. "streaming-job-executor"线程 如何与dag-scheduler-event-loop线程关系?
			* 通过JobHandler.run() -> job.run() -> job.func() -> rdd.count()/foreach()/collect() -> 
				-> sc.runJob() -> dag.runJob()-> eventProcessLoop.post(JobSubmitted())将消息存于缓存队列中, 而"dag-scheduler-event-loop"线程会消费队列并进一步处理;










/** "RecurringTimer - JobGenerator" 线程: 按batchDuration微批间隔 定时发送GenerateJob(time)消息;
* 		- 启动位置: StreamingContext.start() -> JobScheduler.start()-> JobGenerator.startFirstTime();

	RecurringTimer.loop(){
		while (!stopped) {
			triggerActionForNextInterval(){// 不断执行按固定间隔 执行callback();
				clock.waitTillTime(nextTime)//会阻塞等待 直到 nextTime这个时间点; 其内部Thread.sleep()直到特定的时间戳
				callback(nextTime){eventLoop.post(GenerateJobs(new Time(longTime)));} // 发送一个GenerateJobs消息; 该消息会被"JobGenerator"线程的 JobGenerator.processEvent(event)中case GenerateJobs(time) => generateJobs(time)处理
				nextTime += period //重新设置nextTime指,使其按固定间隔后再执行一次 callback;
			}
		}
	}
*/

new RecurringTimer(batchDuration,callback)).loop(){
	// 构造函数: period即任务间隔, callback即具体执行的线程任务;
	class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String);
	{
	StreamingContext.start(){
		{ //timer :RecurringTimer对象的创建
			new StreamingContext(){
				val scheduler = new JobScheduler(this){
					val jobGenerator = new JobGenerator(this){
						val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
					}
				}
			}
		}
		
		ThreadUtils.runInNewThread("streaming-start"){//启一个"streaming-start"线程, 其中启动JobScheduler等流计算
			scheduler.start(){//JobScheduler.start()
				jobGenerator.start(){//JobGenerator.start()
					JobGenerator.startFirstTime(){
						timer.start(startTime.milliseconds)// 这里启动"RecurringTimer - JobGenerator" 线程,
					}
				}
			}
		}
	}
	}
	
	try {
		while (!stopped) {
			triggerActionForNextInterval(){
				// 会阻塞等待 直到 nextTime这个时间点; 其内部Thread.sleep()直到特定的时间戳;
				clock.waitTillTime(nextTime){//SystemClock.waitTillTime(targetTime: Long)
					var currentTime = 0L
					currentTime = System.currentTimeMillis()
					// 算目标时间戳与当前时间 还差多久. 若waitTime <0表示已过 targetTime, 立马结束等待返回;
					var waitTime = targetTime - currentTime
					if (waitTime <= 0) {
					  return currentTime
					}
					
					// 进入这里说明还没到目标时间, 需要继续等待; 将要等待的时长(waitTime)切分成10分,  取waitTime的1/10 (或最少25毫秒)作为休眠间隔
					val pollTime = math.max(waitTime / 10.0, minPollTime).toLong

					while (true) {
						currentTime = System.currentTimeMillis()
						waitTime = targetTime - currentTime
						if (waitTime <= 0) {
						return currentTime
						}
						// 取 25ms(或创始等待时长的十分之一) 作为休眠间隔, 当 最新waitTime < 25ms时仅休眠 最新waitTime时长;
						val sleepTime = math.min(waitTime, pollTime)
						Thread.sleep(sleepTime)
					}
					-1
				}
				
				// 
				callback(nextTime){//callback为构造传入的定义函数
					// 对JobGenerator中的timer = new RecurringTimer()来说就是发送一个GenerateJobs消息
					callback = longTime => eventLoop.post(GenerateJobs(new Time(longTime))){//EventLoop.post(event:T)
						eventQueue.put(event) //JobGenerator.eventLoop.eventQueue: LinkedBlockingDeque
						
						{// 该eventQueue消息队列位于 JobGenerator的成员变量eventLoop中; 
							//GenerateJobs消息会被"JobGenerator"线程的 JobGenerator.processEvent(event)中case GenerateJobs(time) => generateJobs(time)处理
							JobGenerator.start(){//"JobGenerator"线程执行逻辑简述: 1.先调DStreamGraph.generateJobs()根据用户逻辑生成n个Jobs; 2.再将每个Job封装进JobHandler并开启"streaming-job-executor"线程来完成Job的Stage划分和Task提交;
								new EventLoop[JobGeneratorEvent]("JobGenerator").start(){ Thread.run(){
									while (!stopped.get) {
										JobGenerator.onReceive(event){processEvent(event){//JobGenerator.processEvent(event: JobGeneratorEvent)
											case GenerateJobs(time) => generateJobs(time){
												val jobs= graph.generateJobs(time){//DStreamGraph.generateJobs(time): 先创建相关的RDD,并组成若干个Jobs
													// 一次foreachRDD()对应添加一个outputStream, 一个outputStream对应生成一个Job; n次foreachRDD()(或DStream.register())操作对应创建n个Job;
													val jobs = outputStreams.flatMap(outputStream => {outputStream.generateJob(time){//遍历所有outputStreams操作,  ForEachDStream.generateJob(time)
														val someRDD= parent.getOrCompute(time){//递归调用DStream.getOrCompute(time)方法,直到用最早(第一个)DStream算出当前Action的RDD结果;
															createRDDWithLocalProperties(){body{ 
																compute(time){// DStream.compute(time)的实现类
																	DStream.compute(time)// 1. 各DStream的子类,重写其compute(time)方法
																	DirectKafkaInputDStream.compute(time) // 2. 对于DirectKafkaInputDStream,调用其重写的DirectKafkaInputDStream.compute(time)方法;
																}
															}}
														}
													}})
												}
												
												jobs match{case Success(jobs) => jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos)){//将上述创建的Jobs 经由JobScheduler -> SparkContext 提交 DAGScheduler调度, 并最终交由Executor执行;
													jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job))) //分解和提交Job:sc.runJob()->dagScheduler.submitJob(),并给listenerBus发监控消息
												}}
											}
										}}
									}
								}}
							}
						}
					}
				}
				
				prevTime = nextTime
				// 将
				nextTime += period
				logDebug("Callback for " + name + " called at time " + prevTime)
			}
		}
		triggerActionForNextInterval() // 代码同上
    } catch {
		case e: InterruptedException =>
    }
}







/** 启动"JobGenerator"线程, while()循环处理JobGeneratorEvent: GenerateJobs,ClearMetadata
*		case GenerateJobs: generateJobs(time) -> ForEachDStream.generateJob() -> DStream.getOrCompute(time)

	JobGenerator.start(){//"JobGenerator"线程执行逻辑简述: 1.先调DStreamGraph.generateJobs()根据用户逻辑生成n个Jobs; 2.再将每个Job封装进JobHandler并开启"streaming-job-executor"线程来完成Job的Stage划分和Task提交;
		new EventLoop[JobGeneratorEvent]("JobGenerator").start(){ Thread.run(){
			while (!stopped.get) {
				JobGenerator.onReceive(event){processEvent(event){//JobGenerator.processEvent(event: JobGeneratorEvent)
					case GenerateJobs(time) => generateJobs(time){
						val jobs= graph.generateJobs(time){//DStreamGraph.generateJobs(time): 先创建相关的RDD,并组成若干个Jobs
							// 一次foreachRDD()对应添加一个outputStream, 一个outputStream对应生成一个Job; n次foreachRDD()(或DStream.register())操作对应创建n个Job;
							val jobs = outputStreams.flatMap(outputStream => {outputStream.generateJob(time){//遍历所有outputStreams操作,  ForEachDStream.generateJob(time)
								val someRDD= parent.getOrCompute(time){//递归调用DStream.getOrCompute(time)方法,直到用最早(第一个)DStream算出当前Action的RDD结果;
									createRDDWithLocalProperties(){body{ 
										compute(time){// DStream.compute(time)的实现类
											DStream.compute(time)// 1. 各DStream的子类,重写其compute(time)方法
											DirectKafkaInputDStream.compute(time) // 2. 对于DirectKafkaInputDStream,调用其重写的DirectKafkaInputDStream.compute(time)方法;
										}
									}}
								}
							}})
						}
						
						jobs match{case Success(jobs) => jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos)){//将本批生成的Jobs进行提交: 发给ListenerBus和划分Stage/Task并提交;
							jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job))) //遍历每个Job并为之启动一个JobHandler,对应一个"streaming-job-executor-n"线程进行处理: 先Job划分成n个Stage,每个Stage拆分成若干个Tasks提交给"dag-scheduler-event-loop"处理;
						}}
					}
				}}
			}
		}}
	}
*/

JobGenerator.eventLoop.start(){//EventLoop.start()
	{//解释触发机制: 在StreamingContext.start()方法中执行JobScheduler.start()时: jobGenerator.start() -> eventLoop.start()
		ssc.start(){
			state match {case INITIALIZED =>{
				ThreadUtils.runInNewThread("streaming-start") {//在"streaming-start" 线程中,完成"JobScheduler", "JobGenerator"的线程;
					scheduler.start(){//JobScheduler.start() 启动 JobScheduler 相关线程
						new EventLoop[JobSchedulerEvent]("JobScheduler").start() //启动"JobScheduler" 线程
						jobGenerator.start(){ //JobGenerator.start() 方法中完成""
							eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") //创建EventLoop的实现类作为jobGenerator的循环线程方法;
							eventLoop.start()// 这里,正式启动"JobGenerator"线程;
						}
					}
				}
			}}
		}
	}

	onStart(){} // 空代码?
	// 正式启动"JobGenerator"线程:
	eventThread.start(){
		def run(){
			while (!stopped.get) {
				onReceive(event){//抽象类, 由具体继承类实现
					// 1. JobScheduler.onReceive(event: JobSchedulerEvent)
					JobGenerator.eventLoop.onReceive(event: JobSchedulerEvent){
						processEvent(event){//JobGenerator.processEvent(event: JobGeneratorEvent)
							logDebug("Got event " + event)
							event match {
								case GenerateJobs(time) => generateJobs(time){//JobGenerator.generateJobs(time)
									JobGenerator.generateJobs(time: Time){
										ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
										Try {
											jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
											
											/* 第一步: 先创建相关的RDD,并组成1个Job
											*	- OutputDStream.generateJob() 触发 parent.getOrCompute(): 递归生成各级父parentRDD
											* 	- 各父RDD生成后, 再生成 本Action操作的rddOption
											*/
											graph.generateJobs(time){//DStreamGraph.generateJobs(time)
												val jobs = this.synchronized {
												  outputStreams.flatMap { outputStream =>
													val jobOption = outputStream.generateJob(time){//ForEachDStream.generateJob(time)			
														//创建RDD, 
														val someRDD= parent.getOrCompute(time){//DStream.getOrCompute(time)
															generatedRDDs.get(time).orElse {
															  if (isTimeValid(time)) {
																val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
																  SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
																	compute(time)
																  }
																}{//DStream.createRDDWithLocalProperties(time,displayInnerRDDOps)( body: => U): U
																	val prevCallSite = CallSite(
																	  ssc.sparkContext.getLocalProperty(CallSite.SHORT_FORM),
																	  ssc.sparkContext.getLocalProperty(CallSite.LONG_FORM)
																	)
																	val prevScope = ssc.sparkContext.getLocalProperty(scopeKey)
																	val prevScopeNoOverride = ssc.sparkContext.getLocalProperty(scopeNoOverrideKey)
																	try {
																	  if (displayInnerRDDOps) {
																		ssc.sparkContext.setLocalProperty(CallSite.SHORT_FORM, null)
																		ssc.sparkContext.setLocalProperty(CallSite.LONG_FORM, null)
																	  } else {
																		ssc.sparkContext.setCallSite(creationSite)
																	  }
																	  makeScope(time).foreach { s =>
																		ssc.sparkContext.setLocalProperty(scopeKey, s.toJson)
																		if (displayInnerRDDOps) {
																		  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, null)
																		} else {
																		  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, "true")
																		}
																	  }
																	  
																	  // 核心代码在这, body由上述传参进来: => U
																	  body {
																		SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
																			compute(time){//DStream.compute()为抽象方法, 由其继承类实现:
																				//1. 当继承类为 MapPartitioned类型时: 
																				MapPartitionedDStream.compute(){
																					// 原代码: parent.getOrCompute(validTime).map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
																					
																					// 这里递归掉父RDD的getOrCompute(),直到调到 InputDStream()的RDD: KafakRDD, HadoopRDD..
																					val parentRDD = parent.getOrCompute(validTime){//DStream.getOrCompute() 
																						generatedRDDs.get(time).orElse {
																							body{
																								compute(){
																									//... 递归调用
																								}
																							}
																						}
																					}
																					
																					parentRDD.map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
																					
																				}
																				
																				// 2. 当继承类为InputStream类型时: KafakInputDStream, FileInputStream, QueueInputDStream, ReceiverInputDStream等;
																				DirectKafkaInputDStream.compute(time){
																					/* 在SDC-cluster模型中, 涉及kafaka的两个sparkConf参数如下: 
																					*	"spark.streaming.kafka.maxRatePerPartition" -> "400" 执行分区速率的, 由kafakConfigBean.maxRatePerPartition指定;
																					* 	"spark.streaming.kafka.consumer.poll.max.retries" -> "5"	; 默认5, 
																					*/
																					// 计算各Partition/RDD实例 要消费消息的截止位置:utilOffset = 取 额定速率下最大offser 与 最新消息offer 的最小值为utilOffset;
																					val untilOffsets:Map[TopicPartition,Long] = clamp(latestOffsets()){
																						// 这个方法两个作用: poll(0)将新增的分区添加到currentOffsets; 重置所有currentOffsets里所有分区的消费位置到 End位置(是offset最新, 还消息末尾?) 
																						val partsWithLatest:Map[TopicPartition, Long]  = latestOffsets(){//DirectKafkaInputDStream.latestOffsets()		
																							val c = consumer
																							// 作用? paranoid多余的,额外的
																							paranoidPoll(c){//DirectKafkaInputDStream.paranoidPoll(c: Consumer[K, V])
																								val msgs = c.poll(0)//KafkaConsumer.poll(0) 会一直阻塞直到它成功获取了所需的元数据信息,但他一般不返回数据;
																								if (!msgs.isEmpty) {
																									msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
																										val tp = new TopicPartition(m.topic, m.partition)
																										val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
																										acc + (tp -> off)
																									}.foreach { case (tp, off) =>
																										logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate")
																										c.seek(tp, off)
																									}
																								}
																							}
																							// 调用KafkaConsumer.assignment(): Set<TopicPartition> 方法获取本Group绑定的TP及Offset
																							val parts = c.assignment().asScala
																							
																							// 比较出currentOffsets还没有的key, 那就是新分配/绑定的TopicPartiton; 若有新分区,将其消费位置也添加进 currentOffsets:Map[TopicPartition,Long]中;
																							val newPartitions = parts.diff(currentOffsets.keySet)
																							currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap
																							c.pause(newPartitions.asJava) //停止本consumer再从 newPartitions里的分区取拿数据;
																							
																							// 将所有(包括新订阅)的分区,都设置从最末尾位置(end offsets); 从consumer中更新返回;
																							// 这里seek()操作,只在内存中将 TopicPartitionState.resetStrategy设置为 Latest,把TopicPartitionState.position 置为null; 
																							// 在后面的position()操作中,当判断TPState.position==null时, 触发Fetcher.updateFetchPositions()从网络拉取end位置的offset设置TPState.position值;
																							c.seekToEnd(currentOffsets.keySet.asJava){
																								Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
																								for (TopicPartition tp : parts) {
																									log.debug("Seeking to end of partition {}", tp);
																									subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);{//SubscriptionState.needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy)
																										assignedState(partition).awaitReset(offsetResetStrategy);{//TopicPartitionState.awaitReset(OffsetResetStrategy strategy)
																											this.resetStrategy = strategy;
																											this.position = null;
																										}
																									}
																								}
																							}
																							// 在consumer.position(tp)中,触发resetOffset(),从网络查该分区end位置(是所有日志的end/最新消息);
																							parts.map(tp => tp -> {
																								c.position(tp){//KafkaConsumer.position(tp)
																									Long offset = this.subscriptions.position(partition);
																									if (offset == null) {// 当前面执行过 seekToEnd(),(或seek(), seekToBeginning()), 这里的offset就会为null?
																										// 更新获取Offset的位置, 按什么策略更新?
																										updateFetchPositions(Collections.singleton(partition));{//
																											coordinator.refreshCommittedOffsetsIfNeeded();
																											fetcher.updateFetchPositions(partitions);{//Fetcher.updateFetchPositions(tp)
																												// 什么情况下, 要先resetOffset()?  其SubscriptionState.resetStrategy !=null(非空)时,
																												if (subscriptions.isOffsetResetNeeded(tp)) {
																													resetOffset(tp);{//Fetcher.resetOffset(tp)
																														// 更新之前设置的resetStrategy(重置策略:Latest/Earliest/None) 重置其offset
																														OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
																														final long timestamp;
																														if (strategy == OffsetResetStrategy.EARLIEST)
																															timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
																														else if (strategy == OffsetResetStrategy.LATEST)
																															timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
																														else
																															throw new NoOffsetForPartitionException(partition);

																														log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
																														// 真正完成网络获取offset位置的请求: 发异步请求,并循环等待future.succeeded
																														long offset = listOffset(partition, timestamp);{//Fetcher.listOffset(tp,time)
																															while(true){
																																RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
																																client.poll(future);
																																if (future.succeeded()){//查询成功,就中断循环并返回;
																																	return future.value();
																																}
																																if (future.exception() instanceof InvalidMetadataException){
																																	client.awaitMetadataUpdate();
																																}else{
																																	time.sleep(retryBackoffMs);//重试期间,随眠等待 n毫秒
																																}
																															}
																														}
																														// 将本consumer订阅的TP.消费offset位置
																														if (subscriptions.isAssigned(partition)){
																															this.subscriptions.seek(partition, offset);{//SubscriptionState.seek()
																																//class TopicPartitionState(Long position, OffsetAndMetadata committed, boolean paused, OffsetResetStrategy resetStrategy)
																																assignedState(tp).seek(offset);{//TopicPartitionState.seek(long offset)
																																	this.position = offset;
																																	this.resetStrategy = null;
																																}
																															}
																														}
																													}
																												}
																											}
																										}
																										offset = this.subscriptions.position(partition);
																									}
																									return offset;
																								}
																							}).toMap
																							
																						}
																						
																						val clampParts:Map[TopicPartition, Long]= clamp(partsWithLatest){//DirectKafkaInputDStream.clamp(offsets)
																							
																							// 计算每个partiton要处理的最大消息数: 1.先读取计算各Partiton单批次最大处理消息数量; 2. 再与秒级graph.batchDuration值相乘, 求出本批次最大处理数;
																							val someParts = maxMessagesPerPartition(offsets){//DirectKafkaInputDStream.maxMessagesPerPartition()
																								// 反压机制的限速, estimatedRateLimit 表示本批最大处理这个多个数据
																								val estimatedRateLimit = rateController.map(_.getLatestRate(){// 执行RateController.getLatestRate()
																									return rateLimit.get() //rateLimit:AtomicLong = new AtomicLong(-1L) 
																									
																									// RateController.rateLimit的值在"stream-rate-update"线程中, 由PIDRateEstimator.compute()方法算出
																									{
																										// 是在"stream-rate-update"线程中接受处理;  StreamingListenerBus.doPostEvent() case StreamingListenerBatchCompleted时: listener.onBatchCompleted()
																										RateController.onBatchCompleted(){
																											for(elems <- elements.get(streamUID).map(_.numRecords)){
																												computeAndPublish(processingEnd, elems, workDelay, waitDelay){
																													val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay){//PIDRateEstimator.compute()
																														this.synchronized {
																														  if (time > latestTime && numElements > 0 && processingDelay > 0) {
																															// 算出与上一个批次的 秒级 时间差
																															val delaySinceUpdate = (time - latestTime).toDouble / 1000
																															// 回授值: processingDelay是处理用时? 计算秒级的处理速度: (数据量/毫秒用时)*1000 ?
																															val processingRate = numElements.toDouble / processingDelay * 1000
																															//误差: error: 这次下降了多少速率? 
																															val error = latestRate - processingRate
																															
																															// 过去累积误差: 单位: eles/sec 调度用时占用了多少数据; 再摊分到 200毫秒的间隔中, 每1毫秒会delay多少个数据?
																															val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis
																															// 斜率:  每分钟下降的斜率; 下降速度?
																															val dError = (error - latestError) / delaySinceUpdate
																															
																															/* PID控制器: 工控领域常见PID控制器的思想。
																															*	即比例（Proportional）-积分（Integral）-微分（Derivative）控制器，
																															* 	本质上是一种反馈回路（loop feedback）。它把收集到的数据和一个设定值（setpoint）进行比较，
																															* 	然后用它们之间的差计算新的输入值，该输入值可以让系统数据尽量接近或者达到设定值
																															*/
																															val newRate = (latestRate - 
																																proportional*error - 	//比例增益,当前误差		proportional=conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
																																integral* historicalError - //积分增益,累积误差	integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
																																derivative* dError)		//微分增益,将来误差 	derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
																																.max(minRate)			//与minRate取最大值; minRate=conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
																															
																															
																															latestTime = time
																															if (firstRun) {
																															  latestRate = processingRate
																															  latestError = 0D
																															  firstRun = false
																															  logTrace("First run, rate estimation skipped")
																															  None
																															} else {
																															  latestRate = newRate
																															  latestError = error
																															  logTrace(s"New rate = $newRate")
																															  Some(newRate)
																															}
																														  } else {
																															logTrace("Rate estimation skipped")
																															None
																														  }
																														}
																													}
																													
																													newRate.foreach { s =>
																														rateLimit.set(s.toLong)
																														publish(getLatestRate())
																													}
																												}
																											}
																										}
																									}
																								})
																								
																								val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
																									case Some(rate) =>
																										// 先算每个分区 这个batch要拉取多少数据:lag; 让后总拉取数据量: totalLag
																										val lagPerPartition = offsets.map { case (tp, offset) =>
																											tp -> Math.max(offset - currentOffsets(tp), 0)
																										}
																										val totalLag = lagPerPartition.values.sum
																										
																										lagPerPartition.map { case (tp, lag) =>
																											// spark.streaming.kafka.maxRatePerPartition设置的分区上限;
																											val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)//return maxRate=conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
																											
																											// 该分区应消费数据比重 * 本批可处理数量总量 
																											val backpressureRate = Math.round(lag / totalLag.toFloat * rate)
																											tp -> (if (maxRateLimitPerPartition > 0) {
																													// 该分区限压后处理数据量, 与用户设置的maxRateLimitPerPartition 中,取最小值为真正数据量;
																													Math.min(backpressureRate, maxRateLimitPerPartition)
																												} else backpressureRate
																											)
																											
																										}
																									//为空时, 进入这里;
																									case None =>{
																										offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp){
																											val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
																											return maxRate;
																										}}
																									} 
																								}
																								
																								if (effectiveRateLimitPerPartition.values.sum > 0) {//当所有分区 maxRate之和>0, 即有流量限制时,
																									// 将spark微批处理间隔graph.batchDuration, 转换成 秒级单位: 例如 200ms -> 0.2秒
																									//在SDC.cluster运行时, batchDuration==Utils.getKafkaMaxWaitTime(getProperties())== kafkaConfigBean.maxWaitTime的值; 目前2秒?
																									val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
																									
																									// 计算本批次, 对每个RDD/分区要发送Record的数量(及速率限制 RateLimitPerPartition): 批次时长0.2 * 分区处理上限数 500 = 100个数据;
																									Some(effectiveRateLimitPerPartition.map {
																										case (tp, limit) => tp -> (secsPerBatch * limit).toLong
																									})
																								} else {
																									None
																								}
																							}
																							
																							// 取 额定速率下最大offser 与 最新消息offer, 两者最小值为本批次实际untilOffset.
																							someParts.map { mmp =>
																								mmp.map { case (tp, messages) =>
																									val uo = offsets(tp) //offsets即为传参进来的, 各分区的Latest Offset值(即最新的消息)
																									// 取 速率上限offser和 最新日志offset的最小值,为本batch的untilOffset(消费截止offset); 这样保证了不消费还没到的消息; 当没有server没有新消息时, 这个值是相等的.
																									// currentOffsets(tp) + messages 就是额定速率消费offser上限;
																									// uo 就是之前latestOffsets()方法求出的Server端该分区最新消费的offset;
																									tp -> Math.min(currentOffsets(tp) + messages, uo)
																							  }
																							}.getOrElse(offsets)
																						}
																						
																						clampParts
																					}
																					
																					//取上batch的currentOffsets为起始点fromOffset, 并与untilOffset一起封装到OffsetRange对象中;
																					val offsetRanges = untilOffsets.map { case (tp, uo) =>
																						val fo = currentOffsets(tp)
																						OffsetRange(tp.topic, tp.partition, fo, uo)
																					}
																					
																					val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled", true)
																					val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, useConsumerCache)
																					val description = offsetRanges
																						.filter{ offsetRange => offsetRange.fromOffset != offsetRange.untilOffset }
																						.map{ offsetRange => s"topic: " + s"offsets: " }.mkString("\n")
																					
																					// Copy offsetRanges to immutable.List to prevent from being modified by the user
																					val metadata = Map("offsets" -> offsetRanges.toList, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
																					val inputInfo = StreamInputInfo(id, rdd.count, metadata)
																					ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
																					currentOffsets = untilOffsets
																					commitAll()
																					Some(rdd)												
																				}
																				
																			}
																		}
																	  }
																	  
																	} finally {
																	  ssc.sparkContext.setCallSite(prevCallSite)
																	  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, prevScopeNoOverride)
																	}
																}
																
																rddOption.foreach { case newRDD =>
																  if (storageLevel != StorageLevel.NONE) {
																	newRDD.persist(storageLevel)
																	logDebug(s"Persisting RDD ${newRDD.id} for time $time to $storageLevel")
																  }
																  if (checkpointDuration != null && (time - zeroTime).isMultipleOf(checkpointDuration)) {
																	newRDD.checkpoint()
																	logInfo(s"Marking RDD ${newRDD.id} for time $time for checkpointing")
																  }
																  generatedRDDs.put(time, newRDD)
																}
																rddOption
															  } else {
																None
															  }
															}				
														}
														
														someRDD match {
														  case Some(rdd) =>
															val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
															  foreachFunc(rdd, time)
															}
															Some(new Job(time, jobFunc))
														  case None => None
														}
													}
													jobOption.foreach(_.setCallSite(outputStream.creationSite))
													jobOption
												  }
												}
												logDebug("Generated " + jobs.length + " jobs for time " + time)
												jobs
											}
										} match {
											case Success(jobs) => {
												val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
												/* 第二步: 创建一个线程任务JobHandle,并交给线程池jobExecutor: TheadPoolExecutor的workQueue队列中;
												* 	- 遍历每个Job(JobSet),并为每个Job启动一个JobHandler对应的"streaming-job-executor-n"线程,处理;
												* 	- 在每一个JobHandler(job)中,执行Job.run() -> UserFunc.func()->sc.runJob() -> DAGScheduler.runJob()划分Stage ->DAGScheduler.submitStage()切分Stage成Tasks并提交;
												*/
												jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos)){//JobScheduler.submitJobSet(jobSet)
													JobScheduler.submitJobSet(jobSet){
														if (jobSet.jobs.isEmpty) {
														  logInfo("No jobs added for time " + jobSet.time)
														} else {
														  listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
														  jobSets.put(jobSet.time, jobSet)
														  
														  // 创建一个线程任务JobHandle,并交给线程池jobExecutor: TheadPoolExecutor的workQueue队列中
														  jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job))){
															//"streaming-job-executor-n" 线程执行原理: 先是将JobStarted发送给sparkListenerBus, 再执行dagScheduler.runJob()等完成划分Stage,拆分并提交Tasks(给"dag-scheduler-event-loop"线程)的任务;
															new JobHandler(job).run(){
																eventLoop.post(JobStarted(job, clock.getTimeMillis()));//向StreamingJobProgressListener发JobStarted消息, 用于监控/展示;
																SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {job.run(){Try(func()){ //func()= ForEachDStream.generateJob().jobFunc() = () => createRDDWithLocalProperties(time, displayInnerRDDOps)
																	//createRDDWithLocalProperties()将当前线程变量中 spark.rdd.scope/scope.noOverride/callSite.short/ callSite.long等4个ThradLocal变量值移除和重新赋值;
																	body{
																		ForEachDStream.foreachFunc(rdd, time){
																			dstream.foreachRDD(rdd){//DStream.foreachRDD()或DStream的子类触发其foreachRDD()方法;
																				rdd.count()/rdd.foreach()/rdd.collect();{//进行了 触发了RDD的Action动作的操作;
																					sc.runJob(){//SparkContext.runJob()
																						dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get){//DAGScheduler.runJob
																							val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties){//DAGScheduler.submitJob(rdd)
																								eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions, callSite)){//DAGScheudlerEventProcessLoop.post()
																									eventQueue.put(event)// eventQueue:LinkedBlockQueue, 这里只是将JobSubmitted消息缓存与eventQueue队列中,等待"dag-scheduler-event-loop"线程来将Job切分成Stags/Tasks并提交;
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																}}}
															}
														  }
														
														  
														  logInfo("Added jobs for time " + jobSet.time)
														}
													}
												}
											}
											
											case Failure(e) =>
												jobScheduler.reportError("Error generating jobs for time " + time, e)
												PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
										}
										
										eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
									}
								}
								
								// 在完成Job任务后, 从内存和磁盘清空相关数据? 至少在SparkContext.persistentRdds:Map[Int,RDD]会移除相关RDD;
								case ClearMetadata(time) => clearMetadata(time);{//JobGenerator.clearMetadata()
									
								}
								
								case DoCheckpoint(time, clearCheckpointDataLater) => doCheckpoint(time, clearCheckpointDataLater)
								
								case ClearCheckpointData(time) => clearCheckpointData(time)
							}
						}
					}
					
					// 2. 其他eventLoop实现类:  JobScheduler.eventLoop.onReceive(); DAGSchedulerEventProcessLoop.onReceive()
					
				}
			}
		}
	}
	
}


Checkpoint删除相关线程

"JobGenerator"线程的Checkpoint.write()


/* 线程"pool-21-thread-1": 线程池,运行该线程;
* 
*/
Checkpoint.write(checkpoint: Checkpoint, clearCheckpointDataLater: Boolean){
	{
		JobGenerator.processEvent(event): case DoCheckpoint => doCheckpoint(time, clearCheckpointDataLater);{//JobGenerator.doCheckpoint()
			checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater);{
				executor.execute(new CheckpointWriteHandler());// 启动一个线程,来处理checkpoint逻辑;
			}
		}
	}
	
	val bytes = Checkpoint.serialize(checkpoint, conf)
	
    executor.execute(new CheckpointWriteHandler(checkpoint.checkpointTime, bytes, clearCheckpointDataLater));{
		CheckpointWriteHandler.run(){
			if (latestCheckpointTime == null || latestCheckpointTime < checkpointTime) {
				latestCheckpointTime = checkpointTime
			}
			if (fs == null) {
				fs = new Path(checkpointDir).getFileSystem(hadoopConf)
			}
			var attempts = 0
			val startTime = System.currentTimeMillis()
			val tempFile = new Path(checkpointDir, "temp")
			//地址路径: /user/app/.streamsets-spark-streaming/631bab90-6599-11e9-ba99-fda7ac6ea0aa/HJQ_testKafkaPerPartition/sdc_online_debug_group2/ClusterOriginKafkaToKafkaT41271c14-12a8-4761-8f79-4901d995cd88/rdd-checkpoints/checkpoint-1589784175000
			val checkpointFile = Checkpoint.checkpointFile(checkpointDir, latestCheckpointTime)
			val backupFile = Checkpoint.checkpointBackupFile(checkpointDir, latestCheckpointTime)
			while (attempts < MAX_ATTEMPTS && !stopped) {
				attempts += 1
				try {
				  logInfo(s"Saving checkpoint for time $checkpointTime to file '$checkpointFile'")
				  // Write checkpoint to temp file
				  fs.delete(tempFile, true) // just in case it exists
				  val fos = fs.create(tempFile)//先删除,再创建; 如果该路径同时被访问,就可能报异常; 当会被catch;
				  Utils.tryWithSafeFinally {
					fos.write(bytes) // 将序列化后的的RDD数据,存入;
				  } {
					fos.close()
				  }
				  // If the checkpoint file exists, back it up
				  // If the backup exists as well, just delete it, otherwise rename will fail
				  if (fs.exists(checkpointFile)) {
					fs.delete(backupFile, true) // just in case it exists
					if (!fs.rename(checkpointFile, backupFile)) {
					  logWarning(s"Could not rename $checkpointFile to $backupFile")
					}
				  }
				  // Rename temp file to the final checkpoint file
				  if (!fs.rename(tempFile, checkpointFile)) {//修改名字,从temp -> checkpoint-1589784175000;
					logWarning(s"Could not rename $tempFile to $checkpointFile")
				  }
				  // Delete old checkpoint files
				  val allCheckpointFiles = Checkpoint.getCheckpointFiles(checkpointDir, Some(fs))
				  if (allCheckpointFiles.size > 10) {//当old数据大于10个时,删除多余的,只留下10个Checkpoint数据;
					allCheckpointFiles.take(allCheckpointFiles.size - 10).foreach { file =>
					  logInfo(s"Deleting $file")
					  fs.delete(file, true)
					}
				  }
				  // All done, print success
				  val finishTime = System.currentTimeMillis()
				  logInfo(s"Checkpoint for time $checkpointTime saved to file '$checkpointFile'" +
					s", took ${bytes.length} bytes and ${finishTime - startTime} ms")
				  jobGenerator.onCheckpointCompletion(checkpointTime, clearCheckpointDataLater);{//发送一个ClearCheckpointData事件;
						if (clearCheckpointDataLater) {
							eventLoop.post(ClearCheckpointData(time))
						}
				  }
				  return
				} catch {
				  case ioe: IOException =>
					val msg = s"Error in attempt $attempts of writing checkpoint to '$checkpointFile'"
					logWarning(msg, ioe)
					fs = null
				}
				
			}
			logWarning(s"Could not write checkpoint for time $checkpointTime to file '$checkpointFile'")
		}
	}
    
	logInfo(s"Submitted checkpoint of time ${checkpoint.checkpointTime} to writer queue")
	
}






	存于内充中的currentOffsets,所在类位置: jobScheduler.jobGenerator.graph[DStreamGraph].outputStreams[ArrayBuff[ForEachDStream]].parent[DStream] == DirectKafkaInputDStream.currentOffsets;
		* 这个DirectKafkaDStream.compute()在每个batch的第一个计算该Timer时刻的KafkaRDD是把算好的KafakRDD以该batch-Time为key存于DStream类的
		
		KafakDStream一直在AM进程中,持续运行10天以上,应该是线程安全的, 所以不可能是重启任务new DirectInputKafakDStream导致的;只能是运行时出异常导致;
		
		* 关于untilOffset的算法:
			- 1. 获取server端最新数据位移量: latestOffset= latestOffsets()方法, KafakServer端.TP.highest offset(End位置的消息, 最新接受的数据); (假设夏远远说的(leader hw高于副本)不成立: 即 consumer.seetToEnd()+position() 永远>=上次读取到的数据);
			- 2. 计算限速机制下, 某分区本Batch的的最大消息数量 maxMessageNum: 
				- A. 基于上个batch速率,算出本Batch所有分区的最大 处理速率: estimatedRateLimit: 
				- B. 计算某分区应处理积压数据量(/每秒); 与用户配置的kafka.maxRatePerPartition值比较,取其中最小值为有效限速值: effectiveRateLimit;
					* 先算各分区积压量占比, 再*上述 最大速率,得到 本分区应处理积压数据量;
				- C. 将上述effectiveRateLimit * 秒级微批时间(5秒)= 本批次该分区最大处理消息量: maxMessageNum;
			- 3. 依据限速下的分区最大消息量 和 内存中上一个Batch发送的位移量currentOffset, 计算出限速机制所要求的结尾位移量: untilOffset_limitRateBased;
			- 3. 取 latestOffset 和 untilOffset_limitRateBased 的最小值,作为 untilOffset最终结果;
			
			总结: untileOffset= 最新数据  or 历史速率与lag占比算出的最大消息量 or 用户配置的kafka.maxRatePerP速率(500), 取最小值;
		
		* 要出现 untilOffset < currentOffset;只有2中可能: 
			1. untileOffset 异常变小;
			2. currentOffset 异常变大;
			
		* 分析1: 若要untilOffset 异常变小, 
			- 最新数据 < current ? 不可能;
			- 限速算出的最大消息量 < current ? 不可能, 顶多==current ;
			
		* 只有可能是current增大了; 而untilOffset那的还是 最大消息与 限速最大值速率的 最小值; 
		
		
		
		
		
	// 在"JobGenerator"线程中, 算出本batch的KafkaRDD要RDD分区即各分区消费数据范围(offsetRange);
	DirectKafkaInputDStream.compute(time){
		/* 在SDC-cluster模型中, 涉及kafaka的两个sparkConf参数如下: 
		*	"spark.streaming.kafka.maxRatePerPartition" -> "400" 执行分区速率的, 由kafakConfigBean.maxRatePerPartition指定;
		* 	"spark.streaming.kafka.consumer.poll.max.retries" -> "5"	; 默认5, 
		*/
		// 计算各Partition/RDD实例 要消费消息的截止位置:utilOffset = 取 额定速率下最大offser 与 最新消息offer 的最小值为utilOffset;
		val untilOffsets:Map[TopicPartition,Long] = clamp(latestOffsets()){
			// 这个方法两个作用: poll(0)将新增的分区添加到currentOffsets; 重置所有currentOffsets里所有分区的消费位置到 End位置(是offset最新, 还消息末尾?) 
			val partsWithLatest:Map[TopicPartition, Long]  = latestOffsets(){//DirectKafkaInputDStream.latestOffsets()		
				val c = consumer
				// 作用? paranoid多余的,额外的
				paranoidPoll(c){//DirectKafkaInputDStream.paranoidPoll(c: Consumer[K, V])
					val msgs = c.poll(0)//KafkaConsumer.poll(0) 会一直阻塞直到它成功获取了所需的元数据信息,但他一般不返回数据;
					if (!msgs.isEmpty) {
						msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
							val tp = new TopicPartition(m.topic, m.partition)
							val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
							acc + (tp -> off)
						}.foreach { case (tp, off) =>
							logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate")
							c.seek(tp, off)
						}
					}
				}
				// 调用KafkaConsumer.assignment(): Set<TopicPartition> 方法获取本Group绑定的TP及Offset
				val parts = c.assignment().asScala
				
				// 比较出currentOffsets还没有的key, 那就是新分配/绑定的TopicPartiton; 若有新分区,将其消费位置也添加进 currentOffsets:Map[TopicPartition,Long]中;
				val newPartitions = parts.diff(currentOffsets.keySet)
				currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap
				c.pause(newPartitions.asJava) //停止本consumer再从 newPartitions里的分区取拿数据;
				
				// 将所有(包括新订阅)的分区,都设置从最末尾位置(end offsets); 从consumer中更新返回;
				// 这里seek()操作,只在内存中将 TopicPartitionState.resetStrategy设置为 Latest,把TopicPartitionState.position 置为null; 
				// 在后面的position()操作中,当判断TPState.position==null时, 触发Fetcher.updateFetchPositions()从网络拉取end位置的offset设置TPState.position值;
				c.seekToEnd(currentOffsets.keySet.asJava){
					Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
					for (TopicPartition tp : parts) {
						log.debug("Seeking to end of partition {}", tp);
						subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);{//SubscriptionState.needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy)
							assignedState(partition).awaitReset(offsetResetStrategy);{//TopicPartitionState.awaitReset(OffsetResetStrategy strategy)
								this.resetStrategy = strategy;
								this.position = null;
							}
						}
					}
				}
				// 在consumer.position(tp)中,触发resetOffset(),从网络查该分区end位置(是所有日志的end/最新消息);
				parts.map(tp => tp -> {
					c.position(tp){//KafkaConsumer.position(tp)
						Long offset = this.subscriptions.position(partition);
						if (offset == null) {// 当前面执行过 seekToEnd(),(或seek(), seekToBeginning()), 这里的offset就会为null?
							// 更新获取Offset的位置, 按什么策略更新?
							updateFetchPositions(Collections.singleton(partition));{//
								coordinator.refreshCommittedOffsetsIfNeeded();
								fetcher.updateFetchPositions(partitions);{//Fetcher.updateFetchPositions(tp)
									// 什么情况下, 要先resetOffset()?  其SubscriptionState.resetStrategy !=null(非空)时,
									if (subscriptions.isOffsetResetNeeded(tp)) {
										resetOffset(tp);{//Fetcher.resetOffset(tp)
											// 更新之前设置的resetStrategy(重置策略:Latest/Earliest/None) 重置其offset
											OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
											final long timestamp;
											if (strategy == OffsetResetStrategy.EARLIEST)
												timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
											else if (strategy == OffsetResetStrategy.LATEST)
												timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
											else
												throw new NoOffsetForPartitionException(partition);

											log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
											// 真正完成网络获取offset位置的请求: 发异步请求,并循环等待future.succeeded
											long offset = listOffset(partition, timestamp);{//Fetcher.listOffset(tp,time)
												while(true){
													RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
													client.poll(future);
													if (future.succeeded()){//查询成功,就中断循环并返回;
														return future.value();
													}
													if (future.exception() instanceof InvalidMetadataException){
														client.awaitMetadataUpdate();
													}else{
														time.sleep(retryBackoffMs);//重试期间,随眠等待 n毫秒
													}
												}
											}
											// 将本consumer订阅的TP.消费offset位置
											if (subscriptions.isAssigned(partition)){
												this.subscriptions.seek(partition, offset);{//SubscriptionState.seek()
													//class TopicPartitionState(Long position, OffsetAndMetadata committed, boolean paused, OffsetResetStrategy resetStrategy)
													assignedState(tp).seek(offset);{//TopicPartitionState.seek(long offset)
														this.position = offset;
														this.resetStrategy = null;
													}
												}
											}
										}
									}
								}
							}
							offset = this.subscriptions.position(partition);
						}
						return offset;
					}
				}).toMap
				
			}
			
			val clampParts:Map[TopicPartition, Long]= clamp(partsWithLatest){//DirectKafkaInputDStream.clamp(offsets)
				
				// 计算每个partiton要处理的最大消息数: 1.先读取计算各Partiton单批次最大处理消息数量; 2. 再与秒级graph.batchDuration值相乘, 求出本批次最大处理数;
				val someParts = maxMessagesPerPartition(offsets){//DirectKafkaInputDStream.maxMessagesPerPartition()
					// 限速机制下: 本batch每秒最大处理数据量 estimatedRateLimit : 总数据量/second
					val estimatedRateLimit = rateController.map(_.getLatestRate(){// 执行RateController.getLatestRate()
						return rateLimit.get() //rateLimit:AtomicLong = new AtomicLong(-1L) 
						
						// RateController.rateLimit的值在"stream-rate-update"线程中, 由PIDRateEstimator.compute()方法算出
						{
							// 是在"stream-rate-update"线程中接受处理;  StreamingListenerBus.doPostEvent() case StreamingListenerBatchCompleted时: listener.onBatchCompleted()
							RateController.onBatchCompleted(){
								for(elems <- elements.get(streamUID).map(_.numRecords)){
									computeAndPublish(processingEnd, elems, workDelay, waitDelay){
										val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay){//PIDRateEstimator.compute()
											this.synchronized {
											  if (time > latestTime && numElements > 0 && processingDelay > 0) {
												// 算出与上一个批次的 秒级 时间差
												val delaySinceUpdate = (time - latestTime).toDouble / 1000
												// 回授值: processingDelay是处理用时? 计算秒级的处理速度: (数据量/毫秒用时)*1000 ?
												val processingRate = numElements.toDouble / processingDelay * 1000
												//误差: error: 这次下降了多少速率? 
												val error = latestRate - processingRate
												
												// 过去累积误差: 单位: eles/sec 调度用时占用了多少数据; 再摊分到 200毫秒的间隔中, 每1毫秒会delay多少个数据?
												val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis
												// 斜率:  每分钟下降的斜率; 下降速度?
												val dError = (error - latestError) / delaySinceUpdate
												
												/* PID控制器: 工控领域常见PID控制器的思想。
												*	即比例（Proportional）-积分（Integral）-微分（Derivative）控制器，
												* 	本质上是一种反馈回路（loop feedback）。它把收集到的数据和一个设定值（setpoint）进行比较，
												* 	然后用它们之间的差计算新的输入值，该输入值可以让系统数据尽量接近或者达到设定值
												*/
												val newRate = (latestRate - 
													proportional*error - 	//比例增益,当前误差		proportional=conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
													integral* historicalError - //积分增益,累积误差	integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
													derivative* dError)		//微分增益,将来误差 	derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
													.max(minRate)			//与minRate取最大值; minRate=conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
												
												
												latestTime = time
												if (firstRun) {
												  latestRate = processingRate
												  latestError = 0D
												  firstRun = false
												  logTrace("First run, rate estimation skipped")
												  None
												} else {
												  latestRate = newRate
												  latestError = error
												  logTrace(s"New rate = $newRate")
												  Some(newRate)
												}
											  } else {
												logTrace("Rate estimation skipped")
												None
											  }
											}
										}
										
										newRate.foreach { s =>
											rateLimit.set(s.toLong)
											publish(getLatestRate())
										}
									}
								}
							}
						}
					})
					
					// 计算某分区积压占比,及该分区应处理积压数据量; 与用户配置的kafka.maxRatePerPartition值比较,取最小值做为有效RateLimitPerPartiton;
					val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {// 对速率进行过滤,只有rate为正,才会进入;
						case Some(rate) => //rate表示限速机制下的 本批次,每秒最多处理多大的数据量;
							// 先算每个分区 这个batch要拉取多少数据:lag; 让后总拉取数据量: totalLag
							// 这个offsets变量就是前面算出的latestOffsets,
							val lagPerPartition = offsets.map { case (tp, offset) =>
								// 计算出每个分区的 最新数据 与 currentOffset之间的差距,即每个分区的积压数据量Lag; 
								tp -> Math.max(offset - currentOffsets(tp), 0)
							}
							val totalLag = lagPerPartition.values.sum //计算出所有分区的总积压数据量;
							
							lagPerPartition.map { case (tp, lag) =>
								// spark.streaming.kafka.maxRatePerPartition设置的分区上限; 客户当时的配置是100*5= 500;
								val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)//return maxRate=conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
								
								// 该分区积压数据量比重 * 本批应处理总数据量 = 该分区应处理的积压数据量==backpressureRate;
								val backpressureRate = Math.round(lag / totalLag.toFloat * rate) //某分区应处理的积压数;
								tp -> (if (maxRateLimitPerPartition > 0) {//若用户配置的 kafka.maxRatePerPartition>0,则在 maxRatePerPartition 与 应处理积压量之间,取最小值;
										// 该分区限压后处理数据量, 与用户设置的maxRateLimitPerPartition 中,取最小值为真正数据量;
										Math.min(backpressureRate, maxRateLimitPerPartition)
									} else backpressureRate
								)
								
							}
						//为空时, 进入这里;
						case None =>{
							offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp){
								val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
								return maxRate;
							}}
						} 
					}
					
					if (effectiveRateLimitPerPartition.values.sum > 0) {//当所有分区 maxRate之和>0, 即有流量限制时,
						// 将spark微批处理间隔graph.batchDuration, 转换成 秒级单位: 例如 200ms -> 0.2秒
						//在SDC.cluster运行时, batchDuration==Utils.getKafkaMaxWaitTime(getProperties())== kafkaConfigBean.maxWaitTime的值; 目前2秒?
						val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
						
						// 计算本批次, 对每个RDD/分区要发送Record的数量(及速率限制 RateLimitPerPartition): 批次时长0.2 * 分区处理上限数 500 = 100个数据;
						Some(effectiveRateLimitPerPartition.map {
							case (tp, limit) => tp -> (secsPerBatch * limit).toLong
						})
					} else {
						None
					}
				}
				
				// 取 额定速率下最大offser 与 最新消息offer, 两者最小值为本批次实际untilOffset.
				someParts.map { mmp =>
					mmp.map { case (tp, messages) =>
						val uo = offsets(tp) //offsets即为传参进来的, 各分区的Latest Offset值(即最新的消息)
						// 取 速率上限offser和 最新日志offset的最小值,为本batch的untilOffset(消费截止offset); 这样保证了不消费还没到的消息; 当没有server没有新消息时, 这个值是相等的.
						// currentOffsets(tp) + messages 就是额定速率消费offser上限;
						// uo 就是之前latestOffsets()方法求出的Server端该分区最新消费的offset;
						tp -> Math.min(currentOffsets(tp) + messages, uo)
				  }
				}.getOrElse(offsets)
			}
			
			clampParts
		}
		
		
		//取上batch的currentOffsets为起始点fromOffset, 并与untilOffset一起封装到OffsetRange对象中;
		val offsetRanges = untilOffsets.map { case (tp, uo) =>
			val fo = currentOffsets(tp)
			OffsetRange(tp.topic, tp.partition, fo, uo)
		}
		
		val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled", true)
		val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, useConsumerCache)
		val description = offsetRanges
			.filter{ offsetRange => offsetRange.fromOffset != offsetRange.untilOffset }
			.map{ offsetRange => s"topic: " + s"offsets: " }.mkString("\n")
		
		// Copy offsetRanges to immutable.List to prevent from being modified by the user
		val metadata = Map("offsets" -> offsetRanges.toList, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
		val inputInfo = StreamInputInfo(id, rdd.count, metadata)
		ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
		currentOffsets = untilOffsets
		commitAll()
		Some(rdd)												
	}



/** "streaming-job-executor-n" 线程, 分解和提交Job,并给listenerBus发监控消息; JobStart后,会一直阻塞在Job.run()直到UserJobFunc中RDD的逻辑都执行完毕,才会发JobComputled;
*		- 在JobScheduler.generateJobs(time).submitJobSet(jobSet)["JobScheduler"线程]方法中启动该线程;
	在该线程中,先是将JobStarted消息发送给sparkListenerBus, 封装进UIData用于监控和Web可视化;
	执行该Job.func方法(该函数是new Job(time,func)时传入的), 该job.func函数实际=DStream.foreachRDD(()->jobFunc)中的jobFunc, 
	new JobHandler(job).run(){
		eventLoop.post(JobStarted(job, clock.getTimeMillis()));//向StreamingJobProgressListener发JobStarted消息, 用于监控/展示;
		SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {job.run(){Try(func()){ //func()= ForEachDStream.generateJob().jobFunc() = () => createRDDWithLocalProperties(time, displayInnerRDDOps)
			//createRDDWithLocalProperties()将当前线程变量中 spark.rdd.scope/scope.noOverride/callSite.short/ callSite.long等4个ThradLocal变量值移除和重新赋值;
			body{
				ForEachDStream.foreachFunc(rdd, time){
					dstream.foreachRDD(rdd){//DStream.foreachRDD()或DStream的子类触发其foreachRDD()方法;
						rdd.count()/rdd.foreach()/rdd.collect();{//进行了 触发了RDD的Action动作的操作;
							sc.runJob(){//SparkContext.runJob()
								dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get){//DAGScheduler.runJob
									val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties){//DAGScheduler.submitJob(rdd)
										eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions, callSite)){//DAGScheudlerEventProcessLoop.post()
											eventQueue.put(event)// eventQueue:LinkedBlockQueue, 这里只是将JobSubmitted消息缓存与eventQueue队列中,等待"dag-scheduler-event-loop"线程来将Job切分成Stags/Tasks并提交;
										}
									}
								}
							}
						}
					}
				}
			}
		}}}
	}
	
	关于Job.run()和UserJobFunc的执行情况: 
	- 如果UserJobFunc中,没有rdd.collect()这类需要
	
*/

jobExecutor.execute(new JobHandler(job)){
	JobScheduler.JobHandler.run(){
		val oldProps = ssc.sparkContext.getLocalProperties
		try {
			ssc.sparkContext.setLocalProperties(SerializationUtils.clone(ssc.savedProperties.get()))
			val formattedTime = UIUtils.formatBatchTime(job.time.milliseconds, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
			val batchUrl = s"/streaming/batch/?id=${job.time.milliseconds}"
			val batchLinkText = s"[output operation ${job.outputOpId}, batch time ${formattedTime}]"
			ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
			var _eventLoop = eventLoop
			if (_eventLoop != null) {//进入这里
				
				// 将该Job提交到 StreamingJobProgressListener.runningBatchUIData[Map[Time,BatchUIData]]中
				// 发送 StreamingListenerOutputOperationStarted 给 sparkListenerBus, 这个主要是用作监控/展示?
				eventLoop.post(JobStarted(job, clock.getTimeMillis())){//EventLoop.post(event)
					eventQueue[LinkedBlockingDeque].put(event);
					
					{
						//JobScheduler.start()中 启动"JobScheduler"线程, 循环将接受 JobStarted(包括,等JobSchedulerEvent)的 onReceive()方法-> JobScheduler.handleJobStart()
						JobScheduler.handleJobStart(job, startTime){
							if (isFirstJobOfJobSet) {
								listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo)){
									sparkListenerBus.post(new WrappedStreamingListenerEvent(event))
								}
							}
							listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
						}
					}
				}
				
				/** 核心代码: 触发一个Job的提交: Job.run() -> UserFunc.func() -> sc.runJob() -> DAGScheduler.submitStage() -> TaskSchedulerImpl.submitTasks()
				* 	UserFunc.func()必须是一个能触发Action RDD的逻辑, 如: rdd.collect(), rdd.foreach(), rdd.count()
				* 
				*/
				SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
					job.run(){//Job.run()
						_result = Try(func()){
							
							// func()由new Job()时构造传入; new Job()只发生在DStream.generateJob()中;DStream众多子类中只有ForEachDStream重写了该方法; 或者DStream直接调register()主动添加到outputDStream;
							
							// 对于由ds.foreachRDD()触发的逻辑, 都会相应new ForEachDStream()并执行其generateJob(), 其中定义的jobFunc逻辑如下:
							ForEachDStream.generateJob().jobFunc = 		() => createRDDWithLocalProperties(time, displayInnerRDDOps) {
								foreachFunc(rdd, time)
							}{
								DStream.createRDDWithLocalProperties[U](time: Time,displayInnerRDDOps: Boolean)(body: => U): U ={
									val prevCallSite = CallSite( //这三个变量供第三步用;
										ssc.sparkContext.getLocalProperty(CallSite.SHORT_FORM),
										ssc.sparkContext.getLocalProperty(CallSite.LONG_FORM)
									)
									val prevScope = ssc.sparkContext.getLocalProperty(scopeKey)
									val prevScopeNoOverride = ssc.sparkContext.getLocalProperty(scopeNoOverrideKey)
									try {
										// 第一步: 将ThreadLocal中spark.rdd.scope和scope.noOverride这两个线程变量的旧值移除; 并将根据当前scope算出的新的RDDOperationScope的Json设为其新值;
										if (displayInnerRDDOps) {// 默认进入这里, 从ThreadLocal中移除callSite.short, callSite.long这两个值;
											ssc.sparkContext.setLocalProperty(CallSite.SHORT_FORM, null);{//SparkContext.setLocalProperty(key: String, value: String)
												if (value == null) {
													localProperties.get.remove(key)// 获取其ThreadLocal线程变量,并移除值;
												} else {
													localProperties.get.setProperty(key, value)
												}
											}
											ssc.sparkContext.setLocalProperty(CallSite.LONG_FORM, null)
										} else {
											ssc.sparkContext.setCallSite(creationSite)
										}
										makeScope(time).foreach { s:RDDOperationScope => //计算和设置新的rdd.scope线程变量值;
											// 一个RDDOperationScope包括 name(如"foreachRDD@11:00:20"), parent(如None), id(如1_1588302020000)这3个字段;
											ssc.sparkContext.setLocalProperty(scopeKey, s.toJson) //向key=spark.rdd.scope的ThreadLocal变量中插入字符串: {"id":"1_1588302020000","name":"foreachRDD @ 11:00:20"}
											if (displayInnerRDDOps) {//移除ThreadLocal中名为 spark.rdd.scope.noOverride的变量;
												ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, null)
											} else {
												ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, "true")
											}
										}
									  
										// 第二部分-核心: 执行 body函数, body由上述传参进来: => U
										body {
											// 在ForEachDStream中, body内容为 ForEachDStream构造参数传入的用户执行逻辑 foreachFunc(rdd,time)函数:
											/* 
											* 
											*/
											ForEachDStream.foreachFunc(rdd, time){//ForEachDStream构造参数传入, 具体执行用户编写的rdd处理逻辑;
												// 这里执行业务编写的代码:
												// My Demo: 先rdd计算求出结果并传到Driver, Driver端再遍历;
												sparkKafakDemo1.foreachRDD(rdd){
													List<String> collect = rdd.collect();{//JavaRDD.collect()
														sc.runJob(this, (iter: Iterator[T]) => iter.toArray){//SparkContext.runJob(),有5个重载方法,最终调这个: 
															dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get){//DAGScheduler.runJob
																val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties){//DAGScheduler.submitJob(rdd)
																	eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions, callSite)){//DAGScheudlerEventProcessLoop.post()
																		eventQueue.put(event)// eventQueue:LinkedBlockQueue, 这里只是将JobSubmitted消息缓存与eventQueue队列中,等待"dag-scheduler-event-loop"线程来将Job切分成Stags/Tasks并提交;
																	}
																}
															}
														}
													}
													collect.forEach(str-> System.err.println(str));
												}
												
												// StreamSets-Cluster.Driver类 应用逻辑;
												SdcClusterDriverDemo.process().foreachRDD(rdd){
													val kvRDD:RDD[(String,String)] = kafkaRDD.map(x=>(x.key(),x.value()))
													// 情况之前缓存的rdd队列;
													previousIncomingData.foreach((prevData:RDD[(String,String)]) => {
														// 将之前所有的RDD都移除磁盘;
														prevData.unpersist(false)
													})
													previousIncomingData.clear()
													// 清除上一个batch的 GeneratedRDDs 信息
													previousGeneratedRDDs.foreach((prevData:RDD[SdcRecord]) => {
														prevData.unpersist(false)
													})
													previousGeneratedRDDs.clear()
													// 将其添加进队列
													previousIncomingData += kvRDD
													// 有两个rdd了? 一样的?
													val incoming = kvRDD
													previousIncomingData += incoming
													// nextResult封装的是Transformer的结果
													var nextResult:RDD[SdcRecord] = incoming.mapPartitions(it=>{
														// 解析成(key,value)元组;
														val batch:List[(String,String)]= it.map(pair =>{
														  val key = if(pair._1 ==null){"UNKNOWN_PARTITION"
														  }else {pair._1}
														  (key,pair._2)
														}).toList
														val records:Iterator[SdcRecord]= startBatch(batch)
														records
													})
													// 将transformer处理后的结果rdd 放入 previousGenerated队列
													previousGeneratedRDDs += nextResult

													// 先将transforms的RDD 触发并cache结果;
													nextResult.cache().count();{//RDD.count()
														val result:Array[U] = sc.runJob(this, Utils.getIteratorSize _);{
															sc.runJob(this, (iter: Iterator[T]) => iter.toArray){//SparkContext.runJob(),有5个重载方法,最终调这个: 
																dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get){//DAGScheduler.runJob
																	val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties){//DAGScheduler.submitJob(rdd)
																		eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions, callSite)){//DAGScheudlerEventProcessLoop.post()
																			eventQueue.put(event)// eventQueue:LinkedBlockQueue, 这里只是将JobSubmitted消息缓存与eventQueue队列中,等待"dag-scheduler-event-loop"线程来将Job切分成Stags/Tasks并提交;
																		}
																	}
																}
															}
														}
														
														result.sum
													}
													var id = 0
													transformers.foreach(transformer => {
														val result = transformer.transformer(nextResult)
														nextResult.count()
														nextResult = repartition(result)
														previousGeneratedRDDs += nextResult
														id += 1
													})

													// 再触发一次nextResult的计算;
													nextResult.count()
												}
											}
										}

									} finally { 
										// 第三部分: 统一更新callSite.short, callSite.long, rdd.scope, rdd.scope.noOverride 4个线程变量, 但scope和scope.noOverride是移除已有的.
										ssc.sparkContext.setCallSite(prevCallSite)
										ssc.sparkContext.setLocalProperty(scopeKey, prevScope)
										ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, prevScopeNoOverride)
									}
									
								}
							}
							
							
							// 对于ForEachDStream: 其ForEachDStream.generateJob(time)方法中 func()
							val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
								foreachFunc(rdd, time)
							}
							Some(new Job(time, jobFunc))
							
							foreachFunc(rdd, time){
								// 这里执行业务编写的代码:
								List<String> collect = rdd.collect();{//JavaRDD.collect()
									sc.runJob(this, (iter: Iterator[T]) => iter.toArray){
										DAGScheduler.runJob(){
											DAGScheduler.handleJobSubmitted(){
												DAGScheduler.submitStage(){
													TaskSchedulerImpl.submitTasks()
												}
											}
										}
									}
								}
								collect.forEach(str-> System.err.println(str));
								
							}
							
							
							val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
							  SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
								compute(time)
							  }
							}{//DStream.createRDDWithLocalProperties(time,displayInnerRDDOps)( body: => U): U
								val prevCallSite = CallSite(
								  ssc.sparkContext.getLocalProperty(CallSite.SHORT_FORM),
								  ssc.sparkContext.getLocalProperty(CallSite.LONG_FORM)
								)
								val prevScope = ssc.sparkContext.getLocalProperty(scopeKey)
								val prevScopeNoOverride = ssc.sparkContext.getLocalProperty(scopeNoOverrideKey)
								try {
								  if (displayInnerRDDOps) {
									ssc.sparkContext.setLocalProperty(CallSite.SHORT_FORM, null)
									ssc.sparkContext.setLocalProperty(CallSite.LONG_FORM, null)
								  } else {
									ssc.sparkContext.setCallSite(creationSite)
								  }
								  makeScope(time).foreach { s =>
									ssc.sparkContext.setLocalProperty(scopeKey, s.toJson)
									if (displayInnerRDDOps) {
									  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, null)
									} else {
									  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, "true")
									}
								  }
								  
								  // 核心代码在这, body由上述传参进来: => U
								  body {
									SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
										compute(time){//DStream.compute()为抽象方法, 由其继承类实现:
											//1. 当继承类为 MapPartitioned类型时: 
											MapPartitionedDStream.compute(){
												// 原代码: parent.getOrCompute(validTime).map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
												
												// 这里递归掉父RDD的getOrCompute(),直到调到 InputDStream()的RDD: KafakRDD, HadoopRDD..
												val parentRDD = parent.getOrCompute(validTime){//DStream.getOrCompute() 
													generatedRDDs.get(time).orElse {
														body{
															compute(){
																//... 递归调用
															}
														}
													}
												}
												
												parentRDD.map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
												
											}
											
											// 2. 当继承类为InputStream类型时: KafakInputDStream, FileInputStream, QueueInputDStream, ReceiverInputDStream等;
											DirectKafkaInputDStream.compute(time){
												/* 在SDC-cluster模型中, 涉及kafaka的两个sparkConf参数如下: 
												*	"spark.streaming.kafka.maxRatePerPartition" -> "400" 执行分区速率的, 由kafakConfigBean.maxRatePerPartition指定;
												* 	"spark.streaming.kafka.consumer.poll.max.retries" -> "5"	; 默认5, 
												*/
												// 计算各Partition/RDD实例 要消费消息的截止位置:utilOffset = 取 额定速率下最大offser 与 最新消息offer 的最小值为utilOffset;
												val untilOffsets:Map[TopicPartition,Long] = clamp(latestOffsets()){
													// 这个方法两个作用: poll(0)将新增的分区添加到currentOffsets; 重置所有currentOffsets里所有分区的消费位置到 End位置(是offset最新, 还消息末尾?) 
													val partsWithLatest:Map[TopicPartition, Long]  = latestOffsets(){//DirectKafkaInputDStream.latestOffsets()		
														val c = consumer
														// 作用? paranoid多余的,额外的
														paranoidPoll(c){//DirectKafkaInputDStream.paranoidPoll(c: Consumer[K, V])
															val msgs = c.poll(0)//KafkaConsumer.poll(0) 会一直阻塞直到它成功获取了所需的元数据信息,但他一般不返回数据;
															if (!msgs.isEmpty) {
																msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
																	val tp = new TopicPartition(m.topic, m.partition)
																	val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
																	acc + (tp -> off)
																}.foreach { case (tp, off) =>
																	logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate")
																	c.seek(tp, off)
																}
															}
														}
														// 调用KafkaConsumer.assignment(): Set<TopicPartition> 方法获取本Group绑定的TP及Offset
														val parts = c.assignment().asScala
														
														// 比较出currentOffsets还没有的key, 那就是新分配/绑定的TopicPartiton; 若有新分区,将其消费位置也添加进 currentOffsets:Map[TopicPartition,Long]中;
														val newPartitions = parts.diff(currentOffsets.keySet)
														currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap
														c.pause(newPartitions.asJava) //停止本consumer再从 newPartitions里的分区取拿数据;
														
														// 将所有(包括新订阅)的分区,都设置从最末尾位置(end offsets); 从consumer中更新返回;
														// 这里seek()操作,只在内存中将 TopicPartitionState.resetStrategy设置为 Latest,把TopicPartitionState.position 置为null; 
														// 在后面的position()操作中,当判断TPState.position==null时, 触发Fetcher.updateFetchPositions()从网络拉取end位置的offset设置TPState.position值;
														c.seekToEnd(currentOffsets.keySet.asJava){
															Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
															for (TopicPartition tp : parts) {
																log.debug("Seeking to end of partition {}", tp);
																subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);{//SubscriptionState.needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy)
																	assignedState(partition).awaitReset(offsetResetStrategy);{//TopicPartitionState.awaitReset(OffsetResetStrategy strategy)
																		this.resetStrategy = strategy;
																		this.position = null;
																	}
																}
															}
														}
														// 在consumer.position(tp)中,触发resetOffset(),从网络查该分区end位置(是所有日志的end/最新消息);
														parts.map(tp => tp -> {
															c.position(tp){//KafkaConsumer.position(tp)
																Long offset = this.subscriptions.position(partition);
																if (offset == null) {// 当前面执行过 seekToEnd(),(或seek(), seekToBeginning()), 这里的offset就会为null?
																	// 更新获取Offset的位置, 按什么策略更新?
																	updateFetchPositions(Collections.singleton(partition));{//
																		coordinator.refreshCommittedOffsetsIfNeeded();
																		fetcher.updateFetchPositions(partitions);{//Fetcher.updateFetchPositions(tp)
																			// 什么情况下, 要先resetOffset()?  其SubscriptionState.resetStrategy !=null(非空)时,
																			if (subscriptions.isOffsetResetNeeded(tp)) {
																				resetOffset(tp);{//Fetcher.resetOffset(tp)
																					// 更新之前设置的resetStrategy(重置策略:Latest/Earliest/None) 重置其offset
																					OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
																					final long timestamp;
																					if (strategy == OffsetResetStrategy.EARLIEST)
																						timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
																					else if (strategy == OffsetResetStrategy.LATEST)
																						timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
																					else
																						throw new NoOffsetForPartitionException(partition);

																					log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
																					// 真正完成网络获取offset位置的请求: 发异步请求,并循环等待future.succeeded
																					long offset = listOffset(partition, timestamp);{//Fetcher.listOffset(tp,time)
																						while(true){
																							RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
																							client.poll(future);
																							if (future.succeeded()){//查询成功,就中断循环并返回;
																								return future.value();
																							}
																							if (future.exception() instanceof InvalidMetadataException){
																								client.awaitMetadataUpdate();
																							}else{
																								time.sleep(retryBackoffMs);//重试期间,随眠等待 n毫秒
																							}
																						}
																					}
																					// 将本consumer订阅的TP.消费offset位置
																					if (subscriptions.isAssigned(partition)){
																						this.subscriptions.seek(partition, offset);{//SubscriptionState.seek()
																							//class TopicPartitionState(Long position, OffsetAndMetadata committed, boolean paused, OffsetResetStrategy resetStrategy)
																							assignedState(tp).seek(offset);{//TopicPartitionState.seek(long offset)
																								this.position = offset;
																								this.resetStrategy = null;
																							}
																						}
																					}
																				}
																			}
																		}
																	}
																	offset = this.subscriptions.position(partition);
																}
																return offset;
															}
														}).toMap
														
													}
													
													val clampParts:Map[TopicPartition, Long]= clamp(partsWithLatest){//DirectKafkaInputDStream.clamp(offsets)
														
														// 计算每个partiton要处理的最大消息数: 1.先读取计算各Partiton单批次最大处理消息数量; 2. 再与秒级graph.batchDuration值相乘, 求出本批次最大处理数;
														val someParts = maxMessagesPerPartition(offsets){//DirectKafkaInputDStream.maxMessagesPerPartition()
															// 反压机制的限速, estimatedRateLimit 表示本批最大处理这个多个数据
															val estimatedRateLimit = rateController.map(_.getLatestRate(){// 执行RateController.getLatestRate()
																return rateLimit.get() //rateLimit:AtomicLong = new AtomicLong(-1L) 
																
																// RateController.rateLimit的值在"stream-rate-update"线程中, 由PIDRateEstimator.compute()方法算出
																{
																	// 是在"stream-rate-update"线程中接受处理;  StreamingListenerBus.doPostEvent() case StreamingListenerBatchCompleted时: listener.onBatchCompleted()
																	RateController.onBatchCompleted(){
																		for(elems <- elements.get(streamUID).map(_.numRecords)){
																			computeAndPublish(processingEnd, elems, workDelay, waitDelay){
																				val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay){//PIDRateEstimator.compute()
																					this.synchronized {
																					  if (time > latestTime && numElements > 0 && processingDelay > 0) {
																						// 算出与上一个批次的 秒级 时间差
																						val delaySinceUpdate = (time - latestTime).toDouble / 1000
																						// 回授值: processingDelay是处理用时? 计算秒级的处理速度: (数据量/毫秒用时)*1000 ?
																						val processingRate = numElements.toDouble / processingDelay * 1000
																						//误差: error: 这次下降了多少速率? 
																						val error = latestRate - processingRate
																						
																						// 过去累积误差: 单位: eles/sec 调度用时占用了多少数据; 再摊分到 200毫秒的间隔中, 每1毫秒会delay多少个数据?
																						val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis
																						// 斜率:  每分钟下降的斜率; 下降速度?
																						val dError = (error - latestError) / delaySinceUpdate
																						
																						/* PID控制器: 工控领域常见PID控制器的思想。
																						*	即比例（Proportional）-积分（Integral）-微分（Derivative）控制器，
																						* 	本质上是一种反馈回路（loop feedback）。它把收集到的数据和一个设定值（setpoint）进行比较，
																						* 	然后用它们之间的差计算新的输入值，该输入值可以让系统数据尽量接近或者达到设定值
																						*/
																						val newRate = (latestRate - 
																							proportional*error - 	//比例增益,当前误差		proportional=conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
																							integral* historicalError - //积分增益,累积误差	integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
																							derivative* dError)		//微分增益,将来误差 	derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
																							.max(minRate)			//与minRate取最大值; minRate=conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
																						
																						
																						latestTime = time
																						if (firstRun) {
																						  latestRate = processingRate
																						  latestError = 0D
																						  firstRun = false
																						  logTrace("First run, rate estimation skipped")
																						  None
																						} else {
																						  latestRate = newRate
																						  latestError = error
																						  logTrace(s"New rate = $newRate")
																						  Some(newRate)
																						}
																					  } else {
																						logTrace("Rate estimation skipped")
																						None
																					  }
																					}
																				}
																				
																				newRate.foreach { s =>
																					rateLimit.set(s.toLong)
																					publish(getLatestRate())
																				}
																			}
																		}
																	}
																}
															})
															
															val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
																case Some(rate) =>
																	// 先算每个分区 这个batch要拉取多少数据:lag; 让后总拉取数据量: totalLag
																	val lagPerPartition = offsets.map { case (tp, offset) =>
																		tp -> Math.max(offset - currentOffsets(tp), 0)
																	}
																	val totalLag = lagPerPartition.values.sum
																	
																	lagPerPartition.map { case (tp, lag) =>
																		// spark.streaming.kafka.maxRatePerPartition设置的分区上限;
																		val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)//return maxRate=conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
																		
																		// 该分区应消费数据比重 * 本批可处理数量总量 
																		val backpressureRate = Math.round(lag / totalLag.toFloat * rate)
																		tp -> (if (maxRateLimitPerPartition > 0) {
																				// 该分区限压后处理数据量, 与用户设置的maxRateLimitPerPartition 中,取最小值为真正数据量;
																				Math.min(backpressureRate, maxRateLimitPerPartition)
																			} else backpressureRate
																		)
																		
																	}
																//为空时, 进入这里;
																case None =>{
																	offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp){
																		val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
																		return maxRate;
																	}}
																} 
															}
															
															if (effectiveRateLimitPerPartition.values.sum > 0) {//当所有分区 maxRate之和>0, 即有流量限制时,
																// 将spark微批处理间隔graph.batchDuration, 转换成 秒级单位: 例如 200ms -> 0.2秒
																//在SDC.cluster运行时, batchDuration==Utils.getKafkaMaxWaitTime(getProperties())== kafkaConfigBean.maxWaitTime的值; 目前2秒?
																val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
																
																// 计算本批次, 对每个RDD/分区要发送Record的数量(及速率限制 RateLimitPerPartition): 批次时长0.2 * 分区处理上限数 500 = 100个数据;
																Some(effectiveRateLimitPerPartition.map {
																	case (tp, limit) => tp -> (secsPerBatch * limit).toLong
																})
															} else {
																None
															}
														}
														
														// 取 额定速率下最大offser 与 最新消息offer, 两者最小值为本批次实际untilOffset.
														someParts.map { mmp =>
															mmp.map { case (tp, messages) =>
																val uo = offsets(tp) //offsets即为传参进来的, 各分区的Latest Offset值(即最新的消息)
																// 取 速率上限offser和 最新日志offset的最小值,为本batch的untilOffset(消费截止offset); 这样保证了不消费还没到的消息; 当没有server没有新消息时, 这个值是相等的.
																// currentOffsets(tp) + messages 就是额定速率消费offser上限;
																// uo 就是之前latestOffsets()方法求出的Server端该分区最新消费的offset;
																tp -> Math.min(currentOffsets(tp) + messages, uo)
														  }
														}.getOrElse(offsets)
													}
													
													clampParts
												}
												
												//取上batch的currentOffsets为起始点fromOffset, 并与untilOffset一起封装到OffsetRange对象中;
												val offsetRanges = untilOffsets.map { case (tp, uo) =>
													val fo = currentOffsets(tp)
													OffsetRange(tp.topic, tp.partition, fo, uo)
												}
												
												val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled", true)
												val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, useConsumerCache)
												val description = offsetRanges
													.filter{ offsetRange => offsetRange.fromOffset != offsetRange.untilOffset }
													.map{ offsetRange => s"topic: " + s"offsets: " }.mkString("\n")
												
												// Copy offsetRanges to immutable.List to prevent from being modified by the user
												val metadata = Map("offsets" -> offsetRanges.toList, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
												val inputInfo = StreamInputInfo(id, rdd.count, metadata)
												ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
												currentOffsets = untilOffsets
												commitAll()
												Some(rdd)												
											}
											
										}
									}
								  }
								  
								} finally {
								  ssc.sparkContext.setCallSite(prevCallSite)
								  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, prevScopeNoOverride)
								}
							}
																								
							
							
							
						}
					}
					
				}
				
				_eventLoop = eventLoop
				if (_eventLoop != null){
					_eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
				}
			} else {
				
			}
		} finally {
			ssc.sparkContext.setLocalProperties(oldProps)
		}
	}
}


/** stream-rate-update" 线程, 在Job执行完后, ""
*	- 由"SparkListenerBus"线程触发:

*/

LiveListenerBus.doPostEvent(){//位于"SparkListenerBus"线程中代码;
	StreamingListenerBus.postToAll(){
		StreamingListenerBus.doPostEvent(){//位于"SparkListenerBus"线程中代码;
			case batchCompleted: StreamingListenerBatchCompleted => listener.onBatchCompleted(batchCompleted);{//RateController.onBatchCompleted()
				val elements = batchCompleted.batchInfo.streamIdToInputInfo
				for {
					processingEnd <- batchCompleted.batchInfo.processingEndTime
					workDelay <- batchCompleted.batchInfo.processingDelay
					waitDelay <- batchCompleted.batchInfo.schedulingDelay
					elems <- elements.get(streamUID).map(_.numRecords)
				} computeAndPublish(processingEnd, elems, workDelay, waitDelay);{// 同步异步线程, 执行将新算出的newRate速率,更新进RateController.rateLimit变量;
					// 直接将body中的代码,利用线程池另起一新线程执行:
					/* 异步线程"stream-rate-update",完成新速率的计算和更新;
					*	1. 算新速率: newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
					*	2. rateLimit.set(newRate): 更新进rateLimit变量中, 将在KafkaDStream.compute() -> maxMessagesPerPartition()中被引用;
					*/
					val futrue= Future[Unit] {
						val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
						newRate.foreach { s =>
							rateLimit.set(s.toLong)
							publish(getLatestRate())
						}
					};{// impl.Future.apply(body: => T)
						val runnable = new PromiseCompletingRunnable(body)
						executor.prepare.execute(runnable);// 在线程中执行此任务;
						runnable.promise.future
					}
					futrue
				}
			}
		}
	}
}








# Streaming Batch 4: 

Driver端- "streaming-job-executor-n"线程: JobHandler.run()中: 在_eventLoop.post(JobStarted)提交job启动后, 紧接着就提交JobCompleted请求:_eventLoop.post(JobCompleted); 所以JobCompleted的处理逻辑应该是阻塞等待某个完成计算的信号量,才开始clearMetadata的;

	Driver端- "JobScheduler"线程"中: processEvent(event): case JobCompleted(job);
		-> Driver端- "JobScheduler"线程: case JobCompleted() => handleJobCompletion(job, completedTime) -> jobGenerator.onBatchCompletion(jobSet.time) -> eventLoop.post(ClearMetadata(time))
			-> Driver端- "JobGenerator"线程: case ClearMetadata(time): clearMetadata() -> DStreamGraph.clearMetadata() -> DStream.clearMetadata()
				
				-> Driver端-"dispatcher-event-loop" 线程中:BlockManagerMasterEndpoint.receiveAndReply()收到相关清理消息:case RemoveBlock(), RemoveRdd(), RemoveShuffle, RemoveBroadcast
					- 完成Driver端: BlockManagerMasterEndpoint.remoeRDD()/remoeBlock()/removeShuffle()..: 完成Driver端的移除逻辑; 并向Executor端发送相关Remove消息;
						-> Executor 端-"dispatcher-event-loop" 线程: BlockManagerSlaveEndpoint.receiveAndReply(): case RemoveBlock(), RemoveRdd(), RemoveShuffle, RemoveBroadcast; 执行doAsync(){}异步线程完成删除操作;
							-> Executor端, 启动"block-manager-slave-async-thread-pool"线程: 进行RDD/Block/ Shuffle/ Broadcast数据的移除;
						


	开始ClearMetadata清理元数据事件: 
													
			# 阶段4: 开始ClearMetadata清理元数据事件: 
				"JobScheduler"线程?:  processEvent(event): case JobCompleted(job); JobGenerator.onBatchCompletion() -> eventLoop.post(ClearMetadata(time))
					-> "JobGenerator"线程: JobGenerator.processEvent(event): case ClearMetadata(time): JobGenerator.clearMetadata()
				- JobGenerator - Got event ClearMetadata(1589117720000 ms): 		JobGenerator.processEvent
				- DStreamGraph:Clearing metadata for time 1589117720000 ms: 		DStreamGraph.clearMetadata()
					* ForEachDStream:  Clearing references, Cleared 0 RDDs,
					* MappedDStream: Clearing references to old RDDs; Unpersisting old RDDs;		DStream.clearMetadata()
						* MapPartitionsRDD: Removing RDD 71 from persistence list					RDD.unpersist(): "JobGenerator"线程
							- BlockManagerSlaveEndpoint: Done removing block, Sent response: true, removing RDD 71;		BlockManagerSlaveEndpoint.doAsync() : "block-manager-slave-async-thread-pool"
								-> BlockManagerSlaveEndpoint.receiveAndReply():case RemoveBlock(blockId) ->  doAsync("removing block " + blockId) { blockManager.removeBlock(blockId)} 
									->  "block-manager-slave-async-thread-pool": BlockManagerSlaveEndpoint.doAsync(): future.onSuccess {}
							- BlockManager - Removing RDD 71; Removing block rdd_71_0:  "block-manager-slave-async-thread-pool": BlockManager.removeRdd()
								-> BlockManagerSlaveEndpoint.receiveAndReply():case RemoveRdd(blockId) ->  doAsync(){blockManager.removeRdd()}
									-> "block-manager-slave-async-thread-pool": BlockManager.removeRdd()
					* ShuffledDStream: Clearing references to old RDDs; Unpersisting old RDDs: 70
						* ShuffledRDD: Removing RDD 70 from persistence list
							- BlockManagerSlaveEndpoint - removing RDD 70
							- BlockManager - Removing RDD 70
							- BlockManagerSlaveEndpoint - Done removing RDD 70, response is 0
						
								* DStreamGraph - Cleared old metadata for time 1589117720000 ms
								* ReceivedBlockTracker - Deleting batches: 
								* InputInfoTracker - remove old batch metadata: 
						Driver端进行 RemoveRdd/RemoveBlock/ RemoveShuffle/ RemoveBroadcase等的清理逻辑:
						在"dispatcher-event-loop" 线程中:processEvent() -> BlockManagerSlaveEndpoint.receiveAndReply(){
							case RemoveBlock(blockId) => doAsync("removing block "){ blockManager.removeBlock(blockId) }
								-> 启动"block-manager-slave-async-thread-pool"线程: 进行Block的移除;
							case RemoveRdd(rddId) => doAsync("removing block "){  }
								-> 启动"block-manager-slave-async-thread-pool"线程: 进行RDD的移除;
							case RemoveShuffle(shuffleId) => doAsync("removing block "){  }
								-> 启动"block-manager-slave-async-thread-pool"线程: 进行Shuffle数据的移除;
							case RemoveBroadcast(broadcastId, _) => doAsync("removing block "){ }
								-> 启动"block-manager-slave-async-thread-pool"线程: 进行Broadcast数据的移除;
								* 触发: "Spark Context Cleaner"线程: ContextCleaner.keepCleaning()-> doCleanupShuffle() -> BlockManagerMaster.removeShuffle(shuffleId)
							
						}
						
						

















// 1. 创建jssc
JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(3000)){
	StreamingContext ssc = new StreamingContext(conf, batchDuration){
		// 1. 创建核心对象sc:SparkContext
		SparkContext sc= StreamingContext.createNewSparkContext(conf){
			new SparkContext(conf){
				logInfo(s"Running Spark version $SPARK_VERSION")
					* INFO SparkContext: Running Spark version 2.2.0
				
				_conf.validateSettings() // 校验SparkConf中的配置, 校验成功则打印Submitted
				logInfo(s"Submitted application: $appName")
					* INFO SparkContext: Submitted application: JavaStreamingWCDemo
				
				// 创建SparkEnv对象: 432行
				_env = createSparkEnv(_conf, isLocal, listenerBus){
					SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master)){
						val securityManager = new SecurityManager(conf, ioEncryptionKey)
							* INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(it-out-allen); groups with view permissions: Set(); users  with modify permissions: Set(it-out-allen); groups with modify permissions: Set()
						
						// 创建序列化者 serializer: 
						val serializer = instantiateClassFromConf[Serializer]("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
						
						// 创建基于Netty的Rpc通信后台;
						val rpcEnv:NettyRpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port, conf,securityManager, clientMode = !isDriver)
						
							* INFO Utils: Successfully started service 'sparkDriver' on port 58014.
						
						// 创建广播管理器
						val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
						
						// 创建ShuffMangaer
						val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
						
						// 创建BlockManagerMaster,用于接收BM的注册信息;
						val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint),conf,isDriver)
							* INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
							
						// 先创建BM对象,但要晚一点 掉initialize()方法后,才能被使用;
						val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,blockTransferService, securityManager, numUsableCores)
							* INFO DiskBlockManager: Created local directory at C:\Users\it-out-allen\AppData\Local\Temp\blockmgr-da6bb514-0c2f-48d9-8ed2-7c09e134f42d
							* INFO MemoryStore: MemoryStore started with capacity 1992.0 MB

						// 创建运行监控系统:MetricsSystem
						val metricsSystem = if (isDriver) {
							MetricsSystem.createMetricsSystem("driver", conf, securityManager)
						}else{
							conf.set("spark.executor.id", executorId)
							val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager).start()
						}
						
						val envInstance:SparkEnv = new SparkEnv(executorId,rpcEnv,serializer,closureSerializer,serializerManager,mapOutputTracker,shuffleManager,
												broadcastManager,blockManager,securityManager,metricsSystem,memoryManager,outputCommitCoordinator,conf)
						return envInstance;
					}
				}
					* INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.41.1:4040
				
				// 创建心跳检查器
				_heartbeatReceiver = env.rpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))
				
				
				/** 初始化最重要三大对象创建: SchedulerBackend,TaskScheduler,DAGScheduler
				*	
				*
				*/
				// 创建SchedulerBachend,TaskScheduler
				val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
				// 创建DAGScheduler
				_dagScheduler = new DAGScheduler(this)
				// 进行Scheduler相关的启动和初始化
				_taskScheduler.start()
					* INFO Executor: Starting executor ID driver on host localhost
				
				// 初始化BlockManger
				_env.blockManager.initialize(_applicationId){
					blockTransferService.init(this)
						* INFO NettyBlockTransferService: Server created on 192.168.41.1:59715
					shuffleClient.init(appId)
					
					// 向Driver.BlockManagerMaster中注册 BlockManager
					val idFromMaster = master.registerBlockManager(id,maxOnHeapMemory,maxOffHeapMemory,slaveEndpoint);
						* INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.41.1, 59874, None)
					
					logInfo(s"Initialized BlockManager: $blockManagerId")
						* INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.41.1, 59559, None)
				
				}
				
				val localProperties = new InheritableThreadLocal[Properties] 
			}
		}
		
		// 2.将sc封装进 ssc:StreamingContext
		this(sc,null,batchDuration){ // new StreamingContext(){
			// 创建流计算图对象: DStreamGraph
			val graph: DStreamGraph = new DStreamGraph()
			// 用JobScheduler来封装 sc.scheduler
			val scheduler = new JobScheduler(this)
			val waiter = new ContextWaiter
			
			val streamingSource = new StreamingSource(this)
			
		}
	}
	this(ssc){// new JavaStreamingContext()
		val sparkContext = new JavaSparkContext(ssc.sc)
	}
}

// 2. 获取第一个DStream
JavaReceiverInputDStream<String> javaDS = jssc.socketTextStream(mainArgs.host(), mainArgs.port()){
	ssc.socketTextStream(hostname, port){ // StreamingContext.socketTextStream(){
		body = {
				socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel):ReceiverInputDStream[T]={
					new SocketInputDStream[T](this, hostname, port, converter, storageLevel);
				}
		}
		
		withNamedScope(name)(body:=> T):T={
			RDDOperationScope.withScope(sc, name, allowNesting = false, ignoreParent = false)(body){
				val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
				
				body
			}
		}
	}
}


// 2.2 创建一个DirectKafakInputDStream
KafkaUtils.createDirectStream(jssc,locationStrategy,consumerStrategy, perPartitionConfig)){
	new DirectKafkaInputDStream[K, V](ssc, locationStrategy, consumerStrategy, perPartitionConfig){//DirectKafkaInputDStream的构造函数
		val executorKafkaParams = {
			val ekp = new ju.HashMap[String, Object](consumerStrategy.executorKafkaParams)
			KafkaUtils.fixKafkaParams(ekp)
			ekp
		}
		
		var currentOffsets = Map[TopicPartition, Long]()
		protected[streaming] val checkpointData = new DirectKafkaInputDStreamCheckpointData
		protected[streaming] val rateController: Option[RateController] = {
			if (RateController.isBackPressureEnabled(ssc.conf)) {//spark.streaming.backpressure.enabled=true时,进入
			  Some(new DirectKafkaRateController(id,RateEstimator.create(ssc.conf, context.graph.batchDuration)))
			} else {
			  None
			}
		  }
		  
		protected val commitQueue = new ConcurrentLinkedQueue[OffsetRange]
		protected val commitCallback = new AtomicReference[OffsetCommitCallback]
		
		
	}
}



JavaStreamingContext.start(){
	ssc.start(){
		state match {
			case INITIALIZED =>{
				StreamingContext.ACTIVATION_LOCK.synchronized {
					StreamingContext.assertNoOtherContextIsActive()
					validate(){
						graph.validate()
					}
					
					ThreadUtils.runInNewThread("streaming-start") {
						sparkContext.setCallSite(startSite.get)
						sparkContext.clearJobGroup()
						sparkContext.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
						savedProperties.set(SerializationUtils.clone(sparkContext.localProperties.get()))
						scheduler.start(){// JobScheduler.start() 核心的启动和方法;
							logDebug("Starting JobScheduler")
							// EventLoop是抽象类, 
							eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler"){
								override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event){
									
								}
							}
							// 启动
							eventLoop.start(){ // EventLoop.start(){
								onStart(){} // 空代码?
								
								eventThread = new Thread(name){
									override def run(): Unit = {
										while (!stopped.get) {
											onReceive(event: JobSchedulerEvent){
												JobScheduler.processEvent(event){
													event match {
														// 启动一个Job
														case JobStarted(job, startTime) => handleJobStart(job, startTime){
															JobScheduler.handleJobStart(job: Job, startTime: Long){
																val jobSet = jobSets.get(job.time)
																val isFirstJobOfJobSet = !jobSet.hasStarted
																jobSet.handleJobStart(job)
																if (isFirstJobOfJobSet) {//第一个提交的Job进入这里; 失败的job就不提交了;
																	// 发送到RPC的队列; 被哪个线程接受?
																	listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo)){
																		sparkListenerBus.post(new WrappedStreamingListenerEvent(event))
																	}
																}
																job.setStartTime(startTime)
																listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
																logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
															}
														}
														
														// 结束一个Job
														case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
														
														case ErrorReported(m, e) => handleError(m, e)
													}
												}
											}
										}
									}
								};
								eventThread.start()
								
							}
							
							// attach rate controllers of input streams to receive batch completion updates
							for(inputDStream <- ssc.graph.getInputStreams){
								for(rateController <- inputDStream.rateController){
									ssc.addStreamingListener(rateController)
								}
							}
							listenerBus.start()
							receiverTracker = new ReceiverTracker(ssc)
							inputInfoTracker = new InputInfoTracker(ssc)
							
							// 动态扩容相关
							val executorAllocClient: ExecutorAllocationClient = ssc.sparkContext.schedulerBackend match {
							  case b: ExecutorAllocationClient => b.asInstanceOf[ExecutorAllocationClient]
							  case _ => null
							}
							executorAllocationManager = ExecutorAllocationManager.createIfEnabled(executorAllocClient,receiverTracker){//ExecutorAllocationManager.createIfEnabled()
								if (isDynamicAllocationEnabled(conf) && client != null) {
									Some(new ExecutorAllocationManager(client, receiverTracker, conf, batchDurationMs, clock)){//ExecutorAllocationManager的构造函数
										//动态扩容时间间隔, 决定RecurringTimer线程的触发频率;
										private val scalingIntervalSecs = conf.getTimeAsSeconds(SCALING_INTERVAL_KEY,s"${SCALING_INTERVAL_DEFAULT_SECS}s")
										// 处理用时占比 到scalingUpRatio时,为扩容上限;
										private val scalingUpRatio = conf.getDouble(SCALING_UP_RATIO_KEY, SCALING_UP_RATIO_DEFAULT)
										//处理用时占比 到 scalingDownRatio时,为缩容比率;
										private val scalingDownRatio = conf.getDouble(SCALING_DOWN_RATIO_KEY, SCALING_DOWN_RATIO_DEFAULT)
										private val minNumExecutors = conf.getInt(MIN_EXECUTORS_KEY,math.max(1, receiverTracker.numReceivers))
										private val maxNumExecutors = conf.getInt(MAX_EXECUTORS_KEY, Integer.MAX_VALUE)
										private val timer = new RecurringTimer(clock, scalingIntervalSecs * 1000,_ => manageAllocation(), "streaming-executor-allocation-manager")
									
									}
								} else None
							}
							executorAllocationManager.foreach(ssc.addStreamingListener)
							
							receiverTracker.start()
								* INFO ReceiverTracker: ReceiverTracker started
							
							/** 启动"JobGenerator"线程, while()循环处理JobGeneratorEvent: GenerateJobs,ClearMetadata
							*		case GenerateJobs: generateJobs(time) -> ForEachDStream.generateJob() -> DStream.getOrCompute(time)
							*/
							jobGenerator.start(){ // JobGenerator.start()
								if (eventLoop != null) return 
								checkpointWriter
								
								// 创建抽象类EventLoop的实现类: 
								eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
									override protected def onReceive(event: JobGeneratorEvent): Unit = JobGenerator.processEvent(event);
									override protected def onError(e: Throwable): Unit = jobScheduler.reportError("Error in job generator", e)
								}
								
								eventLoop.start(){//EventLoop.start()
									onStart(){} // 空代码?
									
									eventThread = new Thread(name){//线程名 "JobGenerator"
										// 代码如下
									}
									
									eventThread.start(){
										Thread.run(){
											while (!stopped.get) {
												onReceive(event){//抽象类, 由具体继承类实现
													// 1. JobScheduler.onReceive(event: JobSchedulerEvent)
													JobGenerator.eventLoop.onReceive(event: JobSchedulerEvent){
														processEvent(event){//JobGenerator.processEvent(event: JobGeneratorEvent)
															logDebug("Got event " + event)
															event match {
																case GenerateJobs(time) => generateJobs(time){//JobGenerator.generateJobs(time)
																	JobGenerator.generateJobs(time: Time){
																		ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
																		Try {
																			jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
																			
																			/* 第一步: 先创建相关的RDD,并组成1个Job
																			*	- OutputDStream.generateJob() 触发 parent.getOrCompute(): 递归生成各级父parentRDD
																			* 	- 各父RDD生成后, 再生成 本Action操作的rddOption
																			*/
																			graph.generateJobs(time){//DStreamGraph.generateJobs(time)
																				val jobs = this.synchronized {
																				  outputStreams.flatMap { outputStream =>
																					val jobOption = outputStream.generateJob(time){//ForEachDStream.generateJob(time)			
																						//创建RDD, 
																						val someRDD= parent.getOrCompute(time){//DStream.getOrCompute(time)
																							generatedRDDs.get(time).orElse {
																							  if (isTimeValid(time)) {
																								val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
																								  SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
																									compute(time)
																								  }
																								}{//DStream.createRDDWithLocalProperties(time,displayInnerRDDOps)( body: => U): U
																									val prevCallSite = CallSite(
																									  ssc.sparkContext.getLocalProperty(CallSite.SHORT_FORM),
																									  ssc.sparkContext.getLocalProperty(CallSite.LONG_FORM)
																									)
																									val prevScope = ssc.sparkContext.getLocalProperty(scopeKey)
																									val prevScopeNoOverride = ssc.sparkContext.getLocalProperty(scopeNoOverrideKey)
																									try {
																									  if (displayInnerRDDOps) {
																										ssc.sparkContext.setLocalProperty(CallSite.SHORT_FORM, null)
																										ssc.sparkContext.setLocalProperty(CallSite.LONG_FORM, null)
																									  } else {
																										ssc.sparkContext.setCallSite(creationSite)
																									  }
																									  makeScope(time).foreach { s =>
																										ssc.sparkContext.setLocalProperty(scopeKey, s.toJson)
																										if (displayInnerRDDOps) {
																										  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, null)
																										} else {
																										  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, "true")
																										}
																									  }
																									  
																									  // 核心代码在这, body由上述传参进来: => U
																									  body {
																										SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
																											compute(time){//DStream.compute()为抽象方法, 由其继承类实现:
																												//1. 当继承类为 MapPartitioned类型时: 
																												MapPartitionedDStream.compute(){
																													// 原代码: parent.getOrCompute(validTime).map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
																													
																													// 这里递归掉父RDD的getOrCompute(),直到调到 InputDStream()的RDD: KafakRDD, HadoopRDD..
																													val parentRDD = parent.getOrCompute(validTime){//DStream.getOrCompute() 
																														generatedRDDs.get(time).orElse {
																															body{
																																compute(){
																																	//... 递归调用
																																}
																															}
																														}
																													}
																													
																													parentRDD.map(_.mapPartitions[U](mapPartFunc, preservePartitioning))
																													
																												}
																												
																												// 2. 当继承类为InputStream类型时: KafakInputDStream, FileInputStream, QueueInputDStream, ReceiverInputDStream等;
																												DirectKafkaInputDStream.compute(time){
																													/* 在SDC-cluster模型中, 涉及kafaka的两个sparkConf参数如下: 
																													*	"spark.streaming.kafka.maxRatePerPartition" -> "400" 执行分区速率的, 由kafakConfigBean.maxRatePerPartition指定;
																													* 	"spark.streaming.kafka.consumer.poll.max.retries" -> "5"	; 默认5, 
																													*/
																													// 计算各Partition/RDD实例 要消费消息的截止位置:utilOffset = 取 额定速率下最大offser 与 最新消息offer 的最小值为utilOffset;
																													val untilOffsets:Map[TopicPartition,Long] = clamp(latestOffsets()){
																														// 这个方法两个作用: poll(0)将新增的分区添加到currentOffsets; 重置所有currentOffsets里所有分区的消费位置到 End位置(是offset最新, 还消息末尾?) 
																														val partsWithLatest:Map[TopicPartition, Long]  = latestOffsets(){//DirectKafkaInputDStream.latestOffsets()		
																															val c = consumer
																															// 作用? paranoid多余的,额外的
																															paranoidPoll(c){//DirectKafkaInputDStream.paranoidPoll(c: Consumer[K, V])
																																val msgs = c.poll(0)//KafkaConsumer.poll(0) 会一直阻塞直到它成功获取了所需的元数据信息,但他一般不返回数据;
																																if (!msgs.isEmpty) {
																																	msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
																																		val tp = new TopicPartition(m.topic, m.partition)
																																		val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
																																		acc + (tp -> off)
																																	}.foreach { case (tp, off) =>
																																		logInfo(s"poll(0) returned messages, seeking $tp to $off to compensate")
																																		c.seek(tp, off)
																																	}
																																}
																															}
																															// 调用KafkaConsumer.assignment(): Set<TopicPartition> 方法获取本Group绑定的TP及Offset
																															val parts = c.assignment().asScala
																															
																															// 比较出currentOffsets还没有的key, 那就是新分配/绑定的TopicPartiton; 若有新分区,将其消费位置也添加进 currentOffsets:Map[TopicPartition,Long]中;
																															val newPartitions = parts.diff(currentOffsets.keySet)
																															currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap
																															c.pause(newPartitions.asJava) //停止本consumer再从 newPartitions里的分区取拿数据;
																															
																															// 将所有(包括新订阅)的分区,都设置从最末尾位置(end offsets); 从consumer中更新返回;
																															// 这里seek()操作,只在内存中将 TopicPartitionState.resetStrategy设置为 Latest,把TopicPartitionState.position 置为null; 
																															// 在后面的position()操作中,当判断TPState.position==null时, 触发Fetcher.updateFetchPositions()从网络拉取end位置的offset设置TPState.position值;
																															c.seekToEnd(currentOffsets.keySet.asJava){
																																Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
																																for (TopicPartition tp : parts) {
																																	log.debug("Seeking to end of partition {}", tp);
																																	subscriptions.needOffsetReset(tp, OffsetResetStrategy.LATEST);{//SubscriptionState.needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy)
																																		assignedState(partition).awaitReset(offsetResetStrategy);{//TopicPartitionState.awaitReset(OffsetResetStrategy strategy)
																																			this.resetStrategy = strategy;
																																			this.position = null;
																																		}
																																	}
																																}
																															}
																															// 在consumer.position(tp)中,触发resetOffset(),从网络查该分区end位置(是所有日志的end/最新消息);
																															parts.map(tp => tp -> {
																																c.position(tp){//KafkaConsumer.position(tp)
																																	Long offset = this.subscriptions.position(partition);
																																	if (offset == null) {// 当前面执行过 seekToEnd(),(或seek(), seekToBeginning()), 这里的offset就会为null?
																																		// 更新获取Offset的位置, 按什么策略更新?
																																		updateFetchPositions(Collections.singleton(partition));{//
																																			coordinator.refreshCommittedOffsetsIfNeeded();
																																			fetcher.updateFetchPositions(partitions);{//Fetcher.updateFetchPositions(tp)
																																				// 什么情况下, 要先resetOffset()?  其SubscriptionState.resetStrategy !=null(非空)时,
																																				if (subscriptions.isOffsetResetNeeded(tp)) {
																																					resetOffset(tp);{//Fetcher.resetOffset(tp)
																																						// 更新之前设置的resetStrategy(重置策略:Latest/Earliest/None) 重置其offset
																																						OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
																																						final long timestamp;
																																						if (strategy == OffsetResetStrategy.EARLIEST)
																																							timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
																																						else if (strategy == OffsetResetStrategy.LATEST)
																																							timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
																																						else
																																							throw new NoOffsetForPartitionException(partition);

																																						log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
																																						// 真正完成网络获取offset位置的请求: 发异步请求,并循环等待future.succeeded
																																						long offset = listOffset(partition, timestamp);{//Fetcher.listOffset(tp,time)
																																							while(true){
																																								RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
																																								client.poll(future);
																																								if (future.succeeded()){//查询成功,就中断循环并返回;
																																									return future.value();
																																								}
																																								if (future.exception() instanceof InvalidMetadataException){
																																									client.awaitMetadataUpdate();
																																								}else{
																																									time.sleep(retryBackoffMs);//重试期间,随眠等待 n毫秒
																																								}
																																							}
																																						}
																																						// 将本consumer订阅的TP.消费offset位置
																																						if (subscriptions.isAssigned(partition)){
																																							this.subscriptions.seek(partition, offset);{//SubscriptionState.seek()
																																								//class TopicPartitionState(Long position, OffsetAndMetadata committed, boolean paused, OffsetResetStrategy resetStrategy)
																																								assignedState(tp).seek(offset);{//TopicPartitionState.seek(long offset)
																																									this.position = offset;
																																									this.resetStrategy = null;
																																								}
																																							}
																																						}
																																					}
																																				}
																																			}
																																		}
																																		offset = this.subscriptions.position(partition);
																																	}
																																	return offset;
																																}
																															}).toMap
																															
																														}
																														
																														val clampParts:Map[TopicPartition, Long]= clamp(partsWithLatest){//DirectKafkaInputDStream.clamp(offsets)
																															
																															// 计算每个partiton要处理的最大消息数: 1.先读取计算各Partiton单批次最大处理消息数量; 2. 再与秒级graph.batchDuration值相乘, 求出本批次最大处理数;
																															val someParts = maxMessagesPerPartition(offsets){//DirectKafkaInputDStream.maxMessagesPerPartition()
																																// 反压机制的限速, estimatedRateLimit 表示本批最大处理这个多个数据
																																val estimatedRateLimit = rateController.map(_.getLatestRate(){// 执行RateController.getLatestRate()
																																	return rateLimit.get() //rateLimit:AtomicLong = new AtomicLong(-1L) 
																																	
																																	// RateController.rateLimit的值在"stream-rate-update"线程中, 由PIDRateEstimator.compute()方法算出
																																	{
																																		// 是在"stream-rate-update"线程中接受处理;  StreamingListenerBus.doPostEvent() case StreamingListenerBatchCompleted时: listener.onBatchCompleted()
																																		RateController.onBatchCompleted(){
																																			for(elems <- elements.get(streamUID).map(_.numRecords)){
																																				computeAndPublish(processingEnd, elems, workDelay, waitDelay){
																																					val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay){//PIDRateEstimator.compute()
																																						this.synchronized {
																																						  if (time > latestTime && numElements > 0 && processingDelay > 0) {
																																							// 算出与上一个批次的 秒级 时间差
																																							val delaySinceUpdate = (time - latestTime).toDouble / 1000
																																							// 回授值: processingDelay是处理用时? 计算秒级的处理速度: (数据量/毫秒用时)*1000 ?
																																							val processingRate = numElements.toDouble / processingDelay * 1000
																																							//误差: error: 这次下降了多少速率? 
																																							val error = latestRate - processingRate
																																							
																																							// 过去累积误差: 单位: eles/sec 调度用时占用了多少数据; 再摊分到 200毫秒的间隔中, 每1毫秒会delay多少个数据?
																																							val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis
																																							// 斜率:  每分钟下降的斜率; 下降速度?
																																							val dError = (error - latestError) / delaySinceUpdate
																																							
																																							/* PID控制器: 工控领域常见PID控制器的思想。
																																							*	即比例（Proportional）-积分（Integral）-微分（Derivative）控制器，
																																							* 	本质上是一种反馈回路（loop feedback）。它把收集到的数据和一个设定值（setpoint）进行比较，
																																							* 	然后用它们之间的差计算新的输入值，该输入值可以让系统数据尽量接近或者达到设定值
																																							*/
																																							val newRate = (latestRate - 
																																								proportional*error - 	//比例增益,当前误差		proportional=conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
																																								integral* historicalError - //积分增益,累积误差	integral = conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
																																								derivative* dError)		//微分增益,将来误差 	derived = conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
																																								.max(minRate)			//与minRate取最大值; minRate=conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
																																							
																																							
																																							latestTime = time
																																							if (firstRun) {
																																							  latestRate = processingRate
																																							  latestError = 0D
																																							  firstRun = false
																																							  logTrace("First run, rate estimation skipped")
																																							  None
																																							} else {
																																							  latestRate = newRate
																																							  latestError = error
																																							  logTrace(s"New rate = $newRate")
																																							  Some(newRate)
																																							}
																																						  } else {
																																							logTrace("Rate estimation skipped")
																																							None
																																						  }
																																						}
																																					}
																																					
																																					newRate.foreach { s =>
																																						rateLimit.set(s.toLong)
																																						publish(getLatestRate())
																																					}
																																				}
																																			}
																																		}
																																	}
																																})
																																
																																val effectiveRateLimitPerPartition = estimatedRateLimit.filter(_ > 0) match {
																																	case Some(rate) =>
																																		// 先算每个分区 这个batch要拉取多少数据:lag; 让后总拉取数据量: totalLag
																																		val lagPerPartition = offsets.map { case (tp, offset) =>
																																			tp -> Math.max(offset - currentOffsets(tp), 0)
																																		}
																																		val totalLag = lagPerPartition.values.sum
																																		
																																		lagPerPartition.map { case (tp, lag) =>
																																			// spark.streaming.kafka.maxRatePerPartition设置的分区上限;
																																			val maxRateLimitPerPartition = ppc.maxRatePerPartition(tp)//return maxRate=conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
																																			
																																			// 该分区应消费数据比重 * 本批可处理数量总量 
																																			val backpressureRate = Math.round(lag / totalLag.toFloat * rate)
																																			tp -> (if (maxRateLimitPerPartition > 0) {
																																					// 该分区限压后处理数据量, 与用户设置的maxRateLimitPerPartition 中,取最小值为真正数据量;
																																					Math.min(backpressureRate, maxRateLimitPerPartition)
																																				} else backpressureRate
																																			)
																																			
																																		}
																																	//为空时, 进入这里;
																																	case None =>{
																																		offsets.map { case (tp, offset) => tp -> ppc.maxRatePerPartition(tp){
																																			val maxRate = conf.getLong("spark.streaming.kafka.maxRatePerPartition", 0)
																																			return maxRate;
																																		}}
																																	} 
																																}
																																
																																if (effectiveRateLimitPerPartition.values.sum > 0) {//当所有分区 maxRate之和>0, 即有流量限制时,
																																	// 将spark微批处理间隔graph.batchDuration, 转换成 秒级单位: 例如 200ms -> 0.2秒
																																	//在SDC.cluster运行时, batchDuration==Utils.getKafkaMaxWaitTime(getProperties())== kafkaConfigBean.maxWaitTime的值; 目前2秒?
																																	val secsPerBatch = context.graph.batchDuration.milliseconds.toDouble / 1000
																																	
																																	// 计算本批次, 对每个RDD/分区要发送Record的数量(及速率限制 RateLimitPerPartition): 批次时长0.2 * 分区处理上限数 500 = 100个数据;
																																	Some(effectiveRateLimitPerPartition.map {
																																		case (tp, limit) => tp -> (secsPerBatch * limit).toLong
																																	})
																																} else {
																																	None
																																}
																															}
																															
																															// 取 额定速率下最大offser 与 最新消息offer, 两者最小值为本批次实际untilOffset.
																															someParts.map { mmp =>
																																mmp.map { case (tp, messages) =>
																																	val uo = offsets(tp) //offsets即为传参进来的, 各分区的Latest Offset值(即最新的消息)
																																	// 取 速率上限offser和 最新日志offset的最小值,为本batch的untilOffset(消费截止offset); 这样保证了不消费还没到的消息; 当没有server没有新消息时, 这个值是相等的.
																																	// currentOffsets(tp) + messages 就是额定速率消费offser上限;
																																	// uo 就是之前latestOffsets()方法求出的Server端该分区最新消费的offset;
																																	tp -> Math.min(currentOffsets(tp) + messages, uo)
																															  }
																															}.getOrElse(offsets)
																														}
																														
																														clampParts
																													}
																													
																													//取上batch的currentOffsets为起始点fromOffset, 并与untilOffset一起封装到OffsetRange对象中;
																													val offsetRanges = untilOffsets.map { case (tp, uo) =>
																														val fo = currentOffsets(tp)
																														OffsetRange(tp.topic, tp.partition, fo, uo)
																													}
																													
																													val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled", true)
																													val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, useConsumerCache)
																													val description = offsetRanges
																														.filter{ offsetRange => offsetRange.fromOffset != offsetRange.untilOffset }
																														.map{ offsetRange => s"topic: " + s"offsets: " }.mkString("\n")
																													
																													// Copy offsetRanges to immutable.List to prevent from being modified by the user
																													val metadata = Map("offsets" -> offsetRanges.toList, StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
																													val inputInfo = StreamInputInfo(id, rdd.count, metadata)
																													ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
																													currentOffsets = untilOffsets
																													commitAll()
																													Some(rdd)												
																												}
																												
																											}
																										}
																									  }
																									  
																									} finally {
																									  ssc.sparkContext.setCallSite(prevCallSite)
																									  ssc.sparkContext.setLocalProperty(scopeNoOverrideKey, prevScopeNoOverride)
																									}
																								}
																								
																								rddOption.foreach { case newRDD =>
																								  if (storageLevel != StorageLevel.NONE) {
																									newRDD.persist(storageLevel)
																									logDebug(s"Persisting RDD ${newRDD.id} for time $time to $storageLevel")
																								  }
																								  if (checkpointDuration != null && (time - zeroTime).isMultipleOf(checkpointDuration)) {
																									newRDD.checkpoint()
																									logInfo(s"Marking RDD ${newRDD.id} for time $time for checkpointing")
																								  }
																								  generatedRDDs.put(time, newRDD)
																								}
																								rddOption
																							  } else {
																								None
																							  }
																							}				
																						}
																						
																						someRDD match {
																						  case Some(rdd) =>
																							val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
																							  foreachFunc(rdd, time)
																							}
																							Some(new Job(time, jobFunc))
																						  case None => None
																						}
																					}
																					jobOption.foreach(_.setCallSite(outputStream.creationSite))
																					jobOption
																				  }
																				}
																				logDebug("Generated " + jobs.length + " jobs for time " + time)
																				jobs
																			}
																		} match {
																			case Success(jobs) => {
																				val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
																				/* 第二步: 将上述创建的Jobs 经由JobScheduler -> SparkContext 提交 DAGScheduler调度, 并最终交由Executor执行;
																				*	- JobScheduler.submitJobSet() -> Job.run() -> UserFunc.func() 触发Action算子: rdd.collect(), count(), foreach()
																				* 	- rdd.action()操作 -> SparkContext.runJob() -> DAGScheduler.handleJobSubmitted() -> DAGScheduler.submitStage()
																				* 		-> TaskSchedulerImpl.submitTasks() 提交给Executor.runTask() 执行此 TaskSet
																				*/
																				jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos)){//JobScheduler.submitJobSet(jobSet)
																					JobScheduler.submitJobSet(jobSet){
																						if (jobSet.jobs.isEmpty) {
																						  logInfo("No jobs added for time " + jobSet.time)
																						} else {
																						  listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
																						  jobSets.put(jobSet.time, jobSet)
																						  
																						  // 创建一个线程任务JobHandle,并交给线程池jobExecutor: TheadPoolExecutor的workQueue队列中
																						  jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job))){
																							JobScheduler.JobHandler.run(){
																								val oldProps = ssc.sparkContext.getLocalProperties
																								try {
																									ssc.sparkContext.setLocalProperties(SerializationUtils.clone(ssc.savedProperties.get()))
																									val formattedTime = UIUtils.formatBatchTime(job.time.milliseconds, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
																									val batchUrl = s"/streaming/batch/?id=${job.time.milliseconds}"
																									val batchLinkText = s"[output operation ${job.outputOpId}, batch time ${formattedTime}]"
																									ssc.sparkContext.setLocalProperty(RDD.CHECKPOINT_ALL_MARKED_ANCESTORS, "true")
																									var _eventLoop = eventLoop
																									if (_eventLoop != null) {//进入这里
																										
																										// 将该Job提交到 StreamingJobProgressListener.runningBatchUIData[Map[Time,BatchUIData]]中
																										// 发送 StreamingListenerOutputOperationStarted 给 sparkListenerBus, 这个主要是用作监控/展示?
																										eventLoop.post(JobStarted(job, clock.getTimeMillis())){//EventLoop.post(event)
																											eventQueue[LinkedBlockingDeque].put(event);
																											
																											{
																												//JobScheduler.start()中 启动"JobScheduler"线程, 循环将接受 JobStarted(包括,等JobSchedulerEvent)的 onReceive()方法-> JobScheduler.handleJobStart()
																												JobScheduler.handleJobStart(job, startTime){
																													if (isFirstJobOfJobSet) {
																														listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo)){
																															sparkListenerBus.post(new WrappedStreamingListenerEvent(event))
																														}
																													}
																													listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
																												}
																											}
																										}
																										
																										/** 核心代码: 触发一个Job的提交: Job.run() -> UserFunc.func() -> sc.runJob() -> DAGScheduler.submitStage() -> TaskSchedulerImpl.submitTasks()
																										* 	UserFunc.func()必须是一个能触发Action RDD的逻辑, 如: rdd.collect(), rdd.foreach(), rdd.count()
																										* 
																										*/
																										SparkHadoopWriterUtils.disableOutputSpecValidation.withValue(true) {
																											job.run(){//Job.run()
																												_result = Try(func()){
																													foreachFunc(rdd, time){
																														// 这里执行业务编写的代码:
																														List<String> collect = rdd.collect();{//JavaRDD.collect()
																															sc.runJob(this, (iter: Iterator[T]) => iter.toArray){
																																DAGScheduler.runJob(){
																																	DAGScheduler.handleJobSubmitted(){
																																		DAGScheduler.submitStage(){
																																			TaskSchedulerImpl.submitTasks()
																																		}
																																	}
																																}
																															}
																														}
																														collect.forEach(str-> System.err.println(str));
																														
																													}
																												}
																											}
																											
																										}
																										
																										_eventLoop = eventLoop
																										if (_eventLoop != null){
																											_eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
																										}
																									} else {
																										
																									}
																								} finally {
																									ssc.sparkContext.setLocalProperties(oldProps)
																								}
																							}
																						  }
																						
																						  
																						  logInfo("Added jobs for time " + jobSet.time)
																						}
																					}
																				}
																			}
																			
																			case Failure(e) =>
																				jobScheduler.reportError("Error generating jobs for time " + time, e)
																				PythonDStream.stopStreamingContextIfPythonProcessIsDead(e)
																		}
																		
																		eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
																	}
																}
																
																
																case ClearMetadata(time) => clearMetadata(time)
																
																case DoCheckpoint(time, clearCheckpointDataLater) => doCheckpoint(time, clearCheckpointDataLater)
																
																case ClearCheckpointData(time) => clearCheckpointData(time)
															}
														}
													}
													
													// 2. 其他eventLoop实现类:  JobScheduler.eventLoop.onReceive(); DAGSchedulerEventProcessLoop.onReceive()
													
												}
											}
										}
									}
								}
								
								
								if (ssc.isCheckpointPresent) {
									restart()
								}else {//第一个启动JobGenerator, 进入这里, 会初始化 DStreamGraph.start()
									startFirstTime(){//JobGenerator.startFirstTime()
										val startTime = new Time(timer.getStartTime())
										graph.start(startTime - graph.batchDuration){//DStreamGraph.start(Time)
											this.synchronized {
												require(zeroTime == null, "DStream graph computation already started")
												zeroTime = time
												startTime = time
												outputStreams.foreach(_.initialize(zeroTime))
												outputStreams.foreach(_.remember(rememberDuration))
												outputStreams.foreach(_.validateAtStart())
												numReceivers = inputStreams.count(_.isInstanceOf[ReceiverInputDStream[_]])
												inputStreamNameAndID = inputStreams.map(is => (is.name, is.id))
												inputStreams.par.foreach(_.start()){
													inputDStream.start(){
														// 当为KafkaDStream时:
														DirectKafkaInputDStream.start(){
															val c = consumer(){//DirectKafkaInputDStream.consumer()
																if (null == kc) {
																  kc = consumerStrategy.onStart(currentOffsets.mapValues(l => new java.lang.Long(l)).asJava){
																		Subscribe.onStart(currentOffsets: ju.Map[TopicPartition, jl.Long]){
																			val consumer = new KafkaConsumer[K, V](kafkaParams)
																			consumer.subscribe(topics)
																			val toSeek = if (currentOffsets.isEmpty) {
																			  offsets
																			} else {
																			  currentOffsets
																			}
																			if (!toSeek.isEmpty) {
																			  val aor = kafkaParams.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
																			  val shouldSuppress = aor != null && aor.asInstanceOf[String].toUpperCase(Locale.ROOT) == "NONE"
																			  try {
																				consumer.poll(0)
																			  } catch {
																				case x: NoOffsetForPartitionException if shouldSuppress => logWarning("Catching NoOffsetForPartitionException since)
																			  }
																			  toSeek.asScala.foreach { case (topicPartition, offset) =>
																				  consumer.seek(topicPartition, offset)
																			  }
																			  // we've called poll, we must pause or next poll may consume messages and set position
																			  consumer.pause(consumer.assignment())
																			}
																			
																			consumer
																		}
																  }
																}
																kc
															}
															paranoidPoll(c)
															
															// 当currentOffsets 为空时, 就没有数据了;
															if (currentOffsets.isEmpty) {
															  currentOffsets = c.assignment().asScala.map { tp =>
																tp -> c.position(tp)
															  }.toMap
															}
															c.pause(currentOffsets.keySet.asJava)
														}
													}
												}
											}
										}
										timer.start(startTime.milliseconds)
										logInfo("Started JobGenerator at " + startTime)
											* INFO JobGenerator: Started JobGenerator at 1582003515000 ms
									}
								}
							}
	 
							executorAllocationManager.foreach(_.start()){
								ExecutorAllocationManager.start(){
									timer.start(){//RecurringTimer.start()
										start(getStartTime()){//RecurringTimer.start(startTime: Long)
											    nextTime = startTime
												thread.start(){
													run(){
														loop(){//RecurringTimer.loop()
															while (!stopped) {
																triggerActionForNextInterval(){//RecurringTimer.triggerActionForNextInterval
																	clock.waitTillTime(nextTime)
																	callback(nextTime){//就是RecurringTimer构造传参进来的 manageAllocation()方法
																		ExecutorAllocationManager.manageAllocation(){
																			logInfo(s"Managing executor allocation with ratios = [$scalingUpRatio, $scalingDownRatio]")
																			if (batchProcTimeCount > 0) {
																			  val averageBatchProcTime = batchProcTimeSum / batchProcTimeCount
																			  val ratio = averageBatchProcTime.toDouble / batchDurationMs
																			  logInfo(s"Average: $averageBatchProcTime, ratio = $ratio" )
																			  if (ratio >= scalingUpRatio) {
																				logDebug("Requesting executors")
																				val numNewExecutors = math.max(math.round(ratio).toInt, 1)
																				// 进行扩容操作
																				requestExecutors(numNewExecutors){//ExecutorAllocationManager.requestExecutors()
																					require(numNewExecutors >= 1)
																					val allExecIds = client.getExecutorIds()
																					logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")
																					val targetTotalExecutors =
																					  math.max(math.min(maxNumExecutors, allExecIds.size + numNewExecutors), minNumExecutors)
																					client.requestTotalExecutors(targetTotalExecutors, 0, Map.empty)
																					logInfo(s"Requested total $targetTotalExecutors executors")
																				}
																			  } else if (ratio <= scalingDownRatio) {
																				logDebug("Killing executors")
																				// 进行缩容操作
																				killExecutor(){//ExecutorAllocationManager.killExecutor()
																					val allExecIds = client.getExecutorIds()
																					logDebug(s"Executors (${allExecIds.size}) = ${allExecIds}")

																					if (allExecIds.nonEmpty && allExecIds.size > minNumExecutors) {
																					  val execIdsWithReceivers = receiverTracker.allocatedExecutors.values.flatten.toSeq
																					  logInfo(s"Executors with receivers (${execIdsWithReceivers.size}): ${execIdsWithReceivers}")

																					  val removableExecIds = allExecIds.diff(execIdsWithReceivers)
																					  logDebug(s"Removable executors (${removableExecIds.size}): ${removableExecIds}")
																					  if (removableExecIds.nonEmpty) {
																						val execIdToRemove = removableExecIds(Random.nextInt(removableExecIds.size))
																						client.killExecutor(execIdToRemove)
																						logInfo(s"Requested to kill executor $execIdToRemove")
																					  } else {
																						logInfo(s"No non-receiver executors to kill")
																					  }
																					} else {
																					  logInfo("No available executor to kill")
																					}
																				}
																			  }
																			}
																			batchProcTimeSum = 0
																			batchProcTimeCount = 0
																		}
																	}
																	prevTime = nextTime
																	nextTime += period
																	logDebug("Callback for " + name + " called at time " + prevTime)
																}
															}
															triggerActionForNextInterval()
														}
													}
												}
												logInfo("Started timer for " + name + " at time " + nextTime)
												nextTime
										}
									}
									logInfo(s"ExecutorAllocationManager started with " +s"ratios = [$scalingUpRatio, $scalingDownRatio] and interval = $scalingIntervalSecs sec")
								}
							}
							
							logInfo("Started JobScheduler")
								* INFO JobScheduler: Started JobScheduler
						}
					}
					
					state = StreamingContextState.ACTIVE
				}
				
				shutdownHookRef = ShutdownHookManager.addShutdownHook(priority)(stopOnShutdown)
				
				env.metricsSystem.registerSource(streamingSource)
				logInfo("StreamingContext started")
					* INFO StreamingContext: StreamingContext started
			}
			
		}
	}
}



JavaStreamingContext.awaitTermination(){
	ssc[StreamingContext].awaitTermination(){
		waiter[ContextWaiter].waitForStopOrError(timeout: Long = -1){
			lock.lock()
			try{
				// 由ssc.awaitTermination()无参进入是-1, 循环等待;
				if (timeout < 0) {
					while (!stopped && error == null) {
						condition.await()
					}
				}else { // 按 timeout时长 循环等待;
					var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
					while (!stopped && error == null && nanos > 0) {
						nanos = condition.awaitNanos(nanos)
					}
				}
				
				if (error != null) throw error
				// 执行到此,说明已停止了
				stopped
			}finally{
				lock.unlock()
			}
			
		}
	}
}


JobScheduler.handleJobCompletion(job, completedTime){
    val jobSet = jobSets.get(job.time)
	// 当incompleteJobs的HashSet里面为空时, 就打上 processingEndTime时间;
    jobSet.handleJobCompletion(job){//JobSet.handleJobCompletion()
		incompleteJobs -= job
		if (hasCompleted(){return incompleteJobs.isEmpty}){
			processingEndTime = System.currentTimeMillis()
		} 
	}
	
    job.setEndTime(completedTime)
    listenerBus.post(StreamingListenerOutputOperationCompleted(job.toOutputOperationInfo))
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    if (jobSet.hasCompleted) {
		val batchCompleted = StreamingListenerBatchCompleted(jobSet.toBatchInfo){
			val batchInfo:BatchInfo = jobSet.toBatchInfo(){//JobSet.toBatchInfo()
				BatchInfo(
				  time,
				  streamIdToInputInfo,
				  submissionTime,
				  // 把JobSet.processingStartTime作为批处理起始时间;
				  if (hasStarted) Some(processingStartTime) else None,
				  // 把JobSet.processingEndTime作为批处理 结束时间; 这里也是本方法的上面 jobSet.handleJobCompletion(job)生成结束时间;
				  if (hasCompleted) Some(processingEndTime) else None,
				  
				  jobs.map { job => (job.outputOpId, job.toOutputOperationInfo) }.toMap
				)
			}
			
			return StreamingListenerBatchCompleted(batchInfo)
		}
		
		listenerBus.post(batchCompleted){
			
		}
    }
    job.result match {
      case Failure(e) =>
        reportError("Error running job " + job, e)
      case _ =>
        if (jobSet.hasCompleted) {
          jobSets.remove(jobSet.time)
          jobGenerator.onBatchCompletion(jobSet.time)
          logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
            jobSet.totalDelay / 1000.0, jobSet.time.toString, //totalDelay =processingEndTime - time.milliseconds = 实际结束时间- scheduler预分配实际
            jobSet.processingDelay / 1000.0   //processingDelay=processingEndTime - processingStartTime = 实际结束时间 - 实际开始时间
          ))
        }
    }	
}


