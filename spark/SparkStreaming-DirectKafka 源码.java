问题:
* 如何分配Kafak 分区? 可以自定义增加分区吗?
* 如何序列化KafakRDD和 函数, 
* 生成和存储KafakJob的机制如何?
* task执行完后, 如何与Driver交互? 有哪些中间工作? 要多久进入下一个Task的运算?
* KafakConsumer.poll() 如何实现的?



SparkStraming- Kafka 运行原理和源码分析



# 1. Drive端 相关线程和源码;

1.1 "RecurringTimer - JobGenerator" 线程定时发送GenerateJob(time)消息, 触发"JobGenerator"线程执行JobGenerator线程;
	- 启动位置: StreamingContext.start() -> JobScheduler.start()-> JobGenerator.startFirstTime();
	- 原理和源码概览:
		RecurringTimer.loop(){
			while (!stopped) {
				triggerActionForNextInterval(){// 不断执行按固定间隔 执行callback();
					clock.waitTillTime(nextTime)//会阻塞等待 直到 nextTime这个时间点; 其内部Thread.sleep()直到特定的时间戳
					callback(nextTime){eventLoop.post(GenerateJobs(new Time(longTime)));} // 发送一个GenerateJobs消息, 该消息会被"JobGenerator"线程的 JobGenerator.processEvent(event)中case GenerateJobs(time) => generateJobs(time)处理
					nextTime += period //重新设置nextTime指,使其按固定间隔后再执行一次 callback;
				}
			}
		}
	- 下游线程: "JobGenerator"线程: JobGenerator.processEvent(event)中case GenerateJobs(time) => generateJobs(time)处理


1.2 "JobGenerator"线程: 
	- 启动位置:
	- 原理和源码概览:
		JobGenerator.start(){//"JobGenerator"线程执行逻辑简述: 1.先调DStreamGraph.generateJobs()根据用户逻辑生成n个Jobs; 2.再将每个Job封装进JobHandler并开启"streaming-job-executor"线程来完成Job的Stage划分和Task提交;
			new EventLoop[JobGeneratorEvent]("JobGenerator").start(){ Thread.run(){
				while (!stopped.get) {
					JobGenerator.onReceive(event){processEvent(event){//JobGenerator.processEvent(event: JobGeneratorEvent)
						case GenerateJobs(time) => generateJobs(time){// JobGenerator.generateJobs(time)
							val jobs= graph.generateJobs(time){//DStreamGraph.generateJobs(time): 先创建相关的RDD,并组成若干个Jobs
								// 一次foreachRDD()对应添加一个outputStream, 一个outputStream对应生成一个Job; n次foreachRDD()(或DStream.register())操作对应创建n个Job;
								val jobs = outputStreams.flatMap(outputStream => {outputStream.generateJob(time){//遍历所有outputStreams操作,  ForEachDStream.generateJob(time)
									val someRDD= parent.getOrCompute(time){//递归调用DStream.getOrCompute(time)方法,直到用最早(第一个)DStream算出当前Action的RDD结果;
										createRDDWithLocalProperties(){body{ 
											compute(time){// DStream.compute(time)的实现类
												DStream.compute(time)// 1. 各DStream的子类,重写其compute(time)方法: FileInputStream, QueueInputDStream, ReceiverInputDStream等;
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
	- 下游处理线程:"streaming-job-executor-n" 线程: new JobHandler(job).run() -> ForEachDStream.foreachFunc() -> rdd.count()/rdd.foreach()/rdd.collect() 操作 -> sc.runJob() -> eventProcessLoop.post(JobSubmitted())


	//  KafakInputDStream
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
	
	
1.3 "streaming-job-executor-n" 线程, 分解和提交Job,并给listenerBus发监控消息;
	- 启动线程: "JobGenerator"线程 中的 jobScheduler.submitJobSet() -> jobExecutor.execute(new JobHandler(job)))
	- 原理和源码概览:
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
	- 下游处理线程: "dag-scheduler-event-loop" 线程
	
val finalStage= createResultStage()
submitStage(finalStage)


	
1.4 "dag-scheduler-event-loop" 线程:  将一个Job切分成n个Stages,并将每个Stage拆分成partition个Tasks以提交的核心调度;
	- 启动线程: "JobGenerator"线程
	- 原理和源码概览:
		DAGSchedulerEventProcessLoop.doOnReceive(){
			case JobSubmitted(jobId, rdd, func) -> dagScheduler.handleJobSubmitted(jobId, rdd, func){
				createResultStage() // 先创建最末尾的finalStage,用于后面向前递归切分;
				submitStage(finalStage){ //从finalStage开始递归寻找父Stage并提交;
					val missing = getMissingParentStages(stage).sortBy(_.id) //依据是否宽窄依赖而判断其是否有父Stage, 若有创建则创建 parentStage并返回;
					if (missing.isEmpty) {//若没有父Stage了,就提交当前stage;
						submitMissingTasks(stage, jobId.get){// 
							getPreferredLocs(stage.rdd, p)//计算其最佳本地化位置;
							taskBinary = sc.broadcast(taskBinaryBytes) // 将task序列化后放入广播变量进行广播;
							taskScheduler.submitTasks(){//TaskSchedulerImpl.submitTasks() 进行TaskSet的提交;
								val manager = createTaskSetManager(taskSet, maxTaskFailures)//将TaskSet封装进 TaskSetManager中;
								schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties) //将Task加到 rootPool.schedulableQueue任务队列中, 
								backend.reviveOffers(){// 调用backend具体实现类: LocalSchedulerBackend/StandaloneSchedulerBackend/ YarnSchedulerBackend; 
									LocalSchedulerBackend.reviveOffers(){NettyRpcEndpointRef].send(ReviveOffers):} // 其实都是利用RPC通信发 ReviveOffers消息;
									// 从"dag-scheduler-event-loop"线程发出ReviveOffsers消息到缓存队列中, 在"dispatcher-event-loop-*"线程处理该消息,并调用DriverEndpoint.makeOffers()处理并发起Rpc请求;
									// 在DriverEndpoint.makeOffers()中安排cpu/memory等资源给各Task, 再向Executor发LaunchTask的Rpc请求; 
									CoarseGrainedSchedulerBackend.reviveOffers(){// Yarn和Standalone模式,都是此实现
										driverEndpoint.send(ReviveOffers);// 发出Rpc请求,向Driver端发出Rpc请求;
									}
								}
							}
						}
					}else{// 若还有父Stage,则进入这里递归取找父类的父类...
						for(parent <- missing){
							submitStage(parent)// 递归调用,直到找到最顶端/头位置的Stage.
						}
					}
				}
			}
		}
	- 下游处理线程: "dispatcher-event-loop-n"线程处理该消息,并调用DriverEndpoint.makeOffers()处理并发起Rpc请求;


1.5 "driver-revive-thread" 定时线程: 默认每个1秒调用driverEndpoint.sent(ReviveOffers) 触发Executor资源的调度和Tasks的提交;

CoarseGrainedSchedulerBackend.DriverEndpoint.onStart(){
	val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")
    reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
			Option(self).foreach(_.send(ReviveOffers));{//向自生所在Rpc发送ReviveOffers消息;
				NettyRpcEnv.send(ReviveOffers);{
					require(message != null, "Message is null")
					nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
				}
			}
        }
    }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS);
}

	
1.6 "dispatcher-event-loop-n" 线程: 该线程循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint.receive/.receiveAndReply()等方法发Rpc请求;
	- 启动线程: "dag-scheduler-event-loop" 线程
	- 原理和源码概览:
		MessageLoop.run(){//循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint方法发Rpc请求
			while (true) {
				val data = receivers.take()//
				data.inbox.process(Dispatcher.this){
					while (true) {safelyCall(endpoint) {message match {
						case RpcMessage(_sender, content, context) => endpoint.receiveAndReply(context){// RpcEndpoint.receiveAndReply(context)
							CoarseGrainedSchedulerBackend.DriverEndpoint.receiveAndReply(context) // 处理RegisterExecutor, StopDriver, StopExecutors等事件;
							BlockManagerMasterEndpoint.receiveAndReply(context) //处理
							HeartbeatReceiver.receiveAndReply(context)
						}
						case OneWayMessage(_sender, content) => endpoint.receive(){//RpcEndpoint.receive()
							CoarseGrainedSchedulerBackend.DriverEndpoint.receive(){//处理StatusUpdate,ReviveOffers,KillTask,KillExecutorsOnHost等请求;
								case StatusUpdate(executorId, taskId, state, data) => 
								case ReviveOffers => makeOffers()//DriverEndpoint.makeOffers(): 先resourceOffers()安排cpu/memory等资源给各Task, 再向Executor发LaunchTask请求;
							}
						}
						case OnStart => endpoint.onStart()
						case RemoteProcessConnected(remoteAddress) => endpoint.onConnected(remoteAddress)
					}}}
				}
			}
		}
	- 下游处理线程: Executor端代码;
 



 
 
 
 
 
 
 
 
 


# 2. Executor端相关线程 和逻辑;

* 何时 new KafakConsumer()? 什么时候 .close()?
* 何时 执行consumer.poll(), 怎么处理?
* 何时提交 commit()提交offsets?
* 主要耗时和瓶颈在哪儿?



2.1 "main"线程: 构造RpcEnv,SparkEnv,Dispatcher, BlockManager,Serializer等对象完成初始化,并启动"dispatcher-event-loop-n"线程循环处理各种Rpc消息;
	- 启动: 由NodeManager生成executor_launcher.sh脚本并启动该java线程
	CoarseGrainedExecutorBackend.main(args){
		run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath){//CoarseGrainedExecutorBackend.run()
			val env = SparkEnv.createExecutorEnv(driverConf, executorId, hostname, port, cores){SparkEnv.create(){
				val rpcEnv = RpcEnv.create() // 通过RpcEnv.create()->NettyRpcEnv()->new Dispatcher() 启动"dispatcher-event-loop-n"线程来循环处理各种消息;
				val serializer = instantiateClassFromConf[Serializer]
				val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
				val blockManagerMaster = new BlockManagerMaster()
				val blockManager = new BlockManager()
				val metricsSystem= MetricsSystem.createMetricsSystem("executor", conf, securityManager)
				
			}}
			env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(env.rpcEnv, driverUrl, executorId, hostname, cores, userClassPath, env))
		}
	}
	- 下游线程: "dispatcher-event-loop-n"线程; 


2.2 "dispatcher-event-loop-n" 线程: 该线程循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint.receive/.receiveAndReply()等方法发Rpc请求;
	- 启动位置: 在"main"/线程的 CoarseGrainedExecutorBackend.run() ->SparkEnv.createExecutorEnv()-> RpcEnv.create() -> new Dispatcher()构造中完成对"dispatcher-event-loop-n"线程的启动;
	MessageLoop.run(){//循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint方法发Rpc请求
		while (true) {
			val data = receivers.take()//
			data.inbox.process(Dispatcher.this){
				while (true) {safelyCall(endpoint) {message match {
					case RpcMessage(_sender, content, context) => endpoint.receiveAndReply(context){// RpcEndpoint.receiveAndReply(context)
						BlockManagerSlaveEndpoint.receiveAndReply(context){
							case RemoveRdd(rddId) => blockManager.removeRdd(rddId) //异步移除RDD;
						}
						HeartbeatReceiver.receiveAndReply(context)
					}
					case OneWayMessage(_sender, content) => endpoint.receive(){//RpcEndpoint.receive()
						CoarseGrainedExecutorBackend.receive(){//处理RegisteredExecutor,LaunchTask,StopExecutor等请求;
							case RegisteredExecutor => executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false) 
							case LaunchTask => executor.launchTask() -> threadPool.execute(new TaskRunner(context, taskDescription))// 启动 "Executor task launch worker for task" 线程;
						}
					}
					case OnStart => endpoint.onStart()
					case RemoteProcessConnected(remoteAddress) => endpoint.onConnected(remoteAddress)
				}}}
			}
		}
	}
	- 下游线程:  
		* "Executor task launch worker for task" 线程;
		* "block-manager-slave-async-thread-pool-n"线程
		* "driver-heartbeater"线程


2.3 "Executor task launch worker for task "线程: 完成Executor端对一个Task的计算逻辑
	- 启动位置: 由"dispatcher-event-loop-n"线程中: CoarseGrainedExecutorBackend.receive(): case LaunchTask(data) => executor.launchTask(taskDesc)-> threadPool.execute(tr)
	TaskRunner.run(){
		task.run(){runTask(context){
			ResultTask.runTask(){// 	1. 运行ResultTask时: 
				val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)]()// 对RDD/func进行反序列化;
				val it = rdd.iterator(partition, context){// 计算parentRDD
					if (storageLevel != StorageLevel.NONE){//若该RDD开启了Cache缓存,则先从读取结果;
						getOrCompute(split, context)// 若有缓存则读取缓存, 否则计算获得;
					}else{//RDD没有缓存,则计算获得其结果
						computeOrReadCheckpoint(){
							if (isCheckpointedAndMaterialized) {
								firstParent[T].iterator(split, context)
							}else{//若该RDD即没有cache,也未曾被checkpointed,则调用其compute()方法, 计算获取该RDD结果;
								compute(split, context){//MapPartitionsRDD.compute(): 对于MapPartitionsRDD,他会递归调用父RDD.compute(),直到从首个RDD(KafkaRDD/ InputDStream对应RDD)算出数据;
									f(context, split.index, firstParent[T].iterator(split, context))//
								}
							}
						}
					}
				}
				func(context, it) // 执行本RDD的计算逻辑;
			}
			ShuffleMapTask.runTask(){// 2. 运行ShuffleMapTask时: 
			}
		}}
	}
	- 下游线程: "dispatcher-event-loop-n" 线程?

	- KafakRDD.compute()完成从Server拉取数据,并传给下游RDD,即发生在这个线程中;


	// 在RDD.计算中,kafkaRDD.iterator计算逻辑;
	kafkaDStream.foreachRDD(kafkaRDD=>{
		val mapParts= kafkaRDD.mapPartitions(( it:Iterator[ConsumerRecord[String,String]] )=>{// it:KafkaRDDIterator
			val jsons = new ArrayBuffer[JSONObject]
			while(it.hasNext){//KafkaRDDIterator.hasNext() = requestOffset < part.untilOffset 
				val record:ConsumerRecord[String,String] = it.next();{//KafkaRDDIterator.next()
					assert(hasNext(), "Can't call getNext() once untilOffset has been reached") //重新调用下上面的hasNext()方法;
					
					val r = consumer.get(requestOffset, pollTimeout);{//CachedKafkaConsumer.get(offset: Long, timeout: Long)
						logDebug(s"Get $groupId $topic $partition nextOffset $nextOffset requested $offset") //精确打印每个CachedKafkaConsumer.nextOffset 与KafkaRDDIterator.requestOffset是否相同;
						//什么情况下, nextOffset 不等于 KafkaRDDIterator.requestOffset呢?
						if (offset != nextOffset) {//如果CachedKafkaConsumer中的将拉取record的位移量 不是 本次要拿的,则重置KafakConsuemr.subscripts.assginment.TPState.position 为requestOffset;
							logInfo(s"Initial fetch for $groupId $topic $partition $offset")
							seek(offset) //重置KafakConsuemr.subscripts.assginment.TPState.position
							poll(timeout);{//CachedKafkaConsumer.poll(timeout:Long)
								val p = consumer.poll(timeout) //timeout由spark.streaming.kafka.consumer.poll.ms控制, 默认512ms;
								// 这一步是保险起见, 因为该KafakConsuer已经.assign()了这个TopicPartition了; 
								val r = p.records(topicPartition) //过滤出本分区(Kafka分区 & RDD分区); 该consumer可能订阅多个分区吗? 不可能,因为该consumer不是基于subscribe(topics)订阅方式,而是基于assign(tps)直接绑定(绑死)List<TopicPartition>的方式;只从某Kafak分区消息;
								logDebug(s"Polled ${p.partitions()}  ${r.size}")
								buffer = r.iterator //将本地poll()到的数据 缓存到buffer变量中; 所以才加CachedKafak*?
							}
						}
						
						if (!buffer.hasNext()){//若缓存buffer中没有消息, 就再poll()一次; 可能因为服务端繁忙,网络慢, rebalancing, 已无新数据 等原因poll()为空;
							poll(timeout) //还是有可能为空; 虽然可能是第二次拉取; 
						}
						
						//如果上面1次poll()拉取不到数据就抛出Error? 若Driver.RDD.mapPartitions()中不catch的话,会一直往上抛到Executor.TaskRunner.run()而终止"Executor task launch worker"线程;
						assert(buffer.hasNext(),s"Failed to get records for $groupId $topic $partition $offset after polling for $timeout");{
							if (!assertion) throw new java.lang.AssertionError("assertion failed: "+ message)
						}
						var record = buffer.next() //进入这里说明buff中还有数据, 从其中取出1个ConsumerRecord,用于next()返回; 且需要保证 该ConsumerRecord.offset==requestOffset;
						if (record.offset != offset) {//什么情况下不相等?:  commit失败,rebalance,(?)都可能导致 此Record不是requestOffset;
							logInfo(s"Buffer miss for $groupId $topic $partition $offset")
							seek(offset) //重新设置消费位置,从新拉取;
							poll(timeout);{//CachedKafkaConsumer.poll(timeout:Long)
								val p = consumer.poll(timeout) //timeout由spark.streaming.kafka.consumer.poll.ms控制, 默认512ms;
								// 这一步是保险起见, 因为该KafakConsuer已经.assign()了这个TopicPartition了; 
								val r = p.records(topicPartition) //过滤出本分区(Kafka分区 & RDD分区); 该consumer可能订阅多个分区吗? 不可能,因为该consumer不是基于subscribe(topics)订阅方式,而是基于assign(tps)直接绑定(绑死)List<TopicPartition>的方式;只从某Kafak分区消息;
								logDebug(s"Polled ${p.partitions()}  ${r.size}")
								buffer = r.iterator //将本地poll()到的数据 缓存到buffer变量中; 所以才加CachedKafak*?
							}
							
							// 没有数据,抛Error终止TaskRunner.run(), 建议在RDD.mapPartitions()中catch;
							assert(buffer.hasNext(),s"Failed to get records for $groupId $topic $partition $offset after polling for $timeout")
							record = buffer.next()
							assert(record.offset == offset,s"Got wrong record for $groupId $topic $partition even after seeking to offset $offset")
						}
						nextOffset = offset + 1 //nextOffset计数器 +1;
						record
					}
					
					requestOffset += 1 //将该KafkaRDDIterator实例中的requestOffset +1,表示该RDD成功输出了1个数据, 下一次的请求offset位置+1;
				}
				jsons.+=(JSON.parseObject(record.value()))
			}
			
			Array("jsons.size="+jsons.size).iterator
		})
		mapParts.count()
	})
	

	
	
	

2.4 "block-manager-slave-async-thread-pool-n"线程: 执行删除过期RDD,Shuffle,Broadcast(广播变量)的数据;
	-  启动位置: 由"dispatcher-event-loop-n"线程的Inbox.process()-> case RpcMessage() -> BlockManagerSlaveEndpoint.receiveAndReply(context) -> doAsync() 触发;
	BlockManagerSlaveEndpoint.doAsync(){
		Future {body(){executor.execute(command){
			doAsync(){ // 对于BlockManagerSlaveEndpoint.receiveAndReply()中 case RemoveRdd(rddId) =>
				blockManager.removeRdd(rddId);
			}
			doAsync(){// 对于BlockManagerSlaveEndpoint.receiveAndReply()中 case RemoveShuffle(shuffleId) =>
				mapOutputTracker.unregisterShuffle(shuffleId)
				SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
			}
			doAsync(){// 对于BlockManagerSlaveEndpoint.receiveAndReply()中 case RemoveBroadcast(broadcastId, _) =>
				blockManager.removeBroadcast(broadcastId, tellMaster = true)
			}
		}}}
	}
	- 下游线程: ?



2.5 "driver-heartbeater"线程: 定时(默认10秒)发送与Driver的心跳检测? 
	- 启动位置: "dispatcher-event-loop-n" 线程的 CoarseGrainedExecutorBackend.receive():case RegisteredExecutor =>  executor = new Executor()构造时,在 startDriverHeartbeater()启动该线程;
	new Executor().startDriverHeartbeater(){
		heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS); {//按固定频率(默认10秒) 心跳检测: 向Driver端的"dispatcher-event-loop-n"线程发: HeartbeatResponse和RegisterBlockManager请求
			val response = heartbeatReceiverRef.askSync[HeartbeatResponse](message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"));
			if (response.reregisterBlockManager) {//当Driver返回的HeartbeatResponse消息中要求reregisterBM=true时,Executor会RegisterBlockManager请求;
				env.blockManager.reregister();{//发送RegisterBlockManager消息;
					driverEndpoint.askSync[BlockManagerId](RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))//发送RegisterBlockManager消息;
				}
			}
		}
	}
	- 下游线程: 无










# 3. KafakServer 端 Kafak 和Log逻辑


KafkaApi.handleConsumerRequest(){
	// 简述调用和逻辑; 详细源码见Kafak源码
}










ClearMetadata和RemoveRDD的逻辑

JobGenerator.onReceive(event){
	JobGenerator.processEvent(event){
		case ClearMetadata(time) => clearMetadata(time){//JobGenerator.clearMetadata(time)
			ssc.graph.clearMetadata(time);{//DStreamGraph.clearMetadata(time)
				outputStreams.foreach(ds=>{
					ds.clearMetadata(time);{//DStream.clearMetadata(time)
						val unpersistData = ssc.conf.getBoolean("spark.streaming.unpersist", true)
						val oldRDDs = generatedRDDs.filter(_._1 <= (time - rememberDuration))
						generatedRDDs --= oldRDDs.keys
						if (unpersistData) {//默认true;进入这里删除本RDD的元数据(包括持久化的RDD);
						  logDebug(s"Unpersisting old RDDs: ${oldRDDs.values.map(_.id).mkString(", ")}")
						  oldRDDs.values.foreach { rdd =>
							rdd.unpersist(false);{//RDD.unpersist(blocking:Boolean) 移除本RDD的 持久化(内存或磁盘)
								logInfo("Removing RDD " + id + " from persistence list")
								sc.unpersistRDD(id, blocking);{//SparkContext.unpersistRDD(rddId, blocking)
									env.blockManager.master.removeRdd(rddId, blocking);{//删除Driver端的BlockManagerMaster.removeRdd(rddId, blocking)
										
									}
									
									persistentRdds.remove(rddId);//从SparkContext内存中删除persistentRdds:HashMap[Long,RDD]缓存的该rddId对应的数据;
									listenerBus.post(SparkListenerUnpersistRDD(rddId)) // 向Metrics监控系统发送一个删RDD的监控信息;
								}
								storageLevel = StorageLevel.NONE
								this
							}
							// Explicitly remove blocks of BlockRDD
							rdd match {
							  case b: BlockRDD[_] =>
								logInfo(s"Removing blocks of RDD $b of time $time")
								b.removeBlocks()
							  case _ =>
							}
						  }
						}
						
						dependencies.foreach(parentDS=>{
							parentDS.clearMetadata(time);//一样的递归调用DStream.clearMetadata(time); 直到把所有的父RDD的持久化RDD都删除;
						});
					}
				});
			}
		}
	}
}



