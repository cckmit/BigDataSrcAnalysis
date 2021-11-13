Spark Executor相关的线程:
* main 线程:
* dispatcher-event-loop-1: 
* Executor task launch worker for task $taskId
* block-manager-slave-async-thread-pool-n: 

# Shuffle
* shuffle-client-4-1: ?
* shuffle-server-5-1: 
* files-client-6-1
# 通信
* driver-heartbeater: 与Driver通信的心跳机制; ok
* rpc-client-3-1: ?
* netty-rpc-env-timeout: ?




/** 启动"main"线程: 构造RpcEnv,SparkEnv,Dispatcher, BlockManager,Serializer等对象完成初始化,并启动"dispatcher-event-loop-n"线程循环处理各种Rpc消息;
* 		- 触发: 由NodeManager生成executor_launcher.sh脚本并启动该java线程
	功能: 创建Executor端的SparkEnv:初始化各种组件:Rpc通信, BlockManager, 序列化器, Metrics监控系统等;
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
*/

CoarseGrainedExecutorBackend.main(args){
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()
	
	// 将args参数中解析出 dirverUri,hostname,appId等;
    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
		  
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }
    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 || appId == null) {
      printUsageAndExit()
    }
	
	// 启动Executor
    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath){//CoarseGrainedExecutorBackend.run()
		Utils.initDaemon(log)
		SparkHadoopUtil.get.runAsSparkUser {() =>
			Utils.checkHost(hostname)
			// Bootstrap to fetch the driver's Spark properties.
			val executorConf = new SparkConf
			val port = executorConf.getInt("spark.executor.port", 0)
			val fetcher = RpcEnv.create("driverPropsFetcher",hostname,port,executorConf)
			val driver = fetcher.setupEndpointRefByURI(driverUrl)
			val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig)
			val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", appId))
			fetcher.shutdown()
			
			// Create SparkEnv using properties we fetched from the driver.
			val driverConf = new SparkConf()
			for ((key, value) <- props) {
			// this is required for SSL in standalone mode
				if (SparkConf.isExecutorStartupConf(key)) {
					driverConf.setIfMissing(key, value)
				} else {
					driverConf.set(key, value)
				}
			}
			if (driverConf.contains("spark.yarn.credentials.file")) {
				logInfo("Will periodically update credentials from: " + driverConf.get("spark.yarn.credentials.file"))
				SparkHadoopUtil.get.startCredentialUpdater(driverConf)
			}
			
			
			/* "main"线程最重要一步: 创建Executor端的SparkEnv:初始化各种组件:Rpc通信, BlockManager, 序列化器, Metrics监控系统等;
			*	- Rpc组件: 通过RpcEnv.create()->NettyRpcEnv()->new Dispatcher() 启动"dispatcher-event-loop-n"线程来循环处理各种消息;
			*	- 存储系统: BlockManagerMaster
			*	-  序列化器, Metrics监控器, 广播管理器等;
			*/
			val env = SparkEnv.createExecutorEnv(driverConf, executorId, hostname, port, cores, cfg.ioEncryptionKey, isLocal = false){//SparkEnv.createExecutorEnv()
				// 这里面创建一系列的组件对象;
				val env = create(conf,SparkContext.DRIVER_IDENTIFIER,bindAddress, advertiseAddress,Option(port),isLocal,numCores,ioEncryptionKey){//SparkEnv.create()

					val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER
					if (isDriver) {
					  assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
					}
					val securityManager = new SecurityManager(conf, ioEncryptionKey)
						* INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(it-out-allen); groups with view permissions: Set(); users  with modify permissions: Set(it-out-allen); groups with modify permissions: Set()
						
					if (isDriver) {
					  securityManager.initializeAuth()
					}
					val systemName = if (isDriver) driverSystemName else executorSystemName
					
					/* 1. 创建基于Netty的Rpc通信后台, 最主要的是RpcEnv.create()->NettyRpcEnv()->new Dispatcher() 启动"dispatcher-event-loop-n"线程, 完成各种Rpc请求的处理转发;
					* 	- new NettyRpcEnvFactory().create(config) -> new NettyRpcEnv() -> new Dispatcher():
						-> for (i <- 0 until numThreads) 循环中: pool.execute(new MessageLoop) 启动numThreads各线程来循环处理各种Rpc消息;
					*/
					val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf, securityManager, numUsableCores, !isDriver){
						val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager, numUsableCores, clientMode)
						new NettyRpcEnvFactory().create(config){//NettyRpcEnvFactory.create()
							val sparkConf = config.conf
							val javaSerializerInstance =new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
							val nettyEnv =new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress, config.securityManager, config.numUsableCores){// 会创建一个Dispatcher实例;
								private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores){// Dispatcher()的构造函数中根据线程数new MessageLoop,并循环运行;
									private val endpoints: ConcurrentMap[String, EndpointData] =new ConcurrentHashMap[String, EndpointData]
									private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]
									private val receivers = new LinkedBlockingQueue[EndpointData]

									private[netty] val transportConf = SparkTransportConf.fromSparkConf(conf.clone.set("spark.rpc.io.numConnectionsPerPeer", "1"),"rpc",conf.getInt("spark.rpc.io.threads", 0))
									private val threadpool: ThreadPoolExecutor = {
										val availableCores = if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
										
										// 设定NettyRpcEnv.Dispatcher中处理线程数量; 默认可用线程数其大于2{max(2,availableCores)}, spark.rpc.netty.dispatcher.numThreads可设置;
										val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads", math.max(2, availableCores))
										val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
										for (i <- 0 until numThreads) {
										  
										  /* 本线程最重要的启动任务: 在这里正是启动"dispatcher-event-loop-n"线程, 循环处理各种Rpc消息;
										  * 
										  */
										  pool.execute(new MessageLoop){
												MessageLoop.run(){
													try {
														while (true) {
														  try {
															val data = receivers[LinkedBlockingQueue[EndpointData]].take()
															if (data == PoisonPill) {
															  receivers.offer(PoisonPill)
															  return
															}
															data.inbox.process(Dispatcher.this)
														  } catch {
															case NonFatal(e) => logError(e.getMessage, e)
														  }
														}
													} catch {
														case ie: InterruptedException => // exit
													}
												}
										  }
										}
										pool
									}
									private val PoisonPill = new EndpointData(null, null, null)
								}
								private val streamManager = new NettyStreamManager(this)
								private val transportContext = new TransportContext(transportConf, new NettyRpcHandler(dispatcher, this, streamManager))
								private val clientFactory = transportContext.createClientFactory(createClientBootstraps())
							}
							
							if (!config.clientMode) {
							  val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
								nettyEnv.startServer(config.bindAddress, actualPort)
								(nettyEnv, nettyEnv.address.port)
							  }
							  try {
									Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
							  } catch {
									case NonFatal(e) => nettyEnv.shutdown();throw e
							  }
							}
							nettyEnv
						}
					}
						
						* INFO Utils: Successfully started service 'sparkDriver' on port 58014.
					if (isDriver) {
					  conf.set("spark.driver.port", rpcEnv.address.port.toString)
					}
					// 2. 创建序列化者 serializer: 
					val serializer = instantiateClassFromConf[Serializer]("spark.serializer", "org.apache.spark.serializer.JavaSerializer"){
						instantiateClass[T](conf.get(propertyName, defaultClassName))
					}
					val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
					val closureSerializer = new JavaSerializer(conf)
					
					// 3. 创建广播管理器
					val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
					val mapOutputTracker = if (isDriver) {
					  new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
					} else {
					  new MapOutputTrackerWorker(conf)
					}

					mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,new MapOutputTrackerMasterEndpoint(rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf)){
						RpcEndpointRef = {
						  if (isDriver) {
							logInfo("Registering " + name)
							rpcEnv.setupEndpoint(name, endpointCreator)
						  } else {
							RpcUtils.makeDriverRef(name, conf, rpcEnv)
						  }
						}
					}

					
					// 4. 创建BlockManagerMaster,用于接收BM的注册信息;
					val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(BlockManagerMaster.DRIVER_ENDPOINT_NAME, new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)), conf, isDriver)
						* INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
					// 先创建BM对象,但要晚一点 掉initialize()方法后,才能被使用; NB: blockManager is not valid until initialize() is called later.
					val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster, serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager, blockTransferService, securityManager, numUsableCores)
						* INFO DiskBlockManager: Created local directory at C:\Users\it-out-allen\AppData\Local\Temp\blockmgr-da6bb514-0c2f-48d9-8ed2-7c09e134f42d
						* INFO MemoryStore: MemoryStore started with capacity 1992.0 MB
					
					// 创建运行监控系统:MetricsSystem
					val metricsSystem = if (isDriver) {
					  MetricsSystem.createMetricsSystem("driver", conf, securityManager)
					} else {
					  conf.set("spark.executor.id", executorId)
					  val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
					  ms.start()
					  ms
					}
					val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
					  new OutputCommitCoordinator(conf, isDriver)
					}
					val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator", new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
					outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)


					return envInstance;
					
				}
				
				
				SparkEnv.set(env)
				env
			}
			
			env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(env.rpcEnv, driverUrl, executorId, hostname, cores, userClassPath, env))
			workerUrl.foreach { url =>env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))}
			
			env.rpcEnv.awaitTermination()
			
			SparkHadoopUtil.get.stopCredentialUpdater()
		  
		}
		
	}
    System.exit(0)
	
}

2. 线程 dispatcher-event-loop-n:
- 启动:在"main"线程的 CoarseGrainedExecutorBackend.run() -> SparkEnv.createExecutorEnv() -> SparkEnv.create()



/** 启动 "dispatcher-event-loop-n" 线程: 该线程循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint.receive/.receiveAndReply()等方法发Rpc请求;
*		- 启动位置: 在"main"/线程的 CoarseGrainedExecutorBackend.run() ->SparkEnv.createExecutorEnv()-> RpcEnv.create() -> new Dispatcher()构造中完成对"dispatcher-event-loop-n"线程的启动;
*	
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
*/

pool.execute(new MessageLoop){
	
	{// 解释该线程的启动: 在new SparkContext()函数创建SparkEnv的方法中 createDriverEnv() -> RpcEnv.create()
		CoarseGrainedExecutorBackend.main() -> run() -> SparkEnv.createExecutorEnv() -> SparkEnv.create() -> RpcEnv.create()
			-> new NettyRpcEnvFactory().create(config) -> new NettyRpcEnv(){
				dispatcher: Dispatcher = new Dispatcher(this){
					private val threadpool: ThreadPoolExecutor = {
						// 默认创建至少2个线程资源的 ThreadPool, 其名称为dispatcher-event-loop-n, 用于处理各种Rpc消息通信;
						val numThreads = nettyEnv.conf.getInt("spark.rpc.netty.dispatcher.numThreads",math.max(2, Runtime.getRuntime.availableProcessors()))
						val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
						for (i <- 0 until numThreads) {
							pool.execute(new MessageLoop)// 其中代码:while(true)循环从receivers:LinkedBlockingQueue[EndpointData]队列拉取(take)消息并调data.inbox.process()进行处理;
						}
						pool
					}
				}
			}
	}
	
	def run(){//MessageLoop.run()
		while (true) {//循环从队列中拉取消息,和进行消息处理;
			try {
				val data = receivers.take() //从LinkedBlockingQueue[EndpointData] 中取
				if (data == PoisonPill) {//正常不进入这里; 若为空,直接结束循环?
				  receivers.offer(PoisonPill)
				  return
				}
				
				data.inbox.process(Dispatcher.this);{// Inbox.process(dispatcher: Dispatcher)
					var message: InboxMessage = null
					// 从messages:LinkedList<InboxMessage> 中获取first元素消息; 
					inbox.synchronized { 
						if (!enableConcurrent && numActiveThreads != 0) {// 不进入;
							return
						}
						message = messages.poll() //拉取消息;从LinkedList执行一次poll()操作, 将该List头部的firstNode元素返回;
						if (message != null) {//但有消息时, 将numActiveThreads数量 +1;
							numActiveThreads += 1
						} else { //若messages的List中没有消息, 直接返回, 不再进入下面的while()处理循环了;
							return
						}
					}
					
					// 进入这里,则代表有 msg要处理, messages列表不为空;
					while (true) {
					  safelyCall(endpoint) {
						message match {
							case RpcMessage(_sender, content, context) => //处理RpcEndpointRef.ask 请求, 需要reply答复
								val partialFunc:PartialFunction = endpoint.receiveAndReply(context){//RpcEndpoint.receiveAndReply(context), 可看做抽象方法;
									BlockManagerSlaveEndpoint.receiveAndReply(context){
										case RemoveRdd(rddId) => doAsync[Int]("removing RDD " + rddId, context)(()->{blockManager.removeRdd(rddId)}){//doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T) {
											val future = Future {
												logDebug(actionMessage)
												body
											}{//Future.apply[T](body: =>T)
												val runnable = new PromiseCompletingRunnable(body)
												// 在这里 真正将runnable:Runnable 提交给线程池执行: 启动"block-manager-slave-async-thread-pool-n"线程任务执行 blockManager.removeRdd(rddId)逻辑;
												executor.prepare.execute(runnable){//ExecutionContextImpl.execute(command:Runnable)
													executor.execute(command)// executor为一个Java的ThreadPoolExecutor线程池类, 这里其实就是: ThreadPoolExecutor.execute(command)
													{//启动"block-manager-slave-async-thread-pool-n"线程任务执行 删除RDD的逻辑;
														Runnable.run(){
															blockManager.removeRdd(rddId);
														}
													}
												}
												runnable.promise.future //将该命令的Future 异步返回;
											}
											future.onSuccess { case response => context.reply(response)}
											future.onFailure { case t: Throwable =>context.sendFailure(t)}
										}
										
										case RemoveShuffle() => 
										case RemoveBroadcast() => 
									}
								}
								partialFunc.applyOrElse[Any, Unit](content, { msg =>throw new SparkException()})
							
						  case OneWayMessage(_sender, content) => // 处理RpcEndpointRef.send()请求, 不用直接回复;
							val partialFunc:PartialFunction= endpoint.receive(){//RpcEndpoint.receive(context)
								
								// Executor - CoarseGrainedExecutorBackend 作为 endpoint:RpcEndpoint的实现类:
								CoarseGrainedExecutorBackend.receive(){
									case RegisteredExecutor => executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false){//启动一个已注册的Executor; new Executor()构造方法中:
										startDriverHeartbeater(){// 启动一个默认10秒的定时线程"",定时向Driver发心跳检测消息:
											heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS);
										}
									} 
									
									case RegisterExecutorFailed(message) => exitExecutor(1, "Slave registration failed: " + message) // 
									case LaunchTask(data) => {//当接收到LaunchTask消息时, 调用Executor.launchTask()来启动一个"Executor task launch worker for task" 线程,完成该Task的RDD计算;
										if (executor == null) {
											exitExecutor(1, "Received LaunchTask command but executor was null")
										}else{
											val taskDesc = TaskDescription.decode(data.value)
											logInfo("Got assigned task " + taskDesc.taskId)
											executor.launchTask(this, taskDesc){//Executor.launchTask()
												threadPool.execute(new TaskRunner(context, taskDescription))// 启动 "Executor task launch worker for task" 线程;
											}
										}
									}
									case StopExecutor => self.send(Shutdown)
								}
							}
							
							partialFunc.applyOrElse[Any, Unit](content, { msg =>
							  throw new SparkException(s"Unsupported message $message from ${_sender}")
							})

						  case OnStart =>
							endpoint.onStart()
							if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
							  inbox.synchronized {
								if (!stopped) {
								  enableConcurrent = true
								}
							  }
							}

						  case OnStop =>
							val activeThreads = inbox.synchronized { inbox.numActiveThreads }
							assert(activeThreads == 1, s"There should be only a single active thread but found $activeThreads threads.")
							dispatcher.removeRpcEndpointRef(endpoint)
							endpoint.onStop()
							assert(isEmpty, "OnStop should be the last message")

						  case RemoteProcessConnected(remoteAddress) =>
							endpoint.onConnected(remoteAddress)

						  case RemoteProcessDisconnected(remoteAddress) =>
							endpoint.onDisconnected(remoteAddress)

						  case RemoteProcessConnectionError(cause, remoteAddress) =>
							endpoint.onNetworkError(cause, remoteAddress)
						}
					  }
					  
					  inbox.synchronized {
						// "enableConcurrent" will be set to false after `onStop` is called, so we should check it
						// every time.
						if (!enableConcurrent && numActiveThreads != 1) {
						  // If we are not the only one worker, exit
						  numActiveThreads -= 1
						  return
						}
						message = messages.poll()
						if (message == null) {
						  numActiveThreads -= 1
						  return
						}
					  }
					}
				}
			} catch {
			case NonFatal(e) => logError(e.getMessage, e)
			}
		}
	}
}





/** 线程 Executor task launch worker for task n": 完成Executor端对一个Task的计算逻辑
*		- 触发逻辑: 由由"dispatcher-event-loop-n"线程中: CoarseGrainedExecutorBackend.receive(): case LaunchTask(data) => executor.launchTask(taskDesc)
* 	Executor.launchTask(taskDesc) -> threadPool.execute(new TaskRunner(taskDesc))
*		-> TaskRunner.run() -> task.run()
			-> ResultTask.runTask() -> 
				* 1. 反序列化RDD和用户函数:	val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)]
				* 2. 递归调用各级父RDD.compute()直到算出上级RDD数据结果:  rdd.iterator(partition, context)
				* 3. 执行Action 定义的函数: func(context, it)
			
			-> ShuffleMapTask.runTask()
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

*/

Executor.launchTask(context: ExecutorBackend, taskDescription: TaskDescription){
	val tr = new TaskRunner(context, taskDescription) // 创建Runnable线程实现类:TaskRunner;
    runningTasks.put(taskDescription.taskId, tr)
	// 正式启动一个线程任务, 该线程名为:Executor task launch worker for task $taskId
    threadPool.execute(tr){ //启动"Executor task launch worker for task $taskId" 线程,进行Task的反序列化和RDD计算;
		TaskRunner.run(){
			threadId = Thread.currentThread.getId
			Thread.currentThread.setName(threadName)
			val threadMXBean = ManagementFactory.getThreadMXBean
			val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
			val deserializeStartTime = System.currentTimeMillis()
			val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
					threadMXBean.getCurrentThreadCpuTime
				} else 0L
			Thread.currentThread.setContextClassLoader(replClassLoader)
			val ser = env.closureSerializer.newInstance()
			logInfo(s"Running $taskName (TID $taskId)")
			execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
			var taskStart: Long = 0
			var taskStartCpu: Long = 0
			startGCTime = computeTotalGcTime()

			try {
				Executor.taskDeserializationProps.set(taskDescription.properties)
				updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)
				task = ser.deserialize[Task[Any]](
				  taskDescription.serializedTask, Thread.currentThread.getContextClassLoader)
				task.localProperties = taskDescription.properties
				task.setTaskMemoryManager(taskMemoryManager)
				logDebug("Task " + taskId + "'s epoch is " + task.epoch)
				env.mapOutputTracker.updateEpoch(task.epoch)
				taskStart = System.currentTimeMillis()
				taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
				  threadMXBean.getCurrentThreadCpuTime
				} else 0L
				var threwException = true
				
				/** 启动Task运行: task.run()
				*	Task.run()->Task.runTask():
				*		-> ResultTask.runTask() : 1. 选序列化(rdd,func),2. 再执行func(context, rdd.iterator()); 
							- 返回的是具体该RDD[T]中T所指实例对象,如: RDD[String]->String, RDD[JSONObject]=>JSONObject.class;
				*		-> ShuffleMapTask.runTask(): 返回MapStatus对象;
							- 返回MapStatus的子类 CompressedMapStatus(loc: BlockManagerId,compressedSizes: Array[Byte])的实例对象;
				*/
				val value = try {
					val res = task.run(taskAttemptId = taskId,attemptNumber = taskDescription.attemptNumber,metricsSystem = env.metricsSystem){//Task.run()
						SparkEnv.get.blockManager.registerTask(taskAttemptId)
						context = new TaskContextImpl(stageId,partitionId,taskAttemptId)
						TaskContext.setTaskContext(context)
						taskThread = Thread.currentThread()
						if (_reasonIfKilled != null) {
						  kill(interruptThread = false, _reasonIfKilled)
						}
						new CallerContext("TASK",SparkEnv.get.conf.get(APP_CALLER_CONTEXT), appId,appAttemptId,jobId).setCurrentContext()
						try {
							
							/* 根据最终结果或Shuffle 分别执行不同任务计算: ResultTask, ShuffleMapTask
							*
							*/
							runTask(context){
								// 1. 对于最终结果的Task: ResultTask
								ResultTask.runTask(context: TaskContext): U ={
									val threadMXBean = ManagementFactory.getThreadMXBean
									val deserializeStartTime = System.currentTimeMillis()
									val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
									  threadMXBean.getCurrentThreadCpuTime
									} else 0L
									val ser = SparkEnv.get.closureSerializer.newInstance()
									val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
									  ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
									_executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
									_executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
									  threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
									} else 0L
									
									// Executor重要环节, 执行用户逻辑
									val it = rdd.iterator(partition, context){//RDD.iterator(split: Partition, context: TaskContext)
										if (storageLevel != StorageLevel.NONE) {
										  getOrCompute(split, context)
										} else {//第一次计算进入这里
										  computeOrReadCheckpoint(split, context){//RDD.computeOrReadCheckpoint()
											if (isCheckpointedAndMaterialized) {
											  firstParent[T].iterator(split, context)
											} else {//进入这里;不同的实现类,不同的执行方法
												compute(split, context){
													// 1. MapPartitionsRDD算子
													MapPartitionsRDD.compute(split: Partition, context: TaskContext){
														val parentResout = firstParent[T]{
															dependencies.head.rdd.asInstanceOf[RDD[U]]
														}.iterator(split, context){
															// 进入RDD.iterator()的递归
															if (storageLevel != StorageLevel.NONE) {
															  getOrCompute(split, context)
															} else {//第一次计算进入这里
															  computeOrReadCheckpoint(split, context)
															}
														}
														
														f(context, split.index, parentResout){//f() 由用户自定义函数 进由MapPartitionsRDD构造函数传入
															
														}
													}
													
													// 2. 其他算子?
												  
												  
												}
											}
										  }
										}
									}
									
									// 执行foreach(), count()等Action算子中定义的逻辑;
									func(context, it)
									
									
								}
								
								// 2. 对于中间Shuffle操作的 ShuffleMapTask:
								ShuffleMapTask.runTask(){
									
									
								}
							  
								// FakeTask 是什么? 测试的?
								
							}
						} catch {
						  case e: Throwable =>
							try {
							  context.markTaskFailed(e)
							} catch {
							  case t: Throwable => e.addSuppressed(t)
							}
							context.markTaskCompleted(Some(e))
							throw e
						} finally {
						  try {
							context.markTaskCompleted(None)
						  } finally {
							try {
							  Utils.tryLogNonFatalError {
								SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
								SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP)
								val memoryManager = SparkEnv.get.memoryManager
								memoryManager.synchronized { memoryManager.notifyAll() }
							  }
							} finally {
							  TaskContext.unset()
							}
						  }
						}
						
						
						
					}
					threwException = false
					res
				} finally {
				  val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
				  val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()
				  if (freedMemory > 0 && !threwException) {
					val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
					if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
					  throw new SparkException(errMsg)
					} else {
					  logWarning(errMsg)
					}
				  }

				  if (releasedLocks.nonEmpty && !threwException) {
					val errMsg =s"${releasedLocks.size} block locks were not released by TID = $taskId:\n")
					if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
					  throw new SparkException(errMsg)
					} else {
					  logInfo(errMsg)
					}
				  }
				}
				task.context.fetchFailed.foreach { fetchFailure =>
				  logError(s"TID ${taskId} completed successfully though internally it encountered", fetchFailure)
				}
				val taskFinish = System.currentTimeMillis()
				val taskFinishCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
				  threadMXBean.getCurrentThreadCpuTime
				} else 0L

				task.context.killTaskIfInterrupted()
				val resultSer = env.serializer.newInstance()//创建用于将结果序列化的对象,默认JavaSerializerInstance
				val beforeSerialization = System.currentTimeMillis()
				val valueBytes = resultSer.serialize(value)		//将计算结果value:CompressedMapStatus
				val afterSerialization = System.currentTimeMillis()
				// Note: accumulator updates must be collected after TaskMetrics is updated
				val accumUpdates = task.collectAccumulatorUpdates()
				// TODO: do not serialize value twice
				val directResult = new DirectTaskResult(valueBytes, accumUpdates)//将valueBytes和Metrics更新结果,一起封装进DirectTaskResult中,再进行一次序列化;
				val serializedDirectResult = ser.serialize(directResult)//将 DirectTaskResult也序列化,用于传给Driver
				val resultSize = serializedDirectResult.limit //返回序列化后bytes的有效长度;
				
				task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime) // 对应Stage页面中单个Task的[Duration]指标(TaskMetrics.executorRunTime);
				task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)// 在Stage页面的Task详情中,可以看到[Result Serialization Time]指标值;
				
				// directSend = sending directly back to the driver
				val serializedResult: ByteBuffer = {// 当size不超过maxDirectResultSize(1M)时,serializedResult== serializedDirectResult
				  if (maxResultSize > 0 && resultSize > maxResultSize) {//当resultSize > spark.driver.maxResultSize的值(默认1G)时,warn提醒并丢弃; 
					logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize dropping it.")
					ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))//IndirectTaskResult只是个引用, 被传到Driver端时只有blockId,resultSize;
				  } else if (resultSize > maxDirectResultSize) {//maxDirectResultSize= min( "spark.task.maxDirectResultSize"(默认1M),"spark.rpc.message.maxSize"(默认128M) ); 
					val blockId = TaskResultBlockId(taskId) // 默认情况,进入这里就是 1M < resultSize < 1G;
					env.blockManager.putBytes(blockId,new ChunkedByteBuffer(serializedDirectResult.duplicate()), StorageLevel.MEMORY_AND_DISK_SER){//BlockManager.putBytes()将序列化Result存于BM的内存和磁盘
						require(bytes != null, "Bytes is null")
						doPutBytes(blockId, bytes, level, implicitly[ClassTag[T]], tellMaster){
							val putBody=  (info)=>{
								//代码详见下面 doPut()中 putBody()部分代码;
							}
							
							val someResult = doPut(blockId, level, classTag, tellMaster = tellMaster, keepReadLock = keepReadLock)(putBody){//BlockManager.doPut(blockId: BlockId)(putBody: BlockInfo => Option[T]): Option[T] 
								require(blockId != null, "BlockId is null")
								require(level != null && level.isValid, "StorageLevel is null or invalid")
								val putBlockInfo = {// 将存储级别/类信息等封装到 BlockInfo对象中;
									val newInfo = new BlockInfo(level, classTag, tellMaster)
									if (blockInfoManager.lockNewBlockForWriting(blockId, newInfo)) {//正常进入这里,即先获得写出锁;
										newInfo
									} else {
										logWarning(s"Block $blockId already exists on this machine; not re-adding it")
										if (!keepReadLock) {releaseLock(blockId)}
										return None
									}
								}
								val startTimeMs = System.currentTimeMillis
								var exceptionWasThrown: Boolean = true
								val result: Option[T] = try {
									val res = putBody(putBlockInfo){// 上面doPut()()的第二个参数体: (info)=>{}
										val startTimeMs = System.currentTimeMillis
										
										val replicationFuture = if (level.replication > 1) {// _replication默认==1, 所以不进入;
											Future {
												replicate(blockId, new ByteBufferBlockData(bytes, false), level, classTag)
											}(futureExecutionContext)
										} else {//正常进入这里;
											null
										}

										val size = bytes.size
										// 若有选memory,则优先storeMemory,包括(12种的8种): MEMORY_ONLY, MEMORY_ONLY_SER, MEMORY_AND_DISK, MEMORY_AND_DISK_SER, OFF_HEAP
										if (level.useMemory) {//StorageLevel._userMemory=true时, 只要有选storeMemory,就会先进入这里; 若Memory存的下,就不会存Disk了;
											val putSucceeded = if (level.deserialized) {//当选序列化存储时: MEMORY_ONLY, MEMORY_AND_DISK; StorageLevel._deserialized=true,表示需要先反序列化成Java对象;
												// 若选MEMORY_ONLY, MEMORY_AND_DISK, 这需要额外的序列化操作;
												val values =serializerManager.dataDeserializeStream(blockId, bytes.toInputStream())(classTag)
												memoryStore.putIteratorAsValues(blockId, values, classTag) match {
													case Right(_) => true
													case Left(iter) => iter.close(); false
											  }
											} else {//若StorageLevel._deserialized=false,表示不需要 反序列化存储; 则直接存储;
												val memoryMode = level.memoryMode
												
												memoryStore.putBytes(blockId, size, memoryMode, () => {
													if (memoryMode == MemoryMode.OFF_HEAP && bytes.chunks.exists(buffer => !buffer.isDirect)) {
													  bytes.copy(Platform.allocateDirectBuffer)
													} else {
													  bytes
													}
												});{//MemoryStore.putBytes(blockId: BlockId, _bytes: () => ChunkedByteBuffer)
													require(!contains(blockId), s"Block $blockId is already present in the MemoryStore") //先确保该blockId未在内存中,若已存在则抛IllegalArgumentEx异常; 被哪个catch?
													
													/* 利用两种内存分配器中的一种计算 storagePool.freeMemory(可用内存) 是否> size(要存储字节大小), 若够用了则返回true;
													*	1.若用动态内存分配器: 
															- 若storageMemroy不够先从executionMemory执行内存中挤占内存, 
															- 若挤占executionMemory后还不够, 则删除已存数据entries中的MemoryEntrys,来腾出内存;
															- 若 挤占executionMemory和删除缓存entries 后还不够, 这时才返回false;
														2. 若用静态内存分配器: StaticMemoryManager
													*/
													val canStoreMem = memoryManager.acquireStorageMemory(blockId, size, memoryMode);{
														//默认 MemoryManger中此每次分配算法: 动态内存分配概念
														UnifiedMemoryManager.acquireStorageMemory(blockId: BlockId,numBytes: Long,memoryMode: MemoryMode){
															assertInvariants();{//UnifiedMemoryManager.assertInvariants(): 保证总堆内存和总堆外内存都是等于 执行内存+存储内存之和;
																assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)// 保证执行内存+store内存==总可用堆内存;
																assert(offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory) //保证堆外内存是相对的;
															}
															assert(numBytes >= 0)
															
															// 获取用于相应存储的内存池资源, 即该种存储的总可用内存maxMemory;
															val (executionPool, storagePool, maxMemory) = memoryMode match {// 根据是onHeap还是offHeap策略,返回相应的计算和存储内存池: executionPool & storagePool
																case MemoryMode.ON_HEAP => (onHeapExecutionMemoryPool, onHeapStorageMemoryPool, maxOnHeapStorageMemory)
																case MemoryMode.OFF_HEAP => (offHeapExecutionMemoryPool, offHeapStorageMemoryPool, maxOffHeapStorageMemory)
															}
															if (numBytes > maxMemory) {//若要存的字节数 比 总可用内存(计算+存储的总可用内存),直接返回false, 不存了;
																logInfo(s"Will not store $blockId as the required space ")
																return false
															}
															if (numBytes > storagePool.memoryFree) {//当 总可用内存还有但存储内存不够时: 从执行内存中挤出空间来用于存储, 这样影响性能?
																//numBytes - storagePool.memoryFree 算出存储内存的缺口, 若缺口比 execPool的剩余还大, 则把execPool所有剩余内存都分配给storagePool; 此时就会存储也不够,计算内存也不够了;
																val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree, numBytes - storagePool.memoryFree)
																executionPool.decrementPoolSize(memoryBorrowedFromExecution)
																storagePool.incrementPoolSize(memoryBorrowedFromExecution)
															}
															storagePool.acquireMemory(blockId, numBytes);{//StorageMemoryPool.acquireMemory()
																// 表示numBytesToFree 表示 存储不够时还需要额外的存储空间; 当存储空间够用是==0;
																val numBytesToFree = math.max(0, numBytes - memoryFree) //正常返回0, 正常情况memoryFree 大于数据字节数时, numBytes - memoryFree <0,非负数;
																acquireMemory(blockId, numBytes, numBytesToFree);{//StorageMemoryPool.acquireMemory(numBytesToAcquire: Long, numBytesToFree: Long)
																	assert(numBytesToAcquire >= 0) //验证要存数据的大小 >=0;
																	assert(numBytesToFree >= 0)		//numBytesToFree: 存储还需扩容的大小,  这个肯定大于=0;
																	assert(memoryUsed <= poolSize)	
																	if (numBytesToFree > 0) {//当存储空间不够用时,进入这里: 从已存储数据中删除部分 以腾出空间;
																		memoryStore.evictBlocksToFreeSpace(Some(blockId), numBytesToFree, memoryMode);{//MemoryStore.evictBlocksToFreeSpace()
																			assert(space > 0)
																			memoryManager.synchronized {
																				var freedMemory = 0L
																				val rddToAdd = blockId.flatMap(getRddId)
																				val selectedBlocks = new ArrayBuffer[BlockId]
																				def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
																					entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
																				}
																				entries.synchronized {//在这里取出所有缓存的内存数据 entries,从中计算哪些BlockIds/MemoryEntirys需要被删除; 
																					val iterator = entries.entrySet().iterator()
																					// 该算法应该有问题, entries是LinkedHashMap结构, 会先头开始遍历,而头元素往往都是最新添加进去的.就有可能末尾的过期/陈旧的元素迟迟不被删除;
																					while (freedMemory < space && iterator.hasNext) {//freedMemory代表清理所释放出的空间, 当freedMemory还< 要求腾出的空间space时,一直循环删除entries中元素MemoryEntry;
																					  val pair = iterator.next()
																					  val blockId = pair.getKey
																					  val entry = pair.getValue
																					  if (blockIsEvictable(blockId, entry)) {//先判断该MemoryEntry是否是可删除的; 判断依据是? 该block正在被读取reading;
																						if (blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
																						  selectedBlocks += blockId //选中该BlockId, 这里只是计算哪些Block/ MemoryEntry将被删除, 尚不执行remove操作;
																						  freedMemory += pair.getValue.size //用于计算释放空间;
																						}
																					  }
																					}
																				}

																				def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
																				val data = entry match {
																				  case DeserializedMemoryEntry(values, _, _) => Left(values)
																				  case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
																				}
																				val newEffectiveStorageLevel =
																				  blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
																				if (newEffectiveStorageLevel.isValid) {
																					blockInfoManager.unlock(blockId)
																				} else {
																					blockInfoManager.removeBlock(blockId)
																				}
																				}

																				// 真正执行删除内存缓存数据的操作;
																				if (freedMemory >= space) {//本次所释放的空间够用; 进入这里,所以存储空间+执行空间不够这次存储, 当释放了freedMemory空间后就够了;
																					logInfo(s"${selectedBlocks.size} blocks selected for dropping " + s"(${Utils.bytesToString(freedMemory)} bytes)")
																					for (blockId <- selectedBlocks) {
																						val entry = entries.synchronized { entries.get(blockId) }
																						if (entry != null) {
																							dropBlock(blockId, entry)//从内存entries中删除该entry数据;
																						}
																					}
																					logInfo(s"After dropping ${selectedBlocks.size} blocks, " + s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
																					freedMemory
																				} else {// 进入这里, 说明存储空间不够, 即便删除所有(可删除)的缓存MemoryEntiry 仍不够; 就放弃本次存储;
																					blockId.foreach { id =>
																						logInfo(s"Will not store $id")
																					}
																					selectedBlocks.foreach { id =>
																					  blockInfoManager.unlock(id)
																					}
																					0L
																				}

						}
																		}
																	}
																	//计算是否空间足够了, 若空间足够则 enoughMemory==true; 若空间还不够,则enoughMemory==false;
																	val enoughMemory = numBytesToAcquire <= memoryFree; //若上面的 挤占执行内存/移除memory缓存 后有足够空间了:enoughMemory ==true,
																	if (enoughMemory) {//若空间足够,就在这里扣除掉 本次存储数据的大小; 并返回true; 后面 memoryManager.acquireStorageMemory()==true, 进入if()语句取执行 entries.put()操作;
																		_memoryUsed += numBytesToAcquire
																	}
																	enoughMemory
																}
															}
														}
														// 当"spark.memory.useLegacyMode"=true时(默认==false), 采用静态内存分配: 对各部分内存静态划分好后便不可变化
														StaticMemoryManager.acquireStorageMemory(blockId: BlockId,numBytes: Long,memoryMode: MemoryMode){
															
														}
														
													}
													if (canStoreMem) {//当内存还够用时: 先将bytes封装到SerializedMemoryEntry对象中,让后put进: MemoryStore.entries:HashMap[BlockId, MemoryEntry[_]] 这个哈希表中;
														val bytes = _bytes()
														assert(bytes.size == size)
														val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
														entries.synchronized {
															entries.put(blockId, entry) //将该数据缓存进内存: 以BlockId为key, 用SerializedMemoryEntry对象封装数据存入;
														}
														logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
														blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
														true
													}else{ false }
												}
											  
											  
											}
											if (!putSucceeded && level.useDisk) {// 若存内存(不够)StoreMemory失败,且开启了存Disk时, 这里进入distStore: 
												logWarning(s"Persisting block $blockId to disk instead.")
												diskStore.putBytes(blockId, bytes){//调用distStore的Api将序列化的数据存磁盘: 
													
												}
											}
										} else if (level.useDisk) {//只有当用户 StorageLevel.DISK_ONLY时,才进入这里;
											diskStore.putBytes(blockId, bytes)
										}

										val putBlockStatus = getCurrentBlockStatus(blockId, info)
										val blockWasSuccessfullyStored = putBlockStatus.storageLevel.isValid
										if (blockWasSuccessfullyStored) {
											info.size = size
											if (tellMaster && info.tellMaster) {
												reportBlockStatus(blockId, putBlockStatus)
											}
											addUpdatedBlockStatusToTaskMetrics(blockId, putBlockStatus)
										}
										logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))
										if (level.replication > 1) {
											try {
												ThreadUtils.awaitReady(replicationFuture, Duration.Inf)
											} catch {
												case NonFatal(t) => throw new Exception("Error occurred while waiting for replication to finish", t)
											}
										}
										if (blockWasSuccessfullyStored) {
											None
										} else {
											Some(bytes)
										}
										
									}
									
									exceptionWasThrown = false
									  if (res.isEmpty) {
										if (keepReadLock) {
										  blockInfoManager.downgradeLock(blockId)
										} else {
										  blockInfoManager.unlock(blockId)
										}
									  } else {
										removeBlockInternal(blockId, tellMaster = false)
										logWarning(s"Putting block $blockId failed")
									  }
									res
								} finally {
									if (exceptionWasThrown) {
										logWarning(s"Putting block $blockId failed due to an exception")
										removeBlockInternal(blockId, tellMaster = tellMaster)
										addUpdatedBlockStatusToTaskMetrics(blockId, BlockStatus.empty)
									}
								}
								if (level.replication > 1) {
								  logDebug("Putting block %s with replication took %s"
									.format(blockId, Utils.getUsedTimeMs(startTimeMs)))
								} else {
								  logDebug("Putting block %s without replication took %s"
									.format(blockId, Utils.getUsedTimeMs(startTimeMs)))
								}
								result
							
							
							}
								
							someResult.isEmpty
						}
					}
					logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
					ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
				  } else {
					logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
					serializedDirectResult
				  }
				}

				setTaskFinishedAndClearInterruptStatus()
				
				execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult){//ExecutorBackend接口的抽象方法: 只有LocalSchedulerBackend和CoarseGrainedExecutorSBackend两个子类;
					LocalSchedulerBackend.statusUpdate(){
						localEndpoint.send(StatusUpdate(taskId, state, serializedData))// 向Driver端发送Rpc事件: StatusUpdate
					}
					CoarseGrainedExecutorSBackend.statusUpdate(){//Standalone和Yarn模式,都是这个方法处理: 
						val msg = StatusUpdate(executorId, taskId, state, data)
						driver match {
							case Some(driverRef) => driverRef.send(msg)//向Driver端发送Rpc事件: StatusUpdate
							
							{//解释Driver端对StatusUpdate的响应: "dispatcher-event-loop"线程 -> CoarseGrainedSchedulerBackend.receive():case StatusUpdate
								CoarseGrainedSchedulerBackend.receive(){
									case StatusUpdate(executorId, taskId, state, data) => 
								}
							}
					}
				}

			}catch {
				case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
				  val reason = task.context.fetchFailed.get.toTaskFailedReason
				  setTaskFinishedAndClearInterruptStatus()
				  execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))
				  
				case t: Throwable =>
				  logError(s"Exception in $taskName (TID $taskId)", t)
				  val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))
					try {
					  ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
					} catch {
					  case _: NotSerializableException =>ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
					}
				  
				  setTaskFinishedAndClearInterruptStatus()
				  execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
				  if (Utils.isFatalError(t)) {
					uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
				  }
			}finally {
				runningTasks.remove(taskId)
			}		


		}catch{

			//对于其他各种异常;
			case t: Throwable => { // 从这里可以看出,executor所有的异常,都会被catch到,并 包装进 StatusUpdate()中传给Driver;
				val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))
				val serializedTaskEndReason =  ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
				setTaskFinishedAndClearInterruptStatus()
				execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)
			}
		}finally{
			
		}
	}
}

问题:
*	task.run()中会其哪些线程吗?
* task.run()后哪些线程;如何结束? 中间多久进入一个新Task?



/** 启动 "block-manager-slave-async-thread-pool-n"线程: 执行删除过期RDD,Shuffle,Broadcast(广播变量)的数据;
*		- 触发线程: 由"dispatcher-event-loop-n"线程的Inbox.process()-> case RpcMessage() -> BlockManagerSlaveEndpoint.receiveAndReply(context) -> doAsync() 触发;
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
	- 下游线程: 
*/

BlockManagerSlaveEndpoint.doAsync[T](actionMessage: String, context: RpcCallContext)(body: => T){
	val future = Future {body(){//执行Future.apply[T](body: =>T): 将body命令封装进Runnable,并交由ThreadPoolExecutor去异步执行;
		val runnable = new PromiseCompletingRunnable(body)
		// 在这里 真正将runnable:Runnable 提交给线程池执行: 启动"block-manager-slave-async-thread-pool-n"线程任务执行 blockManager.removeRdd(rddId)逻辑;
		executor.prepare.execute(runnable){//ExecutionContextImpl.execute(command:Runnable)
			// 启动"block-manager-slave-async-thread-pool-n"线程任务, 执行删除RDD的逻辑;
			executor.execute(command){//ThreadPoolExecutor.execute(command), 这里executor实际是一个Java的ThreadPoolExecutor线程池类;
				Runnable.run(){command(){
					// 对于BlockManagerSlaveEndpoint.receiveAndReply()中 case RemoveRdd(rddId) =>
					doAsync(){
						blockManager.removeRdd(rddId);{
							logInfo(s"Removing RDD $rddId")
							// 先过滤出该RDD来;
							val blocksToRemove = blockInfoManager.entries.flatMap(_._1.asRDDId).filter(_.rddId == rddId)
							blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
							blocksToRemove.size
						}
					}
					
					// 对于BlockManagerSlaveEndpoint.receiveAndReply()中 case RemoveShuffle(shuffleId) =>
					doAsync(){
						if (mapOutputTracker != null) {
							mapOutputTracker.unregisterShuffle(shuffleId)
						}
						SparkEnv.get.shuffleManager.unregisterShuffle(shuffleId)
					}
					
					// 对于BlockManagerSlaveEndpoint.receiveAndReply()中 case RemoveBroadcast(broadcastId, _) =>
					doAsync(){
						blockManager.removeBroadcast(broadcastId, tellMaster = true){
							val blocksToRemove = blockInfoManager.entries.map(_._1).collect {
								case bid @ BroadcastBlockId(`broadcastId`, _) => bid
							}
							blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
							blocksToRemove.size
						}
					}
				}}
			}
		}
		runnable.promise.future //将该命令的Future 异步返回;
		
	}}
	future.onSuccess { case response => context.reply(response)}
	future.onFailure { case t: Throwable =>context.sendFailure(t)}
}






/** 启动"driver-heartbeater"线程: 定时(默认10秒)发送与Driver的心跳检测? 
*	- 触发线程: "dispatcher-event-loop-n" 线程的 CoarseGrainedExecutorBackend.receive():case RegisteredExecutor =>  executor = new Executor()构造时,在 startDriverHeartbeater()启动该线程;
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
*/

new Executor().startDriverHeartbeater(){
	// 在new Executor()时,创建成员变量heartbeater:ScheduledThreadPoolExecutor 线程池;
	private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")
	
	val heartbeatTask = new Runnable() {// 定义heartbeatTask任务: reportHeartBeat()
		override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
	//按固定频率(默认10秒) 进行一次心跳检测: 向Driver端的"dispatcher-event-loop-n"线程发: HeartbeatResponse和RegisterBlockManager请求;由HeartbeatReceiver.receiveAndReply()响应;
	heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS); {
		def run(){
			Utils.logUncaughtExceptions(reportHeartBeat()){//Executor.reportHeartBeat()
				val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
				val curGCTime = computeTotalGcTime()
				for (taskRunner <- runningTasks.values().asScala) {//测算每个Task的当前 Metrics
					if (taskRunner.task != null) {
						taskRunner.task.metrics.mergeShuffleReadMetrics()
						taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime) //计算其GC用时;
						accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
					}
				}

				val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
				try {
					val response = heartbeatReceiverRef.askSync[HeartbeatResponse](message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"));{//NettyRpcEndpointRef.askSync()
						val future = ask[T](message, timeout) // 发送HeartbeatResponse请求;
						timeout.awaitResult(future)
						
						{// Driver端的"dispatcher-event-loop-n"线程接收该Rpc请求,并转发到 HeartbeatReceiver的实例对象处理;
							data.inbox.process(){
								while (true) {
									message match {case RpcMessage => endpoint.receiveAndReply(context){//HeartbeatReceiver.receiveAndReply(context)
										case ExecutorRegistered(executorId) =>
										case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId)//这里处理Executor端发来的心跳检测;
									}}
								}
							}
						}
					}
					if (response.reregisterBlockManager) { //发送RegisterBlockManager请求;
						logInfo("Told to re-register on heartbeat")
						env.blockManager.reregister(){
							logInfo(s"BlockManager $blockManagerId re-registering with master")
							master.registerBlockManager(blockManagerId, maxOnHeapMemory, maxOffHeapMemory, slaveEndpoint){//BlockManagerMaster.registerBlockManager()
								logInfo(s"Registering BlockManager $blockManagerId")
								val updatedId = driverEndpoint.askSync[BlockManagerId](RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint))//发送RegisterBlockManager消息;
								logInfo(s"Registered BlockManager $updatedId")
								updatedId
							}
							reportAllBlocks()//若是Info级别就打印日志;
						}
					}
					heartbeatFailures = 0
				} catch {
					case NonFatal(e) =>{
						heartbeatFailures += 1
						if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
						  logError(s"Exit as unable to send heartbeats to driver " +s"more than $HEARTBEAT_MAX_FAILURES times")
						  System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
						}
					}
				}
			}
		}
	}
}













