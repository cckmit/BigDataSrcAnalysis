* 一个Job从开始到结束的整个执行过程; 在Driver哪里触发? 经过哪些线程被提交到Executor? Executor从收到请求到完成计算经历哪些步骤?
* RDD是如何封装数据的, 各Partition数据如何存储? Shuffle的数据是怎样的, 如何保证在内存和磁盘中; 一次计算其数据如何被存储在内存和磁盘中的; 
* 一次完整计算, 其内存和磁盘中存在哪些数据? 




SparkCore源码学习目标:
* 有哪些线程? 这些线程的功能是什么? 其触发的代码和运行代码如何?
	- dispatcher-event-loop 线程	: 将一个Job切分成n个Stages,并将每个Stage拆分成partition个Tasks以提交的核心调度;
	- dag-scheduler-event-loop线程	: 循环从DAGScheduler.eventQueue中take()事件并处理的线程;
	- state-store-maintenance-task	: Executor内部, 扫描delta文件，生成snapshot文件，清理陈旧文件; 该线程定期“compact”中间数据;
	- stream execution thread for 	: 流执行每次触发和提交Jobs的主线程;
	- Executor task launch worker for task $taskId
	
	未知: 
	- spark-listener-group-eventLog	:	
	- spark-listener-group-appStatus:	
	

	
	

//1. 创建spark内核对象 :new SparkContext(SparkConf conf)

new SparkContext(conf){
	logInfo(s"Running Spark version $SPARK_VERSION")
		* INFO  SparkContext - Running Spark version 2.3.2
	
	// 核心初始化方法, 一堆的创建对象和初始化;
	try{
		_conf = config.clone()
		_conf.validateSettings(){// 校验SparkConf中的配置, 校验成功则打印Submitted
			val executorOptsKey = "spark.executor.extraJavaOptions"
			val executorClasspathKey = "spark.executor.extraClassPath"
			val driverOptsKey = "spark.driver.extraJavaOptions"
			val driverClassPathKey = "spark.driver.extraClassPath"
			val driverLibraryPathKey = "spark.driver.extraLibraryPath"
			val sparkExecutorInstances = "spark.executor.instances"
			
			// Validate memory fractions: 校验内存比例, 应该 >0, <1;
			// Warn against deprecated memory fractions (unless legacy memory management mode is enabled)
			
			if (contains("spark.submit.deployMode")) {// deployMode 只能为cluster或者 client
			  get("spark.submit.deployMode") match {
				case "cluster" | "client" =>
				case e => throw new SparkException("spark.submit.deployMode can only be \"cluster\" or " +
				  "\"client\".")
			  }
			}

			val executorTimeoutThreshold = getTimeAsSeconds("spark.network.timeout", "120s")
			val executorHeartbeatInterval = getTimeAsSeconds("spark.executor.heartbeatInterval", "10s")
			require(executorTimeoutThreshold > executorHeartbeatInterval, "must be no less than the value of executorHeartbeatInterval")
			
		} 
		logInfo(s"Submitted application: $appName")
			* INFO  SparkContext - Submitted application: StructuredStreamingDemo
		
		//  当用yarn cluster模式时, 必须设置 spark.yarn.app.id; 废弃原来的yarn cluster模式;
		if (master == "yarn" && deployMode == "cluster" && !_conf.contains("spark.yarn.app.id")) {
		  throw new SparkException("Detected yarn cluster mode, Please use spark-submit.")
		}
		
		//若没有配置参数, 就用 本机hostname:0 设置为 host:port
		_conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
		_conf.setIfMissing("spark.driver.port", "0")
		_conf.set("spark.executor.id", SparkContext.DRIVER_IDENTIFIER)// executor.Id也设为"driver",难道后面yarn模式时,再重新设置?
		
		/** 从配置中spark.jars 和 spark.files开头的参数中, 解析出jars和resource相关路径;
		* 
		*/
		_jars = Utils.getUserJars(_conf){
			val sparkJars = conf.getOption("spark.jars")
			sparkJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
		}
		_files = _conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
		
		// 当开启spark的eventLog功能后, 默认用 /tmp/spark-events作为eventLog的目录;
		val isEventLogEnabled: Boolean = _conf.getBoolean("spark.eventLog.enabled", false)
		_eventLogDir =
			if (isEventLogEnabled) {
				val unresolvedDir = conf.get("spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR).stripSuffix("/")
				Some(Utils.resolveURI(unresolvedDir))
			} else {
				None
			}
		_eventLogCodec = {//对EventLog的压缩方式; 由spark.eventLog.compress开关,默认false, 压缩方式由spark.io.compression.codec指定,默认lz4
			val compress = _conf.getBoolean("spark.eventLog.compress", false)
			if (compress && isEventLogEnabled) {
				Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
			}else {
				None
			}
		}
		
		
		_listenerBus = new LiveListenerBus(_conf){
			private[spark] val metrics = new LiveListenerBusMetrics(conf)
			private val started = new AtomicBoolean(false)
			private val stopped = new AtomicBoolean(false)
			private val queues = new CopyOnWriteArrayList[AsyncEventQueue]()
			private[scheduler] var queuedEvents = new mutable.ListBuffer[SparkListenerEvent]()
		}
		
		// 干嘛? 有什么作用? 
		_statusStore = AppStatusStore.createLiveStore(conf){
			val store = new ElementTrackingStore(new InMemoryStore(), conf)
			val listener = new AppStatusListener(store, conf, true)
			new AppStatusStore(store, listener = Some(listener))
		}
		listenerBus.addToStatusQueue(_statusStore.listener.get){//LiveListenerBus.addToStatusQueue(listener: SparkListenerInterface)
			addToQueue(listener, APP_STATUS_QUEUE){
				queues.asScala.find(_.name == queue) match {
					case Some(queue) => queue.addListener(listener)
					
					case None => // new SparkContext是queue为None,进入这里
						val newQueue = new AsyncEventQueue(queue, conf, metrics, this)
						newQueue.addListener(listener)
						if (started.get()) {
						  newQueue.start(sparkContext)
						}
						queues.add(newQueue)
				}
			}
		}
		
		
		/** 创建SparkEnv对象: 432行, 创建对象包括:
		*	- securityManager: SecurityManager
		* 	- serializer: JavaSerializer
		*	- rpcEnv:NettyRpcEnv
		*	- broadcastManager: BroadcastManager
		*	- shuffleManager: ShuffleManager
		* 	- blockManagerMaster: BlockManagerMaster
		*	- blockManager: BlockManager
		* 	- metricsSystem: MetricsSystem
		*	- envInstance:SparkEnv
		*/
		_env = createSparkEnv(_conf, isLocal, listenerBus){//SparkEnv.createDriverEnv()
			SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master)){//SparkEnv.createDriverEnv()
				assert(conf.contains(DRIVER_HOST_ADDRESS), s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
				assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
				val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
				val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
				val port = conf.get("spark.driver.port").toInt
				val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
					Some(CryptoStreamUtils.createKey(conf))
				} else {
					None
				}
				
				// 这里面创建一系列的组件对象;
				create(conf,SparkContext.DRIVER_IDENTIFIER,bindAddress, advertiseAddress,Option(port),isLocal,numCores,ioEncryptionKey){//SparkEnv.create()

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
					
					//创建基于Netty的Rpc通信后台; SparkContext.createSparkEnv()->SparkEnv.create()->RpcEnv.create()->NettyRpcEnv()->new Dispatcher()构造函数中赋值threadpool时,执行pool.execute(new MessageLoop)
					val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf, securityManager, numUsableCores, !isDriver){
						val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager, numUsableCores, clientMode)
						new NettyRpcEnvFactory().create(config){//NettyRpcEnvFactory.create()
							val sparkConf = config.conf
							val javaSerializerInstance =new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
							val nettyEnv =new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress, config.securityManager, config.numUsableCores){
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
										  // 启动n个线程:以dispatcher-event-loop开头命名, 逻辑: 从receivers中取任务并处理;
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
					// 创建序列化者 serializer: 
					val serializer = instantiateClassFromConf[Serializer]("spark.serializer", "org.apache.spark.serializer.JavaSerializer"){
						instantiateClass[T](conf.get(propertyName, defaultClassName))
					}
					val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)
					val closureSerializer = new JavaSerializer(conf)
					
					// 创建广播管理器
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

					// Let the user specify short names for shuffle managers
					val shortShuffleMgrNames = Map("sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName, "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
					val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
					val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
					val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
					val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
					val memoryManager: MemoryManager ={
						  if (useLegacyMemoryManager) {
							new StaticMemoryManager(conf, numUsableCores)
						  } else {
							UnifiedMemoryManager(conf, numUsableCores)
						  }
						}
					
					val blockManagerPort = if (isDriver) {
					  conf.get(DRIVER_BLOCK_MANAGER_PORT)
					} else {
					  conf.get(BLOCK_MANAGER_PORT)
					}
					val blockTransferService = new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress, blockManagerPort, numUsableCores)
					
					// 创建BlockManagerMaster,用于接收BM的注册信息;
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
					val envInstance = new SparkEnv(executorId, rpcEnv, serializer,conf)
					if (isDriver) {
					  val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
					  envInstance.driverTmpDir = Some(sparkFilesDir)
					}

					return envInstance;
					
				}
				
			}
		}
			* INFO  SecurityManager - Changing view acls to: 86177		(开始)
			* INFO  SparkEnv - Registering OutputCommitCoordinator		(结束)
		SparkEnv.set(_env) //设置为静态资源的全局变量;
		
		
		/** 创建Server服务 (new SparkUI), 由spark.ui.enabled默认开启; 并启动绑定到4040端口(bind() ); 
		*	handlers: ArrayBuffer[ServletContextHandler]	总共25个handlers,拦截各种url并处理请求:  主要是:
		* 		- /jobs: 	/jobs/json; /jobs/job/json; /jobs/job/kill;
		*		- /stages:	/stages/json; /stages/stage; /stages/stage/json; /stages/pool; /stages/pool/json; 	
		*		- /storage:	/storage/json; /storage/rdd;	/storage/rdd/json;
		* 		- /environment:	/environment/json
		* 		- /executors:	/executors/json; /executors/threadDump; /executors/threadDump/json
		* 		- /static:	
		* 		- /api:	
		*/
		_ui =
		  if(conf.getBoolean("spark.ui.enabled", true)) {
			Some(SparkUI.create(Some(this), _statusStore, _conf, _env.securityManager, appName, "", startTime)){
				new SparkUI(store, sc, conf, securityManager, appName, basePath, startTime, appSparkVersion){
					val killEnabled = sc.map(_.conf.getBoolean("spark.ui.killEnabled", true)).getOrElse(false)
					
					// 核心: 
					initialize(){
						val jobsTab = new JobsTab(this, store)
						attachTab(jobsTab)
						val stagesTab = new StagesTab(this, store)
						attachTab(stagesTab)
						attachTab(new StorageTab(this, store))
						attachTab(new EnvironmentTab(this, store))
						attachTab(new ExecutorsTab(this))
						attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))
						attachHandler(createRedirectHandler("/", "/jobs/", basePath = basePath))
						attachHandler(ApiRootResource.getServletHandler(this))

						// These should be POST only, but, the YARN AM proxy won't proxy POSTs
						attachHandler(createRedirectHandler("/jobs/job/kill", "/jobs/", jobsTab.handleKillRequest, httpMethods = Set("GET", "POST")))
						attachHandler(createRedirectHandler("/stages/stage/kill", "/stages/", stagesTab.handleKillRequest, httpMethods = Set("GET", "POST")))
						
					}
				}
			}
		  }else{
			// For tests, do not enable the UI
			None
		  }	
		_ui.foreach(_.bind()){//WebUI.bind()
			assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
			try {
			  val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
			  serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name)){//JettyUtils.startJettyServer()
					addFilters(handlers, conf)
					val pool = new QueuedThreadPool.setDaemon(true)
					val server = new Server(pool)
					val collection = new ContextHandlerCollection
					server.setHandler(collection)
					val serverExecutor = new ScheduledExecutorScheduler(s"$serverName-JettyScheduler", true)
					
					try{
						server.start()
						val securePort = sslOptions.createJettySslContextFactory().map {}
						val (httpConnector, httpPort) = Utils.startServiceOnPort[ServerConnector](port, httpConnect, conf, serverName)
						handlers.foreach { h=>
							h.setVirtualHosts(toVirtualHosts(SPARK_CONNECTOR_NAME))
							val gzipHandler = new GzipHandler()
							gzipHandler.setHandler(h)
							collection.addHandler(gzipHandler)
							gzipHandler.start()
						}
							
						pool.setMaxThreads(math.max(pool.getMaxThreads, minThreads))
						ServerInfo(server, httpPort, securePort, conf, collection)
					}catch {
						case e: Exception => server.stop();throw e;
					}
					
			  }
			  logInfo(s"Bound $className to $host, and started at $webUrl")
				* INFO  SparkUI - Bound SparkUI to 0.0.0.0, and started at http://ldsver50:4040
				
			} catch {
			  case e: Exception =>logError(s"Failed to bind $className", e)
				System.exit(1)
			}
		}
		
		_hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)
		
		
		if (jars != null) {
		  jars.foreach(addJar)
		}
		if (files != null) {
		  files.foreach(addFile)
		}
		
		// 什么意思? 依次从
		_executorMemory = _conf.getOption("spark.executor.memory")
			.orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY"))).orElse(Option(System.getenv("SPARK_MEM"))
			.map(warnSparkMem)).map(Utils.memoryStringToMb).getOrElse(1024)
		  
		executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"  
		executorEnvs ++= _conf.getExecutorEnv
		executorEnvs("SPARK_USER") = sparkUser
		
		
		// 创建心跳检查器
		_heartbeatReceiver = env.rpcEnv.setupEndpoint(HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))


		/** 初始化最重要三大对象创建: SchedulerBackend,TaskScheduler,DAGScheduler
		*	- 创建SchedulerBachend,TaskScheduler
		*
		*/
		
		val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode){
			val MAX_LOCAL_TASK_FAILURES = 1
			master match {
			  case "local" =>
				val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
				val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
				scheduler.initialize(backend)
				(backend, scheduler)
			
			  case LOCAL_N_REGEX(threads) => {//local[4]是进入这里; 
					def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
					// 若为 * 则获取所有可用的core, 否则用local[num]中的num;
					val threadCount = if (threads == "*") localCpuCount else threads.toInt
					if (threadCount <= 0) {
					  throw new SparkException(s"Asked to run locally with $threadCount threads")
					}
					val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true){
						private val speculationScheduler =ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")
						val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s") // TaskSet饥饿超时时间
						val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1) //每个任务(Task)所需的CPU个数, 默认1;
						private[scheduler] val taskIdToTaskSetManager = new ConcurrentHashMap[Long, TaskSetManager]
						val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
						private val schedulingModeConf = conf.get(SCHEDULER_MODE_PROPERTY, SchedulingMode.FIFO.toString) // 调度策略, 默认FIFO先进先出; 通过spark.scheduler.mode修改;
						val rootPool: Pool = new Pool("", schedulingMode, 0, 0)
						
					}
					
					val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount){
						private val launcherBackend = new LauncherBackend() {
							override def conf: SparkConf = LocalSchedulerBackend.this.conf
							override def onStopRequest(): Unit = stop(SparkAppHandle.State.KILLED)
						}
						
						launcherBackend.connect(){//LauncherBackend.connect()
							val port = conf.getOption(LauncherProtocol.CONF_LAUNCHER_PORT)
							  .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_PORT))
							  .map(_.toInt)
							val secret = conf.getOption(LauncherProtocol.CONF_LAUNCHER_SECRET)
							  .orElse(sys.env.get(LauncherProtocol.ENV_LAUNCHER_SECRET))
							if (port != None && secret != None) {
							  val s = new Socket(InetAddress.getLoopbackAddress(), port.get)
							  connection = new BackendConnection(s)
							  connection.send(new Hello(secret.get, SPARK_VERSION)){
								  
							  }
							  clientThread = LauncherBackend.threadFactory.newThread(connection)
							  clientThread.start()
							  _isConnected = true
							}
												
						}
						
					}
					
					// 初始化,主要把backend的实现类 LocalSchedulerBackend传入scheduler,并 构建FIFO/Fair的调度器
					scheduler.initialize(backend){//TaskSchedulerImpl.initialize(backend: SchedulerBackend)
						this.backend = backend
						schedulableBuilder = {
						  schedulingMode match {
							case SchedulingMode.FIFO =>
							  new FIFOSchedulableBuilder(rootPool)
							case SchedulingMode.FAIR =>
							  new FairSchedulableBuilder(rootPool, conf)
							case _ =>
							  throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: "+s"$schedulingMode")
						  }
						}
						schedulableBuilder.buildPools()
					}
					(backend, scheduler)
			   }

			  case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>{
					def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
					// local[*, M] means the number of cores on the computer with M failures
					// local[N, M] means exactly N threads with M failures
					val threadCount = if (threads == "*") localCpuCount else threads.toInt
					val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
					val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
					scheduler.initialize(backend)
					(backend, scheduler)
			   }
			  
			  case SPARK_REGEX(sparkUrl) =>
				val scheduler = new TaskSchedulerImpl(sc)
				val masterUrls = sparkUrl.split(",").map("spark://" + _)
				val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
				scheduler.initialize(backend)
				(backend, scheduler)

			  case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>{
					// Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
					val memoryPerSlaveInt = memoryPerSlave.toInt
					if (sc.executorMemory > memoryPerSlaveInt) {
					  throw new SparkException(
						"Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
						  memoryPerSlaveInt, sc.executorMemory))
					}

					val scheduler = new TaskSchedulerImpl(sc)
					val localCluster = new LocalSparkCluster(
					  numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
					val masterUrls = localCluster.start()
					val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
					scheduler.initialize(backend)
					backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
					  localCluster.stop()
					}
					(backend, scheduler)
			    }
				
				
			  case masterUrl =>{//Yarn 集群是这里启动;
					val cm = getClusterManager(masterUrl) match {
					  case Some(clusterMgr) => clusterMgr
					  case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
					}
					try {
					  val scheduler = cm.createTaskScheduler(sc, masterUrl)
					  val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
					  cm.initialize(scheduler, backend){
						TaskSchedulerImpl.start(){
							backend[YarnClusterSchedulerBackend].start()
						}
					  }
					  (backend, scheduler)
					} catch {
					  case se: SparkException => throw se
					  case NonFatal(e) =>
						throw new SparkException("External scheduler cannot be instantiated", e)
					}
				}
			  
			}
			
		}
		_schedulerBackend = sched
		_taskScheduler = ts
		// 创建DAGScheduler
		_dagScheduler = new DAGScheduler(this){// new DAGScheduler(sc)
			this(sc, sc.taskScheduler){
				this(sc,taskScheduler,sc.listenerBus,sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],sc.env.blockManager.master,sc.env){
					private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
					taskScheduler.setDAGScheduler(this)
					
					// 核心步骤之: 启动 DAGSchedulerEventProcessLoop[其父类EventLoop].eventThread线程,叫"dag-scheduler-event-loop"; 循环处理eventQueue
					eventProcessLoop.start(){
						if (stopped.get) {
							throw new IllegalStateException(name + " has already been stopped")
						}
						// Call onStart before starting the event thread to make sure it happens before onReceive
						onStart()
						
						eventThread.start(){//Thread.start()
							override def run(){
								try {
									// 循环从 eventQueue队列尾部取Event,并通过各自的实现类进一步处理;
									while (!stopped.get) {
									  val event = eventQueue.take()
									  try {
										onReceive(event){//该抽象方法由 DAGSchedulerEventProcessLoop实现; 对JobSubmitted, ExecutorAdded,JobCancelled等event的处理;
											val timerContext = timer.time()
											try {
											  doOnReceive(event){// DAGSchedulerEventProcessLoop.doOnReceive(event: DAGSchedulerEvent): Unit
												
												case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
													dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties){//DAGScheduler.handleJobSubmitted()
														var finalStage: ResultStage = null
														try {
															// New stage creation may throw an exception if, for example, jobs are run on a
															// HadoopRDD whose underlying HDFS files have been deleted.
															finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite){
																
															}
														} catch {
															case e: Exception =>listener.jobFailed(e); return
														}
														val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
														clearCacheLocs()
														logInfo("Got job %s (%s) with %d output partitions".format(job.jobId, callSite.shortForm, partitions.length))
														logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
														logInfo("Parents of final stage: " + finalStage.parents)
														logInfo("Missing parents: " + getMissingParentStages(finalStage))
															* 

														val jobSubmissionTime = clock.getTimeMillis()
														jobIdToActiveJob(jobId) = job
														activeJobs += job
														finalStage.setActiveJob(job)
														val stageIds = jobIdToStageIds(jobId).toArray
														val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
														listenerBus.post(SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
														
														// 核心方法: 尝试提交finalStage,进而递归划分和提交父Stage
														submitStage(finalStage){//DAGScheduler.submitStage(stage: Stage)
															val jobId = activeJobForStage(stage)
															if (jobId.isDefined) {
															  logDebug("submitStage(" + stage + ")")
															  if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
																val missing = getMissingParentStages(stage).sortBy(_.id)
																logDebug("missing: " + missing)
																if (missing.isEmpty) {
																	logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
																	
																	// 执行到此步, 说明Job已经为切分到头了, 先把头一个Stage提交给Executor执行;
																	submitMissingTasks(stage, jobId.get){
																		// 获取分区
																		val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
																		val properties = jobIdToActiveJob(jobId).properties

																		runningStages += stage
																		stage match {
																		  case s: ShuffleMapStage =>
																			outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
																		  case s: ResultStage =>
																			outputCommitCoordinator.stageStart(
																			  stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
																		}
																		
																		/* 计算Stage最佳位置的重要算法:
																		  * 	1. 先看是否被checkPoint过
																		  *		2. 再看是否被cache过
																		  *		3. 在调用查看其父类中是否有被cache过.
																		  */
																		val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
																		  stage match {
																			case s: ShuffleMapStage =>
																			  partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap {
																				
																				  DAGScheduler.getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation]={
																						getPreferredLocsInternal(rdd, partition, new HashSet){
																							
																							if (!visited.add((rdd, partition))) {
																							  // Nil has already been returned for previously visited partitions.
																							  return Nil
																							}
																							// If the partition is cached, return the cache locations
																							val cached = getCacheLocs(rdd)(partition)
																							if (cached.nonEmpty) {
																							  return cached
																							}
																							// If the RDD has some placement preferences (as is the case for input RDDs), get those
																							val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
																							if (rddPrefs.nonEmpty) {
																							  return rddPrefs.map(TaskLocation(_))
																							}
																							rdd.dependencies.foreach {
																							  case n: NarrowDependency[_] =>
																								for (inPart <- n.getParents(partition)) {
																								  val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
																								  if (locs != Nil) {
																									return locs
																								  }
																								}

																							  case _ =>
																							}

																							Nil
																							
																						}
																				  }
																			  }
																			  
																			case s: ResultStage =>
																			  partitionsToCompute.map { id =>
																				val p = s.partitions(id)
																				(id, getPreferredLocs(stage.rdd, p))
																			  }.toMap
																		  }
																		} catch {
																		  case NonFatal(e) =>
																			stage.makeNewStageAttempt(partitionsToCompute.size)
																			listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
																			abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
																			runningStages -= stage
																			return
																		}

																		stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)

																		if (partitionsToCompute.nonEmpty) {
																		  stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
																		}
																		listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

																		var taskBinary: Broadcast[Array[Byte]] = null
																		var partitions: Array[Partition] = null
																		try{
																		  var taskBinaryBytes: Array[Byte] = null
																		  RDDCheckpointData.synchronized {
																			taskBinaryBytes = stage match {
																			  case stage: ShuffleMapStage =>
																				JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
																			  case stage: ResultStage =>
																				JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
																			}

																			partitions = stage.rdd.partitions
																		  }
																		  // 将task序列化后放入广播变量
																		  taskBinary = sc.broadcast(taskBinaryBytes)
																		}catch {
																		  // In the case of a failure during serialization, abort the stage.
																		  case e: NotSerializableException =>
																			abortStage(stage, "Task not serializable: " + e.toString, Some(e))
																			runningStages -= stage
																			// Abort execution
																			return
																		  case NonFatal(e) =>
																			abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
																			runningStages -= stage
																			return
																		}
																		
																		// 创建相应的ShuffleMapStage 或 ResultStage
																		val tasks: Seq[Task[_]] = try {
																		  val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
																		  stage match {
																			case stage: ShuffleMapStage =>
																			  stage.pendingPartitions.clear()
																			  partitionsToCompute.map { id =>
																				val locs = taskIdToLocations(id)
																				val part = partitions(id)
																				stage.pendingPartitions += id
																				new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber, taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
																				  Option(sc.applicationId), sc.applicationAttemptId)
																			  }

																			case stage: ResultStage =>
																			  partitionsToCompute.map { id =>
																				val p: Int = stage.partitions(id)
																				val part = partitions(p)
																				val locs = taskIdToLocations(id)
																				new ResultTask(stage.id, stage.latestInfo.attemptNumber,taskBinary, part, locs, id, properties, serializedTaskMetrics,
																				  Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
																			  }
																		  }
																		} catch {
																		  case NonFatal(e) =>
																			abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
																			runningStages -= stage
																			return
																		}

																		if (tasks.size > 0) {
																		  logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) )")
																			* 
																		  
																		  /** 向Executor提交TaskSet任务;(进入TaskScheduler源码)
																		  *	
																		  */
																		  taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties)){//TaskSchedulerImpl.submitTasks()
																			val tasks = taskSet.tasks
																			logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
																			this.synchronized {
																			  val manager = createTaskSetManager(taskSet, maxTaskFailures)
																			  val stage = taskSet.stageId
																			  val stageTaskSets =
																				taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
																			  stageTaskSets(taskSet.stageAttemptId) = manager
																			  val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
																				ts.taskSet != taskSet && !ts.isZombie
																			  }
																			  if (conflictingTaskSet) {
																				throw new IllegalStateException(s"more than one active taskSet for stage $stage:")
																			  }
																			  schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

																			  if (!isLocal && !hasReceivedTask) {
																				starvationTimer.scheduleAtFixedRate(new TimerTask() {
																				  override def run() {
																					if (!hasLaunchedTask) {
																					} else {
																					  this.cancel()
																					}
																				  }
																				}, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
																			  }
																			  hasReceivedTask = true
																			}
																			
																			//作用? 
																			backend.reviveOffers(){
																				//1. LocalSchedulerBackend.reviveOffers()
																				LocalSchedulerBackend.reviveOffers(){
																					{//展示localEndpoint 所指的实际对象
																						localEndpoint:RpcEndpointRef =rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint){
																							
																						}
																					}
																					
																					localEndpoint.send(ReviveOffers)
																				}
																				
																				
																				// 2. StandaloneSchedulerBackend.reviveOffers()
																				StandaloneSchedulerBackend.reviveOffers(){
																					driverEndpoint.send(ReviveOffers)
																				}
																				
																				
																				// 3. YarnSchedulerBackend.reviveOffers()
																				YarnSchedulerBackend.reviveOffers(){// 由其父类CoarseGrainedSchedulerBackend实现
																					CoarseGrainedSchedulerBackend.reviveOffers(){
																						{// 展示driverEndpoint 所指的实际对象;
																							driverEndpoint: RpcEndpointRef= createDriverEndpointRef(properties){
																								rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties)){
																									
																								}
																							}
																						}
																						
																						driverEndpoint.send(ReviveOffers){
																							
																						}
																					}
																				}
																				
																			}
																			
																		  }
																		} else {
																		  // Because we posted SparkListenerStageSubmitted earlier, we should mark
																		  // the stage as completed here in case there are no tasks to run
																		  markStageAsFinished(stage, None)

																		  stage match {
																			case stage: ShuffleMapStage => markMapStageJobsAsFinished(stage)
																			case stage : ResultStage => logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
																		  }
																		  submitWaitingChildStages(stage)
																		}
																																		
																	
																	}
																} else {
																	for (parent <- missing) {
																		// 递归遍历
																		submitStage(parent)
																	}
																	waitingStages += stage
																}
															  }
															} else {
															  abortStage(stage, "No active job for stage " + stage.id, None)
															}
															
														}
													  
													}

												case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
												  dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

												case StageCancelled(stageId, reason) =>
												  dagScheduler.handleStageCancellation(stageId, reason)

												case JobCancelled(jobId, reason) =>
												  dagScheduler.handleJobCancellation(jobId, reason)

												case JobGroupCancelled(groupId) =>
												  dagScheduler.handleJobGroupCancelled(groupId)

												case AllJobsCancelled =>
												  dagScheduler.doCancelAllJobs()

												case ExecutorAdded(execId, host) =>
												  dagScheduler.handleExecutorAdded(execId, host)
												  
												// to do...
	
												  
											  }
											} finally {
											  timerContext.stop()
											}
										}
									  } catch {
										case NonFatal(e) =>
										  try {
												onError(e)
										  } catch {
												case NonFatal(e) => logError("Unexpected error in " + name, e)
										  }
									  }
									}
								} catch {
									case ie: InterruptedException => // exit even if eventQueue is not empty
									case NonFatal(e) => logError("Unexpected error in " + name, e)
								}
								
							}
						}
					}
				}
			}
			
			
			
		}
		_heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet){//RpcEndpointRef.ask(message: Any)
			ask(message, defaultAskTimeout){//NettyRpcEnv.ask(message: Any, timeout: RpcTimeout)
				nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout){//NettyRpcEnv.ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T]
					val promise = Promise[Any]()
					val remoteAddr = message.receiver.address
					if (remoteAddr == address) {
						val p = Promise[Any]()
						p.future.onComplete {
						  case Success(response) => onSuccess(response)
						  case Failure(e) => onFailure(e)
						}(ThreadUtils.sameThread)
						dispatcher.postLocalMessage(message, p)
					} else {
						val rpcMessage = RpcOutboxMessage(message.serialize(this), onFailure, (client, response) => onSuccess(deserialize[Any](client, response)))
						postToOutbox(message.receiver, rpcMessage)
						promise.future.failed.foreach {
						  case _: TimeoutException => rpcMessage.onTimeout()
						  case _ =>
						}(ThreadUtils.sameThread)
					}
					
					val timeoutCancelable = timeoutScheduler.schedule(new Runnable {
						override def run(): Unit = {
						  onFailure(new TimeoutException(s"Cannot receive any reply from ${remoteAddr} " +s"in ${timeout.duration}"))
						}
					}, timeout.duration.toNanos, TimeUnit.NANOSECONDS)
					  promise.future.onComplete { v => timeoutCancelable.cancel(true)}(ThreadUtils.sameThread)
					
					promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
					
				}
			}
		}
		
		// 进行Scheduler相关的启动和初始化
		_taskScheduler.start(){//TaskSchedulerImpl.start()
			backend.start(){
				//1. 当Local模式时: 实现LocalSchedulerBackend.start()
				LocalSchedulerBackend.start(){
					val rpcEnv = SparkEnv.get.rpcEnv
					val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
					localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint){//NettyRpcEnv.setupEndpoint()
						dispatcher.registerRpcEndpoint(name, endpoint){//Dispatcher.registerRpcEndpoint(name: String, endpoint: RpcEndpoint)
							val addr = RpcEndpointAddress(nettyEnv.address, name)
							val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
							synchronized {
							  if (stopped) {
								throw new IllegalStateException("RpcEnv has been stopped")
							  }
							  if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
								throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
							  }
							  val data = endpoints.get(name)
							  endpointRefs.put(data.endpoint, data.ref)
							  
							  // for the OnStart message
							  receivers[LinkedBlockQueue].offer(data)
							  //创建基于Netty的Rpc通信后台; SparkContext.createSparkEnv()->SparkEnv.create()->RpcEnv.create()->NettyRpcEnv()->new Dispatcher()构造函数中赋值threadpool时,执行pool.execute(new MessageLoop)
							  
							}
							endpointRef
							
						}
					}
					
					// 
					listenerBus.post(SparkListenerExecutorAdded(
						System.currentTimeMillis, executorEndpoint.localExecutorId,
						new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty))){//LiveListenerBus.post()
							metrics.numEventsPosted.inc()
							synchronized {
								if (!started.get()) {
									queuedEvents += event
									return
								}
							}
						}
					launcherBackend.setAppId(appId)
					launcherBackend.setState(SparkAppHandle.State.RUNNING)
				}
				
				// 2. StandaloneSchedulerBackend.start(){}
				StandaloneSchedulerBackend.start(){}
				
			}
			
			if (!isLocal && conf.getBoolean("spark.speculation", false)) {// 
				logInfo("Starting speculative execution thread")
				speculationScheduler.scheduleWithFixedDelay(new Runnable {
					override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
					  checkSpeculatableTasks()
					}
				}, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
			}
			
		}
		_applicationId = _taskScheduler.applicationId()
		_applicationAttemptId = taskScheduler.applicationAttemptId()
		_conf.set("spark.app.id", _applicationId)
		_ui.foreach(_.setAppId(_applicationId))

		
		// 初始化BlockManger
		_env.blockManager.initialize(_applicationId){//BlockManager.initialize()
			blockTransferService.init(this)
				* INFO NettyBlockTransferService: Server created on 192.168.41.1:59715
			shuffleClient.init(appId)
			
			blockReplicationPolicy = {
				  val priorityClass = conf.get("spark.storage.replication.policy", classOf[RandomBlockReplicationPolicy].getName)
				  val clazz = Utils.classForName(priorityClass)
				  val ret = clazz.newInstance.asInstanceOf[BlockReplicationPolicy]
				  logInfo(s"Using $priorityClass for block replication policy")
				  ret
			}
			
			// 向Driver.BlockManagerMaster中注册 BlockManager
			val id =BlockManagerId(executorId, blockTransferService.hostName, blockTransferService.port, None)
			val idFromMaster = master.registerBlockManager(id,maxOnHeapMemory,maxOffHeapMemory,slaveEndpoint);{//BlockManagerMaster.registerBlockManager()
				logInfo(s"Registering BlockManager $blockManagerId")
					* BlockManagerMaster - Registering BlockManager null
				val updatedId = driverEndpoint.askSync[BlockManagerId](RegisterBlockManager(blockManagerId, maxOnHeapMemSize, maxOffHeapMemSize, slaveEndpoint)){
					RpcEndpointRef.askSync(){//askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T 
						val future = ask[T](message, timeout)
						timeout.awaitResult(future)
					}
				}
				
				logInfo(s"Registered BlockManager $updatedId")
				updatedId
			}
			
			blockManagerId = if (idFromMaster != null) idFromMaster else id
			shuffleServerId = if (externalShuffleServiceEnabled) {
				logInfo(s"external shuffle service port = $externalShuffleServicePort")
				BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
			} else {
				blockManagerId
			}
			
			logInfo(s"Initialized BlockManager: $blockManagerId")
				* INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.41.1, 59559, None)
			
		}
			
		_env.metricsSystem.start()
		_env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))
		
		// 创建eventLog的管理组件;
		_eventLogger =
			if (isEventLogEnabled) {// 默认关闭, 可通过spark.eventLog.enabled开启;
				val logger = new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get, _conf, _hadoopConfiguration)
				logger.start(){//EventLoggingListener.start()
					if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
					  throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
					}
					val workingPath = logPath + IN_PROGRESS
					val path = new Path(workingPath)
					val uri = path.toUri
					val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
					val isDefaultLocal = defaultFs == null || defaultFs == "file"
	
					val dstream =
						if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
							new FileOutputStream(uri.getPath)
						} else {
							hadoopDataStream = Some(fileSystem.create(path))
							hadoopDataStream.get
						}
					
					try {
					  val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
					  val bstream = new BufferedOutputStream(cstream, outputBufferSize)

					  EventLoggingListener.initEventLog(bstream, testing, loggedEvents){//EventLoggingListener.initEventLog(
							val metadata = SparkListenerLogStart(SPARK_VERSION)
							val eventJson = JsonProtocol.logStartToJson(metadata)
							val metadataJson = compact(eventJson) + "\n"
							logStream.write(metadataJson.getBytes(StandardCharsets.UTF_8))
							if (testing && loggedEvents != null) {
								loggedEvents += eventJson
							}
					  }
					  fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
					  writer = Some(new PrintWriter(bstream))// 对于Local模式, 写出就是PrintWriter
					  logInfo("Logging events to %s".format(logPath))
						* EventLoggingListener - Logging events to file:/E:/studyAndTest/eventLogDir/local-1586004614114
					} catch {
						case e: Exception => dstream.close(); throw e; 
					}
					
				}
				listenerBus.addToEventLogQueue(logger)
				Some(logger)
			} else {
				None
			}
		
		// 动态扩容机制
		val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf) // spark.dynamicAllocation.enabled 决定, 默认false不开启;
		_executorAllocationManager =
			if (dynamicAllocationEnabled) {
				schedulerBackend match {
				  case b: ExecutorAllocationClient =>
					Some(new ExecutorAllocationManager(schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf, _env.blockManager.master))
				  case _ => None
				}
			} else {
				None
			}
		_executorAllocationManager.foreach(_.start()){//ExecutorAllocationManager.start()
			listenerBus.addToManagementQueue(listener)
			val scheduleTask = new Runnable() {
			  override def run(): Unit = {
				try {
					schedule(){//ExecutorAllocationManager.schedule()
					    val now = clock.getTimeMillis
						updateAndSyncNumExecutorsTarget(now)
						val executorIdsToBeRemoved = ArrayBuffer[String]()
						removeTimes.retain { case (executorId, expireTime) =>
							val expired = now >= expireTime
							if (expired) {
								initializing = false
								executorIdsToBeRemoved += executorId
							}
							!expired
						}
						if (executorIdsToBeRemoved.nonEmpty) {
							removeExecutors(executorIdsToBeRemoved)
						}
					}
				} catch {
				  case ct: ControlThrowable =>
					throw ct
				  case t: Throwable =>
					logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
				}
			  }
			}
			executor.scheduleWithFixedDelay(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
			client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
			
		}

		
		// 自动清理器: 默认开启, spark.cleaner.referenceTracking关闭;
		_cleaner =
			if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
				Some(new ContextCleaner(this))
			} else {
				None
			}
		_cleaner.foreach(_.start()){//ContextCleaner.start()
			cleaningThread.setDaemon(true)
			cleaningThread.setName("Spark Context Cleaner")
			cleaningThread.start()
			
			/* 每periodicGCInterval秒来一次Full GC;
			*	GC频率: periodicGCInterval : spark.cleaner.periodicGC.interval 决定, 默认30min分钟;
			* 	默认从30分钟后开启执行
			*/
			periodicGCService.scheduleAtFixedRate(new Runnable {
			  override def run(): Unit = System.gc()
			}, periodicGCInterval, periodicGCInterval, TimeUnit.SECONDS)
		}
		
		setupAndStartListenerBus()
		postEnvironmentUpdate()
		postApplicationStart(){
			listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls)){
				if (stopped.get()) {
					return
				}
				metrics.numEventsPosted.inc()
				if (queuedEvents == null) {
					postToQueues(event){//LiveListenerBus.postToQueues(event: SparkListenerEvent)
						val it = queues.iterator()
						while (it.hasNext()) {
							it.next().post(event){//AsyncEventQueue.post()
								if (stopped.get()) {
									return
								}
								eventCount.incrementAndGet()
								if (eventQueue.offer(event)) {// 从队列若取到数据,就直接返回;
									return
								}
								
								eventCount.decrementAndGet()
								droppedEvents.inc()
								droppedEventsCounter.incrementAndGet()
								val droppedCount = droppedEventsCounter.get
								if (droppedCount > 0) {
								  if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
									if (droppedEventsCounter.compareAndSet(droppedCount, 0)) {
									  val prevLastReportTimestamp = lastReportTimestamp
									  lastReportTimestamp = System.currentTimeMillis()
									  val previous = new java.util.Date(prevLastReportTimestamp)
									  logWarning(s"Dropped $droppedCount events from $name since $previous.")
									}
								  }
								}
								
							}
						}
					}
				  return
				}
				synchronized {
				  if (!started.get()) {
					queuedEvents += event
					return
				  }
				}
				postToQueues(event)
	
			}
		}
		
		// Post init: 启动相关后台服务? 
		_taskScheduler.postStartHook()
		_env.metricsSystem.registerSource(_dagScheduler.metricsSource)
		_env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
		_executorAllocationManager.foreach { e =>
		  _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
		}
		
		_shutdownHookRef = ShutdownHookManager.addShutdownHook(ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
		  logInfo("Invoking stop() from shutdown hook")
		  stop()
		}
		
		//SparkContext构造方法中的长 try()方法结束: 其末尾打印的日志为:
			* BlockManager - Initialized BlockManager: BlockManagerId(driver, ldsver50, 60851, None)
			* ContextHandler - Started o.s.j.s.ServletContextHandler@77f991c{/metrics/json,null,AVAILABLE,@Spark}
	}
	
	private val nextShuffleId = new AtomicInteger(0)
	private val nextRddId = new AtomicInteger(0)
	
	// 构造SparkContext结束代码: 将
	private val allowMultipleContexts: Boolean =config.getBoolean("spark.driver.allowMultipleContexts", false)
	SparkContext.setActiveContext(this, allowMultipleContexts){//SparkContext.setActiveContext(sc: SparkContext, allowMultipleContexts: Boolean)
		SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
			assertNoOtherContextIsRunning(sc, allowMultipleContexts)
			contextBeingConstructed = None
			activeContext.set(sc)// AtomicReference[SparkContext].set(sc)
		}
	}

}


//1.2 其他构建SparkContext的接口
	//2.1 新建或复用该线程已存在的
	SparkContext.getOrCreate(conf:SparkConf){
		SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
		  if (activeContext.get() == null) {// 新建并存于 ThreadLocal中;
			setActiveContext(new SparkContext(config), allowMultipleContexts = false)
		  } else {// 复用原来, 直接从ThreadLocal取出返回;
			if (config.getAll.nonEmpty) {
			  logWarning("Using an existing SparkContext; some configuration may not take effect.")
			}
		  }
		  activeContext.get()
		}
	}


// 发起JobSubmit的线程名: stream exection thread
StreamExecution.queryExecutionThread:QueryExecutionThread = new QueryExecutionThread(s"stream execution thread for $prettyIdString") {
	override def run(): Unit = {
        sparkSession.sparkContext.setCallSite(callSite)
        runStream(){//StreamExecution.runStream()
			
		}
    }
}




/** 处理一个Job: 通过DAGScheduler划分成若干个Stages, 每个Stage拆分成partition数量个TaskSetManager并提交; 最终将TaskSetManager缓存与eventQueue队列并由"dag-scheduler-event-loop"进一步划分Stage/Tasks并提交;
* 	RDD.foreach(){sc.runJob(thisRDD,(it)=>it.foreach(cleanF))} Action类算子调用sc.runJob()已该算子为基础跑任务;
	sc.runJob(this, (iter: Iterator[T]) => iter.toArray){//SparkContext.runJob(),有5个重载方法,最终调这个: 
		dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get){//DAGScheduler.runJob
			val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties){//DAGScheduler.submitJob(rdd)
				eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions, callSite)){//DAGScheudlerEventProcessLoop.post()
					eventQueue.put(event)// eventQueue:LinkedBlockQueue, 这里只是将JobSubmitted消息缓存与eventQueue队列中,等待"dag-scheduler-event-loop"线程来将Job切分成Stags/Tasks并提交;
				}
			}
		}
	}
*/

sc.runJob(rdd, func, partitions){//SparkContext.runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U ): Array[U]
	{
		// 1. 在Streaming流式计算中: 可能由"streaming-job-executor-n" 线程中的 JobHandler.run() -> job.run() -> UserFunc.func() sc.runJob()
		
		// 2. 对普通的sc提交任务, 可能是"main"线程或其他;
	}
	
	val cleanedFunc = clean(func)
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions){
		val results = new Array[U](partitions.size)
		runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res){//SparkContext.runJob(rdd, func, partitions,resultHandler)
			//SparkContext.runJob[T, U: ClassTag](rdd: RDD[T],func: (TaskContext, Iterator[T]) => U,partitions: Seq[Int],resultHandler: (Int, U) => Unit)
			if (stopped.get()) {
				throw new IllegalStateException("SparkContext has been shutdown")
			}
			
			val callSite = getCallSite
			val cleanedFunc = clean(func)
			logInfo("Starting job: " + callSite.shortForm)
				* SparkContext - Starting job: start at SSparkHelper.scala:81
			if (conf.getBoolean("spark.logLineage", false)) {
				logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
			}
			
			dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get){//DAGScheduler.runJob
				val start = System.nanoTime
				val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties){//DAGScheduler.submitJob(rdd)
					val maxPartitions = rdd.partitions.length
					partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
					  throw new IllegalArgumentException("Attempting to access a non-existent partition: Total number of partitions: " + maxPartitions)
					}
					val jobId = nextJobId.getAndIncrement()
					if (partitions.size == 0) {
					  return new JobWaiter[U](this, jobId, 0, resultHandler)
					}

					assert(partitions.size > 0)
					val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
					val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
					
					
					// 将job信息封装进JobSubmitted消息中,并缓存到eventQueue内存队列中, 由另一"dag-scheduler-event-loop"线程循环处理(划分Stages和拆分成Tasks);
					eventProcessLoop.post(JobSubmitted(jobId, rdd, func2, partitions.toArray, callSite, waiter,SerializationUtils.clone(properties))){//DAGScheudlerEventProcessLoop.post()
						EventLoop.post(event){//EventLoop.(event: E): Unit 
							// eventQueue位于DAGSchedulerEventProcessLoop的父类EventLoop中; 在其父类的 run()方法中会循环的从eventQueue中取Event并处理;
							eventQueue.put(event)//eventQueue:LinkedBlockQueue, 这里只是将JobSubmitted消息缓存与eventQueue队列中,等待"dag-scheduler-event-loop"线程来将Job切分成Stags/Tasks并提交;
						}
						
						{ //"dag-scheduler-event-loop" 线程:  将一个Job切分成n个Stages,并将每个Stage拆分成partition个Tasks以提交的核心调度;
							DAGSchedulerEventProcessLoop.doOnReceive(){
								case JobSubmitted(jobId, rdd, func) -> dagScheduler.handleJobSubmitted(jobId, rdd, func){
									createResultStage() // 先创建最末尾的finalStage,用于后面向前递归切分;
									submitStage(finalStage){ //从finalStage开始递归寻找父Stage并提交;
										val missing = getMissingParentStages(stage).sortBy(_.id) //依据是否宽窄依赖而判断其是否有父Stage, 若有创建则创建 parentStage并返回;
										if (missing.isEmpty) {//若没有父Stage了,就提交当前stage;
											submitMissingTasks(stage, jobId.get){// 
												getPreferredLocs(stage.rdd, p)//计算其最佳本地化位置;
												taskBinary = sc.broadcast(taskBinaryBytes) // 将task序列化后放入广播变量进行广播;
												taskScheduler.submitTasks(){// 进行TaskSet的提交;
													val manager = createTaskSetManager(taskSet, maxTaskFailures)//将TaskSet封装进 TaskSetManager中;
													schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties) //将Task加到 rootPool.schedulableQueue任务队列中, 
													backend.reviveOffers(){// 调用backend具体实现类: LocalSchedulerBackend/StandaloneSchedulerBackend/ YarnSchedulerBackend; 
														LocalSchedulerBackend.reviveOffers(){NettyRpcEndpointRef].send(ReviveOffers):} // 其实都是利用RPC通信发 ReviveOffers消息;
														// 从"dag-scheduler-event-loop"线程发出ReviveOffsers消息到缓存队列中, 在"dispatcher-event-loop-*"线程处理该消息,并调用DriverEndpoint.makeOffers()处理并发起Rpc请求;
														// 在DriverEndpoint.makeOffers()中安排cpu/memory等资源给各Task, 再向Executor发LaunchTask的Rpc请求; 
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
						}
					}
					
					return waiter
				}
				
				//重要方法, 会一直阻塞等待waiter.completionFuture变为Success or Failture;
				ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf);{//ThreadUtils.awaitReady(awaitable:Awaitable[T])
					val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
					awaitable.ready(atMost)(awaitPermission);{//Promise.DefaultPromise.ready(atMost:Duration)
						val isCompleted = tryAwait(atMost);{//DefaultPromise.tryAwait()
							case e if e eq Undefined => throw new IllegalArgumentException("cannot wait for Undefined period")
							
							case Duration.Inf  =>
								val l = new CompletionLatch[T]()
								onComplete(l)(InternalCallbackExecutor)
								// 等待JobWaiter.taskSucceeded()完成执行, 判断当finishedTask.数量达到totalTasks时,就把 jobPromise.success(()) 
								l.acquireSharedInterruptibly(1);{
									if (tryAcquireShared(arg) < 0)doAcquireSharedInterruptibly(arg);{//AbstractQueuedSynchronizer.doAcquireSharedInterruptibly()
										for (;;) {
											final Node p = node.predecessor();
											if (p == head) {
												int r = tryAcquireShared(arg);{//Promise.tryAcquireShared(ignored)
													val state = getState();{
														return state;
														
														{// 
															// 在"dag-scheduler-event-loop"线程中, 没执行一个Task,都会JobWaiter.taskSucceeded: 
															JobWaiter.taskSucceeded(index: Int, result: Any){
																// 先对finishedTasks递增+1; 当finishedTasks的最终数量 == 总认为数时, jobPromise
																if (finishedTasks.incrementAndGet() == totalTasks) {
																	jobPromise.success(());
																}
															}
														}
														
													}
													if (state != 0) 1 else -1
												}
												if (r >= 0) {
													setHeadAndPropagate(node, r);
													return;
												}
											}
										}
									}
								}
							case Duration.MinusInf   => // Drop out
						}
						if(isCompleted){//
							this
						}else{
							throw new TimeoutException("Futures timed out after [" + atMost + "]")
						}
					}
				}
				
				waiter.completionFuture.value.get match {
				  case scala.util.Success(_) =>
					logInfo("Job %d finished: %s, took %f s".format(waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
					
				  case scala.util.Failure(exception) =>
					logInfo("Job %d failed: %s, took %f s".format(waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
					val callerStackTrace = Thread.currentThread().getStackTrace.tail
					exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
					throw exception
				}			
			}
			progressBar.foreach(_.finishAll())
			rdd.doCheckpoint()
		}
		results
	}
}



// 进行Scheduler相关的启动和初始化
_taskScheduler.start(){//TaskSchedulerImpl.start()
	backend.start(){
		//1. 当Local模式时: 实现LocalSchedulerBackend.start()
		LocalSchedulerBackend.start(){
			val rpcEnv = SparkEnv.get.rpcEnv
			val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
			localEndpoint = rpcEnv.setupEndpoint("LocalSchedulerBackendEndpoint", executorEndpoint){//NettyRpcEnv.setupEndpoint()
				dispatcher.registerRpcEndpoint(name, endpoint){//Dispatcher.registerRpcEndpoint(name: String, endpoint: RpcEndpoint)
					val addr = RpcEndpointAddress(nettyEnv.address, name)
					val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
					synchronized {
					  val data = endpoints.get(name)
					  endpointRefs.put(data.endpoint, data.ref)
					  
					  // for the OnStart message
					  receivers[LinkedBlockQueue].offer(data)
					  //创建基于Netty的Rpc通信后台; SparkContext.createSparkEnv()->SparkEnv.create()->RpcEnv.create()->NettyRpcEnv()->new Dispatcher()构造函数中赋值threadpool时,执行pool.execute(new MessageLoop)
					  
					}
					endpointRef
					
				}
			}
			
			// 
			listenerBus.post(SparkListenerExecutorAdded(
				System.currentTimeMillis, executorEndpoint.localExecutorId,
				new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty))){//LiveListenerBus.post()
					metrics.numEventsPosted.inc()
					synchronized {
						if (!started.get()) {
							queuedEvents += event
							return
						}
					}
				}
			launcherBackend.setAppId(appId)
			launcherBackend.setState(SparkAppHandle.State.RUNNING)
		}
		
		// 2. StandaloneSchedulerBackend.start(){}
		StandaloneSchedulerBackend.start(){}
	}
}







/** 启动 "dag-scheduler-event-loop" 线程:  将一个Job切分成n个Stages,并将每个Stage拆分成partition个Tasks以提交的核心调度;
*		- 启动位置: 在DAGScheduler的构造函数的末尾, 执行eventProcessLoop.start() -> eventThread.start() 启动该线程;
*	DAGSchedulerEventProcessLoop.eventThread.run() 方法中是一个while(!stopped.get)循环,循环调用eventQueue.take() 接收处理各种DAGSchedulerEvent:
*	包括:JobSubmitted(提交Job),MapStageSubmitted(?), StageCancelled(取消Stage), JobCancelled(取消Job), JobGroupCancelled(?),AllJobsCancelled(取消所有Jobs), ExecutorLost,CompletionEvent
	
	DAGSchedulerEventProcessLoop.doOnReceive(){
		case JobSubmitted(jobId, rdd, func) -> dagScheduler.handleJobSubmitted(jobId, rdd, func){
			createResultStage() // 先创建最末尾的finalStage,用于后面向前递归切分;
			submitStage(finalStage){ //从finalStage开始递归寻找父Stage并提交;
				val missing = getMissingParentStages(stage).sortBy(_.id) //依据是否宽窄依赖而判断其是否有父Stage, 若有创建则创建 parentStage并返回;
				if (missing.isEmpty) {//若没有父Stage了,就提交当前stage;
					submitMissingTasks(stage, jobId.get){// 
						getPreferredLocs(stage.rdd, p)//计算其最佳本地化位置;
						taskBinary = sc.broadcast(taskBinaryBytes) // 将task序列化后放入广播变量进行广播;
						taskScheduler.submitTasks(){// 进行TaskSet的提交;
							val manager = createTaskSetManager(taskSet, maxTaskFailures)//将TaskSet封装进 TaskSetManager中;
							schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties) //将Task加到 rootPool.schedulableQueue任务队列中, 
							backend.reviveOffers(){// 调用backend具体实现类: LocalSchedulerBackend/StandaloneSchedulerBackend/ YarnSchedulerBackend; 
								LocalSchedulerBackend.reviveOffers(){NettyRpcEndpointRef].send(ReviveOffers):} // 其实都是利用RPC通信发 ReviveOffers消息;
								// 从"dag-scheduler-event-loop"线程发出ReviveOffsers消息到缓存队列中, 在"dispatcher-event-loop-*"线程处理该消息,并调用DriverEndpoint.makeOffers()处理并发起Rpc请求;
								// 在DriverEndpoint.makeOffers()中安排cpu/memory等资源给各Task, 再向Executor发LaunchTask的Rpc请求; 
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
*/

DAGScheduler.DAGSchedulerEventProcessLoop.start(){
	{//解释 eventProcessLoop的创建
		// 在DAGScheduler的构造函数中 创建DAGSchedulerEventProcessLoop实例;
		private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this){
			// DAGSchedulerEventProcessLoop继承EventLoop, 在EventLoop中定义了其 eventThread变量:
			private val eventThread = new Thread(name) {override def run(): Unit = {}}
		}
	}
	
	onStart()
	
	// 这里启动了 "dag-scheduler-event-loop" 线程
	eventThread.start(){// 即上面的DAGSchedulerEventProcessLoop.EventLoop.eventThread.start()
		override def run(){
			try {
				// 循环从 eventQueue队列尾部取Event,并通过各自的实现类进一步处理;
				while (!stopped.get) {
				  val event = eventQueue.take()
				  try {
					onReceive(event){//该抽象方法由 DAGSchedulerEventProcessLoop实现; 对JobSubmitted, ExecutorAdded,JobCancelled等event的处理;
						val timerContext = timer.time()
						try {
						  doOnReceive(event){// DAGSchedulerEventProcessLoop.doOnReceive(event: DAGSchedulerEvent): Unit
							case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) => 
								dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties){//DAGScheduler.handleJobSubmitted()
									var finalStage: ResultStage = null
									try {
										finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite){
											
										}
									} catch {
										case e: Exception =>listener.jobFailed(e); return
									}
									val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
									clearCacheLocs()
									logInfo("Got job %s (%s) with %d output partitions".format(job.jobId, callSite.shortForm, partitions.length))
									logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
									logInfo("Parents of final stage: " + finalStage.parents)
									logInfo("Missing parents: " + getMissingParentStages(finalStage))
										* 
										
									val jobSubmissionTime = clock.getTimeMillis()
									jobIdToActiveJob(jobId) = job
									activeJobs += job
									finalStage.setActiveJob(job)
									val stageIds = jobIdToStageIds(jobId).toArray
									val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
									listenerBus.post(SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
									
									// 核心方法: 尝试提交finalStage,进而递归划分和提交父Stage
									submitStage(finalStage){//DAGScheduler.submitStage(stage: Stage)
										val jobId = activeJobForStage(stage)
										if (jobId.isDefined) {
										  logDebug("submitStage(" + stage + ")")
										  if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
											val missing = getMissingParentStages(stage).sortBy(_.id)
											logDebug("missing: " + missing)
											if (missing.isEmpty) {
												logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
												
												// 执行到此步, 说明Job已经为切分到头了, 先把头一个Stage提交给Executor执行;
												submitMissingTasks(stage, jobId.get){//DAGScheduler.submitMissingTasks()
													// 获取分区
													val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()
													val properties = jobIdToActiveJob(jobId).properties

													runningStages += stage
													stage match {
													  case s: ShuffleMapStage =>
														outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
													  case s: ResultStage =>
														outputCommitCoordinator.stageStart(
														  stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
													}
													
													/* 计算Stage最佳位置的重要算法:
													  * 	1. 先看是否被checkPoint过
													  *		2. 再看是否被cache过
													  *		3. 在调用查看其父类中是否有被cache过.
													  */
													val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
													  stage match {
														case s: ShuffleMapStage =>
														  partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap {
															
															  DAGScheduler.getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation]={
																	getPreferredLocsInternal(rdd, partition, new HashSet){
																		
																		if (!visited.add((rdd, partition))) {
																		  // Nil has already been returned for previously visited partitions.
																		  return Nil
																		}
																		// If the partition is cached, return the cache locations
																		val cached = getCacheLocs(rdd)(partition)
																		if (cached.nonEmpty) {
																		  return cached
																		}
																		// If the RDD has some placement preferences (as is the case for input RDDs), get those
																		val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
																		if (rddPrefs.nonEmpty) {
																		  return rddPrefs.map(TaskLocation(_))
																		}
																		rdd.dependencies.foreach {
																		  case n: NarrowDependency[_] =>
																			for (inPart <- n.getParents(partition)) {
																			  val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
																			  if (locs != Nil) {
																				return locs
																			  }
																			}

																		  case _ =>
																		}

																		Nil
																		
																	}
															  }
														  }
														  
														case s: ResultStage =>
														  partitionsToCompute.map { id =>
															val p = s.partitions(id)
															(id, getPreferredLocs(stage.rdd, p))
														  }.toMap
													  }
													} catch {
													  case NonFatal(e) =>
														stage.makeNewStageAttempt(partitionsToCompute.size)
														listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
														abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
														runningStages -= stage
														return
													}

													stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)

													if (partitionsToCompute.nonEmpty) {
													  stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
													}
													listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

													var taskBinary: Broadcast[Array[Byte]] = null
													var partitions: Array[Partition] = null
													try{
													  var taskBinaryBytes: Array[Byte] = null
													  RDDCheckpointData.synchronized {
														taskBinaryBytes = stage match {
														  case stage: ShuffleMapStage =>
															JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
														  case stage: ResultStage =>
															JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
														}

														partitions = stage.rdd.partitions
													  }
													  // 将task序列化后放入广播变量
													  taskBinary = sc.broadcast(taskBinaryBytes)
													}catch {
													  // In the case of a failure during serialization, abort the stage.
													  case e: NotSerializableException =>
														abortStage(stage, "Task not serializable: " + e.toString, Some(e))
														runningStages -= stage
														// Abort execution
														return
													  case NonFatal(e) =>
														abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
														runningStages -= stage
														return
													}
													
													// 创建相应的ShuffleMapStage 或 ResultStage
													val tasks: Seq[Task[_]] = try {
													  val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
													  stage match {
														case stage: ShuffleMapStage =>
														  stage.pendingPartitions.clear()
														  partitionsToCompute.map { id =>
															val locs = taskIdToLocations(id)
															val part = partitions(id)
															stage.pendingPartitions += id
															new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber, taskBinary, part, locs, properties, serializedTaskMetrics, Option(jobId),
															  Option(sc.applicationId), sc.applicationAttemptId)
														  }

														case stage: ResultStage =>
														  partitionsToCompute.map { id =>
															val p: Int = stage.partitions(id)
															val part = partitions(p)
															val locs = taskIdToLocations(id)
															new ResultTask(stage.id, stage.latestInfo.attemptNumber,taskBinary, part, locs, id, properties, serializedTaskMetrics,
															  Option(jobId), Option(sc.applicationId), sc.applicationAttemptId)
														  }
													  }
													} catch {
													  case NonFatal(e) =>
														abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
														runningStages -= stage
														return
													}

													if (tasks.size > 0) {
													  logInfo(s"Submitting ${tasks.size} missing tasks from $stage (${stage.rdd}) )")
														* 
													  
													  /** 向Executor提交TaskSet任务;(进入TaskScheduler源码)
													  *	
													  */
													  taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.latestInfo.attemptNumber, jobId, properties)){//TaskSchedulerImpl.submitTasks()
														val tasks = taskSet.tasks
														logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
														this.synchronized {
														  val manager = createTaskSetManager(taskSet, maxTaskFailures)
														  val stage = taskSet.stageId
														  val stageTaskSets = taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
														  stageTaskSets(taskSet.stageAttemptId) = manager
														  val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
															ts.taskSet != taskSet && !ts.isZombie
														  }
														  if (conflictingTaskSet) {
															throw new IllegalStateException(s"more than one active taskSet for stage $stage:")
														  }
														  schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

														  if (!isLocal && !hasReceivedTask) {
															starvationTimer.scheduleAtFixedRate(new TimerTask() {
															  override def run() {
																if (!hasLaunchedTask) {
																} else {
																  this.cancel()
																}
															  }
															}, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
														  }
														  hasReceivedTask = true
														}
														
														//作用? 
														backend.reviveOffers(){//使用 RpcEndpointRef.send() 向?发送数据;
															//1. LocalSchedulerBackend.reviveOffers()
															LocalSchedulerBackend.reviveOffers(){
																localEndpoint[NettyRpcEndpointRef].send(ReviveOffers){//NettyRpcEndpointRef.send(message: Any)
																	require(message != null, "Message is null")
																	nettyEnv.send(new RequestMessage(nettyEnv.address, this, message)){//NettyRpcEnv.send(message: RequestMessage)
																		val remoteAddr = message.receiver.address
																		if (remoteAddr == address) {
																		  // Message to a local RPC endpoint.
																		  try {
																			dispatcher.postOneWayMessage(message){//Dispatcher.postOneWayMessage(message)
																				postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),(e) => throw e){//Dispatcher.postMessage()
																					val error = synchronized {
																					  val data = endpoints.get(endpointName)
																					  if (stopped) { Some(new RpcEnvStoppedException())
																					  } else if (data == null) { Some(new SparkException(s"Could not find $endpointName."))
																					  } else {
																						data.inbox.post(message){//Inbox.post
																							if (stopped){
																								onDrop(message)
																							}else{
																								messages[RpcMessage].add(message)
																								false
																							}
																						}
																						// 向localEndpoint的LocalSchedulerBackend.localEndpoint[NettyRpcEnd].dispatcher[Dispatcher].receivers[LinkedBlockingQueue]的队列末尾追加数据;
																						// 问题,receivers把任务放入localEndpoint.dispatcher.receivers 队里中, 哪个线程负责取出发送? 
																						receivers[LinkedBlockingQueue].offer(data)
																						
																						//创建基于Netty的Rpc通信后台; SparkContext.createSparkEnv()->SparkEnv.create()->RpcEnv.create()->NettyRpcEnv()->new Dispatcher()构造函数中赋值threadpool时,执行pool.execute(new MessageLoop)
																						{//
																							
																						}
																						
																						None
																					  }
																					}
																					// We don't need to call `onStop` in the `synchronized` block
																					error.foreach(callbackIfStopped)
																					
																				}
																			}
																		  } catch {
																			case e: RpcEnvStoppedException => logDebug(e.getMessage)
																		  }
																		} else {
																		  // Message to a remote RPC endpoint.
																		  postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
																		}
																		
																	}
																}
															}
															
															
															// 2. StandaloneSchedulerBackend.reviveOffers()
															StandaloneSchedulerBackend.reviveOffers(){
																driverEndpoint.send(ReviveOffers)
															}
															
															
															// 3. YarnSchedulerBackend.reviveOffers()
															YarnSchedulerBackend.reviveOffers(){// 由其父类CoarseGrainedSchedulerBackend实现
																CoarseGrainedSchedulerBackend.reviveOffers(){
																	{// 展示driverEndpoint 所指的实际对象;
																		driverEndpoint: RpcEndpointRef= createDriverEndpointRef(properties){
																			rpcEnv.setupEndpoint(ENDPOINT_NAME, createDriverEndpoint(properties)){
																				
																			}
																		}
																	}
																	
																	driverEndpoint.send(ReviveOffers){
																		
																	}
																}
															}
															
															
														}
														
													  }
													} else {
													  // Because we posted SparkListenerStageSubmitted earlier, we should mark
													  // the stage as completed here in case there are no tasks to run
													  markStageAsFinished(stage, None)

													  stage match {
														case stage: ShuffleMapStage => markMapStageJobsAsFinished(stage)
														case stage : ResultStage => logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
													  }
													  submitWaitingChildStages(stage)
													}
																													
												
												}
											} else {
												for (parent <- missing) {
													// 递归遍历
													submitStage(parent)
												}
												waitingStages += stage
											}
										  }
										} else {
										  abortStage(stage, "No active job for stage " + stage.id, None)
										}
										
									}
								  
								}
								
							// to do...
						  }
						} finally {
						  timerContext.stop()
						}
					}
				  } catch {
					case NonFatal(e) =>
						try {
							onError(e)
						} catch {
							case NonFatal(e) => logError("Unexpected error in " + name, e)
						}
				  }
				}
			} catch {
				case ie: InterruptedException => // exit even if eventQueue is not empty
				case NonFatal(e) => logError("Unexpected error in " + name, e)
			}
		}
	}
}




/** 线程"Yarn application state monitor": 
*
*/

new SparkContext(){
	_taskScheduler.start(){//TaskSchedulerImpl.start()
		backend.start(){//YarnClientSchedulerBackend/LocalSchedulerBackend/StandaloneSchedulerBackend.start()
			YarnClientSchedulerBackend.start(){
				val monitorThread = new MonitorThread()
					.setName("Yarn application state monitor")
					.setDaemon(true)
				monitorThread.start() //启动"Yarn application state monitor"线程;
			}
		}
	}
}

TaskSchedulerImpl.start(){
	backend.start(){//YarnClientSchedulerBackend/LocalSchedulerBackend/StandaloneSchedulerBackend.start()
		// 对于Yarn模式:
		YarnClientSchedulerBackend.start(){
			val monitorThread = new MonitorThread().setName("Yarn application state monitor").setDaemon(true)
			monitorThread.start(){//启动"Yarn application state monitor"线程;
				override def run() {
					val (state, _) = client.monitorApplication(appId.get, logApplicationReport = false){//Client.monitorApplication()
						val interval = sparkConf.get(REPORT_INTERVAL)
						var lastState: YarnApplicationState = null
						while (true) {
							Thread.sleep(interval)
							// 调用Yarn的Client Api查询该AppId的当前任务状态
							val report: ApplicationReport =
								try {
									getApplicationReport(appId){//Client.getApplicationReport(appId); 调用Yarn的Api获取该appId的状态;
										yarnClient.getApplicationReport(appId){//YarnClientImpl.getApplicationReport()
											GetApplicationReportRequest request = Records.newRecord(GetApplicationReportRequest.class);
											request.setApplicationId(appId);
											response = rmClient.getApplicationReport(request);
											return response.getApplicationReport();
										}
									}
								}catch {
									case e: ApplicationNotFoundException =>
										cleanupStagingDir(appId)
										return (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED)
									case NonFatal(e) => return (YarnApplicationState.FAILED, FinalApplicationStatus.FAILED)
								}
							val state = report.getYarnApplicationState
							
							if (lastState != state) {//当与上次Yarn状态不一样时, 分析
								state match {
									case YarnApplicationState.RUNNING => reportLauncherState(SparkAppHandle.State.RUNNING)
									case YarnApplicationState.FINISHED => report.getFinalApplicationStatus match { // 将返回状态(APP_FAILED) 通过convertFromProtoFormat(p.getFinalApplicationStatus())解析出来(成FAILED)
											case FinalApplicationStatus.FAILED =>reportLauncherState(SparkAppHandle.State.FAILED){//Client.reportLauncherState(State)
												launcherBackend.setState(state){//LaunchBackend.setState(state)
													if (connection != null && lastState != state) {// 当与上次状态不一样时,才更新LaunchBackend的状态;
														connection.send(new SetState(state))
														lastState = state
													}
												}
											}
											
											case FinalApplicationStatus.KILLED =>reportLauncherState(SparkAppHandle.State.KILLED)
											case _ =>reportLauncherState(SparkAppHandle.State.FINISHED)
										}
									case YarnApplicationState.FAILED => reportLauncherState(SparkAppHandle.State.FAILED)
									case YarnApplicationState.KILLED => reportLauncherState(SparkAppHandle.State.KILLED)
									case _ =>
								}
							}

							if (state == YarnApplicationState.FINISHED ||
							state == YarnApplicationState.FAILED ||
							state == YarnApplicationState.KILLED) {
							cleanupStagingDir(appId)
							return (state, report.getFinalApplicationStatus)
							}

							if (returnOnRunning && state == YarnApplicationState.RUNNING) {
							return (state, report.getFinalApplicationStatus)
							}

							lastState = state
						}

						throw new SparkException("While loop is depleted! This should never happen...")
					}
					logError(s"Yarn application has already exited with state $state!")
					allowInterrupt = false
					sc.stop()
				}
			} 
		}
		
		// 对于Standalone模式:
		StandaloneSchedulerBackend.start(){}
		
	}
}








/** 启动 "dispatcher-event-loop-n" 线程: 该线程循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint.receive/.receiveAndReply()等方法发Rpc请求;
*		- 启动位置: 在"main"/线程的SparkEnv.createDriverEnv() -> new Dispatcher()构造中完成对"dispatcher-event-loop-n"线程的启动;
*	
*	MessageLoop.run(){//循环从缓存队列中拉取各EndpointData消息,并调用其 endpoint方法发Rpc请求
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
*/

pool.execute(new MessageLoop){
	
	{// 解释该线程的启动: 在new SparkContext()函数创建SparkEnv的方法中 createDriverEnv() -> RpcEnv.create()
		new SparkContext() -> SparkEnv.createDriverEnv()-> SparkEnv.create() -> RpcEnv.create()
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
									//RpcEndpoint可以理解为抽象方法, 一般由其子类实现:
									// 对于BlockMangerMaster的Rpc实现端:
									BlockManagerMasterEndpoint.receiveAndReply(context: RpcCallContext){
									  //todo
									}

									// 对于心跳检测的Rpc终端:
									HeartbeatReceiver.receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] {
										case ExecutorRegistered(executorId) =>
											executorLastSeen(executorId) = clock.getTimeMillis()
											context.reply(true)
										
										case ExecutorRemoved(executorId) => 
											executorLastSeen.remove(executorId)
											context.reply(true)
										
										case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>
										
											if (scheduler != null) {
												if (executorLastSeen.contains(executorId)) {//对于之前存在的Executor, 更新时间并启动""?线程异步回复;
												  executorLastSeen(executorId) = clock.getTimeMillis()
												  eventLoopThread.submit(new Runnable {// 启动"heartbeat-receiver-event-loop-thread" 线程来回复;
													override def run(): Unit = Utils.tryLogNonFatalError {
														val unknownExecutor = !scheduler.executorHeartbeatReceived(executorId, accumUpdates, blockManagerId)
														val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
														context.reply(response)
													}
												  })
												} else {
													context.reply(HeartbeatResponse(reregisterBlockManager = true))
												}
											} else {//scheduler为空,重新注册BlockManager
												context.reply(HeartbeatResponse(reregisterBlockManager = true))
											}
										
									}
									
								}
								
								partialFunc.applyOrElse[Any, Unit](content, { msg =>throw new SparkException()})
							
						  case OneWayMessage(_sender, content) => // 处理RpcEndpointRef.send()请求, 不用直接回复;
							val partialFunc:PartialFunction= endpoint.receive(){//RpcEndpoint.receive(context)
								
								// SchedulerBackend的RpcEndpoint实现类: 分LocalEndpoint,StandaloneEndpoint,YarnEndpoint;
								SchedulerBackend-Endpoint.receive(){
									YarnSchedulerEndpoint.receive(){
										case RegisterClusterManager(am) =>
										
										case AddWebUIFilter(filterName, filterParams, proxyBase) =>
										
										case r @ RemoveExecutor(executorId, reason) =>
										
									}
									
									LocalEndpoint.receive(){
										case ReviveOffers => reviveOffers(){// 直接分配CPU/Memory资源给各Task,并对每个Task调 executor.launchTask(executorBackend,task)运算任务;
											val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
											// 先用调度算法计算可以分配的Task和Executor执行资源;
											val hasResourceTasks: Seq[Seq[TaskDescription]] = scheduler.resourceOffers(offers){//TaskSchedulerImpl.resourceOffers(offers)
												var newExecAvail = false
												for (o <- offers) {
												  if (!hostToExecutors.contains(o.host)) {
														hostToExecutors(o.host) = new HashSet[String]()
												  }
												  if (!executorIdToRunningTaskIds.contains(o.executorId)) {
														hostToExecutors(o.host) += o.executorId
														executorAdded(o.executorId, o.host)
														executorIdToHost(o.executorId) = o.host
														executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
														newExecAvail = true
												  }
												  for (rack <- getRackForHost(o.host)) {
														hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
												  }
												}
												
												blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())
												
												val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
												  offers.filter { offer =>
													!blacklistTracker.isNodeBlacklisted(offer.host) &&
													  !blacklistTracker.isExecutorBlacklisted(offer.executorId)
												  }
												}.getOrElse(offers)

												val shuffledOffers = shuffleOffers(filteredOffers)
												val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
												val availableCpus = shuffledOffers.map(o => o.cores).toArray
												val sortedTaskSets = rootPool.getSortedTaskSetQueue
												for (taskSet <- sortedTaskSets) {
												  logDebug("parentName: %s, name: %s, runningTasks: %s".format(
													taskSet.parent.name, taskSet.name, taskSet.runningTasks))
												  if (newExecAvail) {
													taskSet.executorAdded()
												  }
												}

												for (taskSet <- sortedTaskSets) {
												  var launchedAnyTask = false
												  var launchedTaskAtCurrentMaxLocality = false
												  for (currentMaxLocality <- taskSet.myLocalityLevels) {
													do {
													  launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
														taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
													  launchedAnyTask |= launchedTaskAtCurrentMaxLocality
													} while (launchedTaskAtCurrentMaxLocality)
												  }
												  if (!launchedAnyTask) {
													taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
												  }
												}

												if (tasks.size > 0) {
												  hasLaunchedTask = true
												}
												return tasks
											}
											
											for (task <- hasResourceTasks.flatten) {
												freeCores -= scheduler.CPUS_PER_TASK
												executor.launchTask(executorBackend, task)
											}
										}
										
										case StatusUpdate(taskId, state, serializedData) =>
										
										case KillTask(taskId, interruptThread, reason) =>
										
									}
									
									// Driver端 SchedulerBackend, 被Standalone和Yarn两种模式继承和使用;
									CoarseGrainedSchedulerBackend.DriverEndpoint.receive(){
										case StatusUpdate(executorId, taskId, state, data) =>
										
										case ReviveOffers => makeOffers(){//DriverEndpoint.makeOffers(): 先resourceOffers()安排cpu/memory等资源给各Task, 再向Executor发LaunchTask请求;
											val taskDescs = CoarseGrainedSchedulerBackend.this.synchronized {
												val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
												val workOffers = activeExecutors.map { 
													case (id, executorData) => new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
												}.toIndexedSeq
												
												val hasResourceTasks: Seq[Seq[TaskDescription]] = scheduler.resourceOffers(offers){//TaskSchedulerImpl.resourceOffers(offers)
													var newExecAvail = false
													for (o <- offers) {
													  if (!hostToExecutors.contains(o.host)) {
															hostToExecutors(o.host) = new HashSet[String]()
													  }
													  if (!executorIdToRunningTaskIds.contains(o.executorId)) {
															hostToExecutors(o.host) += o.executorId
															executorAdded(o.executorId, o.host)
															executorIdToHost(o.executorId) = o.host
															executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
															newExecAvail = true
													  }
													  for (rack <- getRackForHost(o.host)) {
															hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
													  }
													}
													
													blacklistTrackerOpt.foreach(_.applyBlacklistTimeout())
													
													val filteredOffers = blacklistTrackerOpt.map { blacklistTracker =>
													  offers.filter { offer =>
														!blacklistTracker.isNodeBlacklisted(offer.host) &&
														  !blacklistTracker.isExecutorBlacklisted(offer.executorId)
													  }
													}.getOrElse(offers)

													val shuffledOffers = shuffleOffers(filteredOffers)
													val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
													val availableCpus = shuffledOffers.map(o => o.cores).toArray
													val sortedTaskSets = rootPool.getSortedTaskSetQueue
													for (taskSet <- sortedTaskSets) {
													  logDebug("parentName: %s, name: %s, runningTasks: %s".format(
														taskSet.parent.name, taskSet.name, taskSet.runningTasks))
													  if (newExecAvail) {
														taskSet.executorAdded()
													  }
													}

													for (taskSet <- sortedTaskSets) {
													  var launchedAnyTask = false
													  var launchedTaskAtCurrentMaxLocality = false
													  for (currentMaxLocality <- taskSet.myLocalityLevels) {
														do {
														  launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(
															taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks)
														  launchedAnyTask |= launchedTaskAtCurrentMaxLocality
														} while (launchedTaskAtCurrentMaxLocality)
													  }
													  if (!launchedAnyTask) {
														taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
													  }
													}

													if (tasks.size > 0) {
													  hasLaunchedTask = true
													}
													return tasks
												}
												return hasResourceTasks;
											}
											if (!taskDescs.isEmpty) {
												// Launch tasks returned by a set of resource offers
												launchTasks(taskDescs){//DriverEndpoint.launchTasks(tasks: Seq[Seq[TaskDescription]])
													for(task <- tasks.flatten){
														val serializedTask = TaskDescription.encode(task)
														if (serializedTask.limit >= maxRpcMessageSize) {//该Task因为序列化大小超过限制的原因,被废弃;
															scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
																try {
																  var msg = "Serialized task %s:%d was %d bytes,for large values."
																  msg = msg.format(task.taskId, task.index, serializedTask.limit, maxRpcMessageSize)
																  taskSetMgr.abort(msg)
																} catch {
																  case e: Exception => logError("Exception in error callback", e)
																}
															}
														} else {//正常进入这里
															val executorData = executorDataMap(task.executorId)
															executorData.freeCores -= scheduler.CPUS_PER_TASK // 将可用Executor资源数量 扣除这次调度分配出去的;
															logDebug(s"Launching task ${task.taskId} on executor id: ${task.executorId} hostname: ${executorData.executorHost}.")
															// 调用Executor的通信终端: 
															executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
															
															{//这里发送的LaunchTask消息, 最终被 CoarseGrainedExecutorBackend.receive()方法中 case LaunchTask(data) => executor.launchTask() 处理;
																// 在Executor进程中, ?线程负责循环处理各种Rpc消息, 包括LaunchTask消息;
																
																CoarseGrainedExecutorBackend.receive(){//CoarseGrainedExecutorBackend.receive()
																	case LaunchTask(data) => executor.launchTask(this, taskDesc){
																		val tr = new TaskRunner(context, taskDescription)// 将Task封装进 TaskRunner[implement Runnable]对象中 
																		threadPool.execute(tr) //从线程池中拿资源, 启动一个叫"Executor task launch worker for task n" 的线程完成该Task任务的计算;
																	}
																}
															}
														}
													}
												}
											}
										}
										
										case KillTask(taskId, executorId, interruptThread, reason) =>
										
										case KillExecutorsOnHost(host) => 
									}
									
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
	
 #1 本线程 RpcMessage 消息:receiveAndReply()的一些具体实现方法:

 
 #2	本线程 OneWayMessage 消息:receive()的一些具体实现方法:
	CoarseGrainedSchedulerBackend.receive(){
		case StatusUpdate(executorId, taskId, state, data) => {
			scheduler.statusUpdate(taskId, state, data.value){//TaskSchedulerImpl.statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer)
				var failedExecutor: Option[String] = None
				var reason: Option[ExecutorLossReason] = None
				synchronized {
				  try {
					taskIdToTaskSetManager.get(tid) match {//从内存中读取该TaskId(tid)对应的TaskSetManager
					  case Some(taskSet) => //进入这里;
						if (state == TaskState.LOST) {//只有Mesos运行模式,才会有 LOST状态;
							val execId = taskIdToExecutorId.getOrElse(tid, throw new IllegalStateException("taskIdToTaskSetManager.contains(tid)"))
							if (executorIdToRunningTaskIds.contains(execId)) {
								reason = Some(SlaveLost(s"Task $tid was lost, so marking the executor as lost as well."))
								removeExecutor(execId, reason.get)
								failedExecutor = Some(execId)
							}
						}
						/* 判断task是否处于Finished状态: 包括Finished, Failed, Killed, Lost
						*	TaskState包括6中状态:
						*		- 活跃状态: 
									* Launching , 
									* Running : 
								- 非活跃状态(4中):
									* Finished: 正常结束;
									* Failed: 失败结束;
									* Killed: 被kill掉结束;
									* Lost:	  Mesos模式特有状态;
						*/
						if (TaskState.isFinished(state)) {//当Finished/Failed/Killed,Lost4种状态时;
							// 删除TaskSchedulerImpl中 taskIdToTaskSetManager/ taskIdToExecutorId/ executorIdToRunningTaskIds 这3个Hash表中该TaskId对应元素;
							cleanupTaskState(tid){//TaskSchedulerImpl.cleanupTaskState()
								taskIdToTaskSetManager.remove(tid)//从TaskSchedulerImpl的内中移除该Task;
								taskIdToExecutorId.remove(tid) //从TaskSchedulerImpl.taskIdToExecutorId表中 移除该Task;
									.foreach { executorId => executorIdToRunningTaskIds.get(executorId) // 从Executor执行Task表中,移除该Task;
										.foreach { _.remove(tid) }
								}
		
							}
							// 将其TaskSetManger中runningTasksSet删除该TaskId,并将Pool.runningTasks数量减1;
							taskSet.removeRunningTask(tid){//TaskSetManager.removeRunningTask(tid)
								if (runningTasksSet.remove(tid) && parent != null) {//从runningTasksSet中成功删除该TaskId, 且其parent总调度池rootPool:Pool 不为null;
									parent.decreaseRunningTasks(1){//将父任务池rootPool:Pool中的runningTask:Int 减一;
										runningTasks -= taskNum
										if (parent != null) { parent.decreaseRunningTasks(taskNum)} //当FAIR公平调度算法时,此处不为空,进一步让其parent减一;
									}
								}
							}
							if (state == TaskState.FINISHED) {//当该Task是正常计算完成时, 启用"task-result-getter"线程,异步获取 TaskDirectResult结果;
								taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData){//TaskResultGetter.enqueueSuccessfulTask()
									
									getTaskResultExecutor.execute(new Runnable {//这里,另起一线程:"task-result-getter", 获取Task计算结果;
										override def run(): Unit = Utils.logUncaughtExceptions {
											val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
												case directResult: DirectTaskResult[_] => (directResult, serializedData.limit())
												case IndirectTaskResult(blockId, size) => {
													val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
													val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]]()
													(deserializedResult, size)
												}
											}
											result.accumUpdates = result.accumUpdates.map()
											scheduler.handleSuccessfulTask(taskSetManager, tid, result)
										}
									});
								}
							} else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
								taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
							}
						}
						
					  case None =>logError(("Ignoring").format(state, tid))
					}
				  } catch {
					case e: Exception => logError("Exception in statusUpdate", e)
				  }
				}
				// Update the DAGScheduler without holding a lock on this, since that can deadlock
				if (failedExecutor.isDefined) {
				  assert(reason.isDefined)
				  dagScheduler.executorLost(failedExecutor.get, reason.get)
				  backend.reviveOffers()
				}
			}
			if (TaskState.isFinished(state)) {
				executorDataMap.get(executorId) match {
					case Some(executorInfo) =>
						executorInfo.freeCores += scheduler.CPUS_PER_TASK
						makeOffers(executorId)
					case None => logWarning(s"Ignored task status update from unknown executor with ID $executorId")
				}
			}
		}
	}
	* case StatusUpdate: 接受Executor端"Executor task launch.."线程 算完Task后的更新状态请求, 并启动"task-result-getter" 线程去获取taskResult结果;
	
	
	
/** 启动"task-result-getter" 线程, 获取Task计算结果 TaskResult
* 
*/
getTaskResultExecutor.execute(new Runnable {//这里,另起一线程:"task-result-getter", 获取Task计算结果;
	override def run(): Unit = Utils.logUncaughtExceptions {
		// 先对结果进行反序列化;
		val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
			case directResult: DirectTaskResult[_] => (directResult, serializedData.limit())
			case IndirectTaskResult(blockId, size) => {
				val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
				val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]]()
				(deserializedResult, size)
			}
		}
		result.accumUpdates = result.accumUpdates.map()
		// 
		scheduler.handleSuccessfulTask(taskSetManager, tid, result){//TaskSchedulerImpl.handleSuccessfulTask()
			taskSetManager.handleSuccessfulTask(tid, taskResult){
				val info = taskInfos(tid)
				info.markFinished(TaskState.FINISHED, clock.getTimeMillis()){//TaskInfo.markFinished()
					assert(time > 0)
					finishTime = time //这个time就是上面clock[SystemClock].getTimeMillis()系统当前时间
				}
			}
		}
		
	}
});

	// 处理Task异常;
	TaskResultGetter.enqueueFailedTask(){
		var reason : TaskFailedReason = UnknownReason
		 getTaskResultExecutor.execute(new Runnable {
			override def run(): Unit = Utils.logUncaughtExceptions {
				try{
					reason = serializer.get().deserialize[TaskFailedReason](serializedData, loader)
				}catch{
					
				}finally{
					scheduler.handleFailedTask(taskSetManager, tid, taskState, reason);{
						taskSetManager.handleFailedTask(tid, taskState, reason);{
							numFailures(index) += 1
							if (numFailures(index) >= maxTaskFailures) {
								abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
									.format(index, taskSet.id, maxTaskFailures, failureReason), failureException){//TaskSetManager.abort()
										sched.dagScheduler.taskSetFailed(taskSet, message, exception);{
											eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception));//向Driver发送 TaskSetFailed信号;
										}
										maybeFinishTaskSet()
									}
							}
						}
					}
				}
				
			}
		 });
	}
	
/** 启动"heartbeat-receiver-event-loop-thread" 线程: 
*		- 在Driver的"dispatcher-event-loop-n"线程中, HeartbeatReceiver.receiveAndReply():case heartbeat @ Heartbeat()时
*/

HeartbeatReceiver.receiveAndReply(context){
	
	case ExpireDeadHosts => //从哪里收到?
		expireDeadHosts(){//HeartbeatReceiver.expireDeadHosts()
			val now = clock.getTimeMillis()
			for ((executorId, lastSeenMs) <- executorLastSeen) {//遍历各ExecutorId的最新更新时间?
				if (now - lastSeenMs > executorTimeoutMs) {//当超时>executorTimeoutMs (先spark.network.timeout参数,若无spark.storage.blockManagerSlaveTimeoutMs参数,若无默认120s;
					logWarning(s"Removing executor $executorId with no recent heartbeats: " +s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
					//将该ExecutorId移除内存,?
					scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +s"timed out after ${now - lastSeenMs} ms")){//TaskSchedulerImpl.executorLost()
						synchronized {
						  if (executorIdToRunningTaskIds.contains(executorId)) {
							val hostPort = executorIdToHost(executorId)
							logExecutorLoss(executorId, hostPort, reason)
							removeExecutor(executorId, reason)
							failedExecutor = Some(executorId)
						  } else {
							executorIdToHost.get(executorId) match {
							  case Some(hostPort) =>
								logExecutorLoss(executorId, hostPort, reason)
								removeExecutor(executorId, reason)
							  case None =>
								logError(s"Lost an executor $executorId (already removed): $reason")
							}
						  }
						}
						// Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
						if (failedExecutor.isDefined) {
						  dagScheduler.executorLost(failedExecutor.get, reason)
						  backend.reviveOffers()
						}
					}
					
					// killExecutor, 可能需要很久, 也可能失败,所以异步; Asynchronously kill the executor to avoid blocking the current thread
					killExecutorThread.submit(new Runnable {
					  override def run(): Unit = Utils.tryLogNonFatalError {
						// Note: we want to get an executor back after expiring this one,so do not simply call `sc.killExecutor` here (SPARK-8119)
						sc.killAndReplaceExecutor(executorId);{
							schedulerBackend match {
								case b: CoarseGrainedSchedulerBackend => b.killExecutors(Seq(executorId), replace = true, force = true).nonEmpty
								case _ =>logWarning("Killing executors is only supported in coarse-grained mode")
									false
							}
						}
					  }
					})
					executorLastSeen.remove(executorId)
				}
			}
		}
		context.reply(true)
	
	case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId) =>{
		if (scheduler != null) {if(executorLastSeen.contains(executorId)) {
			// 正式启动"heartbeat-receiver-event-loop-thread" 线程: 
			eventLoopThread.submit(new Runnable {
				override def run(): Unit = Utils.tryLogNonFatalError {
					val unknownExecutor = !scheduler.executorHeartbeatReceived(executorId, accumUpdates, blockManagerId){//TaskSchedulerImpl.executorHeartbeatReceived()
						val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = synchronized {
						  accumUpdates.flatMap { case (id, updates) =>
							val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
							taskIdToTaskSetManager.get(id).map { taskSetMgr =>
							  (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
							}
						  }
						}
						// 在DAGScheduler中,发出异步Rpc请求: BlockManagerHeartbeat
						dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId){//DAGScheduler.executorHeartbeatReceived()
							listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates))//发送Metrics监测消息给ListenerBus;
							blockManagerMaster.driverEndpoint.askSync[Boolean](BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat")){//RpcEndpointRef.askSync(message: Any, timeout: RpcTimeout)
								val future = ask[T](message, timeout)
								timeout.awaitResult(future)//600秒/10分钟超时等待结果;
							}
						}
					}
					val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
					context.reply(response)
				}
			})
		}}								
	}
}







# 其他重要附件和方法: 


/* BlockManger直接存储序列化后的bytes: BlockManager.putBytes() 和RDD存储计算方法: BlockManager.getOrElseUpdate()
*	- BlockManager.putBytes() 调用该方法地方: 
*		1. "dag-scheduler"在将Stage分成多个Task提交的 DAGScheduler.submitMissingTasks(stage)中, 会将stage序列化成task字节数组并广播给各Executor:  sc.broadcast(taskBinaryBytes)
*		2. "Executor task launch.."中某个Task计算完将序列化的serDirResult结果要返回给Driver时,但其大小> maxDirectResultSize时,按MEMORY_AND_DISK_SER先存于内存中: Executor.TaskRunner.run(): env.blockManager.putBytes()
*	- BlockManager.getOrElseUpdate() 被调用的地方: 
*		1. "Executor task launch.."中, Task.runTask()进行Task计算而调RDD.getOrCompute()时,先尝试从缓存读取,若没有再计算和更新: SparkEnv.get.blockManager.getOrElseUpdate()
*/

BlockManager.putBytes(blockId,new ChunkedByteBuffer(serializedDirectResult.duplicate()), StorageLevel.MEMORY_AND_DISK_SER){//BlockManager.putBytes()将序列化Result存于BM的内存和磁盘
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




BlockManager.getOrElseUpdate(blockId: BlockId,level: StorageLevel,makeIterator: () => Iterator[T]){
	val blockResult: Option[BlockResult]= get[T](blockId)(classTag);{//BlockManager.get(blockId: BlockId)
		//1. 先尝试从本地/本机器上获取该Blcok: 
		val local = getLocalValues(blockId){//BlockManager.getLocalValues(blockId)
			logDebug(s"Getting local block $blockId")
			val someBlockInfo:Option[BlockInfo] = blockInfoManager.lockForReading(blockId);{//blockInfoManager.lockForReading(blockId,blocking: Boolean = true):默认阻塞方式获取该Info对象;
				do {
				  infos.get(blockId) match {
					case None => return None // 若用该blockId查询为空, 则返回空,后面会通过rdd.compute()来计算结果;
					
					case Some(info) => //若读到BlockInfo, 则判断该Block当前是否还尚处于写入(Writer)状态, 若尚在写入可能数据不完整; 
					  if (info.writerTask == BlockInfo.NO_WRITER) {// 当该Block不是在 Writting,就可以读取了;
						info.readerCount += 1 //给该Block的读取计数+1;
						readLocksByTask(currentTaskAttemptId).add(blockId)
						logTrace(s"Task $currentTaskAttemptId acquired read lock for $blockId")
						return Some(info) //读取 BlockInfo数据就结束方法返回;
					  }
				  }
				  if (blocking) {
					wait()
				  }
				} while (blocking)
			}
			
			someBlockInfo match {
			  case None => None
			  case Some(info) => //当有BlockInfo时,
				val level = info.level
				logDebug(s"Level for block $blockId is $level")
				val taskAttemptId = Option(TaskContext.get()).map(_.taskAttemptId())
				// 先尝试从内存中读取; 再从磁盘读取;
				if (level.useMemory && memoryStore.contains(blockId)) {// 先尝试从内存中读取: 只有配了storeMem且内存中有该blockId,才去读内存;
					val iter: Iterator[Any] = if (level.deserialized) {//先判断[已存储的]数据是否是 反序列化后的对象数据在存储; 若deserialized=true(已经反序列化成对象了),就直接返回而不需要再反序列化了;
						memoryStore.getValues(blockId).get ;{//对象数据(已Desc了),直接返回;
							val entry = entries.synchronized { entries.get(blockId) }
							entry match {
								case null => None
								case e: SerializedMemoryEntry[_] => //getValues()要求直接返回对象, 若entry是序列化的数据则要抛 IllegalArgument异常;
									throw new IllegalArgumentException("should only call getValues on deserialized blocks")
								case DeserializedMemoryEntry(values, _, _) => //将对象 values 返回; 
									val x = Some(values)
									x.map(_.iterator)
							}
						} 
					} else {// deserialized=false,还是字节数组,未反序列化, 还需要反序列化成对象返回;
						serializerManager.dataDeserializeStream(blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)// 先读取bytes,再将其进行反序列化;
					}
					val ci = CompletionIterator[Any, Iterator[Any]](iter, {releaseLock(blockId, taskAttemptId)})//将获取的数据iter封装进迭代器 CompletionIterator中; 
					Some(new BlockResult(ci, DataReadMethod.Memory, info.size)) //由进一步将CompletionIterator封装进 BlockResult对象中,同一接口返回;
				  
				} else if (level.useDisk && diskStore.contains(blockId)) {//再尝试从磁盘读取;当上面内存中没有(可以没配Memory或配了storeMemory但没有该blcokId)
					val diskData = diskStore.getBytes(blockId);{//DiskStore.getBytes(blockId)
						val file = diskManager.getFile(blockId.name){//DiskBlockManager.getFile(): 根据blockId的哈希值映射到目录/子目录/blockId的文件,或新建此文件;
							val hash = Utils.nonNegativeHash(filename) //算出blockId的哈希值;
							val dirId = hash % localDirs.length // 对本地文件数量取余, 即将该blockId哈希映射到本地(多个)文件的某一个文件中; 得到的值为本地文件编号;
							val subDirId = (hash / localDirs.length) % subDirsPerLocalDir //? 按照localDirs切分,再按照64个子文件目录切分;?
							
							// Create the subdirectory if it doesn't already exist
							//subDirs是二维数组: File[][]: 外面那层是dir-> dirId-> subDirs(dirId), 里面那层是subDir->subDirId->subDirs(dirId)(subDirId)
							val subDir = subDirs(dirId).synchronized {// 先定位到目录dir,锁定该目录
								val old = subDirs(dirId)(subDirId) //再定位到子目录 subDir,
								if (old != null) {
									old
								} else {//若该目录没有子目录,则创建子目录: 去subDirId的十六进制为目录名称;
									val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
									if (!newDir.exists() && !newDir.mkdir()) {throw new IOException(s"Failed to create local dir in $newDir.")}
									subDirs(dirId)(subDirId) = newDir
									newDir //将该新建的目录返回;
								}
							}
							new File(subDir, filename) //有了子目录subDir,又知道要创建的文件名filname,新建此文件;
						}
						val blockSize = getSize(blockId)
						
						securityManager.getIOEncryptionKey() match {
							case Some(key) => new EncryptedBlockData(file, blockSize, conf, key)
							  
							case _ =>{//正常进入这里;
								val channel = new FileInputStream(file).getChannel()//打开文件FileIO
								if (blockSize < minMemoryMapBytes) {//当数据量 < 2097152 (2M大小)时,为小文件; 直接读取而不用buff了; 
								  // For small files, directly read rather than memory map.
								  Utils.tryWithSafeFinally {
									val buf = ByteBuffer.allocate(blockSize.toInt)
									JavaUtils.readFully(channel, buf)
									buf.flip() // 刷新缓存; 直接读取;
									new ByteBufferBlockData(new ChunkedByteBuffer(buf), true)
								  } {
									channel.close()
								  }
								} else {// 对于大于2M的数据,需要在内存总映射;
								  Utils.tryWithSafeFinally {
									new ByteBufferBlockData(// 先读取到缓存中;
									  new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length)), true)
								  } {
									channel.close()
								  }
								}
							}
						}
					}
					
					val iterToReturn: Iterator[Any] = {
						if (level.deserialized) {
						  val diskValues = serializerManager.dataDeserializeStream(blockId,diskData.toInputStream())(info.classTag)
						  maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
						} else {// 需要反序列化成Java对象,才能返回;
						  val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskData)
							.map { _.toInputStream(dispose = false) }
							.getOrElse { diskData.toInputStream() }
						  serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
						}
					}
					val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, {releaseLockAndDispose(blockId, diskData, taskAttemptId)})
					Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
				} else {
					handleLocalReadFailure(blockId)
				}
			}
		}
		if (local.isDefined) {// 若本地能(从内存或磁盘)读取到该blockId的数据,则返回;
			logInfo(s"Found block $blockId locally")
			* INFO  BlockManager - Found block rdd_8153_13 locally
			return local
		}
		
		// 进入这里说明 local 没有该blockId的数据,需要从远程去获取下; 如果远程也没有该Block,就需要重新计算了;
		val remote = getRemoteValues[T](blockId);{//BlockManager.getRemoteValues(blockId: BlockId)
			val ct = implicitly[ClassTag[T]]
			val someBuffDate:Option[ChunkedByteBuffer]= getRemoteBytes(blockId);{//BlockManager.getRemoteBytes(blockId: BlockId)
				logDebug(s"Getting remote block $blockId")
				require(blockId != null, "BlockId is null")
				var runningFailureCount = 0
				var totalFailureCount = 0
				val locations = getLocations(blockId);{//BlockManager.getLocations(blockId):: Seq[BlockManagerId]
					val blocks: Seq[BlockManagerId]= master.getLocations(blockId);{//BlockMangerMaster.getLocations(blockId)
						driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId));{//RpcEndpointRef.askSync(message): 发同步请求,实际是异步变同步;
							val future = ask[T](message, timeout)// 发出异步Rpc请求
							timeout.awaitResult(future) //阻塞等待同步结果;默认timeout等待时间是 120sec = 2分钟;"spark.rpc.askTimeout", "spark.network.timeout" 中取第一个有的; 这里取spark.rpc.askTimeout的120;
						}
						
						{//解释响应GetLocations请求的driver代码:
							
						}
					}
					val locs = Random.shuffle(blocks)
					val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
					preferredLocs ++ otherLocs
				}
				val maxFetchFailures = locations.size
				var locationIterator = locations.iterator//将该Block数据分布哪些机器/BlockMangerIds的列表装入迭代器,迭代;
				while (locationIterator.hasNext) {//遍历各blockMangerId,若拿到的data不为空就结束返回;
					val loc = locationIterator.next()// BlockManagerId即为其Blcok块存储节点所在;
					logDebug(s"Getting remote block $blockId from $loc")
					val data = try {
						val managedBuff: ManagedBuffer= blockTransferService.fetchBlockSync(loc.host, loc.port, loc.executorId, blockId.toString);{//BlockTransferService.fetchBlockSync(host, port, executorId, blockId)
							val result = Promise[ManagedBuffer]()
							fetchBlocks(host, port, execId, Array(blockId), new BlockFetchingListener {
									override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
										result.failure(exception)
									}
									override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
										val ret = ByteBuffer.allocate(data.size.toInt)
										ret.put(data.nioByteBuffer())
										ret.flip()
										result.success(new NioManagedBuffer(ret))
									}
								}, shuffleFiles = null);{//fetchBlocks()是抽象方法,由NettyBlockTransferService.fetchBlocks()实现
									try {
										val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
											override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
												val client = clientFactory.createClient(host, port)
												new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener, transportConf, shuffleFiles).start()
											}
										}

										val maxRetries = transportConf.maxIORetries()
										if (maxRetries > 0) {
											new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener)
											.start();{//RetryingBlockFetcher.start()
												fetchAllOutstanding();{//RetryingBlockFetcher.fetchAllOutstanding
													String[] blockIdsToFetch;
													int numRetries;
													RetryingBlockFetchListener myListener;
													synchronized (this) {
														blockIdsToFetch = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
														numRetries = retryCount;
														myListener = currentListener;
													}
													
													// Now initiate the fetch on all outstanding blocks, possibly initiating a retry if that fails.
													try {
														fetchStarter.createAndStart(blockIdsToFetch, myListener);{//抽象方法, 有上面匿名内部类代码实现;
															val client = clientFactory.createClient(host, port)
															new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener, transportConf, shuffleFiles)
																.start();{//OneForOneBlockFetcher.start()
																	client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
																		@Override
																		public void onSuccess(ByteBuffer response) {
																			try {
																			  streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
																			  logger.trace("Successfully opened blocks {}, preparing to fetch chunks.", streamHandle);
																			  for (int i = 0; i < streamHandle.numChunks; i++) {
																				if (shuffleFiles != null) {
																					client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i), new DownloadCallback(shuffleFiles[i], i));
																				} else {
																					client.fetchChunk(streamHandle.streamId, i, chunkCallback);
																				}
																			  }
																			} catch (Exception e) {
																			  logger.error("Failed while starting block fetches after success", e);
																			  failRemainingBlocks(blockIds, e);
																			}
																		}
																		@Override
																		public void onFailure(Throwable e) {
																			logger.error("Failed while starting block fetches", e);
																			failRemainingBlocks(blockIds, e);
																		}
																	});
																}
														}
													} catch (Exception e) {
													  logger.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
														blockIdsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);

													  if (shouldRetry(e)) {
														initiateRetry();
													  } else {
														for (String bid : blockIdsToFetch) {
														  listener.onBlockFetchFailure(bid, e);
														}
													  }
													}
												}
											}
										} else {
											blockFetchStarter.createAndStart(blockIds, listener)
										}
									} catch {
										case e: Exception =>
											logError("Exception while beginning fetchBlocks", e)
											blockIds.foreach(listener.onBlockFetchFailure(_, e))
									}
								}
							
							ThreadUtils.awaitResult(result.future, Duration.Inf)
						}
						managedBuff.nioByteBuffer()//远程可能有多个文件,会先用缓存装着?
					} catch {
						case NonFatal(e) =>{
							runningFailureCount += 1
							totalFailureCount += 1
							if (totalFailureCount >= maxFetchFailures) {
								return None
							}
							null
						}
					}
					
					if (data != null) {//当从该远程BlockMangerId中拿到数据,就结束循环,返回;
						return Some(new ChunkedByteBuffer(data))
					}
					logDebug(s"The value of block $blockId is null")
				}
				logDebug(s"Block $blockId not found")
				None
			}
			
			someBuffDate.map { data =>
				val values = serializerManager.dataDeserializeStream(blockId, data.toInputStream(dispose = true))(ct)
				new BlockResult(values, DataReadMethod.Network, data.size)
			}
		}
		if (remote.isDefined) {
			logInfo(s"Found block $blockId remotely")
			return remote
		}
		None
	}

	blockResult match {
      case Some(block) => return Left(block)
      case _ => // Need to compute the block.进入下面的计算
    }
	
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
		case None =>
			val blockResult = getLocalValues(blockId).getOrElse {
				releaseLock(blockId)
				throw new SparkException(s"get() failed for block $blockId even though we held a lock")
			}
			releaseLock(blockId)
			Left(blockResult)
		case Some(iter) => Right(iter)
    }
}


/* "Spark Context Cleaner"线程:循环清理断开引用的弱联结数据: 删除内存和磁盘中 过期的(5种类型)数据:RDD/Shuffle/ Broadcase/ Accum/ Checkpoint;
*	- 线程触发位置: new SparkContext()构造函数中: _cleaner.foreach(_.start()) -> ContextCleaner.start() -> cleaningThread.start() -> ContextCleaner.keepCleaning() 
*/

ContextCleaner.keepCleaning(){// 在"Spark Context Cleaner"线程中, 循环处理referenceQueue队列中的 清理任务;
	
	{//解释 referenceQueue队列中数据来源;
		private val referenceQueue = new ReferenceQueue[AnyRef]
		// 在"JobGenerator"线程中, 当RDD需要持久化时rdd.persist()触发 ContextCleaner.registerForCleanup() 注册一个弱引用;
		ContextCleaner.registerForCleanup(){
			referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue));// 给该task注册一个CleanupTaskWeakReference弱引用; 
			// 当该task断开与其引用对象的联结时,就变为弱引用,从能被GC中回收; 并存于referenceQueue队列中;   下面的while()循环不断从referenceQueue队列中取元素进行回收;
		}
		
		JobGenerator.generateJobs(time) -> DStreamGraph.generateJobs()-> outputStream.generateJob(time)
			-> ForEachDStream.generateJob()->parent.getOrCompute(time) -> DStream.getOrCompute() 中generatedRDDs.get(time)为空,而调用orElse{}递归计算给RDD
				-> newRDD.persist(storageLevel) -> RDD.persist() -> sc.cleaner.foreach(_.registerRDDForCleanup(this)) -> ContextCleaner.registerRDDForCleanup(rdd)
					-> registerForCleanup(rdd, CleanRDD(rdd.id)) -> referenceBuffer.add(new CleanupTaskWeakReference(task, objectForCleanup, referenceQueue));
			* 总结: 即在JobGenerate生成Jobs和相应RDD时,当碰到有持久化的RDD时(rdd.cache()/.persist())时, 会调用sc.cleaner.foreach(_.registerRDDForCleanup(this)) 将该RDD注册进/存入ContextCleaner.referenceBuffer:HashSet缓存中;
				- 封装的CleanupTaskWeakReference并不会被马上清理，而是等到变为弱可达时才会被加入referenceQueue中去被清理
				- 问题: 什么时候回变成弱引用?
					* 引用的对象(task)只和当前的WeakReference对象联结，那么在GC中会被回收，并放入referenceQueue中。
					* Java中对Reference有几种不同的分类: StrongReference,强引用很难被GC;WeakReference 弱引用, 如果引用对象与当前对象联结,就放入GC中; 会被GC回收,并放入referenceQueue中
					* referenceBuffer的作用是保证WeakReference在处理前不被GC;
					* Spark将注册的Accumulator封装到CleanupTask，并基于task初始化了一个WeakReference。当Accumulator不再被引用时，task会被放入referenceQueue中，而此时cleaningThread从referenceQueue中提取即将要GC的对象做处理（见上面的清理过程代码）;
	
	}
	
	while (!stopped) {
		// 循环从清理任务队列:referenceQueue中取出末尾的, 进行判断类型和清理逻辑;
		val reference = Option(referenceQueue.remove(ContextCleaner.REF_QUEUE_POLL_TIMEOUT)).map(_.asInstanceOf[CleanupTaskWeakReference])
        synchronized {
          reference.foreach { ref =>
            logDebug("Got cleaning task " + ref.task)
            referenceBuffer.remove(ref)
            ref.task match {//根据该数据类型,根据其存储的不同方式和介质,进行清理;
				case CleanRDD(rddId) =>doCleanupRDD(rddId, blocking = blockOnCleanupTasks)
					-> sc.unpersistRDD(rddId, blocking)
						-> env.blockManager.master.removeRdd(rddId, blocking) -> BlockMangerMaster.removeRDd(rddId): 
							->  val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId)) 向Driver端发Rpc消息: RemoveRdd
								- Driver端-"dispatcher-event-loop"线程接收: BlockManagerMasterEndpoint.receiveAndReply():case RemoveRdd => context.reply(removeRdd(rddId))
									-> BlockManagerMasterEndpoint.removeRdd(rddId) ->  bm.slaveEndpoint.ask[Int](removeMsg):向Executor端发Rpc消息: RemoveRdd
										
										* Executor端- "dispatcher-event-loop"线程接收: BlockManagerSlaveEndpoint.receiveAndReply():case RemoveShuffle() => doAsync(){blockManager.removeRdd(rddId)}
											-> 在Executor端-新启一个线程"block-manager-slave-async-thread-pool" 完成 blockManager.removeBlock()的操作;
				
				case CleanShuffle(shuffleId) =>doCleanupShuffle(shuffleId, blocking = blockOnShuffleCleanupTasks)
					-> BlockManagerMaster.removeShuffle(shuffleId)
						-> val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId)): 向Driver发出RemoveShuffle消息, 
							->Driver端-"dispatcher-event-loop"线程接收: BlockManagerMaster.receiveAndReply():case RemoveShuffle => context.reply(removeShuffle(shuffleId)) -> bm.slaveEndpoint.ask[Boolean](removeMsg)
								
								* Executor端"dispatcher-event-loop" 线程接受: BlockManagerSlaveEndpoint.receiveAndReply():case RemoveShuffle() => doAsync(){mapOutputTracker.unregisterShuffle(shuffleId)}
									-> 在Executor端- 启动"block-manager-slave-async-thread-pool"线程: 进行Shuffle数据的移除;
				
				case CleanBroadcast(broadcastId) =>doCleanupBroadcast(broadcastId, blocking = blockOnCleanupTasks)
				case CleanAccum(accId) =>doCleanupAccum(accId, blocking = blockOnCleanupTasks)
				case CleanCheckpoint(rddId) =>doCleanCheckpoint(rddId)
            }
          }
        }
	}
}



