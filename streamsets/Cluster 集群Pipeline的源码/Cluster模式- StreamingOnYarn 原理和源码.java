Cluster Pipeline运行 代调研问题
1. 启动运行ClusterPipeline的整个流程: 入口, ClusterRunner线程, ApplicationMaster进程, Driver代码; Executor框架, Executor中SDCSlave代码; Yarn调度算法;

* 1. RunOnCluster的运行原理和源码实现
* 2. SparkTransformer和Driver的关系
* 3. Kafka算子的实现原理和优化
* 4. SparkStreamingOnYarn的调度分配算法;
* 5. SparkOnYarn的易故障点和优化办法;




关于ClusterOnYarn的 PipelineState管理

	Yarn任务AppId的保存和更新
	1. 在(包括重新)提交Yarn任务时: 会把 yarnAppId 保存在 pipelineState.json的 attributes.[cluster.application.state].id/sdcToken中;
		* 所以, 在从Stopped持久化成Running状态前, 是已经有了 yarnAppId. 到底是在Starting前还是后?
	2. 当 Cluster-Pipeline流不停,而直接关闭 DataCollect容器, 则 pipelineState.json.attributes中 保持 yarnAppId不变;
		* 问题, 会更新msg, 还好更新其他字段内容吗?  答 state.json中attributes字段不变? 其statu字段会变为Disconnected,并metrics中增加监控指标;
	3. 当 先Stop Pipeline操作, 则:
		-> Stopping状态: attributes仍然不变, 以免停流失败,仍然能追踪是哪个 yarnAppId 
		-> Stopped 状态: 删除 attributes中 cluster.application.state信息和字段, 再次运行后将以新yarn任务提交;

问题: 

	1. 当重启是, 原来的集群任务failed时?
	
	

	1. 如果Pipeline原来是Connected, 重启流后原 yarnAppId已经 killed/finished等, 怎么办?
		* 要么重新提交任务 要么, 失败, 状态是 stopped?
		
		

1. 如果DC正在运行, 遇到集群任务的状态变为 failed时, 怎么办?
	* 应该是 ManagerRunnable.run()->checkStatus() 会检查到YarnApp任务 Failed状态: 会把PipelineState持久化为RunError; 
		- 会取消重试?  当 !isActive || Retry状态时, ManagerRunnable.cancelRunnable()取消 check和ClusterRunner运行;

1. 当重启是, 原来Running的集群任务failed时? 


2. 如果Pipeline关闭是是ConnectedError, 怎么办?



3. ClusterPipeline的重试机制和重试次数, 是什么规则, 如何实现的?
	* 什么情况下,会重试 ? 
		- 提一次提交任务失败后
		- 已提交的Application任务, ApplicationMaster存活,Executor挂掉
		- 问题: 已经提交成功的任务,会重试吗?
	* 重试的次数是怎么样 ?
	* 如何取消重试 ?


1.2 DC运运行时, Pipeline的启动Start:
ManagerResource.startPipeline() -> AclRunner.start():
	* StandaloneRunner.start(): ProductionPipelineRunnable.run() 
		-> ProductionPipeline.run()->Pipeline.run() -> ProductionPipelineRunner.run():
			1. Push数据源: ProductionPipelineRunner.runPushSource():
			2. Poll数据源: ProductionPipelineRunner.runPollSource(): PipeRunner.executeBatch() -> Pipe.process() -> DProcessor.process()
	
	* ClusterRunner.start(): ClusterRunner.doStart(): SystemProcess.start(environment);



	Cluster Pipeline运行 代调研问题
1. 启动运行ClusterPipeline的整个流程: 入口, ClusterRunner线程, ApplicationMaster进程, Driver代码; Executor框架, Executor中SDCSlave代码; Yarn调度算法;

/*  源码分析任务: 流任务运行的入口, ClusterRunner线程的原理
	* ClusterRunner -> StandaloneAndClusterPipelineManager.recoverPipelineStatus() -> Cluster.onDataCollectorStart()
	* Yarn上 Application.main() -> Application.main()
	* AppMaster上 App的Driver逻辑 -> Driver.foreach()? 
	* spark的 ExecutorBackend 进程 -> ExecutorBackend.revice()?
	* Executor进程上 SDC线程 -> EmaddedSDC.Main.doMain();
	* Yarn的ResourceManager 进程
	* Yarn的NodeMangaer进程
	
*/
	

/** 1.1 启动DataCollector时 恢复 ClusterPipeline流状态的过程
* 	入口触发过程: 
		sdc3.1: 	Main.do() -> task.run() -> StandaloneAndClusterPipelineManager.runTask() -> 直接执行 
			-> runner.prepareForDataCollectorStart(); 
			-> runner.onDataCollectorStart(user)
		enosSdc3.2: Main.do() -> task.run() -> StandaloneAndClusterPipelineManager.runTask() -> if(haModel) -> recoverPipelineStatus();
			-> runner.prepareForDataCollectorStart(); 
			-> runner.onDataCollectorStart(user);
*/

// 恢复流运行状态
StandaloneAndClusterPipelineManager.recoverPipelineStatus(){
	boolean restartPipelines = configuration.get(RUNNER_RESTART_PIPELINES, DEFAULT_RUNNER_RESTART_PIPELINES);
	List<PipelineInfo> pipelineInfoList = pipelineStore.getPipelines();
	
	for (PipelineInfo pipelineInfo : pipelineInfoList) {
		// 从缓存中加载State, 初次的话从磁盘 pipelineState.json文件中读取
		PipelineState pipelineState = pipelineStateStore.getState(name, rev);
		
		// 获取或创建该Pipeline对的StandaloneRunner or ClusterRunner 
		Runner runner = getRunner(name, rev, executionMode);
		// 准备启动状态,若还处于Ing,则强制变为 断开连接; 若为ed的,表示已就绪;
		runner.prepareForDataCollectorStart(pipelineState.getUser()){
			ClusterRunner.prepareForDataCollectorStart(){
				PipelineStatus status = getState().getStatus();
				switch (status) {
					// 正运行 active的状态, 将其强制为 Connected, 即断开连接;
					case STARTING:
					case RETRY:
					case CONNECTING:
					case RUNNING:
					case CONNECT_ERROR:
					case STOPPING:
						msg = "Pipeline was in {status} state, forcing it to DISCONNECTING";
						break;
					
					// 若是Disconnected, edited等 之前过程已结束的状态,则代表已准备好启动, 直接返回; 
					case DISCONNECTED:
					case EDITED:
					case FINISHED:
					case KILLED:
					case START_ERROR:
					case STOPPED:
						return;
					
					default:throw new IllegalStateException(Utils.format("Pipeline in undefined state: '{}'", status));
				}
				
				validateAndSetStateTransition(user, PipelineStatus.DISCONNECTED, msg);
			}
		}
		
		if (restartPipelines && runner.getState().getStatus() == PipelineStatus.DISCONNECTED) {
			runnerCache.put(getNameAndRevString(name, rev), new RunnerInfo(runner, executionMode));
			
			GroupsInScope.executeIgnoreGroups(()->{
				runner.onDataCollectorStart(user){
					//  只有流状态已变为 Connected[断开连接], 才会进入 连接远程任务启集群任务的 connectOrStart()方法;
					ClusterRunner.onDataCollectorStart(){
						PipelineStatus status = getState().getStatus();
						switch (status) {
							case DISCONNECTED:
								// 核心1: 将状态更改为 CONNECTING
								validateAndSetStateTransition(user, PipelineStatus.CONNECTING, msg);
								
								connectOrStart(user){
									// 从pipelineState.json的 attributes字段中获取 appState
									ApplicationState appState = new ApplicationState((Map) attributes.get(APPLICATION_STATE));
									
									// appId为空, 代表初次运行,或上次任务已完成并清理了; 此时提交一个新Yarn任务;
									if (appState.getId() == null) {
										retryOrStart(user){
											if (pipelineState.getRetryAttempt() == 0) {
												prepareForStart(user, runtimeParameters);
												start(user){ // ClusterRunner.start() 启动
													doStart(){
														
														ApplicationState applicationState = clusterHelper.submit(pipelineConf,environment);
														attributes.put(APPLICATION_STATE, applicationState.getMap()); // 将appId放入attributes字段中;
														validateAndSetStateTransition(PipelineStatus.RUNNING);
														
														// 重要步骤2: 设置(默认30秒)定时任务checkStatus(), 调用yarn -status命令获取当前任务状态;
														scheduleRunnable(user, pipelineConf){
															int peroid = new Random(System.currentTimeMillis()).nextInt(30) + statusUpdateTimeout;
															ManagerRunnable managerRunnable = new ManagerRunnable(){ @override public void run(){
																checkStatus(){ // ManagerRunnable.checkStatus()
																	if (clusterRunner.getState().getStatus().isActive()) {
																		PipelineState ps = clusterRunner.getState();
																		ApplicationState appState = new ApplicationState((Map) ps.getAttributes().get(APPLICATION_STATE));
																		clusterRunner.connect(runningUser, appState, pipelineConf){
																			try{
																				clusterPipelineState = clusterHelper.getStatus(appState, pipelineConf){
																					return clusterProviderImap.getStatue(){ // 实际调Yarn 命令获取该appId对应的状态
																						_cluster-manager status applicationId  
																							->实际执行: yarn application -status applicationId; 
																					}
																				}
																			}catch(Exception ex){
																				// 若从Yarn获取该appId任务状态异常, 则标为 CONNECT_ERROR 状态;
																				validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
																			}
																			
																			if (clusterPipelineState == ClusterPipelineStatus.RUNNING) {
																				validateAndSetStateTransition(user, PipelineStatus.RUNNING, msg);
																			}else if (clusterPipelineState == ClusterPipelineStatus.FAILED) {
																				// 状态变为RunError,并移除applicationState字段, 就持久化状态;
																				postTerminate(user, appState, PipelineStatus.RUN_ERROR, msg);
																			}else if (clusterPipelineState == ClusterPipelineStatus.KILLED) {
																				// 状态变为Killed,并移除applicationState字段, 就持久化状态;
																				postTerminate(user, appState, PipelineStatus.KILLED, msg);
																			}else if (clusterPipelineState == ClusterPipelineStatus.SUCCEEDED) {
																				postTerminate(user, appState, PipelineStatus.FINISHED, msg);
																			}
																		}
																	}
																	
																	// 当状态为非活跃时,或为Retry时, 取消本线程任务;
																	if (!clusterRunner.getState().getStatus().isActive() || clusterRunner.getState().getStatus() == PipelineStatus.RETRY) {
																		clusterRunner.cancelRunnable();
																	}
																	
																}
															}}
															
															// 定时执行, 去调用Yarn命令获取该applicationId当前的状态, (sdc.cluster.job.status.update.timeout, 默认30秒)
															runnerExecutor.scheduleAtFixedRate(managerRunnable,0,statusUpdateTimeout,TimeUnit.SECONDS);
															
														}
														
													}
												}
											}else{
												// 若干重启? 会怎么样
												validateAndSetStateTransition(user, PipelineStatus.RETRY, "Changing the state to RETRY on startup");
											}
										
										}
									} else{ // 有相应集群任务在跑时
										slaveCallbackManager.setClusterToken(appState.getSdcToken());
										
										// ClusterProviderImpl.getState()利用Yarn -status命令获取当前状态; 若未活跃状态, 则定时检查状态;
										clusterRunner.connect(runningUser, appState, pipelineConf){
											try{
												clusterPipelineState = clusterHelper.getStatus(appState, pipelineConf){
													return clusterProviderImap.getStatue(){ // 实际调Yarn 命令获取该appId对应的状态
														_cluster-manager status applicationId  
															->实际执行: yarn application -status applicationId; 
													}
												}
											}catch(Exception ex){
												// 若从Yarn获取该appId任务状态异常, 则标为 CONNECT_ERROR 状态;
												validateAndSetStateTransition(user, PipelineStatus.CONNECT_ERROR, msg);
											}
											
											if (clusterPipelineState == ClusterPipelineStatus.RUNNING) {
												validateAndSetStateTransition(user, PipelineStatus.RUNNING, msg);
											}else if (clusterPipelineState == ClusterPipelineStatus.FAILED) {
												// 状态变为RunError,并移除applicationState字段, 就持久化状态;
												postTerminate(user, appState, PipelineStatus.RUN_ERROR, msg);
											}else if (clusterPipelineState == ClusterPipelineStatus.KILLED) {
												// 状态变为Killed,并移除applicationState字段, 就持久化状态;
												postTerminate(user, appState, PipelineStatus.KILLED, msg);
											}else if (clusterPipelineState == ClusterPipelineStatus.SUCCEEDED) {
												postTerminate(user, appState, PipelineStatus.FINISHED, msg);
											}
										}
										
										if (getState().getStatus().isActive()) {
											scheduleRunnable(user, pipelineConf); // 实现代码如上述: 
										}
										
									}
								}
								break;
							default:  LOG.error(Utils.format("Pipeline has unexpected status");
						}
					}
				}
			});
			
		}
		
	}
	
}






/** 1.2  ApplicationMaster Yarn上的代码
* 入口触发过程: 
*	spark2.2:	Application.main() -> run() -> runDriver() -> Application.startUserApplication() -> new Thread(){mainMethod.invoke();}.start()
		* BootstrapClusterStreaming.main() 
			-> SparkStreamingBinding.init() ->  JavaStreamingContextFactoryImpl.create() 
				* new JavaStreamingContext()
				* createDStream() 将StreamSets的Stage算子构建成DStream 以切分成两个Spark Job来提交执行;
			-> SparkStreamingBinding.startContext() 
				* 将DStream切分成具体Job,并提交执行
				* 安排循环调度, 管理offset并循环运行
			-> SparkStreamingBinding.awaitTermination()
* 	
*/

ApplicationMaster.main(){
	SparkHadoopUtil.get.runAsSparkUser { () =>
		ApplicationMaster master = new ApplicationMaster(amArgs, new YarnRMClient);
		master.run(){
			if(isClusterMode){
				ApplicationMaster.runDriver(securityMgr);{
					# 定义用户App应用的运行(RecordWindowAggr.main()),并结束AppMaster:finish(SUCCEEDED)
					userClassThread:Thread = startUserApplication(){
						// spark应用中Driver类main入口所在类:如 BootstrapClusterStreaming.main()
						val mainMethod = userClassLoader.loadClass(args.userClass).getMethod("main", classOf[Array[String]])
						userThread:Thread = new Thread(){ run(){
							mainMethod.invoke(userArgs.toArray) #RecordWindowAggr.main()
						}}
						userThread.setName("Driver") // 此处, 在AppMaster进程中,应该能看到名叫Driver的线程;
						userThread.start();{//
							userThread.run(){
								mainMethod.invoke(userArgs.toArray);{//BootstrapClusterStreaming.main()
									// BootstrapClusterStreaming的主要执行逻辑
									BootstrapClusterStreaming.main(){
										binding = SparkStreamingBindingFactory.build(BootstrapCluster.getProperties());{
											return bindingFactory[Kafka010SparkStreamingBindingFactory].create(){new Kafka010SparkStreamingBinding();};
										}
										binding.init();{//SparkStreamingBindingFactory.init()
											Configuration hadoopConf = new SparkHadoopUtil().newConfiguration(conf);
											offsetHelper = new SdcClusterOffsetHelper(checkPointPath, hdfs, Utils.getKafkaMaxWaitTime(properties));
											JavaStreamingContextFactory javaStreamingContextFactory = getStreamingContextFactory();
											ssc = javaStreamingContextFactory.create();{//JavaStreamingContextFactoryImpl.create()
												JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
												return createDStream(jssc, props);{//Kafka010SparkStreamingBindingFactory.createDStream()
													JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = KafkaUtils.createDirectStream();//创建KafkaDStream;
													//Driver逻辑:KafkaRDD消费并在mapPartition中通过 dataChannel.dataQueue队列转到SDC框架取消费处理;
													Driver$.MODULE$.foreach(stream.dstream(), KafkaOffsetManagerImpl.get());{
														dstream.foreachRDD(rdd => {
															rdd.mapPartitions(iterator => {
																val batch = iterator.map({ pair  => new Pair(pair._1, pair._2)});
																ClusterFunctionProvider.getClusterFunction.startBatch(batch).asInstanceOf[java.util.Iterator[Record]].asScala;
															}
															offsetManager.saveOffsets(rdd)
														})
													}
													stream.foreachRDD(new CommitOffset(stream));
													return jssc;
												}
											}
											ssc.checkpoint(rddCheckpointDir.toString());
										}
										BootstrapCluster.createTransformers(binding.getStreamingContext().sparkContext());
										binding.startContext();
										binding.awaitTermination();
									}
								}
							}
						}
						return userThread;
					}
					userClassThread[Thread].join();//等待 BootstrapClusterStreaming.main()线程的执行完成;
				}
			}
		}
	}
}


// BootstrapClusterStreaming的主要执行逻辑
BootstrapClusterStreaming.main(){
	binding = SparkStreamingBindingFactory.build(BootstrapCluster.getProperties());{
		return bindingFactory[Kafka010SparkStreamingBindingFactory].create(){new Kafka010SparkStreamingBinding();};
	}
    binding.init();{//SparkStreamingBindingFactory.init()
		Configuration hadoopConf = new SparkHadoopUtil().newConfiguration(conf);
		offsetHelper = new SdcClusterOffsetHelper(checkPointPath, hdfs, Utils.getKafkaMaxWaitTime(properties));
		JavaStreamingContextFactory javaStreamingContextFactory = getStreamingContextFactory();
		ssc = javaStreamingContextFactory.create();{//JavaStreamingContextFactoryImpl.create()
			JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(duration));
			return createDStream(jssc, props);{//Kafka010SparkStreamingBindingFactory.createDStream()
				JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = KafkaUtils.createDirectStream();//创建KafkaDStream;
				//Driver逻辑:KafkaRDD消费并在mapPartition中通过 dataChannel.dataQueue队列转到SDC框架取消费处理;
				Driver$.MODULE$.foreach(stream.dstream(), KafkaOffsetManagerImpl.get());{
					dstream.foreachRDD(rdd => {
						rdd.mapPartitions(iterator => {
							val batch = iterator.map({ pair  => new Pair(pair._1, pair._2)});
							ClusterFunctionProvider.getClusterFunction.startBatch(batch).asInstanceOf[java.util.Iterator[Record]].asScala;
						}
						offsetManager.saveOffsets(rdd)
					})
				}
				stream.foreachRDD(new CommitOffset(stream));
				return jssc;
			}
		}
		ssc.checkpoint(rddCheckpointDir.toString());
	}
    BootstrapCluster.createTransformers(binding.getStreamingContext().sparkContext());
    binding.startContext();
    binding.awaitTermination();
}


BootstrapClusterStreaming.main(){
	try{
		SparkStreamingBinding binding[Kafka010SparkStreamingBinding] = SparkStreamingBindingFactory.build(BootstrapCluster.getProperties()){
			SparkStreamingBindingFactory.create(){
				bindingFactory[Kafka010SparkStreamingBindingFactory].create(){new Kafka010SparkStreamingBinding();};
					* SparkStreamingBinding - Property => kafkaConfigBean.dataFormatConfig.xmlMaxObjectLen => 4096
			}
		}
		
		// 主要任务: 创建ssc:SparkStreamingContext
		/** SparkStreaming相关的初始化: 
		*   1. new JavaStreamingContext()
		*	2. ssc.checkPoint()
		*/
		binding.init();-> SparkStreamingBinding.init(){ // [Kafka010SparkStreamingBinding extends SparkStreamingBinding]
			for (Object key : properties.keySet()) {logMessage(key);}
				logMessage("Property => " + key + " => " + properties.getProperty(key.toString()), isRunningInMesos);
					* SparkStreamingBinding - Property => kafkaConfigBean.dataFormatConfig.xmlMaxObjectLen => 4096
			SparkConf conf = new SparkConf()
			URI hdfsURI = FileSystem.getDefaultUri(hadoopConf);
			hdfs = (new Path(hdfsURI)).getFileSystem(hadoopConf);
					* SparkStreamingBinding - Default FS URI: hdfs://ldsver41:9000/
		
			# 获取kafka的Offset
			offsetHelper = new SdcClusterOffsetHelper(checkPointPath, hdfs, Utils.getKafkaMaxWaitTime(properties));
					* SdcClusterOffsetHelper - SDC Checkpoint File Path : hdfs://ldsver41:9000/user/app/.streamsets-spark-streaming/9f548e2c-0158-11ea-8fc5-137338da775b/HeJiaQing_In_commTest/2020_testDemo_cluster_0202-01/AllenTestDemoClusterRunef5d94c9-dec9-4e67-a25d-500c6287f806

			ssc[JavaStreamingContext] = javaStreamingContextFactory.create(){ // SparkStreamingBinding.JavaStreamingContextFactoryImpl.create()
				sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));
				sparkConf.set("spark.driver.userClassPathFirst", "true");
				sparkConf.set("spark.executor.userClassPathFirst", "true");
				
				/** 创建JavaStreamingContext, 启动Driver
				* 
				*/
				JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
				
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
								val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode){
									val scheduler = cm.createTaskScheduler(sc, masterUrl)
										val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
										cm.initialize(scheduler, backend){
											TaskSchedulerImpl.start(){
												backend[YarnClusterSchedulerBackend].start()
											}
										}
									}
									
									
								}
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
				
				
				
				// 构建Kafka的DStream
				/** 定义SparkStream的 用户处理逻辑:DStream
				*	- 创建Kafka数据源
				* 	- 将SDC.Stages 包装/转换成spark的一个foreach算子;
				* 	- 调用DStream.foreachRDD()算子, 执行对每个RDD的计算并输出;
				*/
				props.put("group.id", groupId);
				props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				createDStream(result, props){ // Kafka010SparkStreamingBinding.createDStream(result:JavaStreamingContext)
					props.put("bootstrap.servers", metaDataBrokerList);
					props.putAll(extraKafkaConfigs);
					JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = KafkaUtils.createDirectStream(result,
								LocationStrategies.PreferConsistent(),ConsumerStrategies.<byte[], byte[]>Subscribe(topics, props));
					
					Driver$.MODULE$.foreach(stream.dstream(), KafkaOffsetManagerImpl.get()){ //Driver.foreach(dstream: DStream[ConsumerRecord], kafkaOffsetManager: KafkaOffsetManager)
						
						dstream.foreachRDD(rdd => {
							// 此处将Kafka一条msg转换转换成(key,value) 元组结构
							kvRDD = rdd.map(c =>(c.key(), c.value()))
							
							Driver.process(kvRDD){ // Driver.process(rdd: RDD[(Array[Byte], Array[Byte])]): Unit 
							private var previousIncomingData: mutable.Queue[RDD[(Array[Byte], Array[Byte])]] = mutable.Queue()	
								//清除上一RDD持久化的数据删除: 
									//? previousIncoming是指? 上个Batch的数据?
								previousIncomingData.foreach(_.unpersist(false))
								previousIncomingData.clear()
								
								// ? previousGeneratedRDDs 是指? 上一Batch计算得出的RDD数据;
								previousGeneratedRDDs.foreach(_.unpersist(false))
								previousGeneratedRDDs.clear()
								
								// 将当前输入rdd, 添加到 队列中
								previousIncomingData += rdd
								
								// 添加SparkTransformes 算子逻辑;
								if (transformers.isEmpty) {
									transformers ++= BootstrapCluster.getTransformers.asScala
								}
								
								// 当transformers不为空, 或开启动态扩容是, 需要重新 repartition()
								val incoming = if (transformers.nonEmpty||dynamicalAlocationisEnabled){
									repartition(rdd){
										if (rdd.partitions.length > partitionCount) {
											JavaRDD.fromRDD(rdd).coalesce(partitionCount).rdd
										}else if(){
											rdd.repartition(partitionCount)
										}else{
											rdd
										}
									}
								}else{
									rdd
								}
								
								previousIncomingData += incoming
								
								var nextResult:RDD[Record]= incoming.mapPartitions(iterator => {
									initialize()
									
									// 转换成Java的List, 
									val batch = iterator.map({ pair  =>
										val key = if (pair._1 == null) {
											"UNKNOWN_PARTITION".getBytes
										} else {
											pair._1
										}
										  
										new com.streamsets.pipeline.impl.Pair(key, pair._2).asInstanceOf[util.Map.Entry[_, _]]
									}).toList.asJava
									
									val iterator:Iterator = ClusterFunctionProvider.getClusterFunction.startBatch(batch){ // ClusterFunctionImpl.startBatch(List<Map.Entry> batch)
										try{
											if (IS_TRACE_ENABLED) { 
												LOG.trace("In executor function " + " " + Thread.currentThread().getName() + ": " + batch.size());
											}
											
											EmbeddedSDC sdc = sdcPool.getNotStartedSDC();
											
											ClusterSource source = sdc.getSource();
											offset = source.put(batch);
											
											Iterator<Object> iterator= getNextBatch(0, sdc){ // ClusterFunctionImpl.getNextBatch(int id, EmbeddedSDC sdc)
												Optional option= Optional.ofNullable(sdc.getSparkProcessorAt(id)){
													return id < sparkProcessors.size() ? sparkProcessors.get(id) : null;
												}
												
												Iterator<Object> iterator= option.flatMap( t ->{
													Object processor = t.getClass().getMethod("get").invoke(t);
													final Method getBatch = processor.getClass().getDeclaredMethod(GET_BATCH);
													Iterator<Object> batch = (Iterator<Object>) getBatch.invoke(processor);
													List<Object> cloned = new ArrayList<>();
													
													batch.forEachRemaining(record -> cloned.add(RecordCloner.clone(record)));
													return Optional.of(cloned.iterator());
												})
													.orElse(Collections.emptyIterator());
												
												return iterator;
											}
											return iterator;
										}finally{
											if (isPipelineCrashed) {
												sdcPool.cleanCrashedSDC(sdc);
											}else{
												doFastForwardToPipelineEnd(0, sdc);
											}
										}
										
										
									}
									// 将Java的Iterator转换成Scala的
									iterator.asInstanceOf[java.util.Iterator[Record]].asScala
								 });
								
								previousGeneratedRDDs += nextResult
								
								// 触发上述逻辑作为一个Job提交和执行
								nextResult.cache().count()
								
								// 执行Transformer算子逻辑 
								transformers.foreach(transformer =>{})
								
								// 提交nextResult的RDD的job,再计算一遍;
								nextResult.count()
								
							}
							
							offsetManager.saveOffsets(rdd){ //KafkaOffsetManagerImpl.saveOffsets(rdd)
								Map<Integer, Long> offset = getOffsetToSave(((HasOffsetRanges) rdd).offsetRanges()){ // KafkaOffsetManagerImpl.getOffsetToSave(OffsetRange[] offsetRanges)
									for (int i = 0; i < offsetRanges.length; i++) {
										partitionToOffset.put(offsetRanges[i].partition(), offsetRanges[i].untilOffset());
									}
								}
								
								if (!offset.isEmpty()) {
									SparkStreamingBinding.offsetHelper.saveOffsets(offset){ //SdcClusterOffsetHelper.saveOffsets()
										// 是将offset持久化到hdfs上? /user/app/streamStage/sdcId/pipelineId/comsumerGroup/topic/ ?
										writeOffsetsToMainOffsetFile(partitionToOffsetMap);
									}
								}else{
									LOG.trace("Offset is empty");
								}
							}
							
						});
					}
					
					stream.foreachRDD(new CommitOffset(stream));
					
					return result[JavaStreamingContext];
				}
						
			}
				
			Runtime.getRuntime().addShutdownHook(shutdownHookThread=new Thread(){run(){
				ssc.stop(true, true);
					* LOG.debug("Gracefully stopping Spark Streaming Application");
					* LOG.info("Application stopped");
			}};
			
			// 创建 rdd在hdfs上的保存目录;
			hdfs.mkdirs(rddCheckpointDir);
			ssc.checkPoint()
				* [thread:Driver]  INFO  SparkStreamingBinding - Making calls through spark context
		}
		
		//  根据pipeline是否有SparkTransformer?解析并创建SparkTransformer的实例?
		BootstrapCluster.createTransformers(binding.getStreamingContext().sparkContext()){
			List<Object> configs = callOnPiplineConfigurationUtil("getSparkTransformers");
			for (Object transformerConfig : configs) {
				// 通过Transformer算子的 getTransformerClass()方法返回其类名
				String transformerClass=(String) transformerConfig.getClass().getMethod("getTransformerClass").invoke(transformerConfig);
				SparkTransformer transformer = (SparkTransformer) (Class.forName(transformerClass).newInstance());
				// 进行Transformer算子的初始化
				transformer.init(context, params);
				transformers.add(transformer);
			}
		}
		
		// 启动SparkStreaming流; 哪spark计算逻辑在哪儿定义? 在Init里面?
		// 执行ssc.start()启动SparkStreaming流: JobScheduler.start() -> 
		binding.startContext(){ssc.start(){ // SparkStreamingContext.start()
			ssc.start( // StreamingContext.start()
				state match {
					case INITIALIZED =>
						startSite.set(DStream.getCreationSite())
						StreamingContext.ACTIVATION_LOCK.synchronized {
							StreamingContext.assertNoOtherContextIsActive()
							validate(){
								graph.validate()
								Checkpoint.serialize(new Checkpoint(this, Time(0)), conf)
							}
							
							ThreadUtils.runInNewThread("streaming-start") {
								sparkContext.clearJobGroup()
								
								/** 启动流任务调度并循环运行的入口: JobScheduler.start()
								*	
								*/
								scheduler[JobScheduler].start(){// JobScheduler.start() 核心的启动和方法;
									logDebug("Starting JobScheduler")
									eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler"){
										override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event);
											JobScheduler.processEvent(event){
												event match {
													case JobStarted(job, startTime) => handleJobStart(job, startTime) // 实现如下:
														// 处理每个Job的启动提交: 
														JobScheduler.handleJobStart(job,startTime){
															jobSet.handleJobStart(job)
															if (isFirstJobOfJobSet) {
																listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
															}
															
															listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
															logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
															
														}
														
													
													case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
													
												}
												
											}
										
										override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e);
									}
									// 启动线程 响应JobSchedulerEvent: JobStarted,JobCompleted,ErrorReported
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
																	val jobSet = jobSets.get(job.time)
																	
																	jobSet.handleJobStart(job){
																		
																	}
																	
																	if (!jobSet.hasStarted) {
																		listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
																	}
																	listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
																	logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
																		* 
																	
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
									
									executorAllocationManager = ExecutorAllocationManager.createIfEnabled(executorAllocClient,receiverTracker)
									executorAllocationManager.foreach(ssc.addStreamingListener)
									
									receiverTracker.start()
										* INFO ReceiverTracker: ReceiverTracker started
									
									/** 启动Job生成相关线程, while()循环去 响应GenerateJobs,DoCheckpoint,ClearCheckpointData 消息
									*		GenerateJobs : 将用户逻辑切分成各Job: DStream-> DAG china -> List<Job> 
									*		DoCheckpoint : 将RDD或Object 持久化到HDFS
									* 		ClearCheckpointData:	清除Checkpoint的数据;
									*/
									jobGenerator.start(){ // JobGenerator.start(){
										// 创建抽象类EventLoop的实现类: 
										eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
											override protected def onReceive(event: JobGeneratorEvent): Unit = JobGenerator.processEvent(event);
											
											private val eventThread = new Thread(name) {
												override def run(): Unit = {
													while (!stopped.get) {
														// 循环从队列中取出, 所以可以削峰平谷;
														val event = eventQueue.take()
														
														onReceive(event) // 抽象类, 由继承类实现: override protected def onReceive(event: JobGeneratorEvent): Unit=processEvent(event);
															// 本示例中的实现: JobGenerator.processEvent(event)
															JobGenerator.processEvent(event){
																event match {
																	case GenerateJobs(time) => generateJobs(time)
																	
																	case ClearMetadata(time) => clearMetadata(time)
																	
																	case DoCheckpoint(time, clearCheckpointDataLater) => doCheckpoint(time, clearCheckpointDataLater)
																	
																	case ClearCheckpointData(time) => clearCheckpointData(time)
																}
															}
													}
													
												}
											}
											
											def start(): Unit = {
												onStart()
													
												eventThread.start()
											}
										}
										
										eventLoop.start(){
											onStart()
											// 启动刚上面定义的私有变量eventThread线程启动
											eventThread.start() // 该线程叫什么名字?
												// 如下日志, 何时打印
												* INFO SocketInputDStream: Initialized and validated org.apache.spark.streaming.dstream.SocketInputDStream@154c7f85
												* INFO TransformedDStream: Initialized and validated org.apache.spark.streaming.dstream.TransformedDStream@796b55f0
												* INFO MapPartitionedDStream: Initialized and validated org.apache.spark.streaming.dstream.MapPartitionedDStream@12edb6ae
												* INFO ForEachDStream: Initialized and validated org.apache.spark.streaming.dstream.ForEachDStream@4cad1c07
												* INFO DAGScheduler: Got job 0 (start at JavaStreamingWCDemo.java:25) with 1 output partitions
												* INFO DAGScheduler: Final stage: ResultStage 0 (start at JavaStreamingWCDemo.java:25)
											
										}
										
										if (ssc.isCheckpointPresent) {
											restart(){ // JobGenerator.restart() 
												val batchDuration = ssc.graph.batchDuration
												
												logInfo("Batches pending processing (" + pendingTimes.length + " batches): " + pendingTimes.mkString(", "));
												timesToReschedule.foreach { time =>
													jobScheduler.receiverTracker.allocateBlocksToBatch(time)
													jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
												}
												logInfo("Restarted JobGenerator at " + restartTime)
											}
										}else {
											startFirstTime(){ // JobGenerator.startFirstTime
												graph.start(startTime - graph.batchDuration)
												timer.start(startTime.milliseconds){ // RecurringTimer.start(startTime:Long)
													nextTime = startTime
													thread.start()
														// 以下为补充的实现类代码:
														thread = new Thread("RecurringTimer - " + name){
															setDaemon(true)
															override def run() { loop(){
																 while (!stopped) {
																	triggerActionForNextInterval(){
																		clock.waitTillTime(nextTime)
																		callback(nextTime){ // callback()方法由RecurringTimer构造函数传入
																			// 在JobGenerator的构造函数中, 执行创建私有变量 private val timer = new RecurringTimer( ()->{}) 传入如下方法
																			longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
																		}
																		prevTime = nextTime
																		nextTime += period
																		logDebug("Callback for " + name + " called at time " + prevTime)
																	}
																}
																triggerActionForNextInterval()
															} }
															
														}
													logInfo("Started timer for " + name + " at time " + nextTime)
													return nextTime;
												}
												
												logInfo("Started JobGenerator at " + startTime)
													* INFO JobGenerator: Started JobGenerator at 1582003515000 ms
											}
										}
										
									}
			
									executorAllocationManager.foreach(_.start())
									logInfo("Started JobScheduler")
										* INFO JobScheduler: Started JobScheduler
								}
							}
							
							state = StreamingContextState.ACTIVE
						}
						
						shutdownHookRef = ShutdownHookManager.addShutdownHook(SHUTDOWN_HOOK_PRIORITY)(stopOnShutdown)
						logInfo("StreamingContext started")
					case ACTIVE =>
						logWarning("StreamingContext has already been started")
					case STOPPED => throw new IllegalStateException("StreamingContext has already been stopped")
				}
			)
		}}
		
		// 循环等待处理数据;
		binding.awaitTermination(){ ssc.awaitTermination(){//SparkStreamingContext.awaitTermination()
			ssc.awaitTermination(){ // StreamingContext.awaitTermination()
				waiter.waitForStopOrError(){ // ContextWaiter.waitForStopOrError(timeout: Long = -1)
					if (timeout < 0) {
						while (!stopped && error == null) {
							condition.await()
						}
					}else{
						var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
						while (!stopped && error == null && nanos > 0) {
							nanos = condition.awaitNanos(nanos)
						}
					}
					
					if (error != null) throw error
					
				}
			}
		}}
	
	}catch (Throwable error) {
		LOG.error("Error trying to invoke BootstrapClusterStreaming.main: " + error, error);
		
		throw new IllegalStateException(msg, error);
	}
}


ApplicationMaster.main(){
	SignalUtils.registerLogger(log)
		- 20/02/01 16:25:50 INFO SignalUtils: Registered signal handler for TERM

	SparkHadoopUtil.get.runAsSparkUser { () =>
		master = new ApplicationMaster(amArgs, new YarnRMClient){
			val sparkConf = new SparkConf()
			val localResources = {resources = HashMap[]; resources.put()...;resources.toMap}
		}
		
		master.run(){ // ApplicationMaster.run()
			val appAttemptId = YarnRMClient.getAttemptId()
				- 20/02/01 16:25:55 INFO ApplicationMaster: ApplicationAttemptId: appattempt_1580439957506_0011_000001
			val securityMgr = new SecurityManager(sparkConf)
				- 20/02/01 16:25:55 INFO SecurityManager: Changing view acls to: app
			
			if(isClusterMode){
				ApplicationMaster.runDriver(securityMgr){
					
					# 定义用户App应用的运行(RecordWindowAggr.main()),并结束AppMaster:finish(SUCCEEDED)
					userClassThread:Thread = startUserApplication(){
						// spark应用中Driver类main入口所在类:如 BootstrapClusterStreaming.main()
						val mainMethod = userClassLoader.loadClass(args.userClass).getMethod("main", classOf[Array[String]])
						
						userThread:Thread = new Thread(){ run(){
							mainMethod.invoke(userArgs.toArray) #RecordWindowAggr.main()
						}}
						userThread.setName("Driver") // 此处, 在AppMaster进程中,应该能看到名叫Driver的线程;
						userThread.start()
						return userThread;
					}
					logInfo("Waiting for spark context initialization...")
						- 20/02/01 21:44:32 INFO ApplicationMaster: Waiting for spark context initialization...
					
					val sc = ThreadUtils.awaitResult(sparkContextPromise.future,Duration()) # 创建SparkContext
							- 20/02/02 15:53:10 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.41.143:39350
							- 20/02/02 15:53:12 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.41.143, 35865, None)
					
					val driverRef = runAMEndpoint()
					logInfo{}
							- 20/02/01 16:26:02 INFO ApplicationMaster: ==================== YARN executor launch context:
					
					# 向YarnClient注册本ApplicationMaster: 
					registerAM(sc.getConf, rpcEnv, driverRef, sc.ui.map(_.webUrl), securityMgr){
						allocator:YarnAllocator = YarnRMClient.register(driverUrl,YarnConf,sparkConf)
							- 20/02/01 16:26:02 INFO YarnRMClient: Registering the ApplicationMaster
							
						allocator[YarnAllocator].allocateResources(){
							updateResourceRequests(){
								val pendingAllocate = getPendingAllocate
								val (localRequests, staleRequests, anyHostRequests) = splitPendingAllocationsByLocality()
									- 20/02/01 21:44:41 INFO YarnAllocator: Submitted 2 unlocalized container requests.
							}
							
							val allocateResponse = amClient.allocate(progressIndicator)
							val allocatedContainers = allocateResponse.getAllocatedContainers()
							val completedContainers = allocateResponse.getCompletedContainersStatuses()
							
							handleAllocatedContainers(allocatedContainers.asScala)
								- 20/02/01 21:44:43 INFO YarnAllocator: Received 1 containers from YARN, launching executors on 1 of them.
						}

						reporterThread = launchReporterThread()
								- 20/02/02 15:57:38 INFO ApplicationMaster: Started progress reporter thread with (heartbeat : 3000, initial allocation : 200) intervals
								- 20/02/02 15:57:39 INFO DAGScheduler: Submitting 6 missing tasks from ShuffleMapStage 0 (MapPartitionsRDD[2] 
					}
					
					// ApplicationMaster.main线程阻塞在此,等待 用户类线程(即Driver)执行完毕; 
					userClassThread[Thread].join();
						//mainMethod.invoke(userArgs.toArray)  mainMethod相应方法先执行
						// 1. 对于我的测试应用,就是 SparkWordCountDemo.main()
						SparkWordCountDemo.main();
						
						
						/** StreamSets的Cluster任务Driver进程入口: BootstrapClusterStreaming.main()
						*  
						*/
						// 2. 对于StreamSets的ClusterPipeline任务,就是BootstrapClusterStreaming
						BootstrapClusterStreaming.main(){
							try{
								SparkStreamingBinding binding[Kafka010SparkStreamingBinding] = SparkStreamingBindingFactory.build(BootstrapCluster.getProperties()){
									SparkStreamingBindingFactory.create(){
										bindingFactory[Kafka010SparkStreamingBindingFactory].create(){new Kafka010SparkStreamingBinding();};
											* SparkStreamingBinding - Property => kafkaConfigBean.dataFormatConfig.xmlMaxObjectLen => 4096
									}
								}
								
								// 主要任务: 创建ssc:SparkStreamingContext
								/** SparkStreaming相关的初始化: 
								*   1. new JavaStreamingContext()
								*	2. ssc.checkPoint()
								*/
								binding.init();-> SparkStreamingBinding.init(){ // [Kafka010SparkStreamingBinding extends SparkStreamingBinding]
									for (Object key : properties.keySet()) {logMessage(key);}
										logMessage("Property => " + key + " => " + properties.getProperty(key.toString()), isRunningInMesos);
											* SparkStreamingBinding - Property => kafkaConfigBean.dataFormatConfig.xmlMaxObjectLen => 4096
									SparkConf conf = new SparkConf()
									URI hdfsURI = FileSystem.getDefaultUri(hadoopConf);
									hdfs = (new Path(hdfsURI)).getFileSystem(hadoopConf);
											* SparkStreamingBinding - Default FS URI: hdfs://ldsver41:9000/
								
									# 获取kafka的Offset
									offsetHelper = new SdcClusterOffsetHelper(checkPointPath, hdfs, Utils.getKafkaMaxWaitTime(properties));
											* SdcClusterOffsetHelper - SDC Checkpoint File Path : hdfs://ldsver41:9000/user/app/.streamsets-spark-streaming/9f548e2c-0158-11ea-8fc5-137338da775b/HeJiaQing_In_commTest/2020_testDemo_cluster_0202-01/AllenTestDemoClusterRunef5d94c9-dec9-4e67-a25d-500c6287f806

									ssc[JavaStreamingContext] = javaStreamingContextFactory.create(){ // SparkStreamingBinding.JavaStreamingContextFactoryImpl.create()
										sparkConf.set("spark.streaming.kafka.maxRatePerPartition", String.valueOf(maxRatePerPartition));
										sparkConf.set("spark.driver.userClassPathFirst", "true");
										sparkConf.set("spark.executor.userClassPathFirst", "true");
										
										/** 创建JavaStreamingContext, 启动Driver
										* 
										*/
										JavaStreamingContext result = new JavaStreamingContext(sparkConf, new Duration(duration));
										
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
														val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode){
															val scheduler = cm.createTaskScheduler(sc, masterUrl)
																val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
																cm.initialize(scheduler, backend){
																	TaskSchedulerImpl.start(){
																		backend[YarnClusterSchedulerBackend].start()
																	}
																}
															}
															
															
														}
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
										
										
										
										// 构建Kafka的DStream
										/** 定义SparkStream的 用户处理逻辑:DStream
										*	- 创建Kafka数据源
										* 	- 将SDC.Stages 包装/转换成spark的一个foreach算子;
										* 	- 调用DStream.foreachRDD()算子, 执行对每个RDD的计算并输出;
										*/
										props.put("group.id", groupId);
										props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
										createDStream(result, props){ // Kafka010SparkStreamingBinding.createDStream(result:JavaStreamingContext)
											props.put("bootstrap.servers", metaDataBrokerList);
											props.putAll(extraKafkaConfigs);
											JavaInputDStream<ConsumerRecord<byte[], byte[]>> stream = KafkaUtils.createDirectStream(result,
														LocationStrategies.PreferConsistent(),ConsumerStrategies.<byte[], byte[]>Subscribe(topics, props));
											
											Driver$.MODULE$.foreach(stream.dstream(), KafkaOffsetManagerImpl.get()){ //Driver.foreach(dstream: DStream[ConsumerRecord], kafkaOffsetManager: KafkaOffsetManager)
												
												dstream.foreachRDD(rdd => {
													// 此处将Kafka一条msg转换转换成(key,value) 元组结构
													kvRDD = rdd.map(c =>(c.key(), c.value()))
													
													Driver.process(kvRDD){ // Driver.process(rdd: RDD[(Array[Byte], Array[Byte])]): Unit 
													private var previousIncomingData: mutable.Queue[RDD[(Array[Byte], Array[Byte])]] = mutable.Queue()	
														//清除上一RDD持久化的数据删除: 
															//? previousIncoming是指? 上个Batch的数据?
														previousIncomingData.foreach(_.unpersist(false))
														previousIncomingData.clear()
														
														// ? previousGeneratedRDDs 是指? 上一Batch计算得出的RDD数据;
														previousGeneratedRDDs.foreach(_.unpersist(false))
														previousGeneratedRDDs.clear()
														
														// 将当前输入rdd, 添加到 队列中
														previousIncomingData += rdd
														
														// 添加SparkTransformes 算子逻辑;
														if (transformers.isEmpty) {
															transformers ++= BootstrapCluster.getTransformers.asScala
														}
														
														// 当transformers不为空, 或开启动态扩容是, 需要重新 repartition()
														val incoming = if (transformers.nonEmpty||dynamicalAlocationisEnabled){
															repartition(rdd){
																if (rdd.partitions.length > partitionCount) {
																	JavaRDD.fromRDD(rdd).coalesce(partitionCount).rdd
																}else if(){
																	rdd.repartition(partitionCount)
																}else{
																	rdd
																}
															}
														}else{
															rdd
														}
														
														previousIncomingData += incoming
														
														var nextResult:RDD[Record]= incoming.mapPartitions(iterator => {
															initialize()
															
															// 转换成Java的List, 
															val batch = iterator.map({ pair  =>
																val key = if (pair._1 == null) {
																	"UNKNOWN_PARTITION".getBytes
																} else {
																	pair._1
																}
																  
																new com.streamsets.pipeline.impl.Pair(key, pair._2).asInstanceOf[util.Map.Entry[_, _]]
															}).toList.asJava
															
															val iterator:Iterator = ClusterFunctionProvider.getClusterFunction.startBatch(batch){ // ClusterFunctionImpl.startBatch(List<Map.Entry> batch)
																try{
																	if (IS_TRACE_ENABLED) { 
																		LOG.trace("In executor function " + " " + Thread.currentThread().getName() + ": " + batch.size());
																	}
																	
																	EmbeddedSDC sdc = sdcPool.getNotStartedSDC();
																	
																	ClusterSource source = sdc.getSource();
																	offset = source.put(batch);
																	
																	Iterator<Object> iterator= getNextBatch(0, sdc){ // ClusterFunctionImpl.getNextBatch(int id, EmbeddedSDC sdc)
																		Optional option= Optional.ofNullable(sdc.getSparkProcessorAt(id)){
																			return id < sparkProcessors.size() ? sparkProcessors.get(id) : null;
																		}
																		
																		Iterator<Object> iterator= option.flatMap( t ->{
																			Object processor = t.getClass().getMethod("get").invoke(t);
																			final Method getBatch = processor.getClass().getDeclaredMethod(GET_BATCH);
																			Iterator<Object> batch = (Iterator<Object>) getBatch.invoke(processor);
																			List<Object> cloned = new ArrayList<>();
																			
																			batch.forEachRemaining(record -> cloned.add(RecordCloner.clone(record)));
																			return Optional.of(cloned.iterator());
																		})
																			.orElse(Collections.emptyIterator());
																		
																		return iterator;
																	}
																	return iterator;
																}finally{
																	if (isPipelineCrashed) {
																		sdcPool.cleanCrashedSDC(sdc);
																	}else{
																		doFastForwardToPipelineEnd(0, sdc);
																	}
																}
																
																
															}
															// 将Java的Iterator转换成Scala的
															iterator.asInstanceOf[java.util.Iterator[Record]].asScala
														 });
														
														previousGeneratedRDDs += nextResult
														
														// 触发上述逻辑作为一个Job提交和执行
														nextResult.cache().count()
														
														// 执行Transformer算子逻辑 
														transformers.foreach(transformer =>{})
														
														// 提交nextResult的RDD的job,再计算一遍;
														nextResult.count()
														
													}
													
													offsetManager.saveOffsets(rdd){ //KafkaOffsetManagerImpl.saveOffsets(rdd)
														Map<Integer, Long> offset = getOffsetToSave(((HasOffsetRanges) rdd).offsetRanges()){ // KafkaOffsetManagerImpl.getOffsetToSave(OffsetRange[] offsetRanges)
															for (int i = 0; i < offsetRanges.length; i++) {
																partitionToOffset.put(offsetRanges[i].partition(), offsetRanges[i].untilOffset());
															}
														}
														
														if (!offset.isEmpty()) {
															SparkStreamingBinding.offsetHelper.saveOffsets(offset){ //SdcClusterOffsetHelper.saveOffsets()
																// 是将offset持久化到hdfs上? /user/app/streamStage/sdcId/pipelineId/comsumerGroup/topic/ ?
																writeOffsetsToMainOffsetFile(partitionToOffsetMap);
															}
														}else{
															LOG.trace("Offset is empty");
														}
													}
													
												});
											}
											
											stream.foreachRDD(new CommitOffset(stream));
											
											return result[JavaStreamingContext];
										}
												
									}
										
									Runtime.getRuntime().addShutdownHook(shutdownHookThread=new Thread(){run(){
										ssc.stop(true, true);
											* LOG.debug("Gracefully stopping Spark Streaming Application");
											* LOG.info("Application stopped");
									}};
									
									// 创建 rdd在hdfs上的保存目录;
									hdfs.mkdirs(rddCheckpointDir);
									ssc.checkPoint()
										* [thread:Driver]  INFO  SparkStreamingBinding - Making calls through spark context
								}
								
								//  根据pipeline是否有SparkTransformer?解析并创建SparkTransformer的实例?
								BootstrapCluster.createTransformers(binding.getStreamingContext().sparkContext()){
									List<Object> configs = callOnPiplineConfigurationUtil("getSparkTransformers");
									for (Object transformerConfig : configs) {
										// 通过Transformer算子的 getTransformerClass()方法返回其类名
										String transformerClass=(String) transformerConfig.getClass().getMethod("getTransformerClass").invoke(transformerConfig);
										SparkTransformer transformer = (SparkTransformer) (Class.forName(transformerClass).newInstance());
										// 进行Transformer算子的初始化
										transformer.init(context, params);
										transformers.add(transformer);
									}
								}
								
								// 启动SparkStreaming流; 哪spark计算逻辑在哪儿定义? 在Init里面?
								// 执行ssc.start()启动SparkStreaming流: JobScheduler.start() -> 
								binding.startContext(){ssc.start(){ // SparkStreamingContext.start()
									ssc.start( // StreamingContext.start()
										state match {
											case INITIALIZED =>
												startSite.set(DStream.getCreationSite())
												StreamingContext.ACTIVATION_LOCK.synchronized {
													StreamingContext.assertNoOtherContextIsActive()
													validate(){
														graph.validate()
														Checkpoint.serialize(new Checkpoint(this, Time(0)), conf)
													}
													
													ThreadUtils.runInNewThread("streaming-start") {
														sparkContext.clearJobGroup()
														
														/** 启动流任务调度并循环运行的入口: JobScheduler.start()
														*	
														*/
														scheduler[JobScheduler].start(){// JobScheduler.start() 核心的启动和方法;
															logDebug("Starting JobScheduler")
															eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler"){
																override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event);
																	JobScheduler.processEvent(event){
																		event match {
																			case JobStarted(job, startTime) => handleJobStart(job, startTime) // 实现如下:
																				// 处理每个Job的启动提交: 
																				JobScheduler.handleJobStart(job,startTime){
																					jobSet.handleJobStart(job)
																					if (isFirstJobOfJobSet) {
																						listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
																					}
																					
																					listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
																					logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
																					
																				}
																				
																			
																			case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
																			
																		}
																		
																	}
																
																override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e);
															}
															// 启动线程 响应JobSchedulerEvent: JobStarted,JobCompleted,ErrorReported
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
																							val jobSet = jobSets.get(job.time)
																							
																							jobSet.handleJobStart(job){
																								
																							}
																							
																							if (!jobSet.hasStarted) {
																								listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
																							}
																							listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
																							logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
																								* 
																							
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
															
															executorAllocationManager = ExecutorAllocationManager.createIfEnabled(executorAllocClient,receiverTracker)
															executorAllocationManager.foreach(ssc.addStreamingListener)
															
															receiverTracker.start()
																* INFO ReceiverTracker: ReceiverTracker started
															
															/** 启动Job生成相关线程, while()循环去 响应GenerateJobs,DoCheckpoint,ClearCheckpointData 消息
															*		GenerateJobs : 将用户逻辑切分成各Job: DStream-> DAG china -> List<Job> 
															*		DoCheckpoint : 将RDD或Object 持久化到HDFS
															* 		ClearCheckpointData:	清除Checkpoint的数据;
															*/
															jobGenerator.start(){ // JobGenerator.start(){
																// 创建抽象类EventLoop的实现类: 
																eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
																	override protected def onReceive(event: JobGeneratorEvent): Unit = JobGenerator.processEvent(event);
																	
																	private val eventThread = new Thread(name) {
																		override def run(): Unit = {
																			while (!stopped.get) {
																				// 循环从队列中取出, 所以可以削峰平谷;
																				val event = eventQueue.take()
																				
																				onReceive(event) // 抽象类, 由继承类实现: override protected def onReceive(event: JobGeneratorEvent): Unit=processEvent(event);
																					// 本示例中的实现: JobGenerator.processEvent(event)
																					JobGenerator.processEvent(event){
																						event match {
																							case GenerateJobs(time) => generateJobs(time)
																							
																							case ClearMetadata(time) => clearMetadata(time)
																							
																							case DoCheckpoint(time, clearCheckpointDataLater) => doCheckpoint(time, clearCheckpointDataLater)
																							
																							case ClearCheckpointData(time) => clearCheckpointData(time)
																						}
																					}
																			}
																			
																		}
																	}
																	
																	def start(): Unit = {
																		onStart()
																			
																		eventThread.start()
																	}
																}
																
																eventLoop.start(){
																	onStart()
																	// 启动刚上面定义的私有变量eventThread线程启动
																	eventThread.start() // 该线程叫什么名字?
																		// 如下日志, 何时打印
																		* INFO SocketInputDStream: Initialized and validated org.apache.spark.streaming.dstream.SocketInputDStream@154c7f85
																		* INFO TransformedDStream: Initialized and validated org.apache.spark.streaming.dstream.TransformedDStream@796b55f0
																		* INFO MapPartitionedDStream: Initialized and validated org.apache.spark.streaming.dstream.MapPartitionedDStream@12edb6ae
																		* INFO ForEachDStream: Initialized and validated org.apache.spark.streaming.dstream.ForEachDStream@4cad1c07
																		* INFO DAGScheduler: Got job 0 (start at JavaStreamingWCDemo.java:25) with 1 output partitions
																		* INFO DAGScheduler: Final stage: ResultStage 0 (start at JavaStreamingWCDemo.java:25)
																	
																}
																
																if (ssc.isCheckpointPresent) {
																	restart(){ // JobGenerator.restart() 
																		val batchDuration = ssc.graph.batchDuration
																		
																		logInfo("Batches pending processing (" + pendingTimes.length + " batches): " + pendingTimes.mkString(", "));
																		timesToReschedule.foreach { time =>
																			jobScheduler.receiverTracker.allocateBlocksToBatch(time)
																			jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
																		}
																		logInfo("Restarted JobGenerator at " + restartTime)
																	}
																}else {
																	startFirstTime(){ // JobGenerator.startFirstTime
																		graph.start(startTime - graph.batchDuration)
																		timer.start(startTime.milliseconds){ // RecurringTimer.start(startTime:Long)
																			nextTime = startTime
																			thread.start()
																				// 以下为补充的实现类代码:
																				thread = new Thread("RecurringTimer - " + name){
																					setDaemon(true)
																					override def run() { loop(){
																						 while (!stopped) {
																							triggerActionForNextInterval(){
																								clock.waitTillTime(nextTime)
																								callback(nextTime){ // callback()方法由RecurringTimer构造函数传入
																									// 在JobGenerator的构造函数中, 执行创建私有变量 private val timer = new RecurringTimer( ()->{}) 传入如下方法
																									longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
																								}
																								prevTime = nextTime
																								nextTime += period
																								logDebug("Callback for " + name + " called at time " + prevTime)
																							}
																						}
																						triggerActionForNextInterval()
																					} }
																					
																				}
																			logInfo("Started timer for " + name + " at time " + nextTime)
																			return nextTime;
																		}
																		
																		logInfo("Started JobGenerator at " + startTime)
																			* INFO JobGenerator: Started JobGenerator at 1582003515000 ms
																	}
																}
																
															}
									
															executorAllocationManager.foreach(_.start())
															logInfo("Started JobScheduler")
																* INFO JobScheduler: Started JobScheduler
														}
													}
													
													state = StreamingContextState.ACTIVE
												}
												
												shutdownHookRef = ShutdownHookManager.addShutdownHook(SHUTDOWN_HOOK_PRIORITY)(stopOnShutdown)
												logInfo("StreamingContext started")
											case ACTIVE =>
												logWarning("StreamingContext has already been started")
											case STOPPED => throw new IllegalStateException("StreamingContext has already been stopped")
										}
									)
								}}
								
								// 循环等待处理数据;
								binding.awaitTermination(){ ssc.awaitTermination(){//SparkStreamingContext.awaitTermination()
									ssc.awaitTermination(){ // StreamingContext.awaitTermination()
										waiter.waitForStopOrError(){ // ContextWaiter.waitForStopOrError(timeout: Long = -1)
											if (timeout < 0) {
												while (!stopped && error == null) {
													condition.await()
												}
											}else{
												var nanos = TimeUnit.MILLISECONDS.toNanos(timeout)
												while (!stopped && error == null && nanos > 0) {
													nanos = condition.awaitNanos(nanos)
												}
											}
											
											if (error != null) throw error
											
										}
									}
								}}
							
							}catch (Throwable error) {
								LOG.error("Error trying to invoke BootstrapClusterStreaming.main: " + error, error);
								
								throw new IllegalStateException(msg, error);
							}
						}
				
				}
			}
		
		
		System.exit(master.run(){
		  	val appAttemptId = YarnRMClient.getAttemptId()
				- 20/02/01 16:25:55 INFO ApplicationMaster: ApplicationAttemptId: appattempt_1580439957506_0011_000001
			val securityMgr = new SecurityManager(sparkConf)
				- 20/02/01 16:25:55 INFO SecurityManager: Changing view acls to: app
			
			if(isClusterMode){runDriver(securityMgr);}ApplicationMaster.runDriver(): 源码详解如下: 
		
		}
    }
}




SparkStreaming 上AppMaster的线程

JobScheduler.JobHandler.run(){
	ssc.sparkContext.setLocalProperties(SerializationUtils.clone(ssc.savedProperties.get()))
	ssc.sc.setJobDescription()
	
	eventLoop[JobScheduler] = new EventLoop[JobSchedulerEvent]("JobScheduler") {
		override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event){
			JobScheduler.processEvent(event){
				event match {
					case JobStarted(job, startTime) => handleJobStart(job, startTime)
						JobScheduler.handleJobStart(job,startTime){
							jobSet.handleJobStart(job)
							if (isFirstJobOfJobSet) {
								listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
							}
							
							listenerBus.post(StreamingListenerOutputOperationStarted(job.toOutputOperationInfo))
							logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
							
						}
						
					
					case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
				}
				
			}
		}
		
	}.start()
	
	eventLoop[JobScheduler].post(JobStarted(job, clock.getTimeMillis()))
	
}

}


在Executor上启动 EmableSDC进程


# 向Spark-Executor上的一个Slave实例SDC发送 getPipeline(pid)请求,响应流程:

PipelineStoreResource.getPipelineInfo(String name){
	// 1. 先从磁盘加载其 pipelineId/info.json
	PipelineInfo pipelineInfo = store[PipelineStoreTask].getInfo(name){//SlavePipelineStoreTask.getInfo(String name)
		return pipelineStore[PipelineStoreTask].getInfo(name){//FilePipelineStoreTask.getInfo(String name)
			return getInfo(name, false){//FilePipelineStoreTask.getInfo(String name, boolean checkExistence)
				    synchronized (lockCache.getLock(name)) {
					  if (checkExistence && !hasPipeline(name)) {
						throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
					  }
					  // 获取该SlaveSDC对应的Data目录,一般是在该容器所在机器本地的Container目录: nm-local-dir 下 containerId位置;
					  Path filePath = getInfoFile(name){
						return getPipelineDir(name).resolve(INFO_FILE);{
							getPipelineDir(name){
								return storeDir[UnixPath].resolve(PipelineUtils.escapedPipelineName(name));{
									// storeDir = /home/app/appdata/hadoop/dataDir/tmpDir/nm-local-dir/usercache/app/appcache/application_1585661170516_0006/container_1585661170516_0006_01_000002/data/pipelines
								}
							}
							.resolve(INFO_FILE);
						}
					  }
					  try (InputStream infoFile = Files.newInputStream(filePath)) {
						PipelineInfoJson pipelineInfoJsonBean = json.readValue(infoFile, PipelineInfoJson.class);
						return pipelineInfoJsonBean.getPipelineInfo();
					  } catch (Exception ex) {
						throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex);
					  }
					}
				
			}
		}
	}
	String title = name;
	
	// 
	switch (get) {
      case "pipeline":
        PipelineConfiguration pipeline = store.load(name, rev);{//SlavePipelineStoreTask.load(String name, String tagOrRev)
			return pipelineStore.load(name, tagOrRev);{//FilePipelineStoreTask.load(String name, String tagOrRev)
				synchronized (lockCache.getLock(name)) {
				  if (!hasPipeline(name)) {
					throw new PipelineStoreException(ContainerError.CONTAINER_0200, name);
				  }
				  // getPipelineFile()是读取pipeline.json, 而getInfo()->getInfoFile()是读取 info.json文件;
				  /*
				  * info.json : 	464B 字节; createed, lastModified, uuid, sdcId, metadate信息;
				  * pipeline.json:	15.2 KB:  其info字段及为 info.json的内容, 此外还包括几个大字段:
						- configuration: Pipeline相关配置: exeMode, deliveryGuraantee等;
						- stages:	各Stages的配置情况;
						- startEventStages + stopEventStages; 
						- issues;
				  */
				  Path pipelineFile = getPipelineFile(name);
				  try (InputStream pipelineFile = Files.newInputStream(getPipelineFile(name))) {
					PipelineInfo info = getInfo(name);
					// 
					PipelineConfigurationJson pipelineConfigBean=json.readValue(pipelineFile, PipelineConfigurationJson.class);
					PipelineConfiguration pipeline = pipelineConfigBean.getPipelineConfiguration();
					pipeline.setPipelineInfo(info);

					Map<String, Map> uiInfo;
					if (Files.exists(getPipelineUiInfoFile(name))) {
					  try (InputStream uiInfoFile = Files.newInputStream(getPipelineUiInfoFile(name))) {
						uiInfo = json.readValue(uiInfoFile, Map.class);
						pipeline = injectUiInfo(uiInfo, pipeline);
					  }
					}

					return pipeline;
				  }
				  catch (Exception ex) {
					throw new PipelineStoreException(ContainerError.CONTAINER_0206, name, ex.toString(), ex);
				  }
				}
				
			}
		}
        PipelineConfigurationValidator validator = new PipelineConfigurationValidator(stageLibrary, name, pipeline);
        // 对于从本地加载到的Pipeline.json, 还要validate()校验一下;否则可能显示不了;
		pipeline = validator.validate();
        data = BeanHelper.wrapPipelineConfiguration(pipeline);// 用PipelineConfigurationJson(pipelineConfiguration)对象包装输出;
        title = pipeline.getTitle() != null ? pipeline.getTitle() : pipeline.getInfo().getPipelineId();
        break;
      case "info":
        data = BeanHelper.wrapPipelineInfo(store.getInfo(name));
        break;
      case "history":// 返回的 PipelineRevInfo 对象仅简单封装了 date,user,rev字段; 可能以后版本再实现pipelineConfigHistory?
        data = BeanHelper.wrapPipelineRevInfo(store.getHistory(name));
        break;
      default:
        throw new IllegalArgumentException(Utils.format("Invalid value for parameter 'get': {}", get));
    }

    if (attachment) {
      Map<String, Object> envelope = new HashMap<String, Object>();
      envelope.put("pipelineConfig", data);

      RuleDefinitions ruleDefinitions = store.retrieveRules(name, rev);
      envelope.put("pipelineRules", BeanHelper.wrapRuleDefinitions(ruleDefinitions));

      return Response.ok().
          header("Content-Disposition", "attachment; filename=\"" + title + ".json\"").
          type(MediaType.APPLICATION_JSON).entity(envelope).build();
    } else {
      return Response.ok().type(MediaType.APPLICATION_JSON).entity(data).build();
    }

}



/home/app/appdata/hadoop/dataDir/tmpDir/nm-local-dir/usercache/app/appcache/application_1585661170516_0006/container_1585661170516_0006_01_000002/data/pipelines

container_1585661170516_0006_01_000002/data/pipelines

昨天:
* 主要上午  捞日志到本地
* 2个线上问题: YingQi StartError, Pipeline.josn显示异常;

今天: pipeline.json 异常问题; 在集群是好的, 在pod机器有有问题了, 看起来是








