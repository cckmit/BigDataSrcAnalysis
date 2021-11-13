1. SDC容器进程启动: BootstrapMain.main()

	libexec/_sdc 启动脚本
	exec ${JAVA} -classpath ${BOOTSTRAP_CLASSPATH} ${SDC_JAVA_OPTS} com.streamsets.pipeline.BootstrapMain \
		   -mainClass ${SDC_MAIN_CLASS} -apiClasspath "${API_CLASSPATH}" -containerClasspath "${CONTAINER_CLASSPATH}" \
		   -streamsetsLibrariesDir "${STREAMSETS_LIBRARIES_DIR}" -userLibrariesDir "${USER_LIBRARIES_DIR}" -configDir ${SDC_CONF} \
		   -libsCommonLibDir "${LIBS_COMMON_LIB_DIR}" ${EXTRA_OPTIONS}

	
BootstrapMain.main() -> doMain(){
	Class klass = containerCL.loadClass(mainClass); // DataCollectorMain
	
	klass.getMethod("setContext", apiCL,containerCL,moduleCLs){// DataCollectorMain.setContext(){
		MemoryUsageCollector.initialize(instrumentation);
		RuntimeModule.setStageLibraryClassLoaders(moduleCLs);
	}
	
	method = klass.getMethod("main", String[].class);
	method.invoke(null, new Object[]{new String[]{}}){ // DataCollectorMain.main(args){
		BootstrapMainMain extands Main doMain(){
			
			int exitStatus = new DataCollectorMain(MainStandalonePipelineManagerModule).[Main]doMain(){
				log.info("Starting ...");
					* INFO  Main - Starting ...
	
				PrivilegedExceptionAction<Void> action = () -> {
					
					// 核心启动A: 进行初始化,主要是StageLib的加载 和 CachePipeline的加载;
					task[TaskWrapper].init(){
						task[PipelineTask extends CompositeTask extands AbstractTask].init() -> AbstractTask.init(){
							Preconditions.checkState(VALID_TRANSITIONS.get(getStatus()).contains(Status.INITIALIZED),"errorMessage");
							setStatus(Status.INITIALIZED);
							
							CompositeTask.initTask(){// 抽象类,由CompositeTask统一实现,包括runTask,stopTask;
								for (initedTaskIndex = 0; initedTaskIndex < subTasks.size(); initedTaskIndex++){
									/** 8个Task:
									1.	* ClassLoaderStageLibraryTask.init();
										* LineagePublisherTaskImpl.init()
										* CredentialStoresTaskImpl.init()
									2.	* CachePipelineStoreTask.init()
										* DataCollectorWebServerTask.init()
										** StandaloneAndClusterPipelineManager.init()
										* NoOpEventHandlerTask.init()
										* SupportBundleManager.init()
									*/
									subTasks[List<Task>].get(initedTaskIndex).init();
								}
								
								//1. 重要初始化: 加载类并校验版本
								ClassLoaderStageLibraryTask[extands AbstractTask].init(){ // 实际执行 AbstractTask.init(){}
									setStatus(Status.INITIALIZED);
									initTask();// 抽象类,ClassLoaderStageLibraryTask.initTask(){
										// 根据 streamsets-libs 和 user-libs 两个lib库,加载两种库所有的 sdcClassLoader;
										stageClassLoaders:List<SDCClassLoader> = runtimeInfo.getStageLibraryClassLoaders();
										
										// 当stagelibs.classpath.validation.enable=true时,进行classpath校验:
										if(configuration.get(CONFIG_CP_VALIDATION, DEFAULT_CP_VALIDATION)) {
											validateStageClasspaths(){
												LOG.info("Validating classpath of all stages");
													* INFO  ClassLoaderStageLibraryTask - Validating classpath of all stages
												
												
												List<ClasspathValidatorResult> validatorResults = validateStageLibClasspath(){
													for (ClassLoader cl : stageClassLoaders) {
														ClasspathValidatorResult validationResult = ClasspathValidator.newValidator(sdcCl.getName()).withURLs(sdcCl.getURLs())
															.validate(loadClasspathWhitelist(cl)){
																// 一个URL 对应一个 .jar包
																for(URL url : urls) {
																	Dependency dependency = DependencyParser.parseURL(url).get();
																	
																	dependecies[ Map<String, Map<String, List<Dependency>>>]
																	.computeIfAbsent(dependency.getName(), (i) -> new HashMap<>())
																	.computeIfAbsent(dependency.getVersion(), (i) -> new LinkedList<>())
																	.add(dependency);
																}
																
																if(entry.getValue().size() > 1) { // 当一个 依赖,包含了两个或以上的版本jar, 则为 非法/无效;
																	resultBuilder.addVersionCollision(entry.getKey(), entry.getValue());
																}
															}
														
														validatorResults.add(validationResult)
													}
												}

												// 当有classpath 非法/无效 时:
												if (corruptedClasspathStages.isEmpty()) {
													LOG.info("Classpath of all stages passed validation");
														* 
												}else {
													LOG.error("The following stages have invalid classpath: {}", StringUtils.join(corruptedClasspathStages, ", "));
														* ERROR ClassLoaderStageLibraryTask - The following stages have invalid classpath: streaming-calculator-stage, streamsets-datacollector-basic-lib, streamsets-datacollector-edh-cluster-kafka_0_10-spark_2_0-lib, streamsets-datacollector-edh_2_7-lib, JavaScriptEvaluator
													
													// 当"stagelibs.classpath.validation.terminate"=true时, 报错并终止启动
													boolean canTerminate = configuration.get(CONFIG_CP_VALIDATION_RESULT,
													if (canTerminate) { throw new RuntimeException("Invalid classpath detected for" , "));}
												}
												
												
												
												
											}
										}
										
									}
										
										// 
										loadStages(){
											for (ClassLoader cl : stageClassLoaders) {
												// 校验jvm版本是否支持
												String unsupportedJvmVersion = getPropertyFromLibraryProperties(cl, JAVA_UNSUPPORTED_REGEXP, null);
												// 从Library中 加载Stage的定义:  StageLibraryDefinition
												StageLibraryDefinition libDef = StageLibraryDefinitionExtractor.get().extract(cl);
												
												// 加载 什么Stage?
												for(Class klass : loadClassesFromResource(libDef, cl, STAGES_DEFINITION_RESOURCE)) {
													StageDefinition stage = StageDefinitionExtractor.get().extract(libDef, klass, Utils.formatL("Library='{}'", libDef.getName()));
													tmpStageMap.put(key, stage);
												}
												
												// Load Lineage publishers: 加载 LineagePublisherDefinition 的类, 一般没有;
												for(Class klass : loadClassesFromResource(libDef, cl, LINEAGE_PUBLISHERS_DEFINITION_RESOURCE)) {
													LineagePublisherDefinition lineage = LineagePublisherDefinitionExtractor.get().extract(libDef, klass);
													tmpLineagePublisherDefinitionMap.put(key, lineage);
												}
												
												// Load Credential stores: 根据CredentialStores.json 来判断是否:CredentialStoreDefinition,并加载
												for(Class klass : loadClassesFromResource(libDef, cl, CREDENTIAL_STORE_DEFINITION_RESOURCE)) {
													CredentialStoreDefinition def = CredentialStoreDefinitionExtractor.get().extract(libDef, klass);
													tmpCredentialStoreDefinitions.add(def);
												}
												
												// Load Services, 根据Services.json, 判断是否 ServiceDefinition
												for(Class klass : loadClassesFromResource(libDef, cl, SERVICE_DEFINITION_RESOURCE)) {
													ServiceDefinition def = ServiceDefinitionExtractor.get().extract(libDef, klass);
													tmpServiceMap.put(def.getProvides(), def);
												}
												
												LOG.info("Loaded '{}' libraries with a total of '{}' stages, '{}' lineage publishers, '{}' services and '{}' credentialStores in '{}ms'");
												* INFO  ClassLoaderStageLibraryTask - Loaded '19' libraries with a total of '172' stages, '0' lineage publishers, '2' services and '3' credentialStores in '1214022ms'
												
											}
										}
								
								}
								
								// 2. 重要初始之 FileCacheStoreTask从本地加载pipeline配置
								CachePipelineStoreTask[extands AbstractTask].init(){
									pipelineStore[FilePipelineStoreTask extands AbstractTask].init() ->  AbstractTask.init(){
										setStatus(Status.INITIALIZED);
										FilePipelineStoreTask.initTask(){
											// 获取 data/pipelines 目录;
											storeDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
											
											if (pipelineStateStore != null) {
												pipelineStateStore[CachePipelineStateStore].init(){ // pipelineStateStore 在外是CachePipelineStateStore, 在其里面,是FilePipelineStateStore
													
													// 缓存Pipeline状态的 GoogleCacheLoader
													LoadingCache<String, PipelineState> pipelineStateCache = CacheBuilder.newBuilder()
														.maximumSize(configuration.get("store.pipeline.state.cache.maximum.size", 100))
														.expireAfterAccess(configuration.get("store.pipeline.state.cache.expire.after.access", 10),TimeUnit.MINUTES)
														.build(new CacheLoader<String, PipelineState>() {
															public PipelineState load(string nameAndRev){
																PipelineState state= pipelineStateStore.getState(getName(nameAndRev), getRev(nameAndRev)){
																	loadState(getNameAndRevString(name, rev)){
																		DataStore ds = new DataStore(getPipelineStateFile(name, rev)){
																			return new File(PipelineDirectoryUtil.getPipelineDir(runtimeInfo, name, rev), STATE_FILE="pipelineState.json");
																		}
																		try (InputStream is = ds.getInputStream()) {
																			PipelineStateJson pipelineStatusJsonBean = ObjectMapperFactory.get().readValue(is, PipelineStateJson.class);
																			pipelineState = pipelineStatusJsonBean.getPipelineState();
																		}
																	}
																	
																	return state;
																}
																
																
															}
														});
													
													pipelineStateStore[FilePipelineStateStore].init(){};//空实现
												}
											}
											
										}
									}
									
									// 将加载的Pipeline信息按 pipelineId,存入缓存
									for (PipelineInfo info: pipelineStore.getPipelines()) {
										pipelineInfoMap[ Map<String,PipelineInfo>].put(info.getPipelineId(), info);
									}
									
								}
								
								// 3. 初始化之: WebServere
								DataCollectorWebServerTask[extands AbstractTask].init(){
									WebServereTask.initTask(){
										Server server = createServer();
										sessionHandler.setMaxInactiveInterval(conf.get("http.session.max.inactive.interval",86400));
										addToPostStart();
									}
								}
								
								// 4. 初始化之: SupportBoundleManger
								SupportBundleManager.init() -> SupportBundleManager.initTask(){
									while((className = reader.readLine()) != null) {
										BundleContentGeneratorDef def = bundleClass.getAnnotation(BundleContentGeneratorDef.class);
										builder.add(new BundleContentGeneratorDefinition(bundleClass,def.description());
									}
									redactor = StringRedactor.createFromJsonFile(runtimeInfo.getConfigDir() + "/" + Constants.REDACTOR_CONFIG);
								}
							
						}
					}
					* init()结束INFO标记: 
						- INFO  ClassLoaderStageLibraryTask - Loaded '19' libraries with a total of '172' stages, '0' lineage publishers, '2' services and '3' credentialStores in '24126ms'
						- INFO  LineagePublisherTaskImpl - No publishers configured
					
					
					// 核心启动B: 运行PipelineTask
					task[TaskWrapper].run(){
						task[PipelineTask extends CompositeTask extands AbstractTask].run() -> AbstractTask.run(){
							Preconditions.checkState(VALID_TRANSITIONS.get(getStatus()).contains(Status.RUNNING),"errorMessage");
							setStatus(Status.RUNNING);
							
							// 依次执行8大 TaskRun,主要是 CacheTask, WebServer,PipelineManager 3个任务的运行;
							CompositeTask.runTask(){ // 抽象类, 由CompositeTask.initTask()实现
								/** 8个Task:
										* ClassLoaderStageLibraryTask.run();
										* LineagePublisherTaskImpl.run()
										* CredentialStoresTaskImpl.run()
										* CachePipelineStoreTask.run()
									1.	* DataCollectorWebServerTask.run()
									2.	* StandaloneAndClusterPipelineManager.run()
										* NoOpEventHandlerTask.run()
										* SupportBundleManager.run()
								*/
								for(Task subTask: subTasks){
									subTask.run();
								}
								
								
								/** 实时检查DataCollect八大subTasks是否为Runing状态;
								* 	- 每50毫秒(0.05秒), 校验一遍8大 subTasks的还是不是Runing状态; 整个DC容器正常运行的前提是其8个subTask全部都处于Running状态;
										* 注意, 此8个Task都Running, 包括PipelineManager的Running, 但各Pipeline任务流可以都是非Running状态,包括Edited,Stoped,DisConnected;
									- 当有任意一个 subTask(如CacheTask? ClassLoaderTask ?)停止了, 该怎么处理? 处理原则看 PipelineTask.Status的状态了?
										* 当整个DC的状态已调整为非Running 如Stoped时:  代表由父到子的正常 DC管理器关闭;
											- 代表整个DC管理器要停止了,一般是正在停止状态, 则不需要操作,让其继续运行即可;
										* 当整个DC的状态还是Running状态: 代表由子组件异常导致的非正常停运;
											- 此时子组件停运了, 而父DataCollector还在Running, 需要调用Pipeline.safeStop(),将整个DC容器和各组件依次销毁;
								*/
								if (monitorSubTasksStatus) { // 默认为 true
									 monitorThread = new Thread(){public void run() {
										// 每隔 50秒,循环检查:
										while (getStatus() == Status.RUNNING) {
											for (Task subTask : subTasks) {
												if (subTask.getStatus() != Status.RUNNING) {
													if (getStatus() == Status.RUNNING) {
														LOG.warn("'{}' status monitor thread detected that subTask '{}' is not running anymore, stopping",getName(), subTask.getName());
															* 
														CompositeTask.this.stopTask();// 停止8大Task;
														CompositeTask.this.stop(); // safeStop()-> 修改状态,并调用stopTask();
														
													}
												}
											}
											
											// 等待50毫秒启动,能保证所有的执行完嘛?
											ThreadUtil.sleep(50);
										}
									 }};
									
									monitorThread.start();// 启动 50毫秒 循环间隔保证8个Task都是Run状态;
								}
								
								foreachTasks(){//foreach subTasks的各具体实现
									
									// 1. DataCollectorWebServerTask.run()
									DataCollectorWebServerTask[extands AbstractTask].run(){ // AbstractTask.run(){ 抽象方法:runTask()}
										WebServerTask.runTask(){
											WebServerTask.runTaskInternal(){
												for (ContextConfigurator cc : contextConfigurators) {
													// 22个 ContextConfigurator中 一两个重新了start()方法,没重新的ContextConfigurator.start()为空
													cc[WebServerModule].start(){
														JmxReporter reporter = JmxReporter.forRegistry(metrics).build();
														reporter.start(){
															registry[MetricRegistry].addListener(listener:JmxListener);
														}
													}
												}
												# 启动Web服务, 耗时较长
												server[Server].start(){}
													- DEBUG ModelResolver - resolveProperty [simple type, class java.lang.String]
													- DEBUG Reader - trying to decorate operation: io.swagger.jersey.SwaggerJersey2Jaxrs@29c76d75
												
												runtimeInfo.setBaseHttpUrl(baseHttpUrl += ":" + port);
												LOG.info("Running on URI : '{}'", getHttpUrl());
													* INFO  WebServerTask - Running on URI : 'http://ldsver41:18630'
												
												postStart(){
													for (Runnable runnable : postStartRunnables) {
														runnable.run(){ DataCollectorWebServerTask.init().addToPostStart();}
													}
												}
											}
											
											WebServerAgentCondition.waitForCredentials();
										}
									}
									
									
									// 2. StandaloneAndClusterPipelineManager.run()
									StandaloneAndClusterPipelineManager[extands AbstractTask].run(){
										Preconditions.checkState(VALID_TRANSITIONS.get(getStatus()).contains(Status.RUNNING),"errorMessage");
										setStatus(Status.RUNNING);
										
										StandaloneAndClusterPipelineManager.runTask(){
											previewerCache = new MetricsCache<>(MetricRegistry,name, Cache);
											runnerCache = new MetricsCache<>();
											
											// HA,依赖于ZK的高可用模式, 默认不开启, 
											// 当不开启HA ,则会在启动 DC管理器时就恢复所有 isActive(激活) 状态的pipelines ;
											//  ? 为什么HA-ZK时, 要手动恢复? 
											boolean haModeEnable = configuration.get("sdc.server.ha.mode.enable", false);
											if(! haModeEnable){
												recoverPipelineStatus(){
													pipelineInfoList = pipelineStore.getPipelines();
													for (PipelineInfo pipelineInfo : pipelineInfoList) {
														PipelineState pipelineState = pipelineStateStore.getState(name, rev);
														if (pipelineState.getStatus().isActive()) {
															Runner runner = getRunner(name, rev, executionMode);
															GroupsInScope.executeIgnoreGroups(() -> {runner.onDataCollectorStart(user);});
														}
													}
												}
											}
											
											
											/** 定时(默认每个小时)检查 PipelineManager中的各Pipline(PipelineRunner)运行状态,是否还 isActive在线状态
													* 非active时:  [Start/Run/Stop]__Error, [Finish/Killed/Stop]ed; 时:
											*	- 当不在线时: 从PipelineManager.runnerCache内存中删除该pipeline数据, 并关闭该Pipeline(及PipelineRunner);
											* 	- 当StateStore中缓存的pipelineState还是 active时, 仍其继续运行,不做关闭;
											
											* 	半个小时后开始检查, 每个小时检查一次;
											*/
											
											Runnable checkStateRun = new Runnable(){
												@Override public void run() {
													for (RunnerInfo runnerInfo : runnerCache.asMap().values()) {
														Runner runner = runnerInfo.runner;
														LOG.debug("Runner for pipeline '{}::{}' is in status: '{}'",  runner.getState());
														removeRunnerIfNotActive(runner){
															boolean isActive= runner.getState(){
																return pipelineStateStore.getState(name, rev);
															}.getStatus().isActive()
															
															// 当该Pipeline不在线时,则从 PipelineManager.runnerCache中清除该pipeline数据,并关闭该Pipeline(及PipelineRunner);
															if (! isActive) {
																// 移除pipelineMgr内中该pipelineRunner的缓存信息;
																runnerCache[Cache<String, RunnerInfo>].invalidate(getNameAndRevString(runner.getName(), runner.getRev()));
																runner.close();//ProductionPipelineRunner?
															}
														}
													}
												}
											}
											runnerExpiryInitialDelay=configuration.get("runner.expiry.initial.delay", 30*60*1000[半小时]);
											runnerExpiryInterval=this.configuration.get("runner.expiry.interval", 60*60*1000 [1小时]);
											runnerExpiryFuture =  managerExecutor.scheduleAtFixedRate(checkStateRun,runnerExpiryInitialDelay,runnerExpiryInterval,TimeUnit.MILLISECONDS);
										}
									}
								}
							}
						}
					}
					* 	run()结束INFO标记: 
						- INFO  WebServerTask - Running on URI : 'http://ldsver41:18630'
						
					
					// 核心启动C:  若开启,进行zk的leader选举;
					haManager[ZookeeperBasedHAManager].init(){
						boolean haManager = configuration.get("sdc.server.ha.mode.enable", false);
						if(haManager){
							selector[ZKLeaderSelector].init(){
								leaderSelector[LoaderSelector].start();// 选择ZK Leader{
									
									Preconditions.checkState(!executorService.isShutdown(), "Already started");
									Preconditions.checkState(!hasLeadership, "Already has leadership");
									client[CuratorFrameworkImpl].getConnectionStateListenable().addListener(listener);
									
									requeue();
									
								}
							}
						}
					}
					
					// 核心启动D: 
					task[TaskWrapper].waitWhileRunning(){
						task[PipelineTask extands CompositeTask].waitWhileRunning(){ //CompositeTask.waitWhileRunning()
							for (Task subTask : subTasks) {
								subTask[AbstractTask].waitWhileRunning(){ // AbstractTask.waitWhileRunning()
									Preconditions.checkState(getStatus()==Status.RUNNING ||getStatus()==Status.STOPPED,"msg");
									if (getStatus() == Status.RUNNING) {
										latch[CountDownLatch].await();
									}
								}
							}
						}
					}
					
					scheduledExecutorService.shutdown();
					
					
				}
				
				// 此处进入 action:PrivilegedExceptionAction 代码开始执行
				SecurityUtil.doAs(securityContext.getSubject(), action);
				
				return shutdownStatus.getExitStatus();
				
			}
			
			System.exit(exitStatus);
		}
		
	}
}

	

	


2. 容器的停止 Main.doMain() -> shutdownHookThread = new Thread{ run(){ task[TaskWrapper].stop(){}}.start();


AdminResource.shutdown(){
	Thread thread = new Thread("Shutdown Request") {
		@Override public void run() {
			// sleeping  500ms to allow the HTTP response to go back: 等待0.5秒以返回http.OK;
			ThreadUtil.sleep(500);
			runtimeInfo[RuntimeInfo].shutdown(0){
				
				Main.doMain(){
					Thread shutdownHookThread = new Thread("Main.shutdownHook") {
						@Override public void run() {
							task[TaskWrapper[PipelineTask]].stop();
						}
					}
					getRuntime().addShutdownHook(shutdownHookThread);
					ShutdownHandler runnable = new ShutdownHandler(finalLog, task, shutdownStatus);
					
				}
				// 关于shutdownRunnable:ShutdownHandler, 详见以上代码
				if(shutdownRunnable !=null){
					shutdownRunnable[ShutdownHandler].run(){
						task[TaskWrapper[PipelineTask]].stop(){ // PipelineTask.stop()-> AbstractTask.stop()
							Preconditions.checkState(VALID_TRANSITIONS.get(getStatus()).contains(Status.STOPPED),"errorMessage");
							
							if (getStatus() != Status.STOPPED) {
								safeStop(Status.STOPPED){// AbstractTask.safeStop(){
									setStatus(endStatus = Status.STOPPED);
									stopTask(){ // 抽象方法,由 CompositeTask.stopTask()实现; 
										CompositeTask.stopTask(){
											// 遍历停止8大 Task; 重点是: Cache, PipelineMgr;
											for (initedTaskIndex--; initedTaskIndex >= 0; initedTaskIndex--) {
												
												subTasks[List<Task>].get(initedTaskIndex).stop(){
													AbstractTask.stop(){
														Preconditions.checkState(VALID_TRANSITIONS.get(getStatus()).contains(Status.STOPPED),"errorMessage");
														if(getStatus() != Status.STOPPED) {
															safeStop(Status.STOPPED){
																Status priorStatus = getStatus(){return status;};
																stopTask();// AbstractTask.stopTask() 为空代码;
																	/*  以上Task重写了 stopTask()方法;
																		- StandaloneAndClusterPipelineManager.stopTask()
																		- DataCollectorWebServerTask.stopTask()
																		- CachePipelineStoreTask.stopTask()
																		- ClassLoaderStageLibraryTask.stopTask()
																	*/	
															}
														}
													}
												}
											}
											
											//1. 重要stop步骤: PipelineMgr 
											StandaloneAndClusterPipelineManager.stop(){ 
												AbstractTask.stop() -> AbstractTask.safeStop() -> StandaloneAndClusterPipelineManager.stopTask(){
													// Cache<String, RunnerInfo> runnerCache = new MetricsCache<>();
													// 遍历每条Pipeline的Runner(StandaloneRunner/ClusterRunner), 执行其close(),onDataCollectorStop()方法
													// 依次保存和停止每天Pipeline任务流;
													for (RunnerInfo runnerInfo : runnerCache.asMap().values()) {
														/**看Pipeline是那种executionMode:[Standalone,ClusterOnYarn,ClusterOnMesos,ClusterBatch]
															* 若 Standalone, runner= StandaloneRunner 
															* 若 ClusterYarnStreaming, runner= ClusterRunner 
															* 若 ClusterMesosStreaming, runner= ClusterRunner 
															* Slave?
														*/
														Runner runner[AsyncRunner] = runnerInfo.runner;
														runner.close(){
															runner.close(){
																ClusterRunner.close(){boolean isClosed = true;} // 若集群模式
																StandaloneRunner.close(){boolean isClosed = true;} // 若Standalone模式;
															}
														}
														
														PipelineState pipelineState = pipelineStateStore.getState(runner.getName(), runner.getRev());
														
														runner[AsyncRunner].onDataCollectorStop(user){
															
															// 1. 若为ClusterRunner,
															runner[ClusterRunner].onDataCollectorStop(user){
																ClusterRunner.stopPipeline(user,isNodeShuttingDown=true){
																	if (isNodeShuttingDown) {
																		if (getState().getStatus() == PipelineStatus.RETRY) {
																			retryFuture.cancel(true);
																		}
																			
																		validateAndSetStateTransition[ClusterRunner](user, PipelineStatus.DISCONNECTED,message){
																			
																			// 状态检验 重要方法: ClusterRunner.validateAndSetStateTransition()
																			validateAndSetStateTransition[ClusterRunner](user, toStatus, message, attributes){
																				PipelineState fromState = getState();
																				
																				if (fromState.getStatus() == toStatus && toStatus != PipelineStatus.STARTING) {
																					// 当 fromStatus==toStatus,即不需要改变状态时, 仅打印日志Debug;
																					LOG.debug(Utils.format("Ignoring status '{}' as this is same as current status", fromState.getStatus()));
																				}else{
																					synchronized (this) {
																						fromState = getState();
																						checkState(Map.get(fromState).contains(toStatus), "errorMessage");
																						
																						// A. 当要切换成 Run_Error时, 情况SlaveMgr.slaveList中父节点信息;
																						if (toStatus == PipelineStatus.RUN_ERROR) {
																							handleErrorCallbackFromSlaves(attributes){
																								slaveCallbackManager.clearSlaveList(CallbackObjectType.ERROR);
																							}
																						}
																						
																						// B.当要切换成 Run_Error并需要重启时: 将状态设为 PipelineStatus.RETRY;
																						if (toStatus == PipelineStatus.RUN_ERROR && shouldRetry){
																							toStatus = PipelineStatus.RETRY;
																							checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(PipelineStatus.RETRY), "");
																						}
																						
																						// 
																						if (toStatus==PipelineStatus.RETRY &&fromState.getStatus()!=PipelineStatus.CONNECTING) {
																							retryAttempt = fromState.getRetryAttempt() + 1;
																						}else if (!toStatus.isActive()) {
																							retryAttempt = 0;
																							nextRetryTimeStamp = 0;
																						}
																						
																						// 当非Active状态,或DISCONNECTED时: Edited,StartError,RunError,Finished,Killed,Stoped, StopError,DissConnect时;
																						// 注意, ConnectError, DisConnedted ,都算是active在线状态;
																						if (!toStatus.isActive()||toStatus==PipelineStatus.DISCONNECTED){
																							// 将Metricy转换成JsonStr用作 FilePipelineStateCache 持久化 ;
																							metricsJSONStr= metricsJSONStr = objectMapper.writer().writeValueAsString(getMetrics());
																						}
																						
																						pipelineState= pipelineStateStore[CachePipelineState].saveState(toStatus,metricsJSONStr){
																							PipelineState pipelineState = pipelineStateStore[FilePipelineStateCache].saveState(){
																								FilePipelineStateCache.register(name, rev);
																								
																								// 将Pipeline信息和状态 持久化(写入磁盘)
																								PipelineState pipelineState = new PipelineStateImpl(user, name, status, message);
																								FilePipelineStateCache.persistPipelineState(pipelineState){
																									PipelineStateJson pipelineStateJson = BeanHelper.wrapPipelineState(pipelineState);
																									pipelineString = ObjectMapperFactory.get().writeValueAsString(pipelineStateJson);
																									
																									// dataStore会在 data/runtimeInfo/pipelineId/0/ 目前下, 对 PipelineState.json, PipelineStateHistory.json 进行更新
																									DataStore dataStore = new DataStore(getPipelineStateFile(pipelineState.getPipelineId(), pipelineState.getRev()));
																									try (OutputStream os = dataStore.getOutputStream()) {
																										os.write(pipelineString.getBytes());
																										dataStore.commit(os);
																									}
																								}
																								
																							}
																							
																							// 将持久化的pipeline最新状态,保存到缓存;
																							pipelineStateCache.put(getNameAndRevString(name, rev), pipelineState);
																							return pipelineState;
																						}
																						
																						if (toStatus == PipelineStatus.RETRY) {
																							retryFuture = scheduleForRetries(user, runnerExecutor);
																						}
																						
																					}
																					
																					
																				}
																				
																			}
																		}
																		
																	}
																}
															}
															
															
															// 2. 若为StandaloneRunner,
															runner[StandaloneRunner extends AbstractRunner[implements Runner]].onDataCollectorStart(user){
																
																// 当还处重试状态时, 取消取消重启,停止任务运行;
																if (getState().getStatus() == PipelineStatus.RETRY){
																	retryFuture.cancel(true);
																	// 校验从当前状态(Retry)->DISCONNECTING的合法性, 并将 stateStore中缓存的状态更新和持久化为: DisConnecting;
																	validateAndSetStateTransition(user, PipelineStatus.DISCONNECTING, null, null);
																	// 校验DisConnecting -> DisConnedted,并更新缓存和磁盘状态为: DisConnedted
																	validateAndSetStateTransition(user, PipelineStatus.DISCONNECTED, "Disconnected as SDC is shutting down", null){
																		/* 校验状态 切换是否合法, 如何合法,则更新内存(pipelineStateStore)和磁盘中的状态;
																		*	- 
																		*/
																		Standalone.validateAndSetStateTransition(user,toStatus,message,attrs){
																			synchronized (this) {
																				fromState = getState();
																				checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(toStatus),"");
																				
																				if (toStatus==PipelineStatus.RETRY&&fromState.getStatus()!=PipelineStatus.CONNECTING) {
																					
																				}else if (!toStatus.isActive()) {
																					retryAttempt = 0;
																					nextRetryTimeStamp = 0;
																				}
																				
																				// 当 非active时 or DISCONNECTED or 重试状态 状态时,
																				if (!toStatus.isActive() || toStatus == PipelineStatus.DISCONNECTED 
																							|| (toStatus == PipelineStatus.RETRY && fromState.getStatus() != PipelineStatus.CONNECTING)) {
																						
																						metricString = objectMapper.writeValueAsString(metrics);
																						eventListenerManager.broadcastMetrics(name, metricString);
																				}
																				
																				pipelineState =pipelineStateStore.saveState(user, name, rev, toStatus, message);
																				
																			}
																			
																			eventListenerManager.broadcastStateChange(fromState,pipelineState,OffsetFileUtil.getOffsets(runtimeInfo, name, rev)){
																				if(stateEventListenerList.size() > 0) {
																					String toStateJson = objectMapper.writer().writeValueAsString(BeanHelper.wrapPipelineState(toState, true));
																					for(StateEventListener stateEventListener : stateEventListenerListCopy) {
																						stateEventListener.onStateChange(fromState, toState, toStateJson, threadUsage, offset);
																					}
																				}
																			}
																		}
																		
																	}
																}
																
																// 当 状态已经变为 非在线时: [Start/Run/Stop]__Error, [Finish/Killed/Stop]ed, +  DisConnected时, 不在线了,就知返回结束; 否则,代表还没停完
																if (!getState().getStatus().isActive()||getState().getStatus()==PipelineStatus.DISCONNECTED) {
																	LOG.info("Pipeline '{}'::'{}' is no longer active", name, rev);
																	return;
																}
																
																// 代码若进入此处, 表示还没停止完, 还处于: [Start/Run/Stop]_ing(or ingError), Retry, Connecting + DisConnecting 状态; 需要强行关闭?
																LOG.info("Stopping pipeline {}::{}", name, rev);
																	* 
																
																try {
																	validateAndSetStateTransition(user, PipelineStatus.DISCONNECTING, "");
																} catch (PipelineRunnerException ex) {
																	LOG.warn("Cannot transition to PipelineStatus.DISCONNECTING: {}", ex.toString(), ex);
																}
																
																// 结束整个进程 ? 
																stopPipeline(true /* shutting down node process */){
																	ProductionPipelineRunnable pipelineRunnable;//里面封装了ProductionPipeline 
																	if (pipelineRunnable != null && !pipelineRunnable.isStopped()) {
																		LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
																			* 
																		
																		pipelineRunnable.stop(sdcShutting);
																		pipelineRunnable = null;
																	}
																	if (metricsEventRunnable != null) {
																		metricsEventRunnable.onStopPipeline();
																		metricsEventRunnable = null;
																	}
																	if (threadHealthReporter != null) {
																		threadHealthReporter.destroy();
																		threadHealthReporter=null;
																	}
																}
																
															}
															
														}
													}
														* INFO  StandaloneRunner - Pipeline 'HJQKafkaStandaloneTestRemoteDebuge97c188a-eb98-426a-adf4-eec4b7cf527b'::'0' is no longer active
														* INFO  StandaloneRunner - Pipeline 'HeJiaQingAllProcessTestv003by08280f1dcb20-738f-4f00-9396-c8c32ea2b382'::'0' is no longer active
													
													// 清空存储PipelineRunner信息的Cache
													runnerCache[Cache<String, RunnerInfo>].invalidateAll();
													
													for (Previewer previewer : previewerCache.asMap().values()){
														previewer.stop();
													}
													previewerCache.invalidateAll();// 清除previewerCache
													
													runnerExpiryFuture.cancel(true);
													LOG.info("Stopped Production Pipeline Manager");
														* INFO  StandaloneAndClusterPipelineManager - Stopped Production Pipeline Manager
													
												}
											}
											
											// 2. 关闭WebServer
											DataCollectorWebServerTask.stop(){
												AbstractTask.stop() -> AbstractTask.safeStop() -> WebServerTask.stopTask(){
													server[Server].stop(){
														doStop();
													}
												}
											}
																				
											// 3. 重要stop步骤: 清空CachePipelineStoreTask和FilePipelineStateStore中的缓存的 pipelineInfos数据;
											CachePipelineStoreTask.stop(){
												pipelineStore[FilePipelineStoreTask].stop(){
													AbstractTask.stop() -> AbstractTask.safeStop() -> FilePipelineStoreTask.stopTask(){
														if (pipelineStateStore[CachePipelineStateStore] != null) {
															pipelineStateStore[CachePipelineStateStore].destroy(){
																// 清除所有缓存的pipelineInfo信息,缓存清空;
																pipelineStateCache[LocalCache<String, PipelineInfo>].invalidateAll();
																
																pipelineStateStore[FilePipelineStateStore].destroy(){}; // FilePipelineStateStore.destroy()为空;
															}
														}
													}
												}
												
												// 清空CachePipelineStoreTask中缓存的 pipelineInfo数据;
												pipelineInfoMap[Map<pipelineId,PipelineInfo>].clear();
											}
											
											
											ClassLoaderStageLibraryTask.stop(){
												AbstractTask.stop() -> AbstractTask.safeStop() -> ClassLoaderStageLibraryTask.stopTask(){
													privateClassLoaderPool[GenericKeyObjectPool].close();//情况cl池中的对象;
													super.stopTask();
												}
											}
											
										}
									}
								}
							}
						
						}
					}
				}
				
				// 备注: RuntimeInfo.shutdownRunnable = Main.doMain()
			}
		}
	};
	
	thread.start();
	return Response.ok().build();
}


AdminResource.restart(){}


2. Web响应的处理逻辑和源码
AclResource.
	

2. Standalone模式,start某pipeline: 		StandaloneRunner.startPipeline();




3. Standalone模式,stop某pipeline; 		StandaloneRunner.stopPipeline();



4. ClusterOnYarn模式, start某pipeline:	ClusterRunner.startPipeline();

5. ClusterOnYarn模式, stop某pipeline:	


6. Preview 运行某条Pipeline;


