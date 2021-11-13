rest/v1/pipeline/{pipelineId}/start 		GET		





DC运行时,Pipeline的 start运行

1.1 DC启动时,未开启HA, Pipeline的加载和恢复Run;

Main.doMain()->CompositeTask().runTask()-> StandaloneAndClusterPipelineManager.runTask():
	1. 当非haModeEnable时,直接recoverPipelineStatus()恢复流状态:
		-> 遍历所有PipelineInfo,并对isActive状态的流调用 getRunner(pipelineId,rev).onDataCollectorStart() 在DC上启动流任务:
			* 当为单机运行Pipeline时: StandaloneRunner.onDataCollectorStart() -> ProductionPipelineRunnable.run() -> ProductionPipeline.run():
				-> Pipeline.run() -> ProductionPipelineRunner.run():
					1. Push数据源: ProductionPipelineRunner.runPushSource(): while()循环处理 生产型(push)数据源的各Batch计算,直到结束
					
					2. Poll数据源: ProductionPipelineRunner.runPollSource(): while()循环处理 消费型(poll)数据源的各Batch计算,直到结束:
						- 先processPipe(originPipe)数据源读取数据 StagePipe.process() -> Dsource.produce();
						- 再runSourceLessBatch()处理其他非Source的各算子:
							-> 在PipeRunner.executeBatch()中for循环遍历其他算子Pipe(StagePipe,ObserverPipe,MultiplexerPipe),并Pipe.process()-> DProcessor.process()/DTarget.writer();
			* 当为Cluster模式跑Pipeline时:
			
			
	2. 若开启了haMode,则暂不处理,等PipelineTask.run()执行完所有Task,再通过ZookeeperHABasedManger.takeLeaderShip()方法中触发manager.recoverPipelineStatus()的恢复动作;


Main.doMain(){
	
	PipelineTask.init(): FilePipelineStoreTask.initTask() 加载目录和Pipeline配置;
	PipelineTask.run():	 StandaloneAndClusterPipelineManager.run(): 检查Pipeline状态,并对要Run的调用 recoverPipelineStatus()启动;
	PipelineTask.run() -> CompositeTask.runTask() -> StandaloneAndClusterPipelineManager.run(){
		// 
		StandaloneAndClusterPipelineManager.run() -> runTask(){
			if(! haModeEnable){ // 当非HA时,直接恢复 active状态
				recoverPipelineStatus(){
					List<PipelineInfo> pipelineInfoList = pipelineStore.getPipelines();
					
					for (PipelineInfo pipelineInfo : pipelineInfoList) {
						if (pipelineState.getStatus().isActive()) {
							Runner runner = getRunner(name, rev, executionMode);
							GroupsInScope.executeIgnoreGroups(() -> {
								// 1. 当以 Standalone模式 运行流时;
								runner[StandaloneRunner].onDataCollectorStart(user){
									PipelineStatus status = getState().getStatus();
									switch (status) {
										case DISCONNECTED:
											retryOrStart(user){
												PipelineState pipelineState = getState();
												
												if (pipelineState.getRetryAttempt() == 0 || pipelineState.getStatus() == PipelineStatus.DISCONNECTED) {
													// 1. 启动前校验合法性(DisConnected -> CONNECTING,STARTING,RETRY )
													prepareForStart(user, runtimeParameters){
														checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(PipelineStatus.STARTING),"");
														validateAndSetStateTransition(user, PipelineStatus.STARTING, null, attributes);
													}
													
													// 当为StandaloneRunner时
													StandaloneRunner.start(user, runtimeParameters){
														
														// 启流1-准备流:  创建PipelineRunnable封装pipeline和ProductPipelineRunner, 并准备该pipeline运行相关环境;
														startPipeline(user, runtimeParameters){
															synchronized (this) {
																LOG.info("Starting pipeline {} {}", name, rev);
																	* StandaloneRunner - Starting pipeline CreatorDemo0111876c72-4cc7-4f51-9940-fef7421e6dfe 0
																UserContext runningUser = new UserContext(user);
																PipelineConfiguration pipelineConfiguration = getPipelineConf(name, rev);
																
																ProductionPipelineRunner runner = (ProductionPipelineRunner) objectGraph.get(PipelineRunner.class);
																prodPipeline = builder.build(runningUser,pipelineConfiguration,getState().getTimeStamp());
																
																ScheduledFuture<?> configLoaderFuture = runnerExecutor.scheduleWithFixedDelay()
																
																// 准备空batch的Runner
																if(pipelineConfigBean.executionMode.equals(ExecutionMode.STANDALONE) && (pipelineConfigBean.runnerIdleTIme > 0)) {
																	int idelTime= pipelineConfigBean.runnerIdleTIme
																	ProduceEmptyBatchesForIdleRunnersRunnable idleRunnersRunnable = new ProduceEmptyBatchesForIdleRunnersRunnable(runner,idelTime*1000);
																	// We'll schedule the task to run every 1/10 of the interval (really an arbitrary number)
																	// 真实一个随便定的数字: 
																	long period = idelTime / 10;
																	taskBuilder.add(runnerExecutor.scheduleWithFixedDelay(idleRunnersRunnable,period,period,TimeUnit.MILLISECONDS));
																}
																
																pipelineRunnable = new ProductionPipelineRunnable(threadHealthReporter, this, prodPipeline, name, rev, taskBuilder.build());															
																
															}
														}
														
														// 启流2-启动流: 通过ProductionPipeline.run()调用pipeline.init()和pipeline.run();
														if(!pipelineRunnable.isStopped()) {
															pipelineRunnable[ProductionPipelineRunnable].run(){
																try{
																	PipelineInfo info = pipeline[ProductionPipeline].getPipelineConf().getInfo();
																	// 运行ProductionPipeline.run(); 校验各Stage,先运行Source, 再run其他Stage
																	pipeline[ProductionPipeline].run(){
																		List<Issue> issues = getPipeline().init(true);
																		stateChanged(PipelineStatus.RUNNING, null, attributes);
																		
																		pipeline[Pipeline].run(){
																			runner[接口类ProductionRunner].run(originPipe, pipes, badRecordsHandler, statsAggregationHandler){
																				// A. 生产环境: 
																				ProductionPipelineRunner.run(){
																					this.runnerPool = new RunnerPool<>(pipes, pipeContext.getRuntimeStats(), runnersHistogram);
																					// 当Push (生产)型 数据源时
																					if (originPipe.getStage().getStage() instanceof PushSource) {
																						ProductionPipelineRunner.runPushSource(){
																							int batchSize = configuration.get("production.maxBatchSize", 1000);
																							// 先运行Source, 会一直阻塞在此,直到运行完;或stop;
																							originPipe[SourcePipe].process(offsetTracker[ProductionSourceOffsetTracker].getOffsets(), batchSize, this){
																								getStage().execute(offsets, batchSize){ // StageRuntime.execute(){
																									Callable<String> callable = () -> {
																										switch (getDefinition().getType()) {
																											case SOURCE:
																												if(getStage() instanceof PushSource) {
																													((PushSource)getStage()).produce(offsets, batchSize){ // RandomDataGeneratorSource.produce(){
																														RandomDataGeneratorSource.produce()
																													}
																													return null;
																												}
																											default:
																												throw new IllegalStateException(Utils.format("Unknown stage type: '{}'", getDefinition().getType()));
																										}
																									};
																									execute(callable, null, null, null);
																								}
																							}
																							
																							// 
																							if(exceptionFromExecution != null) {
																								Throwables.propagate(exceptionFromExecution);
																							}
																							
																						}
																					}
																					// 当 Poll(消费)型 数据源时:
																					else{
																						ProductionPipelineRunner.runPollSource(){
																							// 只要offset还未拉取完,就继续拉取; 每循环一遍,即运行一个批次(Batch)
																							while (!offsetTracker.isFinished() && !stop && !finished) {
																								if (threadHealthReporter != null) {
																									threadHealthReporter.reportHealth("ProductionPipelineRunnable", -1, System.currentTimeMillis());
																								}
																								// 一个pipeBatch即对应运行一个批次Batch 
																								FullPipeBatch pipeBatch = createFullPipeBatch(Source.POLL_SOURCE_OFFSET_KEY, offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
																								
																								// 首先,运行Origin
																								processPipe(originPipe,pipeBatch){
																									// 1. StagePipe(包括SourcePipe) 类Pipe: Original, Processor, Target 等具体三类算子;
																									SourcePipe[extends StagePipe].process(){ // StagePipe.process()
																										Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
																										
																										String newOffset = getStage().execute(){ // StageRuntime.execute(){
																											Callable<String> callable = () -> {
																												switch (getDefinition().getType()) {
																													case SOURCE:
																														newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);
																														break;
																													
																													case PROCESSOR:
																														((Processor) getStage()).process(batch, batchMaker);
																														break;
																													
																													case EXECUTOR:
																													case TARGET:
																														((Target) getStage()).write(batch);
																														break;
																													
																													default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
																													
																												}
																												return newOffset;
																											};
																											
																											String offsetStr= execute(callable, errorSink, eventSink, processedSink){
																												mainClassLoader = Thread.currentThread().getContextClassLoader();
																												context.setPushSourceContextDelegate(this);
																												String newOffset = callable.call();
																												return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;
																											}
																											return offsetStr;
																										}
																									}
																									
																									// 2. ObserverPipe,MultiplexerPipe
																									ObserverPipe.process(){}
																									
																									// 3. MultiplexerPipe.process(){}
																									MultiplexerPipe.process(){}
																									
																								}
																								
																								// 然后, 运行其他算子:Processors, Targets
																								String newOffset = pipeBatch.getNewOffset();
																								ProductionPipelineRunner.runSourceLessBatch(start,pipeBatch,newOffset){
																									try{
																										PipeRunner pipeRunner = runnerPool.getRunner();
																										
																										ProductionPipelineRunner.executeRunner(pipeRunner, start, pipeBatch, entityName, newOffset, memoryConsumedByStage, stageBatchMetrics){
																											// 获取其OffsetCommitTrigger: offset的跟踪器
																											OffsetCommitTrigger offsetCommitTrigger = pipeRunner.getOffsetCommitTrigger();
																											
																											// 运行 封装其他Stages的 Pipe 
																											ThrowingConsumer<Pipe> consumer = (pipe)->{};
																											pipeRunner[PipeRunner].executeBatch(entityName, newOffset, start, consumer){
																												
																												/** 遍历每个Pipe,每个pipe代表Pipeline中一个算子(包括Stage,Multiplexer管道,Observer观察者)
																												*	问题1: 各Stage的拓扑结构是如何定义的? 哪个先哪个后?
																													问题1: 为什么会有Observer? 
																												*/
																												for(Pipe pipe : pipes) {
																													
																													consumer.accept(pipe){// 即 ThrowingConsumer<Pipe> consumer = (pipe)->{};
																														// processPipe() 之前跑Origin时调用过,同一个方法
																														String newOffset= ProductionPipelineRunner.processPipe(pipe, pipeBatch, committed.get(), entityName, newOffset, memoryConsumedByStage, stageBatchMetrics){
																															
																															// 对非延迟Batch, 对AT_MOST_ONCE模式, 只提交一次 committed?
																															if(!pipeBatch.isIdleBatch()) {
																																if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE && pipe.getStage().getDefinition().getType() == StageType.TARGET &&  !committed){
																																	offsetTracker.commitOffset(entityName, newOffset);
																																	committed = true;
																																}
																															}
																															// 核心方法, 三种Stage(Source/Process/Target)和ObserverPipe,MultiplexerPipe,都由此方法运行;
																															pipe[Pipe].process(){
																																// 1. StagePipe(包括SourcePipe) 类Pipe: Original, Processor, Target 等具体三类算子;
																																StagePipe.process(){ // StagePipe.process()
																																	Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
																																	
																																	String newOffset = getStage().execute(){ // StageRuntime.execute(){
																																		Callable<String> callable = () -> {
																																			switch (getDefinition().getType()) {
																																				case SOURCE:
																																					newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker){ // DSource.produce(lastSourceOffset, maxBatchSize, batchMaker){
																																						return getSource().produce(lastSourceOffset, maxBatchSize, batchMaker);{
																																							// 当为KafkaDSource时:
																																							Source source = KafkaDSource.getSource(){
																																								delegatingKafkaSource.getSource(){
																																									BaseKafkaSource delegate;
																																									init(){
																																										boolean isClusterMode = ( getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH 
																																												|| getContext().getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
																																												|| getContext().getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING);
																																										// Preview和Standalone模式,创建StandaloneKafka;
																																										if (getContext().isPreview() ||!isClusterMode) {
																																											delegate = standaloneKafkaSourceFactory.create();
																																										}else{
																																											delegate = clusterKafkaSourceFactory.create();
																																										}
																																									}
																																								}
																																								
																																								return source != null?  delegatingKafkaSource.getSource(return delegate(代码如上);): null;
																																							}
																																							.produce();
																																							
																																							// 当其他DSource时:
																																							
																																						}
																																					}
																																					break;
																																				
																																				case PROCESSOR:
																																					((Processor) getStage()).process(batch, batchMaker){ // DProcessor.process(){
																																						((Processor)getStage()).process(batch, batchMaker){
																																							// 当单个输出通道时, SingleLaneProcessor实现:
																																							SingleLaneProcessor.process(){
																																								SingleLaneBatchMaker slBatchMaker = new SingleLaneBatchMaker() {
																																									@Override public void addRecord(Record record) {
																																										batchMaker.addRecord(record, outputLane);
																																									}
																																									
																																									process(batch, slBatchMaker){ 
																																										// 抽象类, 由具体Processor实现类执行;
																																									}
																																								}
																																							}
																																							
																																							// 当 多个通道输出的算子时, ?
																																							
																																						}
																																					}
																																					break;
																																				
																																				case EXECUTOR:
																																				case TARGET:
																																					((Target) getStage()).write(batch);
																																					break;
																																				
																																				default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
																																				
																																			}
																																			return newOffset;
																																		};
																																		
																																		String offsetStr= execute(callable, errorSink, eventSink, processedSink){
																																			mainClassLoader = Thread.currentThread().getContextClassLoader();
																																			context.setPushSourceContextDelegate(this);
																																			// 此处执行具体的 Stage.produce/process/write()方法;
																																			String newOffset = callable.call();
																																			return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;
																																		}
																																		return offsetStr;
																																	}
																																}
																																
																																// 2. ObserverPipe,MultiplexerPipe
																																ObserverPipe.process(){}
																																
																																// 3. MultiplexerPipe.process(){}
																																MultiplexerPipe.process(){}
																																
																															}
																															
																															// 当要聚合时,聚合?
																															if (pipe instanceof StagePipe) {
																																if (isStatsAggregationEnabled()) {
																																	stageBatchMetrics.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe) pipe).getBatchMetrics());
																																}
																															}
																															return committed;
																														}
																														
																														committed.set(newOffset);
																													}
																													
																													
																													consumer.accept(pipe){
																														
																														StageOutput stageOutput = stagesToSkip.get(pipe.getStage().getInfo().getInstanceName());
																														
																														// 当Stage属于观察者或通道时
																														if (stageOutput == null || (pipe instanceof ObserverPipe) || (pipe instanceof MultiplexerPipe)) {
																															if (!skipTargets || !pipe.getStage().getDefinition().getType().isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
																																
																																pipe[StagePipe].process(pipeBatch){
																																BatchImpl batchImpl = pipeBatch.startStage(this).getBatch(this);
																																Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
																																
																																String newOffset = getStage().execute(previousOffset,batch,batchMaker){}
																																StageRuntime.execute(batchSize,batch,batchMaker){
																																	Callable<String> callable = () -> {
																																		switch (getDefinition().getType()) {
																																			case SOURCE:
																																				newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);
																																				break;
																																			
																																			case PROCESSOR:
																																				((Processor) getStage()).process(batch, batchMaker);
																																				break;
																																			
																																			case EXECUTOR:
																																			case TARGET:
																																				((Target) getStage()).write(batch);
																																				break;
																																			
																																			default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
																																			
																																		}
																																		return newOffset;
																																	};
																																	
																																	return execute(callable, errorSink, eventSink, processedSink){
																																		mainClassLoader = Thread.currentThread().getContextClassLoader();
																																		context.setPushSourceContextDelegate(this);
																																		
																																		String newOffset = callable.call();
																																		return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;
																																	}
																																}
																																
																																if (isSource()) {
																																	pipeBatch.setNewOffset(newOffset);
																																}
																																
																																batchMetrics = finishBatchAndCalculateMetrics(start, pipeBatch, batchMaker, batchImpl);
																																
																															}
																															
																																
																															}else{
																																pipeBatch.skipStage(pipe);
																															}
																														}else{
																															// 当属于Stage的Pipe时,需要计算
																															if (pipe instanceof StagePipe) {
																																pipeBatch.overrideStageOutput((StagePipe) pipe, stageOutput){
																																	
																																}
																															}
																														}
																															
																														
																													}
																												}
																											}
																											
																											
																											// 将ErrorMessage 和ErrorRecords 写入到内存中:
																											retainErrorMessagesInMemory(errorMessages);
																											retainErrorRecordsInMemory(errorRecords);
																											
																										}
																										
																									}finally{
																										// 运行完毕后, pipeRunner需要归还给 RunnerPool
																										if(pipeRunner != null) {
																											runnerPool[RunnerPool].returnRunner(pipeRunner);
																										}
																									}
																								}
																								
																							}
																						}
																					}
																				
																				}	
																				
																				// B. 预览模式: PreviewPipelineRunner.run()
																				
																			}
																		}
																		 
																		 
																	}
																	
																}finally {
																	runningThread = null;
																	cancelTask();
																}
																
																// 当Pipeline运行结束(停止或Source消费完), 发出Stop的请求
																ProductionPipelineRunnable.postStop(){
																	if (pipeline.wasStopped() && !pipeline.isExecutionFailed()) {
																		Map<String, String> offset = pipeline.getCommittedOffsets();
																		if (this.nodeProcessShutdown) {
																			LOG.info("Changing state of pipeline '{}', '{}' to '{}'", name, rev, PipelineStatus.DISCONNECTED);
																				* INFO  ProductionPipelineRunnable - Changing state of pipeline 'Creator02creatorByCreatorAndTestOtherUerViewd46a4a18-9c81-4719-a1dc-86977f945158', '0' to 'DISCONNECTED'
																			
																			pipeline.getStatusListener().stateChanged( PipelineStatus.DISCONNECTED)
																			
																		}
																	}
																}
																
																countDownLatch.countDown();
															}
														}
													}
												} else {
													validateAndSetStateTransition(user, PipelineStatus.RETRY, "Changing the state to RETRY on startup", null);
													metricsForRetry = getState().getMetrics();
												}
												
											}
											break;
										default:
											LOG.error(Utils.format("Pipeline cannot start with status: '{}'", status));
									}
								}
								
								// 2. 当以 Cluster模式 运行流时;
								runner[ClusterRunner].onDataCollectorStart(){
									
								}
								
								
							});
						}
					}
					
				}
			}
		}
		
		
		
	}
	
	if(enableHA){ # 只有ha模式才进入
		ZookeeperHABasedManger.takeLeaderShip(){
			manager.recoverPipelineStatus()
		}
	} 		
	
}


1.2 DC运运行时, Pipeline的启动Start:
ManagerResource.startPipeline() -> AclRunner.start():
	* StandaloneRunner.start(): ProductionPipelineRunnable.run() 
		-> ProductionPipeline.run()->Pipeline.run() -> ProductionPipelineRunner.run():
			1. Push数据源: ProductionPipelineRunner.runPushSource():
			2. Poll数据源: ProductionPipelineRunner.runPollSource(): PipeRunner.executeBatch() -> Pipe.process() -> DProcessor.process()
	
	* ClusterRunner.start(): ClusterRunner.doStart(): SystemProcess.start(environment);

ManagerResource.startPipeline(pipelineId,rev){
	runner[AclRunner].start(user,runtimeParameters){
		// 校验执行权限
		aclStore[CacheAclStoreTask].validateExecutePermission(this.getName(), currentUser);
		// 
		runner[AsyncRunner].start(user,runtimeParameters){
			runner.prepareForStart(user, runtimeParameters){
				PipelineState fromState = getState();
				checkState(VALID_TRANSITIONS.get(fromState.getStatus()).contains(PipelineStatus.STARTING),"");
				LOG.info("Preparing to start pipeline '{}::{}'", name, rev);
				
				validateAndSetStateTransition(user, PipelineStatus.STARTING, "Starting pipeline in " + getState().getExecutionMode() + " mode");
			}
			
			Callable<Object> callable = () -> {
				runner.start(user, runtimeParameters){
					
					// 1. 当runner=StandaloneRunner时
					StandaloneRunner.start(){
						
						// 启流1-准备流:  创建PipelineRunnable封装pipeline和ProductPipelineRunner, 并准备该pipeline运行相关环境;
						startPipeline(user, runtimeParameters){
							synchronized (this) {
								LOG.info("Starting pipeline {} {}", name, rev);
								
								PipelineConfiguration pipelineConfiguration = getPipelineConf(name, rev);
								pipelineRunnable = new ProductionPipelineRunnable(threadHealthReporter, this, prodPipeline, name, rev);
							}
						}
						
						// 启流2-启动流: 通过ProductionPipeline.run()调用pipeline.init()和pipeline.run();
						if(!pipelineRunnable.isStopped()) {
							pipelineRunnable[ProductionPipelineRunnable].run(){
								ProductionPipelineRunnable.run() -> ProductionPipeline.run() -> Pipeline.run(){
									ProductionPipelineRunner.run(){
										1. Push数据源: ProductionPipelineRunner.runPushSource(): while()循环处理 生产型(push)数据源的各Batch计算,直到结束
					
										2. Poll数据源: ProductionPipelineRunner.runPollSource(): while()循环处理 消费型(poll)数据源的各Batch计算,直到结束:
											- 先processPipe(originPipe)数据源读取数据 StagePipe.process() -> Dsource.produce();
											- 再runSourceLessBatch()处理其他非Source的各算子:
												-> 在PipeRunner.executeBatch()中for循环遍历其他算子Pipe(StagePipe,ObserverPipe,MultiplexerPipe),并Pipe.process()-> DProcessor.process()/DTarget.writer();
									}
								}
							}
						}
						
					}
					
					// 2. 当为Cluster模式运行的流时:
					ClusterRunner.start(){
						pipelineConf = getPipelineConf(name, rev);
						
						ClusterRunner.doStart(user, pipelineConf, surceInfo, getAcl(name)){
							PipelineConfigBean pipelineConfigBean = PipelineBeanCreator.get().create(pipelineConf,runtimeParameters);
							
							// 调用SystemProcess.start()方法提交Spark-submit命令
							ApplicationState applicationState = clusterHelper.submit(){
								clusterProvider[ClusterProviderImpl].startPipeline(){
									ClusterProviderImpl.startPipelineInternal(){
										// # 完成各种lib加载和Bean的构建
										Map<String, List<URL>> streamsetsLibsCl, Map<String, List<URL>> userLibsCL, List<StageConfiguration> pipelineConfigurations,Map<String, String> sourceConfigs;
										
										PipelineBean pipelineBean = PipelineBeanCreator.get().create(false, stageLibrary, pipelineConfiguration, errors);
										extractClassLoaderInfo(streamsetsLibsCl, userLibsCL, stageDef.getStageClassLoader(), stageDef.getClassName());
										
										else if (executionMode == ExecutionMode.CLUSTER_YARN_STREAMING) {
											args = generateSparkArgs()
											
										}
										// 调用System函数执行spark-submit命令;
										SystemProcess process = systemProcessFactory.create(ClusterProviderImpl.class.getSimpleName(), outputDir, args);
											* LOG.info("Starting: " + process);
										process.start(environment);
										while(true){
											SystemProcess logsProcess = systemProcessFactory.create()
											logOutput(appId, logsProcess);
										}
									}
								}
							}
							
							validateAndSetStateTransition()
							
							scheduleRunnable(user, pipelineConf){
								// 启动30秒定时现场checkStatus, 若isAtive则连接集群
								runnerExecutor.scheduleAtFixedRate(new ManagerRunnable(run();), 0, 30, TimeUnit.SECONDS){
									ManagerRunnable.run(){
										ClusterRunner.checkStatus(){
											if (clusterRunner.getState().getStatus().isActive()) {
												 clusterRunner.connect(runningUser, appState, pipelineConf){
													clusterPipelineState = clusterHelper.getStatus(appState, pipelineConf);
													if (clusterPipelineState == RUNNING) {
														validateAndSetStateTransition(user, PipelineStatus.RUNNING, msg);
														
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
				return null;
			};
			
			runnerExecutor[MetricSafeScheduledExecutorService].submit(callable);
			
		}
	}
	
	return Response.ok().type(MediaType.APPLICATION_JSON)
						.entity(BeanHelper.wrapPipelineState(runner.getState())).build();
}


DC运运行时, Pipeline的 stop运行原理和源码

ManagerResource.stopPipeline() 响应停pipeline请求后会影响3个线程:
 -> ManagerResource.stopPipeline() ->  AsyncRunner.stop() : 变为Stopping状态,并设置各Runner和Stage.stop状态=true;
 -> ProductionPipeline.run() -> ProductionPipelineRunner.runPollSource()	: 该线程为while()循环跑各批次的数据, 当被打断后Pipeline.destroy()销毁各Stage和Runner数据;
 -> ProductionPipelineRunnable.run() -> StandaloneRunner.stateChanged()		: 该线程负载启动管理整个Pipeline运行, 其run()方法中finally会执行postStop()来讲状态改为:STOPPED

 
 
 
 # =========================================== 2. stopPipeline 停流的逻辑 =================================================
 
 
 
 
 

/** 2.1  "webserver-n" 线程接受到 restful请求: 
*   - ManagerResource.stopPipeline() ->  AsyncRunner.stop() -> 
*       - runner.prepareForStop(user) : 将状态从Running状态, 改为 Stopping状态;
*       - runnerStopExecutor.submit(callable(){runner.stop(user)}); "runnerStop-pool-10-thread" 线程;
*/

ManagerResource.stopPipeline(){
	PipelineInfo pipelineInfo = store[CachePipelineStoreTask].getInfo(pipelineId){
		return pipelineInfoMap[ Map<String,PipelineInf>].get(pipelineId)
	}
	
	Runner runner = manager[StandaloneAndClusterPipelineManager].getRunner(pipelineId, rev){
		RunnerInfo runnerInfo = runnerCache.get(name,()->{ new RunnerInfo(getRunner(name, rev, executionMode),executionMode) });
		// 中间对 ExecutionMode即PipelineStatus状态的检查;
		return runnerInfo.runner;
	}
		
	runner[AsyncRunner].stop(){
        // 关键步骤1:  先把内存和磁盘的pipelineState状态从 Running -> Stopping状态;
		runner.prepareForStop(user){ // 分StandaloneRunner和ClusterRunner两种实现: StandaloneRunner.prepareForStop()
            validateAndSetStateTransition(user, PipelineStatus.STOPPING, null, null){
                pipelineStateStore.saveState(){ // FielPipelineStateStore.saveState()
                    persistPipelineState(pipelineState){
                        DataStore dataStore = new DataStore(getPipelineStateFile(pipelineState.getPipelineId(), pipelineState.getRev()));
                        try (OutputStream os = dataStore.getOutputStream()) {
                            os.write(pipelineString.getBytes());
                            dataStore.commit(os);
                        }
                        
                    }
                }
            }
		}
		
		Callable<Object> callable = () -> {
			runner.stop(user){
				// 1. 当为Standalone的Pipeline时:
				StandaloneRunner.stop();
				
				// 2. 当为Cluster版的的Pipeline时
				ClusterRunner.stop(){}
			}
			return null;
		}
		
		runnerStopExecutor.submit(callable); //异步执行 停流操作,
	}
	
	/** 这块逻辑是 startPipeline() -> StandaloneRunner.start()  线程中的执行代码: 
	*  	在Pipeline.run()方法中,一直循环消费Source而阻塞在ProductionPipelineRunner.runPollSource() 方法中;
	*/
	ProductionPipeline.run(){
		try{
			// ..
			// 如果Pipeline运行起来后,会一直阻塞在此处, 直到Pipline运行完,或被打断
			pipeline.run()
		}finally{
			LOG.debug("Destroying");
			pipeline.destroy(true, stopReason){ // Pipeline.destroy()
				LOG.info("Destroying pipeline with reason={}", stopReason.name());
					* 	INFO  Pipeline - Destroying pipeline with reason=USER_ACTION
				
				// 再停一遍, 保证所有的Stages都已关闭状态; 代码如上诉中的 Pipeline.stop()
				stop();
				
				runner.destroy(originPipe, pipes){ // ProductionPipelineRunner.destroy()
					destroyLock.lock();
					if (runnerPool != null) {
						runnerPool.destroy();
					}
					originPipe.destroy(pipeBatch); // 销毁 Origin数据源算子
					
					// 从最后一个 算子(pipe)开始, 依次销毁;
					for (PipeRunner pipeRunner : Lists.reverse(pipeRunners)) {
						finalPipeBatch.skipStage(originPipe);
						ThrowingConsumer<Pipe> consumer = (pipe)->{};// 实现在如下方法中
						pipeRunner[PipeRunner].executeBatch(null, newOffset, start, consumer){
							for(Pipe pipe : pipes) {
								consumer.accept(pipe){
									((StageContext) pipe.getStage().getContext()).setLastBatchTime(lastBatchTime);
									
									if (pipe instanceof StagePipe) {
										if (pipe.getStage().getConfiguration().isInEventPath()) {
											pipe.process(finalPipeBatch);
										}else{
											finalPipeBatch.skipStage(pipe);
										}
									}else{
										pipe.process(finalPipeBatch);
									}
									
									pipe[Pipe].destroy(finalPipeBatch){ // 分3类Pipe, 只有StagePipe.destroy() 有销毁操作
										// 1. StagePipe: 
										getStage().destroy(errorSink, eventSink, processedSink){ // StageRuntime.destroy()
											getStage().destroy(); //这里执行每个Stage的 destroy();//默认为null;
											stageBean.releaseClassLoader(); // release the stage classloader back to the library
										}
										pipeBatch.completeStage(this);
										
										// 2. 对于ObserverPipe和 MultiplexerPipe ,destroy()为空代码
									}
								}
							}
						}
					}
					destroyLock.unlock();
					
				}
				
				if(stopEventStage != null) {
					if(productionExecution && stopEventStageInitialized) {
						LOG.info("Processing lifecycle stop event");
							* INFO  Pipeline - Processing lifecycle stop event
						
						runner.runLifecycleEvent(createStopEvent(stopReason), stopEventStage){
							// 在跑一个 EventRecord的 运行;
							Batch batch = new BatchImpl(ImmutableList.of(eventRecord))
							stageRuntime.execute(null, 1000, batch, null, errorSink, new EventSink(), new ProcessedSink());
						}
					}
				}
                
				if (scheduledExecutorService != null) {
					scheduledExecutorService.shutdown();
				}
			}
			
			
		}
		
	}
	
	return Response.ok().entity(BeanHelper.wrapPipelineState(runner.getState())).build();
    
}


/** 2.2  "runnerStop-pool-n-thread" 线程: 
*   - 触发线程: "webserver-n" 线程中 ManagerResource.stopPipeline() ->  AsyncRunner.stop() 触发;
*   
*   - 下游: 无创建线程, 有影响以下线程的运行: "ProductionPipelineRunnable-pipelineId"线程, 终止ProductionPipelineRunner.runPollSource()中的while()循环;
*/

// 1. 当为Standalone的Pipeline时:
StandaloneRunner.stop(){
    stopPipeline(false){ //StandaloneRunner.stopPipeline(sdcShutting)
        if (pipelineRunnable != null && !pipelineRunnable.isStopped()) {
            LOG.info("Stopping pipeline {} {}", pipelineRunnable.getName(), pipelineRunnable.getRev());
            
            pipelineRunnable.stop(sdcShutting){ // ProductionPipelineRunnable.stop(nodeProcessShutdown)
                pipeline.stop(){ // ProductionPipeline.stop()
                    // 重要代码: 将ProductionPipelineRunner.stop变量从false置为true, 该类的runPollSource()方法中的 while (!offsetTracker.isFinished() && !stop && !finished) 循环条件终止;
                    pipelineRunner.stop(){ // ProductionPipelineRunner.stop()
                        this.stop = true; // 该stop变量在 ProductionPipelineRunner.runPollSource()方法中被引用:循环判断;
                        
                        if(batchesToCapture > 0) {
                            cancelSnapshot(this.snapshotName);
                            snapshotStore.deleteSnapshot(pipelineName, revision, snapshotName);
                        }
                    }
                    
                    pipeline.stop(){ // Pipeline.stop()
                        ((StageContext)originPipe.getStage().getContext()).setStop(true); //设置Sources数据源的Context.stop==true;
                        
                        // 将所有Stage的Context都设置为stop=true停止状态;
                        for(PipeRunner pipeRunner : pipes) { 
                            ThrowingConsumer<Pipe> consumer = p -> ((StageContext)p.getStage().getContext()).setStop(true);
                            pipeRunner.forEach(consumer){
                                for(Pipe p : pipes) { // 每个Pipe封装流中一段处理逻辑,分3种:ObserverPipe,MultiplexerPipe,StagePipe;
                                    consumer.accept(p);
                                }
                            }
                        }
                    }
                }
                try {
                  countDownLatch.await(); // 阻塞在此,直到所有的(1个) countDown锁都释放完; 
                  {//"ProductionPipelineRunnable-pipelineId"线程中的逻辑和释放 countDownLatch锁的代码:
                    ProductionPipelineRunnable.run(){
                        try{pipeline.run();// do some..
                        }finally {
                            // 只有完成各Stage的desctory()和 修改pipelineState.json中的状态改为 Stopped后,才会释放countDownLatch锁;
                            postStop(); 
                            countDownLatch.countDown(); //在这里释放该锁的资源; 
                        }
                    }
                  }
                } catch (InterruptedException e) {LOG.info("Thread interrupted: {}", e.toString(), e);}
                LOG.info("Pipeline is in terminal state");
                    * INFO  ProductionPipelineRunnable - Pipeline is in terminal state
            }
            pipelineRunnable= null;
        }
        
    }
}
    
    
    
            ProductionPipelineRunnable.run(){
                try{
                    pipeline.run();{
                        
                    }
                } finally {
                    postStop(); // 只有完成各Stage的desctory()和 修改pipelineState.json中的状态改为 Stopped后,才会释放countDownLatch锁;
                    countDownLatch.countDown(); //在这里释放该锁的资源; 
                }
            }





            
/** 2.3 "ProductionPipelineRunnable-pipelineId"线程: 循环发起Batch完成各Stages的串行运行; 当stop状态为true时,完成Stages等资源消耗和 pipelineState状态的更改;
*   - 触发线程:
*   - 源码简介 : 
        ProductionPipelineRunnable.run(){
            try{
                pipeline.run(){ //ProductionPipeline.run()
                    pipeline.run();{//Pipeline.run()
                        ProductionPipelineRunner.run(originPipe, pipes, badRecordsHandler, statsAggregationHandler);
                    }
                }
            } finally {
                postStop(); // 只有完成各Stage的desctory()和 修改pipelineState.json中的状态改为 Stopped后,才会释放countDownLatch锁;
                countDownLatch.countDown(); //在这里释放该锁的资源; 
            }
        }
*   -终止触发线程:  "runnerStop-pool-n-thread" 线程: StandaloneRunner.stop() -> pipelineRunnable.stop() -> pipelineRunner.stop();
*/    

ProductionPipelineRunnable.run(){ // ProductionPipelineRunnable.run()为DC管理器启动的某条pipeline的实际Run线程
    try{
        // 流运行时,会一直阻塞在此; 
        pipeline[ProductionPipelineRunner].run();{
            pipeline.run();{//Pipeline.run()
                runner.run(originPipe, pipes, badRecordsHandler, statsAggregationHandler);{//ProductionPipelineRunner.run()
                    if (originPipe.getStage().getStage() instanceof PushSource) {
                        runPushSource();//当Source算子是Push类型时: DevDataGenerator 等;
                    }else{ // KafkaDSource, DataGenerator等算子是 Poll类型的,走这里;
                        runPollSource();{//ProductionPipelineRunner.runPollSource()
                            //循环阻塞在此, 直到 stop==true (由"runnerStop-pool-n-thread" 线程触发);
                            while (!offsetTracker.isFinished() && !stop && !finished) {
                                FullPipeBatch pipeBatch = createFullPipeBatch(Source.POLL_SOURCE_OFFSET_KEY, offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
                                processPipe(originPipe,pipeBatch); //运行一个 Origin(Source)数据源算子;
                                runSourceLessBatch();// 运行Processor + Targets算子;
                            }
                        }
                    }
                }
            }
        }
    }finally{
        // 当Pipeline运行结束(停止或Source消费完), 发出Stop的请求
        postStop(){ // ProductionPipelineRunnable.postStop()
            if (pipeline.wasStopped() && !pipeline.isExecutionFailed()) {
                Map<String, String> offset = pipeline.getCommittedOffsets();
                if (this.nodeProcessShutdown) {
                    LOG.info("Changing state of pipeline '{}', '{}' to '{}'", name, rev, PipelineStatus.DISCONNECTED);
                        * INFO  ProductionPipelineRunnable - Changing state of pipeline 'Creator02creatorByCreatorAndTestOtherUerViewd46a4a18-9c81-4719-a1dc-86977f945158', '0' to 'DISCONNECTED'
                    
                    // 最后一步: 将缓存和磁盘中 PipelineState状态改为: Stopping -> Stopped;
                    pipeline.getStatusListener().stateChanged( PipelineStatus.STOPPED){ // getStatusListener() 返回的就是StandaloneRunner
                        StandaloneRunner.stateChanged(){
                            validateAndSetStateTransition(getState().getUser(), pipelineStatus, message, allAttributes){
                                pipelineStateStore.saveState(user, name, rev, toStatus, message, attributes, ExecutionMode.STANDALONE){
                                    pipelineStateStore.saveState(){ // FielPipelineStateStore.saveState()
                                        persistPipelineState(pipelineState){
                                            DataStore dataStore = new DataStore(getPipelineStateFile(pipelineState.getPipelineId(), pipelineState.getRev()));
                                            try (OutputStream os = dataStore.getOutputStream()) {
                                                os.write(pipelineString.getBytes());
                                                dataStore.commit(os);
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
        countDownLatch.countDown();
    }
}













// 运行ProductionPipeline.run(); 校验各Stage,先运行Source, 再run其他Stage
ProductionPipeline.run(){
    List<Issue> issues = getPipeline().init(true);
    stateChanged(PipelineStatus.RUNNING, null, attributes);
    
    pipeline[Pipeline].run(){
        runner[接口类ProductionRunner].run(originPipe, pipes, badRecordsHandler, statsAggregationHandler){
            // A. 生产环境: 
            ProductionPipelineRunner.run(){
                this.runnerPool = new RunnerPool<>(pipes, pipeContext.getRuntimeStats(), runnersHistogram);
                // 当Push (生产)型 数据源时
                if (originPipe.getStage().getStage() instanceof PushSource) {
                    ProductionPipelineRunner.runPushSource(){
                        int batchSize = configuration.get("production.maxBatchSize", 1000);
                        // 先运行Source, 会一直阻塞在此,直到运行完;或stop;
                        originPipe[SourcePipe].process(offsetTracker[ProductionSourceOffsetTracker].getOffsets(), batchSize, this){
                            getStage().execute(offsets, batchSize){ // StageRuntime.execute(){
                                Callable<String> callable = () -> {
                                    switch (getDefinition().getType()) {
                                        case SOURCE:
                                            if(getStage() instanceof PushSource) {
                                                ((PushSource)getStage()).produce(offsets, batchSize){ // RandomDataGeneratorSource.produce(){
                                                    RandomDataGeneratorSource.produce()
                                                }
                                                return null;
                                            }
                                        default:
                                            throw new IllegalStateException(Utils.format("Unknown stage type: '{}'", getDefinition().getType()));
                                    }
                                };
                                execute(callable, null, null, null);
                            }
                        }
                        
                        // 
                        if(exceptionFromExecution != null) {
                            Throwables.propagate(exceptionFromExecution);
                        }
                        
                    }
                }
                // 当 Poll(消费)型 数据源时:
                else{
                    ProductionPipelineRunner.runPollSource(){
                        // 只要offset还未拉取完,就继续拉取; 每循环一遍,即运行一个批次(Batch)
                        while (!offsetTracker.isFinished() && !stop && !finished) {
                            if (threadHealthReporter != null) {
                                threadHealthReporter.reportHealth("ProductionPipelineRunnable", -1, System.currentTimeMillis());
                            }
                            // 一个pipeBatch即对应运行一个批次Batch 
                            FullPipeBatch pipeBatch = createFullPipeBatch(Source.POLL_SOURCE_OFFSET_KEY, offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
                            
                            // 首先,运行Origin
                            processPipe(originPipe,pipeBatch){
                                // 1. StagePipe(包括SourcePipe) 类Pipe: Original, Processor, Target 等具体三类算子;
                                SourcePipe[extends StagePipe].process(){ // StagePipe.process()
                                    Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
                                    
                                    String newOffset = getStage().execute(){ // StageRuntime.execute(){
                                        Callable<String> callable = () -> {
                                            switch (getDefinition().getType()) {
                                                case SOURCE:
                                                    newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);
                                                    break;
                                                
                                                case PROCESSOR:
                                                    ((Processor) getStage()).process(batch, batchMaker);
                                                    break;
                                                
                                                case EXECUTOR:
                                                case TARGET:
                                                    ((Target) getStage()).write(batch);
                                                    break;
                                                
                                                default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
                                                
                                            }
                                            return newOffset;
                                        };
                                        
                                        String offsetStr= execute(callable, errorSink, eventSink, processedSink){
                                            mainClassLoader = Thread.currentThread().getContextClassLoader();
                                            context.setPushSourceContextDelegate(this);
                                            String newOffset = callable.call();
                                            return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;
                                        }
                                        return offsetStr;
                                    }
                                }
                                
                                // 2. ObserverPipe,MultiplexerPipe
                                ObserverPipe.process(){}
                                
                                // 3. MultiplexerPipe.process(){}
                                MultiplexerPipe.process(){}
                                
                            }
                            
                            // 然后, 运行其他算子:Processors, Targets
                            String newOffset = pipeBatch.getNewOffset();
                            ProductionPipelineRunner.runSourceLessBatch(start,pipeBatch,newOffset){
                                try{
                                    PipeRunner pipeRunner = runnerPool.getRunner();
                                    
                                    ProductionPipelineRunner.executeRunner(pipeRunner, start, pipeBatch, entityName, newOffset, memoryConsumedByStage, stageBatchMetrics){
                                        // 获取其OffsetCommitTrigger: offset的跟踪器
                                        OffsetCommitTrigger offsetCommitTrigger = pipeRunner.getOffsetCommitTrigger();
                                        
                                        // 运行 封装其他Stages的 Pipe 
                                        ThrowingConsumer<Pipe> consumer = (pipe)->{};
                                        pipeRunner[PipeRunner].executeBatch(entityName, newOffset, start, consumer){
                                            
                                            /** 遍历每个Pipe,每个pipe代表Pipeline中一个算子(包括Stage,Multiplexer管道,Observer观察者)
                                            *	问题1: 各Stage的拓扑结构是如何定义的? 哪个先哪个后?
                                                问题1: 为什么会有Observer? 
                                            */
                                            for(Pipe pipe : pipes) {
                                                
                                                consumer.accept(pipe){// 即 ThrowingConsumer<Pipe> consumer = (pipe)->{};
                                                    // processPipe() 之前跑Origin时调用过,同一个方法
                                                    String newOffset= ProductionPipelineRunner.processPipe(pipe, pipeBatch, committed.get(), entityName, newOffset, memoryConsumedByStage, stageBatchMetrics){
                                                        
                                                        // 对非延迟Batch, 对AT_MOST_ONCE模式, 只提交一次 committed?
                                                        if(!pipeBatch.isIdleBatch()) {
                                                            if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE && pipe.getStage().getDefinition().getType() == StageType.TARGET &&  !committed){
                                                                offsetTracker.commitOffset(entityName, newOffset);
                                                                committed = true;
                                                            }
                                                        }
                                                        // 核心方法, 三种Stage(Source/Process/Target)和ObserverPipe,MultiplexerPipe,都由此方法运行;
                                                        pipe[Pipe].process(){
                                                            // 1. StagePipe(包括SourcePipe) 类Pipe: Original, Processor, Target 等具体三类算子;
                                                            StagePipe.process(){ // StagePipe.process()
                                                                Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
                                                                
                                                                String newOffset = getStage().execute(){ // StageRuntime.execute(){
                                                                    Callable<String> callable = () -> {
                                                                        switch (getDefinition().getType()) {
                                                                            case SOURCE:
                                                                                newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker){ // DSource.produce(lastSourceOffset, maxBatchSize, batchMaker){
                                                                                    return getSource().produce(lastSourceOffset, maxBatchSize, batchMaker);{
                                                                                        // 当为KafkaDSource时:
                                                                                        Source source = KafkaDSource.getSource(){
                                                                                            delegatingKafkaSource.getSource(){
                                                                                                BaseKafkaSource delegate;
                                                                                                init(){
                                                                                                    boolean isClusterMode = ( getContext().getExecutionMode() == ExecutionMode.CLUSTER_BATCH 
                                                                                                            || getContext().getExecutionMode() == ExecutionMode.CLUSTER_YARN_STREAMING
                                                                                                            || getContext().getExecutionMode() == ExecutionMode.CLUSTER_MESOS_STREAMING);
                                                                                                    // Preview和Standalone模式,创建StandaloneKafka;
                                                                                                    if (getContext().isPreview() ||!isClusterMode) {
                                                                                                        delegate = standaloneKafkaSourceFactory.create();
                                                                                                    }else{
                                                                                                        delegate = clusterKafkaSourceFactory.create();
                                                                                                    }
                                                                                                }
                                                                                            }
                                                                                            
                                                                                            return source != null?  delegatingKafkaSource.getSource(return delegate(代码如上);): null;
                                                                                        }
                                                                                        .produce();
                                                                                        
                                                                                        // 当其他DSource时:
                                                                                        
                                                                                    }
                                                                                }
                                                                                break;
                                                                            
                                                                            case PROCESSOR:
                                                                                ((Processor) getStage()).process(batch, batchMaker){ // DProcessor.process(){
                                                                                    ((Processor)getStage()).process(batch, batchMaker){
                                                                                        // 当单个输出通道时, SingleLaneProcessor实现:
                                                                                        SingleLaneProcessor.process(){
                                                                                            SingleLaneBatchMaker slBatchMaker = new SingleLaneBatchMaker() {
                                                                                                @Override public void addRecord(Record record) {
                                                                                                    batchMaker.addRecord(record, outputLane);
                                                                                                }
                                                                                                
                                                                                                process(batch, slBatchMaker){ 
                                                                                                    // 抽象类, 由具体Processor实现类执行;
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                        
                                                                                        // 当 多个通道输出的算子时, ?
                                                                                        
                                                                                    }
                                                                                }
                                                                                break;
                                                                            
                                                                            case EXECUTOR:
                                                                            case TARGET:
                                                                                ((Target) getStage()).write(batch);
                                                                                break;
                                                                            
                                                                            default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
                                                                            
                                                                        }
                                                                        return newOffset;
                                                                    };
                                                                    
                                                                    String offsetStr= execute(callable, errorSink, eventSink, processedSink){
                                                                        mainClassLoader = Thread.currentThread().getContextClassLoader();
                                                                        context.setPushSourceContextDelegate(this);
                                                                        // 此处执行具体的 Stage.produce/process/write()方法;
                                                                        String newOffset = callable.call();
                                                                        return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;
                                                                    }
                                                                    return offsetStr;
                                                                }
                                                            }
                                                            
                                                            // 2. ObserverPipe,MultiplexerPipe
                                                            ObserverPipe.process(){}
                                                            
                                                            // 3. MultiplexerPipe.process(){}
                                                            MultiplexerPipe.process(){}
                                                            
                                                        }
                                                        
                                                        // 当要聚合时,聚合?
                                                        if (pipe instanceof StagePipe) {
                                                            if (isStatsAggregationEnabled()) {
                                                                stageBatchMetrics.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe) pipe).getBatchMetrics());
                                                            }
                                                        }
                                                        return committed;
                                                    }
                                                    
                                                    committed.set(newOffset);
                                                }
                                                
                                                
                                                consumer.accept(pipe){
                                                    
                                                    StageOutput stageOutput = stagesToSkip.get(pipe.getStage().getInfo().getInstanceName());
                                                    
                                                    // 当Stage属于观察者或通道时
                                                    if (stageOutput == null || (pipe instanceof ObserverPipe) || (pipe instanceof MultiplexerPipe)) {
                                                        if (!skipTargets || !pipe.getStage().getDefinition().getType().isOneOf(StageType.TARGET, StageType.EXECUTOR)) {
                                                            
                                                            pipe[StagePipe].process(pipeBatch){
                                                            BatchImpl batchImpl = pipeBatch.startStage(this).getBatch(this);
                                                            Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
                                                            
                                                            String newOffset = getStage().execute(previousOffset,batch,batchMaker){}
                                                            StageRuntime.execute(batchSize,batch,batchMaker){
                                                                Callable<String> callable = () -> {
                                                                    switch (getDefinition().getType()) {
                                                                        case SOURCE:
                                                                            newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);
                                                                            break;
                                                                        
                                                                        case PROCESSOR:
                                                                            ((Processor) getStage()).process(batch, batchMaker);
                                                                            break;
                                                                        
                                                                        case EXECUTOR:
                                                                        case TARGET:
                                                                            ((Target) getStage()).write(batch);
                                                                            break;
                                                                        
                                                                        default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
                                                                        
                                                                    }
                                                                    return newOffset;
                                                                };
                                                                
                                                                return execute(callable, errorSink, eventSink, processedSink){
                                                                    mainClassLoader = Thread.currentThread().getContextClassLoader();
                                                                    context.setPushSourceContextDelegate(this);
                                                                    
                                                                    String newOffset = callable.call();
                                                                    return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;
                                                                }
                                                            }
                                                            
                                                            if (isSource()) {
                                                                pipeBatch.setNewOffset(newOffset);
                                                            }
                                                            
                                                            batchMetrics = finishBatchAndCalculateMetrics(start, pipeBatch, batchMaker, batchImpl);
                                                            
                                                        }
                                                        
                                                            
                                                        }else{
                                                            pipeBatch.skipStage(pipe);
                                                        }
                                                    }else{
                                                        // 当属于Stage的Pipe时,需要计算
                                                        if (pipe instanceof StagePipe) {
                                                            pipeBatch.overrideStageOutput((StagePipe) pipe, stageOutput){
                                                                
                                                            }
                                                        }
                                                    }
                                                        
                                                    
                                                }
                                            }
                                        }
                                        
                                        
                                        // 将ErrorMessage 和ErrorRecords 写入到内存中:
                                        retainErrorMessagesInMemory(errorMessages);
                                        retainErrorRecordsInMemory(errorRecords);
                                        
                                    }
                                    
                                }finally{
                                    // 运行完毕后, pipeRunner需要归还给 RunnerPool
                                    if(pipeRunner != null) {
                                        runnerPool[RunnerPool].returnRunner(pipeRunner);
                                    }
                                }
                            }
                            
                        }
                    }
                }
            
            }	
            
            // B. 预览模式: PreviewPipelineRunner.run()
            
        }
    }
     
     
}





    
    
ManagerResource.forceStopPipeline(String pipelineId){
    //从缓存中(CachePipelineStoreTask.pipelineInfoMap:Map<String,PipelineInf>中读取该pipelineId对应 PipelineInfo信息;
    PipelineInfo pipelineInfo = store.getInfo(pipelineId);{//CachePipelineStoreTask.getInfo()
        return pipelineInfoMap.get(pipelineId)
    }
    
    RestAPIUtils.injectPipelineInMDC(pipelineInfo.getTitle(), pipelineInfo.getPipelineId());//这是给Log4j用的, 向MDC中加一个entry;
    if (manager.isRemotePipeline(pipelineId, rev)) { //判断是否是远程Pipeline, 一般standalone任务,都是不是远程的, ?问题: cluster-pipeline是远程的吗?
      if (!context.isUserInRole(AuthzRole.ADMIN) && !context.isUserInRole(AuthzRole.ADMIN_REMOTE)) {
        throw new PipelineException(ContainerError.CONTAINER_01101, "FORCE_QUIT_PIPELINE", pipelineId);
      }
    }
    
    Runner runner = manager.getRunner(pipelineId, rev);{//StandaloneAndClusterPipelineManager.getRunner(): 从缓存中取出或新建(若缓存为null时)Runner
        RunnerInfo runnerInfo = runnerCache.get(name,()->{ new RunnerInfo(getRunner(name, rev, executionMode),executionMode) });
		// 中间对 ExecutionMode即PipelineStatus状态的检查;
		return runnerInfo.runner;
    }
    //只有Standalone模式采用进行 forceQuit()强停操作; 为什么?(因为cluster模式的即便停了DC里的pipeline,sparkOnYarn任务可能还在跑);
    Utils.checkState(runner.getState().getExecutionMode() == ExecutionMode.STANDALONE,  Utils.format("This operation is not supported in {} mode", runner.getState().getExecutionMode()));
    
    runner.forceQuit(user);{//AsyncRunner.forceQuit(user)
        Callable<Object> callable = () -> {
            
            runner.forceQuit(user);{//StandaloneRunner.forceQuit(user)
                /* 只有处于 FORCE_QUIT_ALLOWED_STATES:List<String> 中包含的5种(ING)状态时,才能进行forceStop()
                        PipelineStatus.STOPPING,
                        PipelineStatus.STOPPING_ERROR,
                        PipelineStatus.STARTING_ERROR,
                        PipelineStatus.RUNNING_ERROR,
                        PipelineStatus.FINISHING
                    为什么只准这5种状态才能强停? 跟强停的操作有关?
                */
                if (pipelineRunnable != null && FORCE_QUIT_ALLOWED_STATES.contains(getState().getStatus())) {
                    LOG.debug("Force Quit the pipeline '{}'::'{}'", name,  rev);
                    pipelineRunnable.forceQuit();{//ProductionPipelineRunnable.forceQuit()
                        synchronized (relatedTasks){
                          if (runningThread != null) {
                            runningThread.interrupt();
                            runningThread = null;
                            cancelTask();
                            postStop();
                          }
                        }
                        countDownLatch.countDown();//释放锁; 让什么线程跑?
                    }
                } else {
                    LOG.info("Ignoring force quit request because pipeline is in {} state", getState().getStatus());
                }
            }
          return null;
        };
        runnerStopExecutor.submit(callable);//强停是异步线程操作, Web收到Resp.ok不代表 forceQuit()就完成了;
    }
    
    return Response.ok()
        .type(MediaType.APPLICATION_JSON)
        .entity(BeanHelper.wrapPipelineState(runner.getState())).build();
}





DC关闭时, Pipeline的stop流程和源码;

昨天: 
* 排除Beta上一直Stopping的问题; 
* 帮助调研 停流相关的步骤和源码; 
* 开会过了3月份的事; 

今天:
* 继续梳理 停流/启流这块的原理, 这块还挺复杂的; 
* 需要梳理下HA ZK相关的机制; 
* 先梳理出逻辑和关键代码, 再看景楠/宏哥的时间, 一起过下; 下午吧;



