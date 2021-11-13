
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




/** BatchMaker.add():Stage结果数据传入下游Stage的clone包装重逻辑: 
*	每个Record会进行2-3遍的clone, 每次clone,对于Map结构的数据, 会递归调用Field.clone()完成所有字段的全新new 新对象包装;
*/
SingleLaneProcessor.process(){
	
	public void process(Batch batch, BatchMaker batchMaker){} //Processor的处理接口;
	public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker){ //DSource的处理接口;
		for(Record record:records){
			batchMaker.addRecord(record, outputLane);{
				// 1. 默认 getRecord 会进行一次 RecordImpl.clone;
				RecordImpl recordCopy = getRecordForBatchMaker(record);{
					// Stage的recordByRef默认为false, 可在@StageDef()时改为true;
					{//new BatchMakerImpl() : 当为Production模式且 @StageDef(recordsByRef = true) 时(默认false)
						recordByRef = !stagePipe.getStage().getContext().isPreview() &&
							  stagePipe.getStage().getDefinition().getRecordsByRef();
					}
					return (recordByRef) ? (RecordImpl) record: ((RecordImpl) record).clone();{// RecordImpl.clone()
						new RecordImpl(this);{
							header = record.header.clone();{//HeaderImpl.clone()
								return new HeaderImpl(this);{
									this.map = new HashMap<>(header.map);
								}
							}
							value = (record.value != null) ? record.value.clone() : null;{//Field.clone()
								return new Field(type, value, attributes);{
									this.type = type;
									this.value = CreateByRef.isByRef() ? value : type.constructorCopy(value);{//Type.constructorCopy(T value)
										return (value != null) ? (T) supporter.create(value) : null;{
											supporter.create(Object value){//TypeSupport.create()是抽象类, 由不同Type的子类实现;
												MapTypeSupport.create(value){
													return clone(value);{//MapTypeSupport.clone()
														Map map = null;
														if (value != null) {
															map = deepCopy((Map<String, Field>)value);{//MapTypeSupport.deepCopy()
																Map<String, Field> copy = new LinkedHashMap<>();
																for (Map.Entry<String, Field> entry : map.entrySet()) {
																	String name = entry.getKey();
																	Utils.checkNotNull(name, "Map cannot have null keys");
																	Utils.checkNotNull(entry.getValue(), Utils.formatL("Map cannot have null values, key '{}'", name));
																	
																	/** 重点在这里, 
																	*		entry.getValue().clone() -> Field.clone() -> Field.clone() ...
																	*	这是一个递归调用; 若是Map结构的数据, 会不断的递归copye每个字段; 
																	*	而我们大部分的数据结构是 Map结构; 
																	*/
																	copy.put(entry.getKey(), entry.getValue().clone());// entry.getValue()也是一个Field,递归克隆;
																}
																return copy;
															}
														}
														return map;
													}
												}
											}
										}
									}
									if (attributes != null) {
									  this.attributes = new LinkedHashMap<>(attributes);
									}
								}
							}
							isInitialRecord = record.isInitialRecord();
						}
					}
				}
				recordCopy.addStageToStagePath(instanceName); //在header.map中加上"_.stagePath" -> instanceName 的kv对;
				recordCopy.createTrackingId();//用sourceId+ stagePath(即instanceName) 平均成trackingId;
				
				// 2. 什么是 InitialRecord ?(源数据?) 为什么对于InitialRecord还要再 clone一遍?
				if (recordCopy.isInitialRecord()) {//对于InitialRecord, 
					RecordImpl recordSource = recordCopy.clone();{// RecordImpl.clone()
						new RecordImpl(this);{
							header = record.header.clone();
							value = (record.value != null) ? record.value.clone() :
							isInitialRecord = record.isInitialRecord();
						}
					}
					
					recordCopy.getHeader().setSourceRecord(recordSource);
					recordCopy.setInitialRecord(false);//这里才将为置位false;
				}
				
				if (getStagePipe().getStage().getDefinition().getType() == StageType.SOURCE) {
					if (rateLimiterOptional.isPresent()) {
						rateLimiterOptional.get().acquire();
					}
				}
				
				//将数据加入通道中,传给下游; 
				if (lanes.length == 0) {// 我们大部分算子都是进入这里, lances传参为空;
					Preconditions.checkArgument(outputLanes.size() == 1, Utils.formatL("No stream has been specified and the stage '{}' has multiple output streams '{}'", instanceName, outputLanes));
					stageOutput.get(singleOutputLane).add(recordCopy);
				}
				
				// 3. 再进行一遍clone, 为什么? (用于快照输出的?)
				if (stageOutputSnapshot != null) {
					recordCopy = recordCopy.clone();
					if (lanes.length == 0) {
						stageOutputSnapshot.get(singleOutputLane).add(recordCopy);
					} else {
						for (String lane : lanes) {
						stageOutputSnapshot.get(lane).add(recordCopy);
						}
					}
				}
				size++;
	
			}
		}
	}
	
}



/** Source=poll模式时的, batch运行原理, 和 Multiplexer管道多输出的逻辑;
*   - StagePipe.process         :       是Original, Processor, Target算子的实际运行;
*   - MultiplexerPipe.process()     :   依据outputLances数量判断是否要clone()后分发给下游;
*/
ProductionPipelineRunner.runPollSource(){
    // 只要offset还未拉取完,就继续拉取; 每循环一遍,即运行一个批次(Batch)
    while (!offsetTracker.isFinished() && !stop && !finished) {
        
        // 一个pipeBatch即对应运行一个批次Batch 
        FullPipeBatch pipeBatch = createFullPipeBatch(Source.POLL_SOURCE_OFFSET_KEY, offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));
        
        // 首先,运行Origin
        processPipe(originPipe,pipeBatch){
            // 1. StagePipe(包括SourcePipe) 类Pipe: Original, Processor, Target 等具体三类算子;
            SourcePipe[extends StagePipe].process(){ // StagePipe.process()
                Batch batch = new FilterRecordBatch(batchImpl, predicates, getStage().getContext());
                String newOffset = getStage().execute(){ // StageRuntime.execute(){
                    String offsetStr= execute(callable, errorSink, eventSink, processedSink){
                        mainClassLoader = Thread.currentThread().getContextClassLoader();
                        context.setPushSourceContextDelegate(this);
                        String newOffset = callable.call();
                        return (def.getRecordsByRef() && !context.isPreview()) ? CreateByRef.call(callable) : newOffset;{
                            Callable<String> callable = () -> {
                                switch (getDefinition().getType()) {
                                    case SOURCE:
                                        newOffset = ((Source) getStage()).produce(previousOffset, batchSize, batchMaker);{
                                            
                                        }
                                        break;
                                    default: throw new IllegalStateException(Utils.format("Unknown stage ", getDefinition().getType()));
                                }
                                return newOffset;
                            };
                        }
                    }
                    return offsetStr;
                }
            }
        }
        
        // 然后, 运行其他算子:Processors, Targets
        String newOffset = pipeBatch.getNewOffset();
        ProductionPipelineRunner.runSourceLessBatch(start,pipeBatch,newOffset){
            try{
                ProductionPipelineRunner.executeRunner(pipeRunner, start, pipeBatch, entityName, newOffset, memoryConsumedByStage, stageBatchMetrics){
                    // 获取其OffsetCommitTrigger: offset的跟踪器
                    OffsetCommitTrigger offsetCommitTrigger = pipeRunner.getOffsetCommitTrigger();
                    // 运行 封装其他Stages的 Pipe 
                    ThrowingConsumer<Pipe> consumer = (pipe)->{};
                    pipeRunner.executeBatch(entityName, newOffset, start, consumer){//PipeRunner.executeBatch()
                        /** 遍历每个Pipe,每个pipe代表Pipeline中一个算子(包括Stage,Multiplexer管道,Observer观察者)
                        *	问题1: 各Stage的拓扑结构是如何定义的? 哪个先哪个后?
                            问题1: 为什么会有Observer? 
                        */
                        for(Pipe pipe : pipes) {
                            consumer.accept(pipe);{
                                pipe -> committed.set(processPipe(pipe, pipeBatch, committed.get(), entityName, newOffset, memoryConsumedByStage, stageBatchMetrics));{//ProductionPipelineRunner.processPipe()
                                    pipe.process(pipeBatch);{//Pipe.process() 执行其实现子类的processor方法
                                        //1. StagePipe(包括SourcePipe) 类Pipe: Original, Processor, Target 等具体三类算子;
                                        StagePipe.process(){// StagePipe.process()
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
                                        
                                        // 2. MultiplexerPipe: 就是用于一对多流转通道的实现;  但接多个输出时(outputLances >1)时,就采用clone的方法;
                                        MultiplexerPipe.process(){
                                            for (int i = 0; i < getInputLanes().size(); i++) {
                                                List<String> outputLanes = LaneResolver.getMatchingOutputLanes(inputStageLane, getOutputLanes());
                                                if (outputLanes.size() == 1) { //当 一对一输出模式时, 直接将records 转个下游用;
                                                    pipeBatch.moveLane(inputPipeLane, outputLanes.get(0));{//FullPipeBatch.moveLane()
                                                        fullPayload.put(outputLane, Preconditions.checkNotNull(fullPayload.remove(inputLane), Utils.formatL("Stream '{}' does not exist", inputLane)));
                                                    }
                                                } else { // 当接的输出有2个以上时, 采用record.clone()的方式; 克隆每一批records给每个outputLance;
                                                    pipeBatch.moveLaneCopying(inputPipeLane, outputLanes);{//FullPipeBatch.moveLaneCopying()
                                                        List<Record> records = Preconditions.checkNotNull(fullPayload.remove(inputLane), Utils.formatL("Stream '{}' does not exist", inputLane));
                                                        for (String lane : outputLanes) {
                                                            fullPayload.put(lane, createCopy(records));{//FullPipeBatch.createCopy()
                                                                List<Record> list = new ArrayList<>(records.size());
                                                                for (Record record : records) {
                                                                  list.add(((RecordImpl) record).clone()); // 每个record,克隆次数=  outputLances.size ;
                                                                }
                                                                return list;
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
    // 1个 Source => Processor -> Target (就3个Stage) Pipeline中的 pipes:List<Pipe> 的元素: 
    0 = {ObserverPipe@9266} "ObserverPipe[instance='MemoryGenerator2RefTrue_01' input='[MemoryGenerator2RefTrue_01OutputLane15952188734070::s]' output='[MemoryGenerator2RefTrue_01OutputLane15952188734070::o]']"
    1 = {MultiplexerPipe@9240} "MultiplexerPipe[instance='MemoryGenerator2RefTrue_01' input='[MemoryGenerator2RefTrue_01OutputLane15952188734070::o]' output='[MemoryGenerator2RefTrue_01OutputLane15952188734070--LastAppender02RefTrue_01::MemoryGenerator2RefTrue_01::m]']"
    2 = {StagePipe@9267} "StagePipe[instance='LastAppender02RefTrue_01' input='[MemoryGenerator2RefTrue_01OutputLane15952188734070--LastAppender02RefTrue_01::MemoryGenerator2RefTrue_01::m]' output='[LastAppender02RefTrue_01OutputLane15952281211860::s]']"
    3 = {ObserverPipe@9268} "ObserverPipe[instance='LastAppender02RefTrue_01' input='[LastAppender02RefTrue_01OutputLane15952281211860::s]' output='[LastAppender02RefTrue_01OutputLane15952281211860::o]']"
    4 = {MultiplexerPipe@9269} "MultiplexerPipe[instance='LastAppender02RefTrue_01' input='[LastAppender02RefTrue_01OutputLane15952281211860::o]' output='[LastAppender02RefTrue_01OutputLane15952281211860--ToError_02::LastAppender02RefTrue_01::m]']"
    5 = {StagePipe@9270} "StagePipe[instance='ToError_02' input='[LastAppender02RefTrue_01OutputLane15952281211860--ToError_02::LastAppender02RefTrue_01::m]' output='[]']"






