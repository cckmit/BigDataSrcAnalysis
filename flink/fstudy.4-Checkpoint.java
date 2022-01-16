
Checkpoint的核心类是CheckpointCoordinator: 检查点协调器
    - coordinates the distributed snapshots of operators and state: 核心是对Operators算子(对象序列化?)和状态进行持久化; 
    - It triggers the checkpoint by sending the messages to the relevant tasks and collects the checkpoint acknowledgements; 
        通过CheckpointCoordinator.timer:ScheduledExecutor 这个定时线程来定时发消息(类?)给每个Task,并接收ack消息来完成Checkpoint;
    - 执行Checkpoint本质就是调用一次 ScheduledTrigger.triggerCheckpoint()方法; 
    

    异步屏障快照(asynchronous barrier snapshotting, ABS)算法
    
触发1次 ScheduledTrigger.triggerCheckpoint()方法的执行逻辑;
    - 先检查是否满足触发条件, 满足的话生成1个 PendingCheckpoint 放入1个以时间戳为key的Map中, 等待发送给Tasks;
    - 各Task接收到 ? 消息, 触发执行TaskExecutor的triggerCheckpoint()方法, 这个方法最终分4步走:        TaskExecutor.triggerCheckpoint()
        * 首先(这个TaskExecutor里的)Source算子, 接受到 检查点屏障(CheckpointBarrier)时生成自己的snapshot快照(主要是offset信息), 并把屏障往下游算子传;      SourceStreamTask.triggerCheckpointAsync();
        * 其次: 每个Operator算子在接受完 它所有上游的检查点屏障(CheckponitBarrier)后, 触发生成自己的snapshot快照(主要是算子的成员变量?和state状态数据), 并把屏障继续往下游传,如此循环
        * 最后: Sink算子昨晚snapshot后,就发个 ack信息给 JobManager的CheckpointCoordinator协调器, 这次checkpont就算成功了; 


 StreamTask.triggerCheckpoint() -> performCheckpoint() 
    operatorChain.prepareSnapshotPreBarrier(checkpointId);
    operatorChain.broadcastCheckpointBarrier();
    checkpointState(checkpointMetaData);
        // 各算子同步执行Checkpoint;
        * for (StreamOperator<?> op : allOperators) 
            op[AbstractStreamOperator].snapshotState();
        // 异步执行Checkpoint
        owner.asyncOperationsThreadPool.execute(new AsyncCheckpointRunnable());
 

 
        
checkpoint:        
- 在所有的source的子任务中注入checkpointBarrier，TaskManager在收到所有上游广播的CheckpointBarrier 后，触发checkpoint。
- 当整个DAG图的子任务的checkpoint都做完之后，会汇报给JobManager    
    
    
    prepareSnapshotPreBarrier(checkpointId);
                                    operatorChain.broadcastCheckpointBarrier();
                                    // 3. 第三步 
                                    checkpointState
    


问题1: Checkpoint 启动线程和方法:
    - 线程: flink-akka.actor.default-sispatcher-3;
    - 启动方法 JobMaster.startScheduling() -> DefaultScheduler.startSchedulingInternal()
        -> executionGraph.transitionToRunning() -> ExecutionGraph.notifyJobStatusChange()
            -> CheckpointCoordinatorDeActivator.jobStatusChanges() ->  CheckpointCoordinator.scheduleTriggerWithDelay()



JobMaster.resetAndStartScheduler(){
    validateRunsInMainThread();
    if (schedulerNG.requestJobStatus() == JobStatus.CREATED) {
        schedulerAssignedFuture = CompletableFuture.completedFuture(null);
        schedulerNG.setMainThreadExecutor(getMainThreadExecutor());
    }else{
        final SchedulerNG newScheduler = createScheduler(newJobManagerJobMetricGroup);
        schedulerAssignedFuture = schedulerNG.getTerminationFuture().handle(
            assignScheduler(newScheduler, newJobManagerJobMetricGroup);
        );
    }

    //如果不是新建 schedulerNG,则执行完 assignScheduler线程后,再执行 startScheduling()线程和方法;
    schedulerAssignedFuture.thenRun(this::startScheduling);{

        JobMaster.startScheduling(){
            schedulerNG.registerJobStatusListener(new JobManagerJobStatusListener());
            schedulerNG.startScheduling();{//JobMaster.startScheduling()
                registerJobMetrics();
                startSchedulingInternal();{//DefaultScheduler.startSchedulingInternal()
                    prepareExecutionGraphForNgScheduling();{//SchedulerBase.
                        executionGraph.transitionToRunning();{
                            if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING){//ExecutionGraph.transitionState()
                                assertRunningInJobMasterMainThread();
                                notifyJobStatusChange(newState, error);{//ExecutionGraph.notifyJobStatusChange()
                                    for (JobStatusListener listener : jobStatusListeners) {
                                        listener.jobStatusChanges(getJobID(), newState, timestamp, serializedError);{

                                            /** 这个是Checkpoint相关的Listener 监听到新job后,启动了定时checkpiontScheculer调度线程;
                                            *
                                            */
                                            CheckpointCoordinatorDeActivator.jobStatusChanges(){//CheckpointCoordinatorDeActivator.jobStatusChanges()
                                                coordinator.startCheckpointScheduler();{
                                                    currentPeriodicTrigger = scheduleTriggerWithDelay(getRandomInitDelay());{//CheckpointCoordinator.scheduleTriggerWithDelay()
                                                        return timer.scheduleAtFixedRate(new ScheduledTrigger(),initDelay, baseInterval, TimeUnit.MILLISECONDS);{//ScheduledExecutorServiceAdapter.
                                                            // 这里就启动 Checkpoint.duration定时频率的线程,取做checkpoint;
                                                            return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
                                                        }
                                                    }
                                                }
                                            }

                                            JobManagerJobStatusListener.jobStatusChanges(){}
                                        }
                                    }
                                }
                            }) {
                                throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
                            }
                        }
                    }
                    schedulingStrategy.startScheduling();
                }
            }
        }
    }
}



// 这还是在 JobMaster进程中; 

timer.scheduleAtFixedRate(new ScheduledTrigger(),initDelay, baseInterval, TimeUnit.MILLISECONDS);{// ScheduledExecutorServiceAdapter.scheduleAtFixedRate()
    // 这里就启动 Checkpoint.duration定时频率的线程,取做checkpoint;
    return scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);{//Executors$DelegatedScheduledExecutorService.
        
        CheckpointCoordinator.ScheduledTrigger.run(){
            triggerCheckpoint(System.currentTimeMillis(), true);{//ScheduledTrigger.triggerCheckpoint()
                return triggerCheckpoint(timestamp, checkpointProperties, null, isPeriodic, false);{//CheckpointCoordinator.triggerCheckpoint()
                    for (Execution execution: executions) {
                        execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);{// Execution.triggerCheckpoint()
                            triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions, false);{
                                taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, 
                                checkpointOptions, advanceToEndOfEventTime);{//RpcTaskManagerGateway.triggerCheckpoint()
                                    // 通过akka 动态代理机制: triggerCheckpoint:-1, $Proxy15  -> invoke:129, AkkaInvocationHandler ..
                                    
                                    // 到另一个进程,或miniCluster中的TM线程里:-> TaskExecutor.triggerCheckpoint()
                                    TaskExecutor.triggerCheckpoint() -> Task.triggerCheckpointBarrier() -> SourceStreamTask.triggerCheckpointAsync()
                                        -> StreamTask.triggerCheckpoint() -> StreamTask.performCheckpoint() -> StreamTask.checkpointState()
                                            -> AbstractStreamOperator.snapshotState() 
                                                -> userFunction.snapshotState() 
                                            // 详细源码 参见下文代码;
                                    
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}


// 2.1 TaskExecutor 接收到 ckp触发信号, 执行所有 算子的ckp 





// 到Task Executor 进程中了
// 到另一个进程,或miniCluster中的TM线程里:-> TaskExecutor.triggerCheckpoint()
TaskExecutor.triggerCheckpoint(){
    task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions, advanceToEndOfEventTime);{//Task.
        invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);{
            SourceStreamTask.triggerCheckpointAsync(){
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);{
                    return mailboxProcessor.getMainMailboxExecutor().submit(() -> triggerCheckpoint(checkpointMetaData, checkpointOptions,checkpointMetaData,checkpointOptions);{
                        MailboxExecutor.submit() -> StreamTask.triggerCheckpoint(){
                            boolean success = performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, advanceToEndOfEventTime);{//StreamTask.
                                final long checkpointId = checkpointMetaData.getCheckpointId();
                                actionExecutor.runThrowing(() -> {
                                    // 先准备1个Barrier,会流转给所有下游 Operators;
                                    operatorChain.prepareSnapshotPreBarrier(checkpointId);
                                    operatorChain.broadcastCheckpointBarrier();
                                    // 3. 第三步 
                                    checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);{//StreamTask.checkpointState()
                                        CheckpointingOperation checkpointingOperation = new CheckpointingOperation();
                                        checkpointingOperation.executeCheckpointing();{
                                            // 同步执行各算子的checkpoint 
                                            for (StreamOperator<?> op : allOperators) {
                                                checkpointStreamOperator(op);{// StreamTask$CheckpointingOperation.checkpointStreamOperator()
                                                    OperatorSnapshotFutures snapshotInProgress = op.snapshotState();{//AbstractStreamOperator.snapshotState()
                                                        OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();
                                                        StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl();
                                                        snapshotState(snapshotContext);{
                                                            StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);{
                                                                while (true) {
                                                                    if (trySnapshotFunctionState(context, backend, userFunction){
                                                                        if (userFunction instanceof CheckpointedFunction) {
                                                                            ((CheckpointedFunction)userFunction).snapshotState(context);
                                                                            return true;
                                                                        }
                                                                        
                                                                        if (userFunction instanceof ListCheckpointed) {
                                                                            List<Serializable> partitionableState = ((ListCheckpointed)userFunction).snapshotState(context.getCheckpointId(), context.getCheckpointTimestamp());{
                                                                                //用户自实现Operator的 保存快照接口方法;
                                                                                ExampleIntegerSource.snapshotState();
                                                                            }
                                                                            ListState listState = backend.getSerializableListState();
                                                                            listState.clear();
                                                                        }
                                                                    }) {
                                                                        break;
                                                                    }
                                                                }
                                                            } 
                                                        }
                                                    }
                                                }
                                            }
                                            
                                            // 生成异步Checkpoint,并线程池执行
                                            AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable();
                                            owner.cancelables.registerCloseable(asyncCheckpointRunnable);
                                            owner.asyncOperationsThreadPool.execute(asyncCheckpointRunnable);
                                        }
                                    }
                                });
                            }
                        }
                    }

                }
            }
            
            // 还有其他Task 类型?
        }
    }
}

Task.run().doRun(){
	StreamTask.invoke()
	StreamTask.runMailboxLoop()
	StreamTask.processInput()
	StreamTaskNetworkInput.emitNext(){
		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
					currentRecordDeserializer = null;
				}
				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					return InputStatus.MORE_AVAILABLE;
				}
			}
			
			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();{//CheckpointedInputGate
				Optional<BufferOrEvent> next = inputGate.pollNext();
				if (!next.isPresent()) { // 什么状态 会是 空buffer ? io还没好? ckp还没到触发时间 ? 
					return handleEmptyBuffer();
				}
				
				// 
				BufferOrEvent bufferOrEvent = next.get();
				if (bufferOrEvent.isEvent()) { // 什么情况下, 
					return handleEvent(bufferOrEvent);{// CheckpointedInputGate.handleEvent
						Class<? extends AbstractEvent> eventClass = bufferOrEvent.getEvent().getClass();
						if (eventClass == CheckpointBarrier.class) {
							barrierHandler.processBarrier(checkpointBarrier, bufferOrEvent.getChannelInfo());{
								// while reading the first barrier , It can handle/track just single checkpoint at a time.
								SingleCheckpointBarrierHandler.processBarrier(barrier,channelInfo){
									if (currentCheckpointId > barrierId || (currentCheckpointId == barrierId && !isCheckpointPending())) {
										return;
									}
									if (currentCheckpointId < barrierId) {
										currentCheckpointId = barrier.getId();
										if (controller.preProcessFirstBarrier(channelInfo, barrier)) {
											notifyCheckpoint(barrier);{//CheckpointBarrierHandler.notifyCheckpoint
												CheckpointMetaData checkpointMetaData =new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());
												toNotifyOnCheckpoint.triggerCheckpointOnBarrier(checkpointMetaData, checkpointBarrier.getCheckpointOptions(), checkpointMetrics);{//StreamTask.
													boolean isCheckpointOk = performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);{//StreamTask.performCheckpoint()
														if (isRunning) {
															actionExecutor.runThrowing(()->{
																subtaskCheckpointCoordinator.checkpointState();{//SubtaskCheckpointCoordinatorImpl.checkpointState()
																	
																}
															});
															return true;
														}else{
															
															return false;
														}
													}
													if (isCheckpointOk) {
														if (isSynchronousSavepointId(checkpointMetaData.getCheckpointId())) {
															runSynchronousSavepointMailboxLoop();
														}
													}
												}
											}
										}
									}
								}
								// Once it has observed all checkpoint barriers for a checkpoint ID, notifies its listener of a completed checkpoint.
								CheckpointBarrierTracker.processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo);
								
							}
						}else if (eventClass == CancelCheckpointMarker.class) {
							barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
						}else if (eventClass == EndOfPartitionEvent.class) {
							barrierHandler.processEndOfPartition();
						} else if (eventClass == EventAnnouncement.class) {
							barrierHandler.processBarrierAnnouncement();
						} else if (bufferOrEvent.getEvent().getClass() == EndOfChannelStateEvent.class) {
							upstreamRecoveryTracker.handleEndOfRecovery(bufferOrEvent.getChannelInfo());
						}
						
					}
				}else if (bufferOrEvent.isBuffer()) {
					barrierHandler.addProcessedBytes(bufferOrEvent.getBuffer().getSize());
				}
				return next;
			}
			
		}
	}
}


	StreamTask.triggerCheckpoint() -> performCheckpoint() 
		operatorChain.prepareSnapshotPreBarrier(checkpointId);
		operatorChain.broadcastCheckpointBarrier();
		checkpointState(checkpointMetaData);
			// 各算子同步执行Checkpoint;
			* for (StreamOperator<?> op : allOperators) 
				op[AbstractStreamOperator].snapshotState();
			// 异步执行Checkpoint
			owner.asyncOperationsThreadPool.execute(new AsyncCheckpointRunnable());
	 

	// Task级别的 checkpoint, 核心就是: 遍历operatorChain 执行每个Operator.snapshotState()
	SubtaskCheckpointCoordinatorImpl.checkpointState(metadata,operatorChain){
		if (lastCheckpointId >= metadata.getCheckpointId()) {
			channelStateWriter.abort(metadata.getCheckpointId(), new CancellationException(), true);
			return;
		}
		// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
		operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());
		// Step (2): Send the checkpoint barrier downstream
		operatorChain.broadcastEvent(new CheckpointBarrier());
		// Step (3): Prepare to spill the in-flight buffers for input and output
		if (options.isUnalignedCheckpoint()) {
			channelStateWriter.finishOutput(metadata.getCheckpointId());
		}
		// Step (4): Take the state snapshot. This should be largely asynchronous, to not impact
		boolean snapshotSyncOk = takeSnapshotSync(snapshotFutures, metadata, metrics, options, operatorChain, isRunning);{//SubtaskCheckpointCoordinatorImpl.takeSnapshotSync
			CheckpointStreamFactory storage =checkpointStorage.resolveCheckpointStorageLocation(checkpointId, checkpointOptions.getTargetLocation());
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				if (!operatorWrapper.isClosed()) {
					OperatorSnapshotFutures snapshotFuture = buildOperatorSnapshotFutures();{// SubtaskCheckpointCoordinatorImpl.buildOperatorSnapshotFutures()
						OperatorSnapshotFutures snapshotInProgress =checkpointStreamOperator();{
							return op.snapshotState(checkpointId,timestamp,checkpointOptions,factory);{// StreamOperator.snapshotState() 子类 AbstractStreamOperator.snapshotState
								return stateHandler.snapshotState();{//StreamOperatorStateHandler.snapshotState()
									KeyGroupRange keyGroupRange = null != keyedStateBackend? keyedStateBackend.getKeyGroupRange(): KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
									StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl();
									snapshotState();{// StreamOperatorStateHandler.snapshotState()
										try {
											streamOperator.snapshotState(snapshotContext);
										}catch (Exception snapshotException) {
											snapshotInProgress.cancel();
											snapshotContext.closeExceptionally();
											throw new CheckpointException(snapshotFailMessage);
										}
									}
									return snapshotInProgress;
								}
							}
						}
						return snapshotInProgress;
					}
					operatorSnapshotsInProgress.put(operatorID,snapshotFuture);
				}
			}
		}
		if (snapshotSyncOk) {
			finishAndReportAsync(snapshotFutures, metadata, metrics, isRunning);
		}else{
			cleanup(snapshotFutures, metadata, metrics, new Exception("Checkpoint declined"));
		}
	}



CheckpointedStreamOperator.snapshotState(StateSnapshotContext context){
	
	// 自定义的 udf的 实现类
	AbstractUdfStreamOperator.snapshotState(context){// extends AbstractStreamOperator [implements StreamOperator,CheckpointedStreamOperator]
		super.snapshotState(context);
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);{
			while (true) {
				boolean snapshotOk = trySnapshotFunctionState(context, backend, userFunction);{//StreamingFunctionUtils.trySnapshotFunctionState()
					if (userFunction instanceof CheckpointedFunction) {
						((CheckpointedFunction) userFunction).snapshotState(context);{
							// 不同的实现类? 
							
							//Hudi 的 Clean算子: Sink function that cleans the old commits.
							hudi.sink.CleanFunction.snapshotState(){
								if (conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED) && !isCleaning) {
									this.writeClient.startAsyncCleaning();{//HoodieFlinkWriteClient.startAsyncCleaning
										this.asyncCleanerService = AsyncCleanerService.startAsyncCleaningIfEnabled(this);{
											if (writeClient.getConfig().isAutoClean() && writeClient.getConfig().isAsyncClean()) {
												asyncCleanerService = new AsyncCleanerService(writeClient, instantTime);
												asyncCleanerService.start(null);{//AsyncCleanerService.start
													Pair<CompletableFuture, ExecutorService> res = startService();{//AsyncCleanerService.startService()
														return Pair.of(CompletableFuture.supplyAsync(() -> {// 异步执行 writeClient.clean() 清理工作
															writeClient.clean(cleanInstantTime);{//AbstractHoodieWriteClient.clean
																return clean(cleanInstantTime, true);{//AbstractHoodieWriteClient.clean()
																	LOG.info("Cleaner started");
																	HoodieCleanMetadata metadata = createTable(config, hadoopConf).clean(context, cleanInstantTime);
																	if (timerContext != null && metadata != null) {
																		long durationMs = metrics.getDurationInMs(timerContext.stop());
																		metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
																	}
																	return metadata;
																}
															}
															return true;
														}), executor);
													}
													future = res.getKey();// 这里生成 future,下面 furtion.get()
													executor = res.getValue();
													// 就是新启线程 futrue.get()并 shutdown
													monitorThreads(onShutdownCallback);{ThreadExecutor.submit(() -> {
														try {
															future.get();
														}finally { // 都进行 shutdown 关闭什么服务? 
															shutdown = true;
															shutdown(false);
														}
													});}
												}
											}
											return asyncCleanerService;
										}
									}
									this.isCleaning = true;
								}
							}
							
						}
						return true;
					}
					if (userFunction instanceof ListCheckpointed) {
						ListState<Serializable> listState = backend.getListState(listStateDescriptor);
						listState.clear();
					}
					return true;
				}
				if (snapshotOk) {
					break;
				}
			}
		}
	}
}


// 2.2 不同算子的 snapshotState 状态快照 实现方法 
ListCheckpointed.snapshotState();
CheckpointedFunction.snapshotState(FunctionSnapshotContext context){
	 //用户自实现Operator的 保存快照接口方法;
    ExampleIntegerSource.snapshotState();
	
	
	AbstractUdfStreamOperator.snapshotState();{
		
	}
	
	
}



	
Caused by: org.apache.flink.util.SerializedThrowable:
 Task java.util.concurrent.FutureTask@7042bcc9 rejected from java.util.concurrent.ThreadPoolExecutor@6acee30f
 [Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
	at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2063) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.reject(ThreadPoolExecutor.java:830) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1379) ~[?:1.8.0_261]
	at java.util.concurrent.AbstractExecutorService.submit(AbstractExecutorService.java:112) ~[?:1.8.0_261]
	at java.util.concurrent.Executors$DelegatedExecutorService.submit(Executors.java:678) ~[?:1.8.0_261]
	at org.apache.hudi.async.HoodieAsyncService.monitorThreads(HoodieAsyncService.java:154) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.async.HoodieAsyncService.start(HoodieAsyncService.java:133) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.AsyncCleanerService.startAsyncCleaningIfEnabled(AsyncCleanerService.java:62) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.HoodieFlinkWriteClient.startAsyncCleaning(HoodieFlinkWriteClient.java:272) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.CleanFunction.snapshotState(CleanFunction.java:84) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.trySnapshotFunctionState(StreamingFunctionUtils.java:118) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.snapshotFunctionState(StreamingFunctionUtils.java:99) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.snapshotState(AbstractUdfStreamOperator.java:89) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:205) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	... 23 more
2022-01-13 17:02:51,753 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
















