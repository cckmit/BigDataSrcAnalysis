/**
Checkpoint Coordinator 向所有 source 节点 trigger Checkpoint. 然后Source Task会在数据流中安插CheckPoint barrier
	source 会向流中触发Barrier，接收到Barrier的节点就会保存快照（包括source）
	
	source 节点向下游广播 barrier，这个 barrier 就是实现 Chandy-Lamport 分布式快照算法的核心，下游的 task 只有收到所有进来的 barrier 才会执行相应的 Checkpoint

下游的 sink 节点收集齐上游两个 input 的 barrier 之后，会执行本地快照，这里特地展示了 RocksDB incremental Checkpoint 的流程，首先 RocksDB 会全量刷数据到磁盘上（红色大三角表示），然后 Flink 框架会从中选择没有上传的文件进行持久化备份
sink 节点在完成自己的 Checkpoint 之后，会将 state handle 返回通知 Coordinator

当 Checkpoint coordinator 收集齐所有 task 的 state handle，就认为这一次的 Checkpoint 全局完成了，向持久化存储中再备份一个 Checkpoint meta 文件





*/


// Checkpoint核心类: 
JobManager进程: 
	CheckpointCoordinator
	ScheduledTrigger
	StateBackend
	
TaskExecutor进程:
	StreamTask
	CheckponitBarrier
	
	


Checkpoint的核心类是 CheckpointCoordinator: 检查点协调器
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




// ckp1. JobManager 中初始化 State Checkpoint 相关类;
// Checkpoint Coordinator


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



// ckp2. JobManager中 固定间隔 触发完成 Checkpoint的 详细流程; 
// 这还是在 JobMaster进程中; 

run:1841, CheckpointCoordinator$ScheduledTrigger (org.apache.flink.runtime.checkpoint)
call:511, Executors$RunnableAdapter (java.util.concurrent)
runAndReset:308, FutureTask (java.util.concurrent)
access$301:180, ScheduledThreadPoolExecutor$ScheduledFutureTask (java.util.concurrent)
run:294, ScheduledThreadPoolExecutor$ScheduledFutureTask (java.util.concurrent)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)


// CheckpointCoordinator.ScheduledTrigger.triggerCheckpoint() 固定频率 checkpointId 事件,

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

	// flink-1.12.2 src: 
	// ckp2.2 每次触发 ckp的入口: CheckpointCoordinator.triggerCheckpoint()
	CheckpointCoordinator.triggerCheckpoint(props,externalSaveLocation,isPeriodic){
		CheckpointTriggerRequest request = new CheckpointTriggerRequest(props, externalSavepointLocation, isPeriodic);
        Optional<CheckpointTriggerRequest> requestOp = chooseRequestToExecute(request);
		requestOp.ifPresent(this::startTriggeringCheckpoint);{//CheckpointCoordinator.startTriggeringCheckpoint()
			preCheckGlobalState(request.isPeriodic);
			final long timestamp = System.currentTimeMillis();
			
			CompletableFuture<CheckpointIdAndStorageLocation> initedCkpStorage = initializeCheckpoint(request.props, request.externalSavepointLocation);{
				return CompletableFuture.supplyAsync(()->{
					long checkpointID = checkpointIdCounter.getAndIncrement();
					CheckpointStorageLocation checkpointStorageLocation =  props.isSavepoint() 
						? checkpointStorage.initializeLocationForSavepoint()
                        : checkpointStorage.initializeLocationForCheckpoint();
					return new CheckpointIdAndStorageLocation(checkpointID, checkpointStorageLocation);
										
				});
			}
			final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture = initedCkpStorage.thenApplyAsync((checkpointIdAndStorageLocation) -> {
								// ckp2.2.1	创建1各Ckp事件和相关对象;
								createPendingCheckpoint();// 源码细节 如下 CheckpointCoordinator.createPendingCheckpoint()详解
							},timer);
			
			final CompletableFuture<?> coordinatorCheckpointsComplete = pendingCheckpointCompletableFuture.thenComposeAsync(
                            (pendingCheckpoint) ->
                                    OperatorCoordinatorCheckpoints
                                            .triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                                                    coordinatorsToCheckpoint,
                                                    pendingCheckpoint,
                                                    timer),
                            timer);
			
			
			final CompletableFuture<?> masterStatesComplete = coordinatorCheckpointsComplete.thenComposeAsync(
                            ignored -> {
                                PendingCheckpoint checkpoint =
                                        FutureUtils.getWithoutException(
                                                pendingCheckpointCompletableFuture);
                                return snapshotMasterState(checkpoint);
                            },
                            timer);
							
			CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete)
					.handleAsync((ignored, throwable) -> {
							final PendingCheckpoint checkpoint =FutureUtils.getWithoutException(pendingCheckpointCompletableFuture);
							if (throwable != null) {
								onTriggerFailure(request, throwable);
							}else{
								if (checkpoint.isDisposed()) {
									onTriggerFailure()
								}else{
									final long checkpointId =checkpoint.getCheckpointId();
									// 持久化 snapshot的入口?
									snapshotTaskState();// 源码细节 如下 CheckpointCoordinator.snapshotTaskState()详解
									
									coordinatorsToCheckpoint.forEach();
									onTriggerSuccess();
								}
							}
							return null;
						}, timer)
                            .exceptionally(
                                    error -> {
                                        if (!isShutdown()) {
                                            throw new CompletionException(error);
                                        } else if (findThrowable(
                                                        error, RejectedExecutionException.class)
                                                .isPresent()) {
                                            LOG.debug("Execution rejected during shutdown");
                                        } else {
                                            LOG.warn("Error encountered during shutdown", error);
                                        }
                                        return null;
                                    }));
			
		}
        return request.onCompletionPromise;
	}

	// ckp2.2.1	创建1个Ckp对象:PendingCheckpoint,并加到pending列表中等待调度; 
	// JobManger: CheckpointCoordinator.ScheduledTrigger.triggerCheckpoint() -> CheckpointCoordinator.triggerCheckpoint() 中 第一给就是异步 createPendingCheckpoint()
	// 日志:  Triggering checkpoint 818 (type=CHECKPOINT) @ 1642768539241 for job 345db01b21c61fb4e441287a7bb77daf.
	CheckpointCoordinator.createPendingCheckpoint(timestamp,checkpointID,checkpointStorageLocation){
		preCheckGlobalState(isPeriodic);
		PendingCheckpoint checkpoint =new PendingCheckpoint();
		pendingCheckpoints.put(checkpointID, checkpoint);
		ScheduledFuture<?> cancellerHandle = timer.schedule();
		// 日志:  Triggering checkpoint 818 (type=CHECKPOINT) @ 1642768539241 for job 345db01b21c61fb4e441287a7bb77daf.
		LOG.info("Triggering checkpoint {} (type={}) @ {} for job {}.",
					checkpointID,//818
					checkpoint.getProps().getCheckpointType(),	//CHECKPOINT
					timestamp,// 1642768539241
					job);//345db01b21c61fb4e441287a7bb77daf
		return checkpoint;
	}
	

	// ckp2.2.2 完成 ckp准备工作后, 异步触发 CheckpointCoordinator.snapshotTaskState()

	CheckpointCoordinator.snapshotTaskState(timestamp,checkpointID,checkpointStorageLocation,props,executions){
		final CheckpointOptions checkpointOptions =CheckpointOptions.create();
        for (Execution execution : executions) {
            if (props.isSynchronous()) {// postCheckpointAction != PostCheckpointAction.NONE; 
				
                execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions);{
					// 代码实现, 通下面一样; 参考下面;
					triggerCheckpointHelper(checkpointId, timestamp, checkpointOptions);
				}
            } else {
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





// ckp3. TaskExecutor中 所有 source 接受到ckp触发信号, 生成 Barrier 
//   TaskExecutor 接收到 ckp触发信号, 执行所有 算子的ckp 

// 到Task Executor 进程中了
// 到另一个进程,或miniCluster中的TM线程里:-> TaskExecutor.triggerCheckpoint()


// ckp3. 上个服务模块: 从JobManger的 CheckpointCoordinator.ScheduledTrigger.triggerCheckpoint() -> Execution.triggerCheckpointHelper() 中触发Rpc调用; 
// ckp3. 本线程主
// Mailbox.run() -> ActorCell.receiveMessage() -> AkkaRpcActor.handleRpcMessage() -> Method.invoke()
// 
TaskExecutor.triggerCheckpoint(){
    task.triggerCheckpointBarrier(checkpointId, checkpointTimestamp, checkpointOptions, advanceToEndOfEventTime);{//Task.
        invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);{
            SourceStreamTask.triggerCheckpointAsync(){
                return super.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);{
					// 另起线程, 异步执行 StreamTask.triggerCheckpoint()
                    return mailboxProcessor.getMainMailboxExecutor().submit(() -> triggerCheckpoint(checkpointMetaData, checkpointOptions,checkpointMetaData,checkpointOptions);{
                        MailboxExecutor.submit() -> StreamTask.triggerCheckpoint(){
                            boolean success = performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, advanceToEndOfEventTime);{//StreamTask.
                                final long checkpointId = checkpointMetaData.getCheckpointId();
                                actionExecutor.runThrowing(() -> {
									
									subtaskCheckpointCoordinator.checkpointState(checkpointMetaData,checkpointOptions);{
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



logCompletedInternal:67, AbstractSnapshotStrategy (org.apache.flink.runtime.state)
logSyncCompleted:54, AbstractSnapshotStrategy (org.apache.flink.runtime.state)
snapshot:237, DefaultOperatorStateBackend (org.apache.flink.runtime.state)
snapshotState:213, StreamOperatorStateHandler (org.apache.flink.streaming.api.operators)
snapshotState:162, StreamOperatorStateHandler (org.apache.flink.streaming.api.operators)
snapshotState:371, AbstractStreamOperator (org.apache.flink.streaming.api.operators)
checkpointStreamOperator:686, SubtaskCheckpointCoordinatorImpl (org.apache.flink.streaming.runtime.tasks)
buildOperatorSnapshotFutures:607, SubtaskCheckpointCoordinatorImpl (org.apache.flink.streaming.runtime.tasks)
takeSnapshotSync:572, SubtaskCheckpointCoordinatorImpl (org.apache.flink.streaming.runtime.tasks)
checkpointState:298, SubtaskCheckpointCoordinatorImpl (org.apache.flink.streaming.runtime.tasks)
lambda$performCheckpoint$9:1004, StreamTask (org.apache.flink.streaming.runtime.tasks)




// ckp4. TaskExecutor 中, operatorChain各算子准备 CheckpointBarrier并广播出去;
	// TaskExecutor.triggerCheckpoint() -> SourceStreamTask.triggerCheckpointAsync() 中新启线程执行 StreamTask.triggerCheckpoint()
	// StreamTask.triggerCheckpoint() -> StreamTask.performCheckpoint() -> SubtaskCheckpointCoordinatorImpl.checkpointState()
	
	// Task级别的 checkpoint, 核心就是: 遍历operatorChain 执行每个Operator.snapshotState()
	SubtaskCheckpointCoordinatorImpl.checkpointState(metadata,operatorChain){
		if (lastCheckpointId >= metadata.getCheckpointId()) {
			channelStateWriter.abort(metadata.getCheckpointId(), new CancellationException(), true);
			return;
		}
		// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
		// 遍历所有未closed的 Operator , 并执行其 prepareSnapshotPreBarrier
		operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());{
			for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators()) {
				if (!operatorWrapper.isClosed()) {
					operatorWrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId);{
						// 各算子集成的 
						StreamSource.prepareSnapshotPreBarrier()
						StreamFilter.prepareSnapshotPreBarrier()
						ProcessOperator.prepareSnapshotPreBarrier()
						StreamMap.prepareSnapshotPreBarrier()
					}
				}
			}
		}
		// Step (2): Send the checkpoint barrier downstream
		operatorChain.broadcastEvent(new CheckpointBarrier(id,timestamp,options));{
			// streamOutputs:RecordWriterOutput<?>[], 
			for (RecordWriterOutput<?> streamOutput : streamOutputs) {
				streamOutput.broadcastEvent(event, isPriorityEvent);{// RecordWriterOutput.broadcastEvent
					recordWriter.broadcastEvent(event, isPriorityEvent);{//RecordWriter.broadcastEvent()
						targetPartition.broadcastEvent(event, isPriorityEvent);{// BufferWritingResultPartition.broadcastEvent
							checkInProduceState();
							finishBroadcastBufferBuilder();
							finishUnicastBufferBuilders();
							
						}
						if (flushAlways) {
							flushAll();
						}
					}
				}
			}
		}
		
		// Step (3): Prepare to spill the in-flight buffers for input and output
		if (options.isUnalignedCheckpoint()) {
			channelStateWriter.finishOutput(metadata.getCheckpointId());
		}
		
		// Step (4): Take the state snapshot. This should be largely asynchronous, to not impact
		// 就是这里 执行各 Operator.snapshotState()方法获取快照;
		boolean snapshotSyncOk = takeSnapshotSync(snapshotFutures, metadata, metrics, options, operatorChain, isRunning);{//SubtaskCheckpointCoordinatorImpl.takeSnapshotSync
			
			CheckpointStreamFactory storage =checkpointStorage.resolveCheckpointStorageLocation(checkpointId, checkpointOptions.getTargetLocation());
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				if (!operatorWrapper.isClosed()) {
					OperatorSnapshotFutures snapshotFuture = buildOperatorSnapshotFutures();{// SubtaskCheckpointCoordinatorImpl.buildOperatorSnapshotFutures()
						OperatorSnapshotFutures snapshotInProgress =checkpointStreamOperator(op,checkpointMetaData,checkpointOptions);{
							return op.snapshotState(checkpointId,timestamp,checkpointOptions,factory);{// StreamOperator.snapshotState() 子类 AbstractStreamOperator.snapshotState
								return stateHandler.snapshotState();{//StreamOperatorStateHandler.snapshotState()
									KeyGroupRange keyGroupRange = null != keyedStateBackend? keyedStateBackend.getKeyGroupRange(): KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
									StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl();
									// 调用内部第二个待 inProgress,context 的 快照方法
									snapshotState(snapshotInProgress,snapshotContext);// 源码实现如下: StreamOperatorStateHandler.snapshotState()
									return snapshotInProgress;
								}
							}
						}
						if (op == operatorChain.getMainOperator()) {
							snapshotInProgress.setInputChannelStateFuture();
						}
						if (op == operatorChain.getTailOperator()) {
							snapshotInProgress.setResultSubpartitionStateFuture();
						}
						
						return snapshotInProgress;
					}
					operatorSnapshotsInProgress.put(operatorID,snapshotFuture);
				}
			}
		}
		
		if (snapshotSyncOk) {// 正常成功进入这里; 
			finishAndReportAsync(snapshotFutures, metadata, metrics, isRunning);{//SubtaskCheckpointCoordinatorImpl.
				asyncOperationsThreadPool.execute(new AsyncCheckpointRunnable(snapshotFutures));{
					AsyncCheckpointRunnable.run(){// 源码详解下面 AsyncCheckpointRunnable.run, 主要遍历各算子并执行callInternal(),最后再 reportCompletedSnapshot报告完成的; 
						for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry :operatorSnapshotsInProgress.entrySet()) {
							callInternal();
							logAsyncSnapshotComplete(startTime);
						}
						reportCompletedSnapshotStates();
					}
				}
			}
		}else{
			cleanup(snapshotFutures, metadata, metrics, new Exception("Checkpoint declined"));
		}
	}

	// ckp4.2 Task中单Stream Operator的snapshotState 状态快照
	// 核心是为该 ckpId的每一个算子, 创建一个AsyncSnapshotCallable,并定义其匿名内部方法: callInternal(),用于后面 finishAndReportAsync()时异步完成持久化; 
	// 定义持久化方法的路径: SubtaskCheckpointCoordinatorImpl.checkpointState() -> StreamOperatorStateHandler.snapshotState() -DefaultOperatorStateBackend.snapshotState(): new AsyncSnapshotCallable(){}
	StreamOperatorStateHandler.snapshotState(snapshotInProgress,snapshotContext){// StreamOperatorStateHandler.snapshotState()
		try {
			// CheckpointedStreamOperator 接口的实现类
			streamOperator.snapshotState(snapshotContext);{//CheckpointedStreamOperator 接口的实现类包括 
				
			}
			
			snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
			snapshotInProgress.setOperatorStateManagedFuture();
			if (null != operatorStateBackend) {
				// 持久化
				RunnableFuture<SnapshotResult> operatorStateManagedFuture =operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions);{
					BatchExecutionKeyedStateBackend
					AbstractKeyedStateBackend
					DefaultOperatorStateBackendSnapshotStrategy
					
					// 默认策略, hdfs, rackdb ?
					DefaultOperatorStateBackend.snapshotState(ckpId,time,streamFactory,ckpOptions){
						RunnableFuture<SnapshotResult> snapshotRunner =snapshotStrategy.snapshot(checkpointId, timestamp,);{//DefaultOperatorStateBackendSnapshotStrategy.
							// 都为空,不需要持久化;
							if (registeredOperatorStates.isEmpty() && registeredBroadcastStates.isEmpty()) {
								return DoneFuture.of(SnapshotResult.empty());
							}
							
							Thread.currentThread().setContextClassLoader(userClassLoader);
							if (!registeredOperatorStates.isEmpty()) {
								for (Map.Entry<String, PartitionableListState<?>> entry :registeredOperatorStates.entrySet()) {
									PartitionableListState<?> listState = entry.getValue();
									if (null != listState) {
										listState = listState.deepCopy();// 先copy 一个对象? 避免修改后内存的影响? 
									}
									registeredOperatorStatesDeepCopies.put(entry.getKey(), listState);
								}
							}
							
							AsyncSnapshotCallable<SnapshotResult> snapshotCallable = new AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>>() {
								SnapshotResult<OperatorStateHandle> callInternal() {
									
								}
								
								void logAsyncSnapshotComplete(long startTime) {
									
								}
							}
							
							FutureTask<SnapshotResult> task =snapshotCallable.toAsyncSnapshotFutureTask(closeStreamOnCancelRegistry);{
								return new AsyncSnapshotTask(taskRegistry);
							}
							
							if (!asynchronousSnapshots) {// 默认asynchronousSnapshots=true,异步,不进入下面 task.run()
								task.run();{//AsyncSnapshotTask.run()
									
								}
							}
							return task;
						}
						snapshotStrategy.logSyncCompleted(streamFactory, syncStartTime);{//AbstractSnapshotStrategy.logSyncCompleted
							logCompletedInternal(LOG_SYNC_COMPLETED_TEMPLATE, checkpointOutDescription, startTime);{
								long duration = (System.currentTimeMillis() - startTime);
								LOG.debug(template, description, checkpointOutDescription, Thread.currentThread(), duration);
							}
						}
						return snapshotRunner;
					}
					
					// 堆内 keyedState 
					HeapKeyedStateBackend.snapshotState(ckpId,time,streamFactory,ckpOptions){
						
					}
					

					
				}
				snapshotInProgress.setKeyedStateManagedFuture(operatorStateManagedFuture);
			}
			
			
		}catch (Exception snapshotException) {
			snapshotInProgress.cancel();
			snapshotContext.closeExceptionally();
			throw new CheckpointException(snapshotFailMessage);
		}
	}


	StreamOperator.snapshotState(){
		
		// 算子执行某次 ckp
		StreamOperator.snapshotState(checkpointId,timestamp,checkpointOptions,factory);{
			//CDC2Hudi的 StreamMap, StreamFilter, StreamSource, ProcessOperator 等是调用 父类方法
			AbstractStreamOperator.snapshotState(checkpointId,timestamp,checkpointOptions,factory){
				return stateHandler.snapshotState(this,getOperatorName(), checkpointId,timestamp);{// StreamOperatorStateHandler.snapshotState()
					KeyGroupRange keyGroupRange = null != keyedStateBackend? keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
					
					StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl();
					snapshotState();{// StreamOperatorStateHandler.snapshotState()
						try {
							streamOperator.snapshotState(snapshotContext);// 进入下面的 AbstractUdfStreamOperator.snapshotState(context) 方法;
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
		
		// 触发执行 context上下文的 ckp; 
		StreamOperator.snapshotState(StateSnapshotContext context){
			
			// checkpoint 
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
			
		}
		
	}

	// ckp4.3  自定义的 udf的 相关Operator的 snapshot方法
	AbstractUdfStreamOperator.snapshotState(context){// extends AbstractStreamOperator [implements StreamOperator,CheckpointedStreamOperator]
		super.snapshotState(context);
		StreamingFunctionUtils.snapshotFunctionState(context, getOperatorStateBackend(), userFunction);{
			while (true) {
				boolean snapshotOk = trySnapshotFunctionState(context, backend, userFunction);{//StreamingFunctionUtils.trySnapshotFunctionState()
					if (userFunction instanceof CheckpointedFunction) {
						// 执行 udf的 snapshotState() 
						((CheckpointedFunction) userFunction).snapshotState(context);{
							// 不同的实现类? 
							
							//Hudi 的 Clean算子: Sink function that cleans the old commits.
							CompactionCommitSink.snapshotState() 调用父方法 CleanFunction.snapshotState(),代码如上;
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
							
							StreamWriterFunction.snapshotState(FunctionSnapshotContext functionSnapshotContext){
								
								flushRemaining(false);{
									this.currentInstant = instantToWrite(hasData());
									if (buckets.size() > 0) {
										this.buckets.values()
											.forEach(bucket -> {
												List<HoodieRecord> records = bucket.writeBuffer();
												bucket.preWrite(records);
												writeStatus.addAll(writeFunction.apply(records, currentInstant));
												records.clear();
												bucket.reset();
											});
									}
									
									final WriteMetadataEvent event = WriteMetadataEvent.builder()
												.writeStatus(writeStatus)
												.lastBatch(true)
												.endInput(endInput)
												.build();
									this.eventGateway.sendEventToCoordinator(event);
									this.writeClient.cleanHandles();
									this.writeStatuses.addAll(writeStatus);
									
								}
								
								reloadWriteMetaState();{// StreamWriterFunction.reloadWriteMetaState()
									this.writeMetadataState.clear();
									WriteMetadataEvent event = WriteMetadataEvent.builder()
										.writeStatus(new ArrayList<>(writeStatuses))
										.bootstrap(true)
										.build();
									this.writeMetadataState.add(event);
									writeStatuses.clear();
								}
							
							}
							
							BucketAssignFunction.snapshotState(FunctionSnapshotContext context){
								this.bucketAssigner.reset();{
									bucketInfoMap.clear();
									newFileAssignStates.clear();
								}
							}
							
						}
						return true;
					}
					if (userFunction instanceof ListCheckpointed) {
						ListState<Serializable> listState = backend.getListState(listStateDescriptor);
						listState.clear();
						return true;
					}
					
					// 非 Checkpointed 自定义函数, 没有 snapshotState方法, 返回false
					return false;
				}
				if (snapshotOk) {
					break;
				}
			}
		}
	}
	

	// ckp4.5 异步完成各ckp的持久化,并报告完成情况; 
	// SubtaskCheckpointCoordinatorImpl.checkpointState() -> finishAndReportAsync() - asyncOperationsThreadPool.execute() 新启线程 异步 checkpoint并 report给 TM/JM; 
	AsyncCheckpointRunnable.run(){
		final long asyncStartDelayMillis = (asyncStartNanos - asyncConstructionNanos) / 1_000_000L;
		for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry :operatorSnapshotsInProgress.entrySet()) {
			OperatorSnapshotFinalizer finalizedSnapshots =new OperatorSnapshotFinalizer(snapshotInProgress);{
				SnapshotResult<OperatorStateHandle> operatorManaged =FutureUtils.runIfNotDoneAndGet(snapshotFutures.getOperatorStateManagedFuture());{//FutureUtils.runIfNotDoneAndGet()
					FutureTask.run()-> call(); {//AsyncSnapshotCallable.call()
						final long startTime = System.currentTimeMillis();
						// 由DefaultOperatorStateBackendSnapshotStrategy$1 中匿名内部类实现
						// SubtaskCheckpointCoordinatorImpl.checkpointState()->StreamOperatorStateHandler.snapshotState()->DefaultOperatorStateBackend.snapshotState(): new AsyncSnapshotCallable(){}
						T result = callInternal();{
							DefaultOperatorStateBackendSnapshotStrategy$1.callInternal(){
								CheckpointStateOutputStream localOut =streamFactory.createCheckpointStateOutputStream();
								snapshotCloseableRegistry.registerCloseable(localOut);
								List<StateMetaInfoSnapshot> broadcastMetaInfoSnapshots =new ArrayList<>(registeredBroadcastStatesDeepCopies.size());
								
								// ... write them all in the checkpoint stream ...
								DataOutputView dov = new DataOutputViewStreamWrapper(localOut);
								OperatorBackendSerializationProxy backendSerializationProxy =new OperatorBackendSerializationProxy(operatorMetaInfoSnapshots, broadcastMetaInfoSnapshots);
								backendSerializationProxy.write(dov);{//OperatorBackendSerializationProxy.write(dov)
									super.write(out);{//VersionedIOReadableWritable.write
										out.writeInt(getVersion());{//DataOutputStream.writeInt()
											out.write((v >>> 24) & 0xFF);{//java.io.OutputStream
												// 对于 hdfs checkpoint方式: 加到 writeBuffer:byte[] 字节流中,批量刷出; 
												FsCheckpointStreamFactory.FsCheckpointStateOutputStream.write(int b){
													if (pos >= writeBuffer.length) {
														flushToFile();
													}
													writeBuffer[pos++] = (byte) b;
												}
											}
											out.write((v >>> 16) & 0xFF);
											out.write((v >>>  8) & 0xFF);
											out.write((v >>>  0) & 0xFF);
											incCount(4);
										}
									}
									
									writeStateMetaInfoSnapshots(operatorStateMetaInfoSnapshots, out);{
										out.writeShort(snapshots.size());{//DataOutputViewStreamWrapper 父类方法 DataOutputStream.writeShort()
											out.write((v >>> 8) & 0xFF);
											out.write((v >>> 0) & 0xFF);
											incCount(2);
										}
										
										for (StateMetaInfoSnapshot state : snapshots) {
											StateMetaInfoSnapshotReadersWriters
												.getWriter(){return CurrentWriterImpl.INSTANCE;}
												.writeStateMetaInfoSnapshot(state, out);{// StateMetaInfoSnapshotReadersWriters.CurrentWriterImpl.writeStateMetaInfoSnapshot()
													final Map<String, String> optionsMap = snapshot.getOptionsImmutable();
													outputView.writeUTF(snapshot.getName());
													outputView.writeInt(snapshot.getBackendStateType().ordinal());
													outputView.writeInt(optionsMap.size());
													
													outputView.writeInt(serializerConfigSnapshotsMap.size());
													for (Map.Entry<String, TypeSerializerSnapshot<?>> entry : serializerConfigSnapshotsMap.entrySet()) {
														final String key = entry.getKey();
														outputView.writeUTF(entry.getKey());{// DataOutputViewStreamWrapper 父类方法 DataOutputStream.writeUTF()
															writeUTF(str, this);{//DataOutputStream.writeUTF(str,out)
																int strlen = str.length();
																int utflen = 0;
																for (int i = 0; i < strlen; i++) {
																	c = str.charAt(i);
																	utflen++;
																}
																
																out.write(bytearr, 0, utflen+2);{// 
																	FsCheckpointStreamFactory.FsCheckpointStateOutputStream.write(byte[] b, int off, int len){
																		if (len < writeBuffer.length) {
																			final int remaining = writeBuffer.length - pos;
																			
																			System.arraycopy(b, off, writeBuffer, pos, len);
																			pos += len;
																		}else{
																			flushToFile();
																			outStream.write(b, off, len);
																		}
																	}
																}
															}
														}
														TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(outputView,entry.getValue(),);
													}
													
												}
										}
									}
									
									writeStateMetaInfoSnapshots(broadcastStateMetaInfoSnapshots, out);
								}
							}
						}
						// 由DefaultOperatorStateBackendSnapshotStrategy$1 中匿名内部类实现
						logAsyncSnapshotComplete(startTime);
						return result;
					}
				}
				SnapshotResult<OperatorStateHandle> operatorRaw =FutureUtils.runIfNotDoneAndGet(snapshotFutures.getOperatorStateRawFuture());
				SnapshotResult<StateObjectCollection<InputChannelStateHandle>> inputChannel =snapshotFutures.getInputChannelStateFuture().get();
				
				SnapshotResult<StateObjectCollection<ResultSubpartitionStateHandle>> resultSubpartition =snapshotFutures.getResultSubpartitionStateFuture().get();
				jobManagerOwnedState =OperatorSubtaskState.builder()
						.setManagedOperatorState(singletonOrEmpty(operatorManaged.getJobManagerOwnedSnapshot()))
						.setRawOperatorState(singletonOrEmpty(operatorRaw.getJobManagerOwnedSnapshot()))
						.setManagedKeyedState(singletonOrEmpty(keyedManaged.getJobManagerOwnedSnapshot()))
						.setRawKeyedState(singletonOrEmpty(keyedRaw.getJobManagerOwnedSnapshot()))
						.setInputChannelState(emptyIfNull(inputChannel.getJobManagerOwnedSnapshot()))
						.setResultSubpartitionState(emptyIfNull(resultSubpartition.getJobManagerOwnedSnapshot()))
						.build();

			}
			
			bytesPersistedDuringAlignment +=finalizedSnapshots.getJobManagerOwnedState().getResultSubpartitionState().getStateSize();
			bytesPersistedDuringAlignment += finalizedSnapshots.getJobManagerOwnedState().getInputChannelState().getStateSize();
			jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(operatorID, finalizedSnapshots.getJobManagerOwnedState());
		}
		checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);
		if (asyncCheckpointState.compareAndSet(AsyncCheckpointState.RUNNING, AsyncCheckpointState.COMPLETED)) {
			reportCompletedSnapshotStates();
		}
	}


write:218, FsCheckpointStreamFactory$FsCheckpointStateOutputStream (org.apache.flink.runtime.state.filesystem)
write:42, ForwardingOutputStream (org.apache.flink.runtime.util)
write:88, DataOutputStream (java.io)
writeString:837, StringValue (org.apache.flink.types)
serialize:68, StringSerializer (org.apache.flink.api.common.typeutils.base)
serialize:31, StringSerializer (org.apache.flink.api.common.typeutils.base)
serialize:349, PojoSerializer (org.apache.flink.api.java.typeutils.runtime)
serialize:147, CompositeSerializer (org.apache.flink.api.common.typeutils)
writeState:136, CopyOnWriteStateMapSnapshot (org.apache.flink.runtime.state.heap)
writeStateInKeyGroup:105, AbstractStateTableSnapshot (org.apache.flink.runtime.state.heap)
writeStateInKeyGroup:38, CopyOnWriteStateTableSnapshot (org.apache.flink.runtime.state.heap)
callInternal:204, HeapSnapshotStrategy$1 (org.apache.flink.runtime.state.heap)
callInternal:167, HeapSnapshotStrategy$1 (org.apache.flink.runtime.state.heap)
call:78, AsyncSnapshotCallable (org.apache.flink.runtime.state)
run:266, FutureTask (java.util.concurrent)
runIfNotDoneAndGet:618, FutureUtils (org.apache.flink.runtime.concurrent)
<init>:54, OperatorSnapshotFinalizer (org.apache.flink.streaming.api.operators)
run:127, AsyncCheckpointRunnable (org.apache.flink.streaming.runtime.tasks)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)


write:218, FsCheckpointStreamFactory$FsCheckpointStateOutputStream (org.apache.flink.runtime.state.filesystem)
writeBoolean:139, DataOutputStream (java.io)
write:123, KeyedBackendSerializationProxy (org.apache.flink.runtime.state)
callInternal:181, HeapSnapshotStrategy$1 (org.apache.flink.runtime.state.heap)
callInternal:167, HeapSnapshotStrategy$1 (org.apache.flink.runtime.state.heap)
call:78, AsyncSnapshotCallable (org.apache.flink.runtime.state)
run:266, FutureTask (java.util.concurrent)
runIfNotDoneAndGet:618, FutureUtils (org.apache.flink.runtime.concurrent)
<init>:54, OperatorSnapshotFinalizer (org.apache.flink.streaming.api.operators)
run:127, AsyncCheckpointRunnable (org.apache.flink.streaming.runtime.tasks)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)


write:218, FsCheckpointStreamFactory$FsCheckpointStateOutputStream (org.apache.flink.runtime.state.filesystem)
writeInt:198, DataOutputStream (java.io)
write:41, VersionedIOReadableWritable (org.apache.flink.core.io)
write:120, KeyedBackendSerializationProxy (org.apache.flink.runtime.state)
callInternal:181, HeapSnapshotStrategy$1 (org.apache.flink.runtime.state.heap)
callInternal:167, HeapSnapshotStrategy$1 (org.apache.flink.runtime.state.heap)
call:78, AsyncSnapshotCallable (org.apache.flink.runtime.state)
run:266, FutureTask (java.util.concurrent)
runIfNotDoneAndGet:618, FutureUtils (org.apache.flink.runtime.concurrent)
<init>:54, OperatorSnapshotFinalizer (org.apache.flink.streaming.api.operators)
run:127, AsyncCheckpointRunnable (org.apache.flink.streaming.runtime.tasks)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)



// ckp5. Operator 算子的 ckp处理;

// 不同算子的 snapshotState 状态快照 实现方法 
ListCheckpointed.snapshotState();
CheckpointedFunction.snapshotState(FunctionSnapshotContext context){
	 //用户自实现Operator的 保存快照接口方法;
    ExampleIntegerSource.snapshotState();
	
	
	AbstractUdfStreamOperator.snapshotState();{
		
	}
	
	
}



// ckp6. Sink 算子的 ckp处理 和 发给 JM





// ckp7. JobManager 收到 sink完成ckp信号, 生成ckp持久化文件;


	// ckp7.1 JobMaster: 收到 sink完成ckp信号, 生成ckp持久化文件;
	// 上游触发: 收到TaskExecutor ? 的 checkpointCoordinatorGateway.acknowledgeCheckpoint() Rpc调用后 SchedulerBase.acknowledgeCheckpoint() -> CheckpointCoordinator.receiveAcknowledgeMessage()
	// 打印日志: CheckpointCoordinator [] Completed checkpoint 818 for job 345db01b21c61fb4e441287a7bb77daf (4126714 bytes in 674 ms).

	JobMaster.acknowledgeCheckpoint(jobID,executionAttemptID,checkpointId,checkpointState){// 实现自CheckpointCoordinatorGateway接口
		schedulerNG.acknowledgeCheckpoint(jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);{//SchedulerBase.acknowledgeCheckpoint()
			mainThreadExecutor.assertRunningInMainThread();
			AcknowledgeCheckpoint ackMessage =new AcknowledgeCheckpoint();
			String taskManagerLocationInfo = retrieveTaskManagerLocation(executionAttemptID);
			if (checkpointCoordinator != null) {
				ioExecutor.execute(()->{
					checkpointCoordinator.receiveAcknowledgeMessage(ackMessage, taskManagerLocationInfo);{//CheckpointCoordinator.receiveAcknowledgeMessage()
						long checkpointId = message.getCheckpointId();
						final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);
						
						if (checkpoint != null && !checkpoint.isDisposed()) {
							TaskAcknowledgeResult ackResult = checkpoint.acknowledgeTask();
							switch (ackResult){
								case SUCCESS:
									if (checkpoint.isFullyAcknowledged()) {
										completePendingCheckpoint(checkpoint);{
											Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();
											sharedStateRegistry.registerAll(operatorStates.values());
											
											completedCheckpoint = pendingCheckpoint.finalizeCheckpoint(checkpointsCleaner, this::scheduleTriggerRequest, executor);
											failureManager.handleCheckpointSuccess(pendingCheckpoint.getCheckpointId());
											
											try{
												completedCheckpointStore.addCheckpoint(completedCheckpoint, checkpointsCleaner, this::scheduleTriggerRequest);
											}finally {
												pendingCheckpoints.remove(checkpointId);
												scheduleTriggerRequest();
											}
											
											dropSubsumedCheckpoints(checkpointId);
											// 日志: Completed checkpoint 818 for job 345db01b21c61fb4e441287a7bb77daf (4126714 bytes in 674 ms).
											LOG.info("Completed checkpoint {} for job {} ({} bytes in {} ms).",checkpointId,job,);
											sendAcknowledgeMessages(checkpointId, completedCheckpoint.getTimestamp());
										}
									}
									break;
								case DUPLICATE: break;
								case UNKNOWN: 
									discardSubtaskState();
									break;
								case DISCARDED:
									discardSubtaskState();
									break;
							}
							return true;
						}
						
					}
				});
			}else{
				log.error(errorMessage, jobGraph.getJobID());
			}
		}
	}




	// ckp7.2 JobMaster: 待 所有task和master ckp都完成后, 通知hudi commit;
	// OperatorCoordinatorHolder.notifyCheckpointComplete() -> StreamWriteOperatorCoordinator.commitInstant() doCommit()
	// 从 Task Executor 中发出的 RPC请求 notifyCheckpointComplete()? 
	// LOG: StreamWriteOperatorCoordinator [] Commit instant [20220124103157] success!
	// LOG: AbstractHoodieWriteClient [] Committing 20220124103157 action deltacommit
	// LOG: StreamWriteOperatorCoordinator [] Commit instant [20220124103157] success!

	OperatorCoordinatorHolder.notifyCheckpointComplete(){
		mainThreadExecutor.execute(() -> coordinator.notifyCheckpointComplete(checkpointId));{//StreamWriteOperatorCoordinator.notifyCheckpointComplete()
			executor.execute(()->{
				final boolean committed = commitInstant(this.instant);{
					if (Arrays.stream(eventBuffer).allMatch(Objects::isNull)) {
						return false;
					}
					List<WriteStatus> writeResults = Arrays.stream(eventBuffer)
						.filter(Objects::nonNull).map(WriteMetadataEvent::getWriteStatuses)
						.flatMap(Collection::stream).collect(Collectors.toList());
						
					doCommit(instant, writeResults);{// StreamWriteOperatorCoordinator.doCommit()
						long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
						long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
						if (!hasErrors || this.conf.getBoolean(FlinkOptions.IGNORE_FAILED)) {
							boolean success = writeClient.commit(instant, writeResults);// 源码详见 HoodieFlinkWriteClient.commit()
							if (success) {
								reset();
								// LOG: StreamWriteOperatorCoordinator [] Commit instant [20220124103157] success!
								LOG.info("Commit instant [{}] success!", instant);
							}else{
								throw new HoodieException(String.format("Commit instant [%s] failed!", instant));
							}
						}else {
							throw new HoodieException(String.format("Commit instant [%s] failed and rolled back !", instant));
						}
					}
					return true;
				}
				if (committed) {
					if (tableState.scheduleCompaction) {
						writeClient.scheduleCompaction(Option.empty());{//AbstractHoodieWriteClient.scheduleCompaction(extraMetadata)
							String instantTime = HoodieActiveTimeline.createNewInstantTime();
							return scheduleCompactionAtInstant(instantTime, extraMetadata) ? Option.of(instantTime) : Option.empty();{
								return scheduleTableService(instantTime, extraMetadata, TableServiceType.COMPACT).isPresent();{//AbstractHoodieWriteClient.
									try{
										this.txnManager.beginTransaction();
										LOG.info("Scheduling table service " + tableServiceType);
										return scheduleTableServiceInternal(instantTime, extraMetadata, tableServiceType);{//AbstractHoodieWriteClient.
											switch (tableServiceType) {
												case CLUSTER:
													Option<HoodieClusteringPlan> clusteringPlan = createTable(config, hadoopConf).scheduleClustering(context, instantTime, extraMetadata);
													return clusteringPlan.isPresent() ? Option.of(instantTime) : Option.empty();
												case COMPACT:
													LOG.info("Scheduling compaction at instant time :" + instantTime);
													// 1. 建表, HoodieFlinkWriteClient.createTable() 实现;
													HoodieTable table = createTable(config, hadoopConf);{//HoodieFlinkWriteClient.createTable()
														return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);{
															HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
																	.setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
																	.setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())))
																	.build();{
																		new HoodieTableMetaClient(conf, basePath,,);{
																			LOG.info("Loading HoodieTableMetaClient from " + basePath);
																			this.metaPath = new Path(basePath, METAFOLDER_NAME).toString();
																			this.timelineLayoutVersion = layoutVersion.orElseGet(() -> tableConfig.getTimelineLayoutVersion().get());
																			// LOG: HoodieTableMetaClient [] Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
																			LOG.info("Finished Loading Table of type " + tableType + "(version=" + timelineLayoutVersion + ", baseFileFormat=" + ") from " + basePath);
																			if (loadActiveTimelineOnLoad) {
																				getActiveTimeline();
																			}
																		}
																	}
															return HoodieFlinkTable.create(config, context, metaClient);
														}
													}
													// 2. (新启任务线程)执行 compact
													Option<HoodieCompactionPlan> compactionPlan = table.scheduleCompaction(context, instantTime, extraMetadata);{//HoodieFlinkMergeOnReadTable.scheduleCompaction
														scheduleCompactionExecutor = new FlinkScheduleCompactionActionExecutor();
														return scheduleCompactionExecutor.execute();{//BaseScheduleCompactionActionExecutor.execute
															HoodieCompactionPlan plan = scheduleCompaction();{//FlinkScheduleCompactionActionExecutor.scheduleCompaction
																boolean compactable = needCompact(config.getInlineCompactTriggerStrategy());
																if (compactable) {
																	// LOG: FlinkScheduleCompactionActionExecutor [] Generating compaction plan for merge on read table hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
																	LOG.info("Generating compaction plan for merge on read table " + config.getBasePath());
																	HoodieFlinkMergeOnReadTableCompactor compactor = new HoodieFlinkMergeOnReadTableCompactor();
																	SyncableFileSystemView fileSystemView = (SyncableFileSystemView) table.getSliceView();{//HoodieTable.getSliceView()
																		return getViewManager().getFileSystemView(metaClient);{// FileSystemViewManager.getFileSystemView()
																			return globalViewMap.computeIfAbsent(metaClient.getBasePath(),(path) -> viewCreator.apply(metaClient, viewStorageConfig));{
																				FileSystemViewManager.createViewManager(context,metadataConfig,config,){
																					final SerializableConfiguration conf = context.getHadoopConf();
																					switch (config.getStorageType()) {
																						case EMBEDDED_KV_STORE:
																							 return new FileSystemViewManager(context, config, (metaClient, viewConf) -> createRocksDBBasedFileSystemView(conf, viewConf, metaClient));
																						case SPILLABLE_DISK:
																						case MEMORY: 
																							LOG.info("Creating in-memory based Table View");
																							return new FileSystemViewManager(context, config,()-> createInMemoryFileSystemView());
																						case REMOTE_ONLY:
																						case REMOTE_FIRST:
																						default: throw new IllegalArgumentException();
																					}
																				}
																			}
																		}
																	}
																	
																	return compactor.generateCompactionPlan(context, table, config, instantTime, fgInPendingCompactionAndClustering);
																}
																return new HoodieCompactionPlan();
															}
															
															if (plan != null && (plan.getOperations() != null) && (!plan.getOperations().isEmpty())) {
																extraMetadata.ifPresent(plan::setExtraMetadata);
																HoodieInstant compactionInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
																table.getActiveTimeline().saveToCompactionRequested(compactionInstant,TimelineMetadataUtils.serializeCompactionPlan(plan));
															}
															return Option.empty();
														}
													}
													
													return compactionPlan.isPresent() ? Option.of(instantTime) : Option.empty();
												case CLEAN: 
													createTable(config, hadoopConf).scheduleCleaning(context, instantTime, extraMetadata);
													return cleanerPlan.isPresent() ? Option.of(instantTime) : Option.empty();
												default: throw new IllegalArgumentException("Invalid TableService " + tableServiceType);
											}
										}
									}finally {
										this.txnManager.endTransaction();
									}
								}
							}
						}
					}
					// start new instant.
					startInstant();
					// sync Hive if is enabled
					syncHiveIfEnabled();{// StreamWriteOperatorCoordinator.syncHiveIfEnabled()
						if (tableState.syncHive) {
							this.hiveSyncExecutor.execute(this::syncHive, "sync hive metadata for instant %s", this.instant);{
								// 另起线程 异步完成 syncHive Hive同步
								StreamWriteOperatorCoordinator.syncHive(); {
									HiveSyncTool syncTool = hiveSyncContext.hiveSyncTool();{
										new HiveSyncTool(this.syncConfig, this.hiveConf, this.fs);
									}
									syncTool.syncHoodieTable();{//HiveSyncTool.syncHoodieTable()
										if (hoodieHiveClient != null){
											doSync();{//HiveSyncTool.doSync
												switch (hoodieHiveClient.getTableType()) {
													case COPY_ON_WRITE:
														syncHoodieTable(snapshotTableName, false, false);
														break;
													case MERGE_ON_READ:
														// sync a RO table for MOR
														syncHoodieTable(roTableName.get(), false, true);
														// sync a RT table for MOR
														syncHoodieTable(snapshotTableName, true, false);
													break;
													default: throw new InvalidTableException(hoodieHiveClient.getBasePath());
												}
											}
										}
									}
								}
							}
						}
					}
					// sync metadata if is enabled
					syncMetadataIfEnabled();
				}
			});
		}
	}

	// ckp7.2.1 完成notifyCheckpointComplete 后 JobMaster 第一步就是提交: hudi数据提交
	HoodieFlinkWriteClient.commit(){//HoodieFlinkWriteClient.commit()
		List<HoodieWriteStat> writeStats = writeStatuses.parallelStream().map(WriteStatus::getStat).collect(Collectors.toList());
		return commitStats(instantTime, writeStats, extraMetadata, commitActionType, partitionToReplacedFileIds);{//AbstractHoodieWriteClient.
			// LOG: AbstractHoodieWriteClient [] Committing 20220124103157 action deltacommit
			LOG.info("Committing " + instantTime + " action " + commitActionType);
			HoodieTable table = createTable(config, hadoopConf);{//HoodieFlinkWriteClient.createTable()
				return HoodieFlinkTable.create(config, (HoodieFlinkEngineContext) context);{
					HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getHadoopConf().get()).setBasePath(config.getBasePath())
							.setLoadActiveTimelineOnLoad(true).setConsistencyGuardConfig(config.getConsistencyGuardConfig())
							.setLayoutVersion(Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())))
							.build();{
								new HoodieTableMetaClient(conf, basePath,,);{
									LOG.info("Loading HoodieTableMetaClient from " + basePath);
									this.metaPath = new Path(basePath, METAFOLDER_NAME).toString();
									this.timelineLayoutVersion = layoutVersion.orElseGet(() -> tableConfig.getTimelineLayoutVersion().get());
									// LOG: HoodieTableMetaClient [] Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
									LOG.info("Finished Loading Table of type " + tableType + "(version=" + timelineLayoutVersion + ", baseFileFormat=" + ") from " + basePath);
									if (loadActiveTimelineOnLoad) {
										getActiveTimeline();
									}
								}
							}
					return HoodieFlinkTable.create(config, context, metaClient);
				}
			}
			
			HoodieCommitMetadata metadata = CommitUtils.buildMetadata(stats, extraMetadata, operationType,commitActionType);
			HeartbeatUtils.abortIfHeartbeatExpired(instantTime, table, heartbeatClient, config);
			this.txnManager.beginTransaction();
			try {
				preCommit(instantTime, metadata);
				commit(table, commitActionType, instantTime, metadata, stats);{// AbstractHoodieWriteClient.commit
					HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
					finalizeWrite(table, instantTime, stats);{
						Timer.Context finalizeCtx = metrics.getFinalizeCtx();
						table.finalizeWrite(context, instantTime, stats);{//HoodieTable.finalizeWrite
							reconcileAgainstMarkers();{//HoodieTable.reconcileAgainstMarkers()
								String basePath = getMetaClient().getBasePath();
								Set<String> invalidDataPaths = getInvalidDataPaths(markers);
								invalidDataPaths.removeAll(validDataPaths);
								deleteInvalidFilesByPartitions(context, invalidPathsByPartition);
							}
						}
					}
					activeTimeline.saveAsComplete(new HoodieInstant());
				}
				postCommit(table, metadata, instantTime, extraMetadata);
				// LOG: AbstractHoodieWriteClient [] Committed 20220124103157
				LOG.info("Committed " + instantTime);
				releaseResources();
			} finally {
				this.txnManager.endTransaction();
			}
			
			runTableServicesInline(table, metadata, extraMetadata);
			emitCommitMetrics(instantTime, metadata, commitActionType);
			return true;
		}
	}
	
	// ckp7.2.2 异步同步 hive 表;
	// OperatorCoordinatorHolder.notifyCheckpointComplete() -> syncHiveIfEnabled() 新启线程执行: StreamWriteOperatorCoordinator.syncHive() -> HiveSyncTool.syncHoodieTable()
	HiveSyncTool.syncHoodieTable(tableName,useRealtimeInputFormat,readAsOptimized){
		
		//LOG: HiveSyncTool [] Trying to sync hoodie table unknown_ro with base path hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi of type MERGE_ON_READ
		LOG.info("Trying to sync hoodie table " + tableName + " with base path " + hoodieHiveClient.getBasePath()+ " of type " + hoodieHiveClient.getTableType());
		
	}














