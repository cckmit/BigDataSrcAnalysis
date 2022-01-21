

	// JmTemp.2 Hudi 与Hive同步,提交作业的 代码;
	StreamWriteOperatorCoordinator.notifyCheckpointComplete(){
		executor.execute(()->{
			final boolean committed = commitInstant(this.instant);
			if (committed) {
				writeClient.scheduleCompaction(Option.empty());
			}
			startInstant();
			// sync Hive if is enabled
			syncHiveIfEnabled();{//StreamWriteOperatorCoordinator.syncHiveIfEnabled
				if (tableState.syncHive) {this.hiveSyncExecutor.execute(this::syncHive, this.instant);}{//StreamWriteOperatorCoordinator.syncHive()
					hiveSyncContext
						.hiveSyncTool(){//HiveSyncContext.hiveSyncTool
							return new HiveSyncTool(this.syncConfig, this.hiveConf, this.fs);{
								super(configuration.getAllProperties(), fs);
								this.hoodieHiveClient = new HoodieHiveClient(cfg, configuration, fs);{
									if (!StringUtils.isNullOrEmpty(cfg.syncMode)) {
										HiveSyncMode syncMode = HiveSyncMode.of(cfg.syncMode);
										switch (syncMode) {
											case HMS: ddlExecutor = new HMSDDLExecutor(configuration, cfg, fs); break;
											case HIVEQL: ddlExecutor = new HiveQueryDDLExecutor(cfg, fs, configuration); break;
											case JDBC: 
												ddlExecutor = new JDBCExecutor(cfg, fs);{
													this.config = config;
													createHiveConnection(config.jdbcUrl, config.hiveUser, config.hivePass);{
														// HiveDriver 位于 hive-exec-xx.jar中 ; 报错ClassNotFoundException: org.apache.hudi.org.apache.hive.jdbc.HiveDriver
														Class.forName("org.apache.hive.jdbc.HiveDriver");
														this.connection = DriverManager.getConnection(jdbcUrl, hiveUser, hivePass);{
															return (getConnection(url, info, Reflection.getCallerClass()));{
																for(DriverInfo aDriver : registeredDrivers) {
																	Connection con = aDriver.driver.connect(url, info);{ //java.sql.Driver.connect()接口
																		HiveDriver.connect(){
																			return this.acceptsURL(url) ? new HiveConnection(url, info) : null;
																		}
																	}
																}
															}
														}
													}
												}
												break;
										}
									}else{
										ddlExecutor = cfg.useJdbc ? new JDBCExecutor(cfg, fs) : new HiveQueryDDLExecutor(cfg, fs, configuration);
									}
									// 这里调用了 hive-exec-xx.jar包的 Hive.get(HiveConf c) 方法; 会报错 NoSuchMethodError: org.apache.hadoop.hive.ql.metadata.Hive.get(Lorg
									this.client = Hive.get(configuration).getMSC();{// org.apache.hadoop.hive.ql.metadata.Hive.get(HiveConf c)
										return getInternal(c, false, false, true);
									}
								}
							}
						}
						.syncHoodieTable();
				}
			}
			
			// sync metadata if is enabled
			syncMetadataIfEnabled();
			
		});
	}














/** 1. JM模块: YarnSessionClusterEntrypoint进程 启动
*
*/




/** 2. JM模块: JobManager 处理JobSubmit并 调度资源,执行;
*
*/



// 2.1 处理Cli提交的JobSubmit(RPC请求):  flink-akka.actor.default-dispatcher-3 线程:
Dispatcher.submitJob(JobGraph jobGraph, Time timeout){//Dispatcher.
    if (isDuplicateJob(jobGraph.getJobID())) {
        return FutureUtils.completedExceptionally(new DuplicateJobSubmissionException(jobGraph.getJobID()));
    }else if (isPartialResourceConfigured(jobGraph)) { //偏爱/倾向于 资源配置?
    }else{ //不是重复的: 正常进入这里;
        return internalSubmitJob(jobGraph);{//Dispatcher.internalSubmitJob
            final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob).thenApply(ignored -> Acknowledge.get());{
                Dispatcher.waitForTerminatingJobManager(){
                    jobManagerTerminationFuture.thenComposeAsync((ignored) -> {
                        jobManagerTerminationFutures.remove(jobId);
                        return action.apply(jobGraph);{//这里的action 即 persistAndRunJob()方法:{
                            persistAndRunJob(){// action = persistAndRunJob()
                                runJob(jobGraph); //代码细节详见下一条:
                                
                            }
                        }
                    }):
                }
            }
    
            return persistAndRunFuture.handleAsync((acknowledge, throwable) -> {});
        }
    }
}




// 2.2 JobManager: flink-akka.actor.default-dispatcher-2 线程: 



FlinkUserCodeClassLoader.loadClass(){
	return loadClassWithoutExceptionHandling(name, resolve);{//ChildFirstClassLoader.loadClassWithoutExceptionHandling
		Class<?> c = findLoadedClass(name);
		if (c == null) {
			for (String alwaysParentFirstPattern : alwaysParentFirstPatterns) {
				if (name.startsWith(alwaysParentFirstPattern)) {
					return super.loadClassWithoutExceptionHandling(name, resolve);{// FlinkUserCodeClassLoader.
						return super.loadClass(name, resolve);{//ClassLoader.loadClass()
							Class<?> c = findLoadedClass(name);
							if (c == null) {
								c = findBootstrapClassOrNull(name);
								if (c == null) {
									// 这里在找 org.apache.flink.connectors.hive.HiveSource 时,进到这里
									c = findClass(name);{//URLClassLoader.findClass()
										result = AccessController.doPrivileged();
										if (result == null) {
											throw new ClassNotFoundException(name);
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




// 其他版本, v1.12.4? 
Dispatcher.persistAndRunJob(){
	jobGraphWriter.putJobGraph(jobGraph);
	runJob(jobGraph, ExecutionType.SUBMISSION);{//Dispatcher
        final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);{
            final RpcService rpcService = getRpcService();
            return CompletableFuture.supplyAsync();
        }
        
        return jobManagerRunnerFuture
            .thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner(){
                // 上面的 CompletableFuture.supplyAsync(); 执行完后, 就触发该startJobManagerRunner()执行;
                Dispatcher.startJobManagerRunner();{
                    jobManagerRunner.getResultFuture().handleAsync(()->{});
                    
                    jobManagerRunner.start();{// JobManagerRunnerImpl.start()
                        leaderElectionService.start(this);{//EmbeddedLeaderService.EmbeddedLeaderElectionService
                            addContender(this, contender);{
                                if (!allLeaderContenders.add(service)) throw new IllegalStateException();
                                
                                updateLeader().whenComplete((aVoid, throwable) -> {fatalError(throwable);});{
                                    EmbeddedLeaderService.updateLeader(){//
                                        EmbeddedLeaderElectionService leaderService = allLeaderContenders.iterator().next();
                                        
                                        return execute(new GrantLeadershipCall(leaderService.contender, leaderSessionId, LOG));{
                                            return CompletableFuture.runAsync(runnable, notificationExecutor);{
                                                GrantLeadershipCall.run(){
                                                    contender.grantLeadership(leaderSessionId);{//JobManagerRunnerImpl.
                                                        leadershipOperation = leadershipOperation.thenCompose((ignored) -> {
                                                            return verifyJobSchedulingStatusAndStartJobManager(leaderSessionID);{
                                                                //代码如下
                                                                final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();
                                                                return jobSchedulingStatusFuture.thenCompose(()->{
                                                                    return startJobMaster(leaderSessionId);
                                                                })
                                                            }
                                                        });
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
            }))
            .thenApply(FunctionUtils.nullFn())
            .whenCompleteAsync(
                (ignored, throwable) -> {
                    if (throwable != null) jobManagerRunnerFutures.remove(jobGraph.getJobID());
                },
                getMainThreadExecutor());
    
	}
}




/** 3.xx JobManager进程: JM启动和管理 : JobMaster.start()
*
*/



// 3.xx mini-cluster-io-thread-2 线程: 
verifyJobSchedulingStatusAndStartJobManager(){//JobManagerRunnerImpl.verifyJobSchedulingStatusAndStartJobManager()
    final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();
    return jobSchedulingStatusFuture.thenCompose(jobSchedulingStatus -> {
        if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
            return jobAlreadyDone();
        } else {
            return startJobMaster(leaderSessionId);{//JobManagerRunnerImpl.startJobMaster()
                runningJobsRegistry.setJobRunning(jobGraph.getJobID());
                startFuture = jobMasterService.start(new JobMasterId(leaderSessionId));{//JobMaster.start()
                    start();{// 父方法 RpcEndpoint.start()
						rpcServer.start();
					}
                    return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);{
                        // 中间一堆的装换;
                        JobMaster.startJobExecution(){
                            startJobMasterServices();{
                                startHeartbeatServices();
                                slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
                                scheduler.start(getMainThreadExecutor());
                                reconnectToResourceManager(new FlinkException("Starting JobMaster component."));
                                
                                resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());{//
                                    EmbeddedLeaderService.EmbeddedLeaderRetrievalService.start(){
                                        addListener(this, listener);{//EmbeddedLeaderService.addListener()
                                            notifyListener(currentLeaderAddress, currentLeaderSessionId, listener);{//EmbeddedLeaderService.notifyListener()
                                                return CompletableFuture.runAsync(new NotifyOfLeaderCall(address, leaderSessionId, listener, LOG), notificationExecutor);{
                                                    NotifyOfLeaderCall.run(){
                                                        listener.notifyLeaderAddress(address, leaderSessionId);{
                                                            runAsync(() -> notifyOfNewResourceManagerLeader(){// 异步执行该 notifyOfNewResourceManagerLeader()方法;
                                                                ResourceManagerLeaderListener.notifyOfNewResourceManagerLeader(){
                                                                    resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
                                                                    reconnectToResourceManager(); // 源码详解下面;
                                                                }
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            log.info("Starting execution of job {} ({}) under job master id {}.");
							
							resetAndStartScheduler();{
								
							}
							return Acknowledge.get();
                        }
                        
                    }
                    
                }
                
                return startFuture.thenAcceptAsync((Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader());
            }
        }
    });
}

// 2.2 一次Job(管理这)的启动和 Job提交; 
JobMaster.start(){//JobMaster.start()
	start();{// 父方法 RpcEndpoint.start()
		rpcServer.start();
	}
	return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);{
		// 中间一堆的装换;
		JobMaster.startJobExecution(){
			startJobMasterServices();{
				startHeartbeatServices();
				slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
				scheduler.start(getMainThreadExecutor());
				reconnectToResourceManager(new FlinkException("Starting JobMaster component."));
				
				resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());{//
					EmbeddedLeaderService.EmbeddedLeaderRetrievalService.start(){
						addListener(this, listener);{//EmbeddedLeaderService.addListener()
							notifyListener(currentLeaderAddress, currentLeaderSessionId, listener);{//EmbeddedLeaderService.notifyListener()
								return CompletableFuture.runAsync(new NotifyOfLeaderCall(address, leaderSessionId, listener, LOG), notificationExecutor);{
									NotifyOfLeaderCall.run(){
										listener.notifyLeaderAddress(address, leaderSessionId);{
											runAsync(() -> notifyOfNewResourceManagerLeader(){// 异步执行该 notifyOfNewResourceManagerLeader()方法;
												ResourceManagerLeaderListener.notifyOfNewResourceManagerLeader(){
													resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
													reconnectToResourceManager(); // 源码详解下面;
												}
											});
										}
									}
								}
							}
						}
					}
				}
			}
			log.info("Starting execution of job {} ({}) under job master id {}.");
			
			resetAndStartScheduler();{// JobManager.resetAndStartScheduler()
				validateRunsInMainThread();
				
				final JobManagerJobMetricGroup newJobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
				final SchedulerNG newScheduler =createScheduler(executionDeploymentTracker, newJobManagerJobMetricGroup);
				
				// 执行资源调度; 
				schedulerAssignedFuture =schedulerNG
                            .getTerminationFuture()
                            .handle((ignored, throwable) -> {
                                        newScheduler.setMainThreadExecutor(getMainThreadExecutor());
                                        assignScheduler(newScheduler, newJobManagerJobMetricGroup);
                                        return null;
                                    });
									
				// 调度完成, 就执行 作业运行; 
				FutureUtils.assertNoException(schedulerAssignedFuture.thenRun(this::startScheduling));{//JobMaster.startScheduling()
					jobStatusListener = new JobManagerJobStatusListener();
					schedulerNG.startScheduling();{//SchedulerBase.startScheduling()
						mainThreadExecutor.assertRunningInMainThread();
						registerJobMetrics();
						// 每个Operator都有在JM端的 Coordinator, 启动所有 OperatorCoordinator
						startAllOperatorCoordinators();{
							final Collection<OperatorCoordinatorHolder> coordinators = getAllCoordinators();
							for (OperatorCoordinatorHolder coordinator : coordinators) {
								coordinator.start();{//OperatorCoordinatorHolder.start()
									mainThreadExecutor.assertRunningInMainThread();
									coordinator.start();{
										// Hudi 算子的 Coordinator, 用于hive的 sync同步; 
										StreamWriteOperatorCoordinator.start(){
											
										}
										
										
									}
								}
							}
						}
						
						// 开始调度执行个Task的运行; 
						startSchedulingInternal();{
							
						}
					}
				}
			}
			return Acknowledge.get();
		}
		
	}
}


	// JM2.2.1 : Hudi的 OperatorCoordinator.start() 启动:  为 hive-sync功能完成 jdbc-hms相关的connect和初始化; 
	StreamWriteOperatorCoordinator.start(){
		reset();
		initTableIfNotExists(this.conf);
		this.executor = new CoordinatorExecutor(this.context, LOG);
		if (tableState.syncHive) { //hive_sync.enable = true(默认=true)
			initHiveSync();{//StreamWriteOperatorCoordinator.initHiveSync()
				this.hiveSyncExecutor = new NonThrownExecutor(LOG, true);
				this.hiveSyncContext = HiveSyncContext.create(conf);{//HiveSyncContext.create()
					HiveSyncConfig syncConfig = buildSyncConfig(conf);{//HiveSyncContext.buildSyncConfig()
						HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
						hiveSyncConfig.basePath = conf.getString(FlinkOptions.PATH);
						hiveSyncConfig.baseFileFormat = conf.getString(FlinkOptions.HIVE_SYNC_FILE_FORMAT);
						hiveSyncConfig.usePreApacheInputFormat = false;
						hiveSyncConfig.databaseName = conf.getString(FlinkOptions.HIVE_SYNC_DB);
						hiveSyncConfig.tableName = conf.getString(FlinkOptions.HIVE_SYNC_TABLE);
						hiveSyncConfig.syncMode = conf.getString(FlinkOptions.HIVE_SYNC_MODE);
						hiveSyncConfig.hiveUser = conf.getString(FlinkOptions.HIVE_SYNC_USERNAME);// hive_sync.username 参数 (默认:hive)
						hiveSyncConfig.hivePass = conf.getString(FlinkOptions.HIVE_SYNC_PASSWORD); // hive_sync.password  (默认:hive)
						hiveSyncConfig.jdbcUrl = conf.getString(FlinkOptions.HIVE_SYNC_JDBC_URL); //hive_sync.jdbc_url, 默认( dbc:hive2://localhost:10000);
						hiveSyncConfig.partitionFields = Arrays.asList(FilePathUtils.extractPartitionKeys(conf));
						hiveSyncConfig.partitionValueExtractorClass = conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME);
						hiveSyncConfig.useJdbc = conf.getBoolean(FlinkOptions.HIVE_SYNC_USE_JDBC);
						return hiveSyncConfig;
					}
					return new HiveSyncContext(syncConfig, hiveConf, fs);
				}
			}
		}
		if (tableState.syncMetadata) {//metadata.enabled =true (默认 false)
			initMetadataSync();{//StreamWriteOperatorCoordinator.initMetadataSync
				this.metadataSyncExecutor = new NonThrownExecutor(LOG, true);
			}
		}
		
	}
	








// 3.xx JM进程: ResourceManager 资源调度: 

// flink-akka.actor.default-dispatcher-3 线程:  这里的作用是 连接 RM 资源管理器?
JobMaster.ResourceManagerLeaderListener.notifyLeaderAddress(){
    runAsync(() -> notifyOfNewResourceManagerLeader(){//JobMaster.notifyOfNewResourceManagerLeader
        resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        reconnectToResourceManager(new FlinkException(String.format( resourceManagerAddress)));{
            closeResourceManagerConnection(cause);
            tryConnectToResourceManager();{//JobMaster.tryConnectToResourceManager()
                if (resourceManagerAddress != null) connectToResourceManager();{
                    resourceManagerConnection = new ResourceManagerConnection()
                    resourceManagerConnection.start();{//ResourceManagerConnection.start()
                        final RetryingRegistration<F, G, S> newRegistration = createNewRegistration();
                        
                        newRegistration.startRegistration();{
                            CompletableFuture<Void> rpcGatewayAcceptFuture = rpcGatewayFuture.thenAcceptAsync((G rpcGateway) -> {
                                
                                // 这里异步线程,执行 register()方法, 并在 thenAcceptAsync()方法中定义注册成功后的动作: 
                                register(rpcGateway, 1);{//RetryingRegistration.register()
                                    CompletableFuture<RegistrationResponse> registrationFuture = invokeRegistration(gateway, fencingToken, timeoutMillis);
                                    
                                    CompletableFuture<Void> registrationAcceptFuture = registrationFuture.thenAcceptAsync(
                                        (RegistrationResponse result) -> {
                                            if (result instanceof RegistrationResponse.Success) {
                                                completionFuture.complete(Tuple2.of(gateway, success));{//CompletableFuture.complete()
                                                    // 下面调用框架来异步执行;
                                                    CompletableFuture.postComplete() -> tryFire() -> uniWhenComplete(){//CompletableFuture.uniWhenComplete()
                                                        c.claim(){
                                                            e.execute(this);{//ScheduledThreadPoolExecutor.execute
                                                                ScheduledThreadPoolExecutor.schedule();{
                                                                    new FutureTask().run() -> Executors.RunnableAdapter.call() -> Completion.run() -> UniWhenComplete.tryFire() -> CompletableFuture.uniWhenComplete(){
                                                                        // 这里执行 上面 RetryingRegistration.createNewRegistration() 方法中 future.whenCompleteAsync()中的方法体:
                                                                        RetryingRegistration.createNewRegistration() -> future.whenCompleteAsync(()->{
                                                                            
                                                                            AkkaRpcActor.handleMessage -> handleRpcMessage() -> handleRunAsync() => runAsync.getRunnable().run(){
                                                                                // 这里触发 ResourceManagerConnection.onRegistrationSuccess()
                                                                                onRegistrationSuccess(result.f1); //具体代码详解下面;
                                                                            }
                                                                        })
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            
                                        }
                                    );
                                }
                                
                            })
                        }
                    }
                }
            }
        }
    });
}




// flink-akka.actor.default-dispatcher-3 线程: RM注册成功后的操作: onRegistrationSuccess(), 即 requestSlot()申请Task插槽以运行Task ? 
AkkaRpcActor.handleMessage()-> handleRpcMessage(message);{
    if (expectedFencingToken == null){
        
    }else{
        super.handleRpcMessage(fencedMessage.getPayload());{//AkkaRpcActor.handleRpcMessage()
            if (message instanceof RunAsync) {
                handleRunAsync((RunAsync) message);{//AkkaRpcActor.handleRunAsync()
                    runAsync.getRunnable().run();{
                        
                        ResourceManagerConnection.onRegistrationSuccess(){
                            runAsync(() -> {
                                if (this == resourceManagerConnection) {
                                    JobMaster.establishResourceManagerConnection(success);{//
                                        final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();
                                        establishedResourceManagerConnection = new EstablishedResourceManagerConnection();
                                        slotPool.connectToResourceManager(resourceManagerGateway);{//SlotPoolImpl.
                                            for (PendingRequest pendingRequest : waitingForResourceManager.values()) {
                                                requestSlotFromResourceManager(resourceManagerGateway, pendingRequest);{
                                                    final AllocationID allocationId = new AllocationID();
                                                    CompletableFuture<Acknowledge> rmResponse = resourceManagerGateway.requestSlot();{ //远程调用;
                                                        // 发起Akka的Rpc请求, 远程 执行 ResourceManager.requestSlot()方法并返回结果;
                                                    }
                                                }
                                            }
                                        }
                                        
                                    }
                                }
                            });    
                        }
                    }
                    
                }
            }
        }
    }
}


// "flink-akka.actor.default-dispatcher-5" : 在TaskManager端发起 申请Slot请求 ?
ResourceManager.requestSlot(){
    checkInit();
    internalRequestSlot(pendingSlotRequest);{//SlotManagerImpl.internalRequestSlot()
        OptionalConsumer.of(findMatchingSlot(resourceProfile))
            .ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))
            .ifNotPresent(() -> fulfillPendingSlotRequestWithPendingTaskManagerSlot(pendingSlotRequest));{
                SlotManagerImpl.allocateSlot(){
                    TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();
                    //gateway 是什么? TaskExecutorGateway => AkkaInvocationHandler => TaskExecutor.requestSlot(), 应该是调远程Rpc传输数据服务;
                    CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot();{
                        // 该Ack发起Rpc请求,并最终调用 TaskExecutor的  方法完成执行;
                        
                        { //线程: flink-akka.actor.default-dispatcher-3
                            TaskExecutor.requestSlot()
                                ->jobLeaderService.addJob(jobId, targetAddress) -> leaderRetrievalService.start(jobManagerLeaderListener);
                                
                        }
                    }
                    
                    requestFuture.whenComplete();
                    
                    completableFuture.whenCompleteAsync();
                }
            }
    }
}




/** yarn RM 调度资源
*
*/




// 2. yarn.ResourceManager进程: "IPC Server handle"线程, 接受的 SubmitApplication 请求,并解析其中的 amContainer;

ResourceManager.SchedulerEventDispatcher.EventProcessor.run(){
	while (!stopped && !Thread.currentThread().isInterrupted()) {
		event = eventQueue.take();
		scheduler.handle(event);{
			// 1.容量调度
			CapacityScheduler.handle(){
				switch(event.getType()) {
					case NODE_RESOURCE_UPDATE:
					case NODE_UPDATE:{
						NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
						RMNode node = nodeUpdatedEvent.getRMNode();
						nodeUpdate(node);{//CapacityScheduler.nodeUpdate
							List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
							for(UpdatedContainerInfo containerInfo : containerInfoList) {
							  newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
							  completedContainers.addAll(containerInfo.getCompletedContainers());
							}
							for (ContainerStatus completedContainer : completedContainers) {
							  ContainerId containerId = completedContainer.getContainerId();
							  LOG.debug("Container FINISHED: " + containerId);
							  completedContainer(getRMContainer(containerId), completedContainer,FINISHED);{
								queue.completedContainer(clusterResource, rmContainer, containerStatus, event, null, true);{
									-> LeafQueue.completedContainer()
									-> FiCaSchedulerApp.containerCompleted()
									-> rmContainer.handle(new RMContainerFinishedEvent(containerId,containerStatus, event));{//RMContainerImpl.handle()
										writeLock.lock();
										RMContainerState oldState = getState();
										stateMachine.doTransition(event.getType(), event);
										if (oldState != getState()) {
											LOG.info(event.getContainerId() + " Container Transitioned from " + oldState + " to " + getState());
										}
									}
									
								}
							  }
							}
						}
					}
					
				}
			}
			// 2. 公平调度
			
		}
	}
}

// IPC Server handle 42 线程: 收到 SubmitApplicationResponse 请求,获取其中其 submissionContext信息并 创建封装到 RMAppImpl对象中; 
// request.prot.applicationSubmissionContext对象中的 amContainerSpec 即为am启动内容; 
Server.Handler.run()
	-> call.connection.user.doAs()->{PrivilegedExceptionAction.run()};
	-> call(call.rpcKind, call.connection.protocolName, call.rpcRequest, call.timestamp);
	-> return getRpcInvoker(rpcKind).call(this, protocol, rpcRequest,receiveTime);
	-> service.callBlockingMethod(methodDescriptor, null, param);{
		ApplicationClientProtocolPBServiceImpl.submitApplication();{
			SubmitApplicationResponse response = real.submitApplication(request);{//ClientRMService.submitApplication()
				// 从请求中获取 submissionContext;
				ApplicationSubmissionContext submissionContext = request.getApplicationSubmissionContext();{
					this.applicationSubmissionContext = convertFromProtoFormat(p.getApplicationSubmissionContext());{
						return new ApplicationSubmissionContextPBImpl(p);
					}
					return this.applicationSubmissionContext;
				}
				ApplicationId applicationId = submissionContext.getApplicationId();
				rmAppManager.submitApplication(submissionContext,System.currentTimeMillis(), user);{//RMAppManager.
					ApplicationId applicationId = submissionContext.getApplicationId();
					RMAppImpl application =createAndPopulateNewRMApp(submissionContext, submitTime, user, false);
					ApplicationId appId = submissionContext.getApplicationId();
					this.rmContext.getDispatcher().getEventHandler().handle(new RMAppEvent(applicationId, RMAppEventType.START));
				}
			}
		}
	}

// AsyncDispatcher event handle 线程, 基于 Accceped Event事件, 创建 RMAppAttempt对象,并把 submissionContext 内容传入; 
AsyncDispatcher.dispatch(Event event){
	EventHandler handler = eventDispatchers.get(type);
	handler.handle(event);{// ResourceManager.ApplicationEventDispatcher.handle()
		ApplicationId appID = event.getApplicationId();
		// rmApp: RMAppImpl, 里面主要封装了 appId,submissionContext 等本次启动Java进程的内容; 
		RMApp rmApp = this.rmContext.getRMApps().get(appID);
		rmApp.handle(event);{//RMAppImpl.handle
			ApplicationId appID = event.getApplicationId();
			this.stateMachine.doTransition(event.getType(), event);{
				currentState = StateMachineFactory.this.doTransition(operand, currentState, eventType, event);{
					return transition.doTransition(operand, oldState, event, eventType);//StateMachineFactory$SingleInternalArc
					-> hook.transition(operand, event);//RMAppImpl$StartAppAttemptTransition
					-> app.createAndStartNewAttempt(false);{//RMAppImpl.createAndStartNewAttempt
						createNewAttempt();{
							ApplicationAttemptId appAttemptId =ApplicationAttemptId.newInstance(applicationId, attempts.size() + 1);
							// 就是在这里, 把submissionContext: ApplicationSubmissionContextPBImpl 传进入了参数; 
							RMAppAttempt attempt =new RMAppAttemptImpl(appAttemptId, rmContext, scheduler, masterService,submissionContext, conf,);
							attempts.put(appAttemptId, attempt);
						}
						handler.handle(new RMAppStartAttemptEvent(currentAttempt.getAppAttemptId(),transferStateFromPreviousAttempt));
					}
				}
			}
		}
	}
}



// 3. yarn.ResourceManager进程: "ApplicationMasterLauncher" 线程: ContainerLaunch 线程

// ApplicationMasterLauncher 线程; 处理收到的 Launch事件, 并把 submissionContext(launchContext) 传进 StartContainerRequest 发给nodeMgr去启动; 
ApplicationMasterLauncher{
	
	final BlockingQueue<Runnable> masterEvents=new LinkedBlockingQueue<Runnable>();
	// 启动线程, 以 ApplicationMasterLauncher.masterEvents 对立,对amLunch事件 以生产/消费者模式进行处理; 
	ApplicationMasterLauncher.serviceStart(){
		launcherHandlingThread.start();{
			// 新启 ApplicationMaster Launcher 线程, 源码如下; 
			// ApplicationMaster Launcher 线程: 从BlockingQueue<Runnable>: masterEvents 取出事件,并执行; 
			ApplicationMasterLauncher.LauncherThread.run(){
				while (!this.isInterrupted()) {
					toLaunch = masterEvents.take();
					launcherPool.execute(toLaunch);{
						AMLauncher.run();// 见下面源码
					}
				}
			}
		}
		super.serviceStart();
	}

	void launch(){
		Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.LAUNCH);{
			Runnable launcher =new AMLauncher(context, application, event, getConfig());
			return launcher;
		}
		masterEvents.add(launcher);
	}
}

AMLauncher.run(){
	switch (eventType) {
		case LAUNCH:
			launch();{
				connect();
				ApplicationSubmissionContext applicationContext =application.getSubmissionContext();
				ContainerLaunchContext launchContext =createAMContainerLaunchContext(applicationContext, masterContainerID);
				StartContainerRequest scRequest =StartContainerRequest.newInstance(launchContext, masterContainer.getContainerToken());
				
				StartContainersResponse response =containerMgrProxy.startContainers(allRequests);{
					ContainerManagementProtocolPBClientImpl.startContainers(){
						StartContainersRequestProto requestProto =((StartContainersRequestPBImpl) requests).getProto();
						return new StartContainersResponsePBImpl(proxy.startContainers(null,requestProto));
					}
				}
				
			}
			handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(), RMAppAttemptEventType.LAUNCHED));
			break;
		case CLEANUP:	
			cleanup();break;
	}
}




// 4. yarn.NodeManager进程: "ContainerLaunch" 线程: 依据 launchContext 内容构建 java YarnSessionClusterEntrypoint 启动命令; 

ResourceLocalizationService.LocalizerRunner.run(){
	nmPrivateCTokensPath =dirsHandler.getLocalPathForWrite();
	writeCredentials(nmPrivateCTokensPath);
	if (dirsHandler.areDisksHealthy()) {
		exec.startLocalizer(nmPrivateCTokensPath, localizationServerAddress,);{//DefaultContainerExecutor.
			createUserLocalDirs(localDirs, user);
			createUserCacheDirs(localDirs, user);
			createAppDirs(localDirs, user, appId);
			createAppLogDirs(appId, logDirs, user);
			Path appStorageDir = getWorkingDir(localDirs, user, appId);
			
			copyFile(nmPrivateContainerTokensPath, tokenDst, user);
			LOG.info("Copying from " + nmPrivateContainerTokensPath + " to " + tokenDst);
			FileContext localizerFc = FileContext.getFileContext(lfs.getDefaultFileSystem(), getConf());
			localizerFc.setWorkingDirectory(appStorageDir);
			ContainerLocalizer localizer =new ContainerLocalizer(localizerFc, user, appId, locId, getPaths(localDirs), RecordFactoryProvider.getRecordFactory(getConf()));
			
			localizer.runLocalization(nmAddr);{// ContainerLocalizer.runLocalization()
				initDirs(conf, user, appId, lfs, localDirs);
				Path tokenPath =new Path(String.format(TOKEN_FILE_NAME_FMT, localizerId));
				credFile = lfs.open(tokenPath);
				lfs.delete(tokenPath, false);
				
				ExecutorService exec = createDownloadThreadPool();
				CompletionService<Path> ecs = createCompletionService(exec);
				localizeFiles(nodeManager, ecs, ugi);
			}
		}
	}
}

// ContainerLaunch 线程
// ContainerLaunch 线程: ContainerLauncher.handle(): case LAUNCH_CONTAINER:containerLauncher.submit(launch);
ContainerLaunch.call(){
	final ContainerLaunchContext launchContext = container.getLaunchContext();
	final List<String> command = launchContext.getCommands();
	localResources = container.getLocalizedResources();
	Map<String, String> environment = launchContext.getEnvironment();
	FileContext lfs = FileContext.getLocalFSFileContext();
	
	exec.writeLaunchEnv(containerScriptOutStream, environment, localResources,launchContext.getCommands());
	if (!shouldLaunchContainer.compareAndSet(false, true)) {
		
	}else{
		exec.activateContainer(containerID, pidFilePath);
		ret = exec.launchContainer(container, nmPrivateContainerScriptPath, appIdStr, containerWorkDir,localDirs, logDirs);{
			DefaultContainerExecutor.launchContainer(){
				copyFile(nmPrivateTokensPath, tokenDst, user);
				Path launchDst =new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
				copyFile(nmPrivateContainerScriptPath, launchDst, user);
	
				setScriptExecutable(sb.getWrapperScriptPath(), user);
				setScriptExecutable(sb.getWrapperScriptPath(), user);
				shExec = buildCommandExecutor(sb,containerIdStr, user, pidFile, container.getResource(),new File(containerWorkDir.toUri().getPath()),container.getLaunchContext().getEnvironment());{
					String[] command = getRunCommand(wrapperScriptPath,containerIdStr, user, pidFile, this.getConf(), resource);
					LOG.info("launchContainer: " + Arrays.toString(command));
					return new ShellCommandExecutor(command,wordDir,environment); 
				}
				if (isContainerActive(containerId)) {
					shExec.execute();
				}
		  
			}
			
		}
	}
	
}






