

// 2.2 JobManager: flink-akka.actor.default-dispatcher-2 线程: 



Dispatcher.persistAndRunJob(){
	jobGraphWriter.putJobGraph(jobGraph);
	
    final CompletableFuture<Void> runJobFuture = runJob(jobGraph);{//Dispatcher.runJob
        final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);{//Dispatcher.createJobManagerRunner
            final RpcService rpcService = getRpcService();
			return CompletableFuture.supplyAsync(()-{
				JobManagerRunner runner =jobManagerRunnerFactory.createJobManagerRunner();{//DefaultJobManagerRunnerFactory.createJobManagerRunner()
					JobMasterConfiguration jobMasterConfiguration =JobMasterConfiguration.fromConfiguration(configuration);
					final SlotPoolFactory slotPoolFactory = SlotPoolFactory.fromConfiguration(configuration);
					final SchedulerNGFactory schedulerNGFactory =SchedulerNGFactoryFactory.createSchedulerNGFactory(configuration);
					final ShuffleMaster<?> shuffleMaster =ShuffleServiceLoader.loadShuffleServiceFactory(configuration).createShuffleMaster(configuration);
					JobMasterServiceFactory jobMasterFactory = new DefaultJobMasterServiceFactory();
					return new JobManagerRunnerImpl(jobGraph,jobMasterFactory,fatalErrorHandler,initializationTimestamp);{//new JobManagerRunnerImpl的构造函数
						ClassLoader userCodeLoader =classLoaderLease.getOrResolveClassLoader();
						leaderElectionService.start(this);
						this.jobMasterService =jobMasterFactory.createJobMasterService(jobGraph, userCodeLoader);{//DefaultJobMasterServiceFactory.
							return new JobMaster(new DefaultExecutionDeploymentTracker());{//new JobMaster() 构造函数中
								resourceManagerLeaderRetriever =highAvailabilityServices.getResourceManagerLeaderRetriever();
								this.schedulerNG = createScheduler(executionDeploymentTracker, jobManagerJobMetricGroup);{
									return schedulerNGFactory.createInstance();{//DefaultSchedulerFactory.
										DefaultSchedulerComponents schedulerComponents =createSchedulerComponents();
										restartBackoffTimeStrategy =RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory().create();
										return new DefaultScheduler();{super(){// SchedulerBase构造方法, 构建执行计划Graph
											this.executionGraph =createAndRestoreExecutionGraph();{
												ExecutionGraph newExecutionGraph =createExecutionGraph();{//SchedulerBase.
													// 核心步骤,构建 物理执行计划
													return ExecutionGraphBuilder.buildGraph();{//ExecutionGraphBuilder.buildGraph()
														JobInformation jobInformation =new JobInformation();
														executionGraph.attachJobGraph(sortedTopology);
														ExecutionGraph executionGraph =new ExecutionGraph();
														executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
														for (JobVertex vertex : jobGraph.getVertices()) {
															vertex.initializeOnMaster(classLoader);
														}
														List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
														executionGraph.attachJobGraph(sortedTopology);{
															ExecutionJobVertex ejv =new ExecutionJobVertex();{
																List<SerializedValue<OperatorCoordinator.Provider>> coordinatorProviders =getJobVertex().getOperatorCoordinators();
																for (final SerializedValue<OperatorCoordinator.Provider> provider :coordinatorProviders) {
																	OperatorCoordinatorHolder.create(provider, this, graph.getUserClassLoader());{
																		TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader);
																		OperatorCoordinator.Provider provider =serializedProvider.deserializeValue(classLoader);
																		
																	}
			
															}
															
														}
														
														CheckpointIDCounter checkpointIdCounter= recoveryFactory.createCheckpointIDCounter(jobId);
														CheckpointStatsTracker checkpointStatsTracker =new CheckpointStatsTracker();
														rootBackend =StateBackendLoader.fromApplicationOrConfigOrDefault();
														
													}
												}
												CheckpointCoordinator checkpointCoordinator =newExecutionGraph.getCheckpointCoordinator();
											}
											inputsLocationsRetriever =new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);
											this.coordinatorMap = createCoordinatorMap();
										}}
									}
								}
							}
						}
						jobMasterCreationFuture.complete(null);
					}
				}
				runner.start();
                return runner;
			});
			
			
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
    
    return runJobFuture.whenComplete(()->{jobGraphWriter.removeJobGraph(jobGraph.getJobID());});
}

