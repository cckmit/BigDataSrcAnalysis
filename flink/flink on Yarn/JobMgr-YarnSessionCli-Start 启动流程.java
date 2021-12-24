
// 1. FlinkYarnSessionCli 进程: "main"线程 构建并向yarnRM 发送 amContainer 

// Cli的main线程主要是创建YarnClient, 并最终构建1个 appContext: ApplicationSubmissionContext ,包含Java命令和相关jar/env变量等; 
// YarnClusterDescriptor.startAppMaster() 中创建 appContext,并调 yarnClient.submitApplication(appContext) 发给Yarn;
// %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%
FlinkYarnSessionCli.run()
	- yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration); 链接Yarn RM,并建立RMClient;
	- yarnClusterDescriptor.deploySessionCluster() 
	- yarnApplication = yarnClient.createApplication(); 创建Application
	- startAppMaster(); 
		* appContext = yarnApplication.getApplicationSubmissionContext(); 创建应用执行上下文 appCtx;
		* amContainer =setupApplicationMasterContainer(); 中创建 %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 格式的命令;
		* yarnClient.submitApplication(appContext); 将 appCtx发给RM/NM 进行远程Container/Java进程启动; 
// 核心 fink yarn client提交核心api: YarnClusterDescriptor.deploySessionCluster()

FlinkYarnSessionCli.main(){
	final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
	final FlinkYarnSessionCli cli = new FlinkYarnSessionCli(flinkConfiguration,configurationDirectory,"");
	retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));{//FlinkYarnSessionCli.run(){
		final CommandLine cmd = parseCommandLineOptions(args, true);
		// 主要耗时1:  链接Yarn ResurceManager
		final YarnClusterDescriptor yarnClusterDescriptor =yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration);{
			return getClusterDescriptor(configuration);{
				final YarnClient yarnClient = YarnClient.createYarnClient();
				yarnClient.init(yarnConfiguration);{
					super.serviceStart();
				}
				yarnClient.start();{//AbstractService.start()
					serviceStart();{//YarnClientImpl.serviceStart()
						rmClient = ClientRMProxy.createRMProxy(getConfig(),ApplicationClientProtocol.class);{
							return createRMProxy(configuration, protocol, INSTANCE);{//RMProxy.createRMProxy()
								RetryPolicy retryPolicy = createRetryPolicy(conf);
								if (HAUtil.isHAEnabled(conf)) {
									RMFailoverProxyProvider<T> provider =instance.createRMFailoverProxyProvider(conf, protocol);
									return (T) RetryProxy.create(protocol, provider, retryPolicy);
								}else{// 非HA, 这里; 
									InetSocketAddress rmAddress = instance.getRMAddress(conf, protocol);
									LOG.info("Connecting to ResourceManager at " + rmAddress);
									T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
									return (T) RetryProxy.create(protocol, proxy, retryPolicy);
								}
							}
						}
						if (historyServiceEnabled) {
							historyClient.start();
						}
					}
				}
				return new YarnClusterDescriptor(configuration,yarnConfiguration,yarnClient);
			}
		}
		if (cmd.hasOption(applicationId.getOpt())) {
			clusterClientProvider = yarnClusterDescriptor.retrieve(yarnApplicationId);
		}else{
			final ClusterSpecification clusterSpecification = yarnClusterClientFactory.getClusterSpecification(effectiveConfiguration);
			// 主要耗时2: 发布应用; 
			clusterClientProvider = yarnClusterDescriptor.deploySessionCluster(clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
				return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint());{
					isReadyForDeployment(clusterSpecification);
					checkYarnQueues(yarnClient);
					final YarnClientApplication yarnApplication = yarnClient.createApplication();
					final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();
					freeClusterMem = getCurrentFreeClusterResources(yarnClient);
					final int yarnMinAllocationMB = yarnConfiguration.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
					ApplicationReport report =startAppMaster();{//YarnClusterDescriptor.startAppMaster()
						final FileSystem fs = FileSystem.get(yarnConfiguration);
						ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
						final List<Path> providedLibDirs =Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);
						final YarnApplicationFileUploader fileUploader =YarnApplicationFileUploader.from();
						
						userJarFiles.addAll(jobGraph.getUserJars().stream()
                            .map(f -> f.toUri()).map(Path::new)
                            .collect(Collectors.toSet()));
						userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
						// 设置AM(ApplicationMaster)的资源: amContainer 主要包括 env,javaCammand, localResource本地jar包资源;
						processSpec =JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);{
							CommonProcessMemorySpec processMemory = PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config);{
								if (options.getRequiredFineGrainedOptions().stream().allMatch(config::contains)) {
									
								}else if (config.contains(options.getTotalFlinkMemoryOption())) {//jobmanager.memory.flink.size
									return deriveProcessSpecWithTotalFlinkMemory(config);
								}else if (config.contains(options.getTotalProcessMemoryOption())) {// jobmanager.memory.process.size
									return deriveProcessSpecWithTotalProcessMemory(config);{
										MemorySize totalProcessMemorySize =getMemorySizeFromConfig(config, options.getTotalProcessMemoryOption());
										// Metaspace默认 256Mb, jobmanager.memory.jvm-metaspace.size
										JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =deriveJvmMetaspaceAndOverheadWithTotalProcessMemory(config, totalProcessMemorySize);
										// 约等于 total - metaspace - overhead = 1024 - 256 -196 = 576Mb
										MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());
										// 又把 576 进一步分层heap /offHeap, 堆内 448, 堆外 128Mb; 
										FM flinkInternalMemory =flinkMemoryUtils.deriveFromTotalFlinkMemory(config, totalFlinkMemorySize);
										return new CommonProcessMemorySpec<>(flinkInternalMemory, jvmMetaspaceAndOverhead);
									}
								}
								return failBecauseRequiredOptionsNotConfigured();
							}
							return new JobManagerProcessSpec(processMemory.getFlinkMemory(), processMemory.getJvmMetaspaceAndOverhead());
						}
                        flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
						final ContainerLaunchContext amContainer =setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec);{
							String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
							javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
							startCommandValues.put("java", "$JAVA_HOME/bin/java");
							startCommandValues.put("jvmmem", jvmHeapMem);{
								jvmArgStr.append("-Xmx").append(processSpec.getJvmHeapMemorySize().getBytes());
								jvmArgStr.append(" -Xms").append(processSpec.getJvmHeapMemorySize().getBytes());
								if (enableDirectMemoryLimit) {//jobmanager.memory.enable-jvm-direct-memory-limit
									jvmArgStr.append(" -XX:MaxDirectMemorySize=").append(processSpec.getJvmDirectMemorySize().getBytes());
								}
								jvmArgStr.append(" -XX:MaxMetaspaceSize=").append(processSpec.getJvmMetaspaceSize().getBytes());
							}
							startCommandValues.put("jvmopts", javaOpts);
							startCommandValues.put("class", yarnClusterEntrypoint);
							startCommandValues.put("args", dynamicParameterListStr);
							
						}
						amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
						// 设置env: _FLINK_CLASSPATH 环境变量
						userJarFiles.addAll(jobGraph.getUserJars().stream().map(f -> f.toUri())); //添加 jobGraph.getUserJars() 中的jars
						userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet())); // 添加 pipeline.jars中的jars;
						final List<String> userClassPaths =fileUploader.registerMultipleLocalResources(
							userJarFiles, // =  jobGraph.getUserJars() + pipeline.jars 
							userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR, LocalResourceType.FILE); // 添加usrlib/目录下
						
						//FLINK_CLASSPATH 1: include-user-jar=first时,把 jobGraph.getUserJars() &pipeline.jars &usrlib 目录下jars 加到前面;
						if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST) classPathBuilder.append(userClassPath).append(File.pathSeparator);//yarn.per-job-cluster.include-user-jar
						// FLINK_CLASSPATH 2: systemClassPaths= shipFiles(yarn.ship-files配置) + logConfigFile +systemShipFiles(Sys.FLINK_LIB_DIR变量) , 包括 localResources中上传的13个flink的lib下jar包;
						addLibFoldersToShipFiles(systemShipFiles);{
							String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);//从系统变量读取FLINK_LIB_DIR 的值;
							effectiveShipFiles.add(new File(libDir));
						}
						for (String classPath : systemClassPaths) classPathBuilder.append(classPath).append(File.pathSeparator);
						// FLINK_CLASSPATH 3: 
						classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
						classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
						classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
						//FLINK_CLASSPATH 6: include-user-jar=last时, 把userClassPath 的jars加到CP后面; 
						if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) classPathBuilder.append(userClassPath).append(File.pathSeparator);
						
						appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
						appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES,fileUploader.getApplicationDir().toUri().toString());
						// 设置 CLASSPATH的参数
						Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
							// 1. 先把 _FLINK_CLASSPATH中 lib中13个flink相关jar包加到CP
							addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
							// 2. yarn.application.classpath + 
							String[] applicationClassPathEntries =conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);{
								String valueString = get(name);// 获取yarn.application.classpath 变量
								if (valueString == null) {// 采用Yarn默认CP: YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH, 包括7个;
									return defaultValue;// 默认YarnCP包括4类: CONF_DIR和 share下的common,hdfs,yar3个模块的目录;
								} else {
									return StringUtils.getStrings(valueString);
								}
							}
							for (String c : applicationClassPathEntries) {
								addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
							}
						}
						amContainer.setEnvironment(appMasterEnv);
						appContext.setAMContainerSpec(amContainer);
						
						// 设置CPU/Memory资源大小; 
						capability.setMemory(clusterSpecification.getMasterMemoryMB());
						capability.setVirtualCores(flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES));
						appContext.setResource(capability);
						
						setApplicationTags(appContext);
						yarnClient.submitApplication(appContext);{//YarnClientImpl.submitApplication()
							SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
							request.setApplicationSubmissionContext(appContext);
							rmClient.submitApplication(request);{
								// yarn 的resourceManager的 resourcemanager.ClientRMService 进行处理
								
							}
							while (true) {// 非waitingStates 就跳出返回 applicationId
								if (!waitingStates.contains(state)) {
									LOG.info("Submitted application " + applicationId);
									break;
								}
							}
							return applicationId;
						}
						LOG.info("Waiting for the cluster to be allocated");
						while (true) {
							report = yarnClient.getApplicationReport(appId);
							YarnApplicationState appState = report.getYarnApplicationState();
							switch (appState) {
								case FAILED: case KILLED:
									throw new YarnDeploymentException();
								case RUNNING:case FINISHED:
									break loop;
								default:
							}
							Thread.sleep(250);
						}
						
					}
					return () -> {return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());};
					
				}
			}
		}
	}
}




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


// 都来自 YarnClusterDescriptor.deploySessionCluster()
// 对于yarn-session启动的, 发送SubmitApplicationRequest事件的是 FlinkYarnSessionCli进程; 触发方法:YarnClientImpl.submitApplication()
// 对于linkis-cli提交的flink作业, 发送SubmitApplicationRequest的是 EngineConnServer/FlinkClient进程;

ClientRMService.submitApplication(SubmitApplicationRequest request):SubmitApplicationResponse {
	// 从请求中获取 submissionContext;
	ApplicationSubmissionContext submissionContext = request.getApplicationSubmissionContext();{
		this.applicationSubmissionContext = convertFromProtoFormat(p.getApplicationSubmissionContext());{
			return new ApplicationSubmissionContextPBImpl(p);
		}
		return this.applicationSubmissionContext;
	}
	ApplicationId applicationId = submissionContext.getApplicationId();
	rmAppManager.submitApplication(submissionContext,System.currentTimeMillis(), user);{//RMAppManager.submitApplication()
		ApplicationId applicationId = submissionContext.getApplicationId();
		RMAppImpl application =createAndPopulateNewRMApp(submissionContext, submitTime, user, false);
		ApplicationId appId = submissionContext.getApplicationId();
		this.rmContext.getDispatcher().getEventHandler().handle(new RMAppEvent(applicationId, RMAppEventType.START));
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


