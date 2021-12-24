
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


