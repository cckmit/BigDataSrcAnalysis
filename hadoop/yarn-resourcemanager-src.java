
/** 1 yarnCli: submitApplication 与Yarn RM通信,提交启动AM; 
	//1. 发送Rpc请求: ProtobufRpcEngine.invoke()
	//2. 通信等待非 waitingStates就结束阻塞,返回 applicationId
*/


YarnClientImpl.submitApplication(ApplicationSubmissionContext appContext){//YarnClientImpl.submitApplication()
	SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
	request.setApplicationSubmissionContext(appContext);
	rmClient.submitApplication(request);{// ApplicationClientProtocolPBClientImpl.submitApplication()
		// yarn 的resourceManager的 resourcemanager.ClientRMService 进行处理
		SubmitApplicationRequestProto requestProto= ((SubmitApplicationRequestPBImpl) request).getProto();
		SubmitApplicationResponseProto proto= proxy.submitApplication(null,requestProto){
			// 实际执行: 
			ProtobufRpcEngine.invoke(Object proxy, Method method, Object[] args){}{
				// method= ApplicationClientProtocol.BlokingInterface.submitApplication()
				RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
				RpcRequestWrapper rpcRequest= new RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId,fallbackToSimpleAuth);
				RpcResponseWrapper val=(RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,rpcRequest);
				Message returnMessage = prototype.newBuilderForType().mergeFrom(val.theResponseRead).build();
				return returnMessage;
			}
		}
		return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,requestProto));
	}
	while (true) {// 非waitingStates 就跳出返回 applicationId
		if (!waitingStates.contains(state)) {
			LOG.info("Submitted application " + applicationId);
			break;
		}
	}
	return applicationId;
}














// 2. Yarn ResourceManager进程: 分配给AM的容器资源并 协调NodeMgr启动 Application容器;
// from : 来自 YarnClusterDescriptor.deploySessionCluster(): startAppMaster() -> YarnClientImpl.submitApplication() 
// 对于yarn-session启动的是FlinkYarnSessionCli进程, linkis-cli提交的flink作业是 EngineConnServer/FlinkClient进程

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






