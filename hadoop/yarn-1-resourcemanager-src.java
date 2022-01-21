
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



// 2.x 资源调度,判断是否有足够资源: CapacityScheduler.addApplicationAttempt()

	CapacityScheduler.handle(SchedulerEvent event){
		switch(event.getType()) {
			case NODE_ADDED:
			case NODE_REMOVED:
			case NODE_UPDATE:
				RMNode node = nodeUpdatedEvent.getRMNode();
				nodeUpdate(node);{
					
				}
				
				if (!scheduleAsynchronously) {
					allocateContainersToNode(getNode(node.getNodeID()));// 源码细节如下: CapacityScheduler.allocateContainersToNode()
				}
				break;
			case APP_ADDED: {
				String queueName =resolveReservationQueueName(appAddedEvent.getQueue());
				if (queueName != null) {
					addApplication(appAddedEvent.getApplicationId(), queueName,appAddedEvent.getUser());
				}
			} 
				break;
			case APP_ATTEMPT_ADDED: 
				addApplicationAttempt(); {// CapacityScheduler.addApplicationAttempt()
					SchedulerApplication<FiCaSchedulerApp> application =applications.get(applicationAttemptId.getApplicationId());
					FiCaSchedulerApp attempt =new FiCaSchedulerApp(applicationAttemptId, application.getUser(),queue, queue.getActiveUsersManager(), rmContext);
					queue.submitApplicationAttempt(attempt, application.getUser());{//LeafQueue.submitApplicationAttempt()
						addApplicationAttempt(application, user);{
							pendingApplications.add(application);
							applicationAttemptMap.put(application.getApplicationAttemptId(), application);
							activateApplications();{//LeafQueue.activateApplications()
								// 这个代表什么? 怎么会是0了; 
								Resource amLimit = getAMResourceLimit();{//LeafQueue.getAMResourceLimit()
									// 队列剩余 <8192,1> 
									//queueResourceLimitsInfo: QueueResourceLimitsInfo{ queueCurrentLimit:Resource, clusterResource:Resource };
									Resource queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
									// 仅比较 内存,取其中最大; <8192,1> 
									Resource queueCap = Resources.max(resourceCalculator, lastClusterResource,absoluteCapacityResource, queueCurrentLimit);{
										return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
									}
									// maxAMResourcePerQueuePercent: AM资源占比, 由 PREFIX(yarn.scheduler.capacity.).maximum-am-resource-percent 指定,默认 0.1f; 
									// 取 queueCap.memory (8192) * maxAMResourcePerQueuePercent(0.1) + 0.5 作为memory; 
									return Resources.multiplyAndNormalizeUp( resourceCalculator, queueCap,maxAMResourcePerQueuePercent,minimumAllocation);{
										return calculator.multiplyAndNormalizeUp(lhs, by, factor);{//DefaultResourceCalculator.multiplyAndNormalizeUp(r,by,stepFactor)
											int a = (int)(r.getMemory() * by + 0.5);// 8192 * 0.1 + 0.5 = 819.5
											int memory = roundUp(a, stepFactor.getMemory());
											return Resources.createResource();
										}
									}
								}
								Resource userAMLimit = getUserAMResourceLimit();
								// 遍历所有的 pendingApplications: Set<FiCaSchedulerApp> 
								for (Iterator<FiCaSchedulerApp> i=pendingApplications.iterator(); i.hasNext(); ) {
									FiCaSchedulerApp application = i.next();
									
									// Check am resource limit
									Resource amIfStarted = Resources.add(application.getAMResource(), queueUsage.getAMUsed());
									boolean lessThan = Resources.lessThanOrEqual( resourceCalculator, lastClusterResource, amIfStarted, amLimit);{ // lessThanOrEqual(
										Resources.lessThanOrEqual(resourceCalculator,clusterResource, lhs, rhs){
											// 仅考虑内存资源, lhs.memory - rhs.memory ; 
											int compareNum = resourceCalculator.compare(clusterResource, lhs, rhs);{//DefaultResourceCalculator.compare()
												return lhs.getMemory() - rhs.getMemory(); 2048 - 0 = 2048;
											}
											return (compareNum <= 0); 2048 <= 0 = false;
										}
									}
									if (! lessThan) {
										if (getNumActiveApplications() < 1) {
											LOG.warn("maximum-am-resource-percent is insufficient to start a single application in queue, it is likely set too low"); 
										}else { // 
											LOG.info("not starting application as amIfStarted exceeds amLimit");
											continue;
										}
									}
									
									// Check user am resource limit
									User user = getUser(application.getUser());
									Resource userAmIfStarted = Resources.add(application.getAMResource(),user.getConsumedAMResources());
									
									if (!Resources.lessThanOrEqual(resourceCalculator, lastClusterResource, userAmIfStarted, userAMLimit)) {
										if (getNumActiveApplications() < 1) {
											LOG.warn("maximum-am-resource-percent is insufficient to start a single application in queue, it is likely set too low"); 
										}else { // 
											LOG.info("not starting application as amIfStarted exceeds userAmLimit");
											continue;
										}
									}
									
									user.activateApplication();
									activeApplications.add(application);
									queueUsage.incAMUsed(application.getAMResource());
									i.remove();
									LOG.info("Application " + application.getApplicationId() + " from user: " + application.getUser() + " activated in queue: " + getQueueName());
									
								}
							}
							
						}
					}
				}
				break;
			
			
		}
	}

	// 2.x.1 : CapacityScheduler.addApplicationAttempt()
	



	// 每秒钟实时刷新的 
	// case NODE_UPDATE: LOG打印: NEW to ALLOCATED
	CapacityScheduler.allocateContainersToNode(){
		
		// Try to schedule more if there are no reservations to fulfill
		if (node.getReservedContainer() == null) {
			if (calculator.computeAvailableContainers(node.getAvailableResource(), minimumAllocation) > 0) {
				root.assignContainers(clusterResource,node,new ResourceLimits(labelManager.getResourceByLabel()));{//ParentQueue.assignContainers()
					CSAssignment assignment = new CSAssignment(Resources.createResource(0, 0), NodeType.NODE_LOCAL);
					while (canAssign(clusterResource, node)) {
						
						// Schedule
						CSAssignment assignedToChild = assignContainersToChildQueues(clusterResource, node, resourceLimits);{//ParentQueue.
							
							printChildQueues();
							for (Iterator<CSQueue> iter = childQueues.iterator(); iter.hasNext();) {
								CSQueue childQueue = iter.next();
							}
							assignment = childQueue.assignContainers(cluster, node, childLimits);{//LeafQueue.assignContainers()
								
								for (FiCaSchedulerApp application : activeApplications) {
									for (Priority priority : application.getPriorities()) {
										Resource required = anyRequest.getCapability();
										// 每秒的请求, 在这里就被 中方返回了;
										int totalReqSize= application.getTotalRequiredResources(priority);{//ScheulerApplicationAttempt.
											return getResourceRequest(priority, ResourceRequest.ANY){
												return this.appSchedulingInfo.getResourceRequest(priority, resourceName);{
													Map<String, ResourceRequest> nodeRequests = requests.get(priority);
													return (nodeRequests == null) ? null : nodeRequests.get(resourceName);
												}
											}
												.getNumContainers();{//ResourceRequestPBImpl.getNumContainers()
													ResourceRequestProtoOrBuilder p = viaProto ? proto : builder;
													return (p.getNumContainers());
												}
										}
										// 如果没有资源了, 直接返回; 
										if (totalReqSize <= 0) {
											continue; // 
										}
			  
										Resource userLimit = computeUserLimitAndSetHeadroom(application, clusterResource, required, requestedNodeLabels); 
										application.addSchedulingOpportunity(priority);
										// Try to schedule
										CSAssignment assignment =  assignContainersOnNode(clusterResource, node, application, priority, null, currentResourceLimits);{
											MutableObject allocatedContainer = new MutableObject();
											// Data-local
											
											// Off-switch
											ResourceRequest offSwitchResourceRequest = application.getResourceRequest(priority, ResourceRequest.ANY);
											assigned =assignOffSwitchContainers();{
												if (canAssign(application, priority, node, NodeType.OFF_SWITCH,reservedContainer)) {
													return assignContainer(clusterResource, node, application,NodeType.OFF_SWITCH);{//LeafQueue.assignContainer()
														Resource capability = request.getCapability();
														Resource available = node.getAvailableResource();
														
														Container container = getContainer(rmContainer, application, node, capability, priority);
														int availableContainers =  resourceCalculator.computeAvailableContainers(available, capability);
														if (availableContainers > 0) {
															// Inform the application
															RMContainer allocatedContainer = application.allocate(type, node, priority, request, container);{//FiCaSchedulerApp.allocate()
																if (getTotalRequiredResources(priority) <= 0) {
																	return null;
																}
																RMContainer rmContainer = new RMContainerImpl(container, this.getApplicationAttemptId(), this.rmContext);
																Resources.addTo(currentConsumption, container.getResource());
																rmContainer.handle( new RMContainerEvent(container.getId(), RMContainerEventType.START));{//RMContainerImpl.handle()
																	stateMachine.doTransition(event.getType(), event);
																	// Container Transitioned from NEW to ALLOCATED
																}
																
																return rmContainer;
															}
															// Inform the node
															node.allocateContainer(allocatedContainer);{//SchedulerNode.allocateContainer
																Container container = rmContainer.getContainer();
																deductAvailableResource(container.getResource());
																++numContainers;
																launchedContainers.put(container.getId(), rmContainer);
																
															}
															
															LOG.info("assignedContainer" + clusterResource);
															return container.getResource();
														}
														
													}
												}
				
											}
											
			  
										}
										
										Resource assigned = assignment.getResource();
										if (Resources.greaterThan(resourceCalculator, clusterResource, assigned, Resources.none())) {
											allocateResource(clusterResource, application, assigned,node.getLabels());{//LeafQueue.allocateResource()
												super.allocateResource(clusterResource, resource, nodeLabels);
												user.assignContainer(resource, nodeLabels);
												Resources.subtractFrom(application.getHeadroom(), resource); // headroom
												metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
											}
											return assignment; // Done
										}else{
											break;
										}
										
									}
								}
								
								
								RMContainer reservedContainer = node.getReservedContainer();
								if (reservedContainer != null) {
									FiCaSchedulerApp application = getApplication(reservedContainer.getApplicationAttemptId());
									return assignReservedContainer(application, node, reservedContainer,clusterResource);
								}
								return NULL_ASSIGNMENT;
							}
						}
						
						// Done if no child-queue assigned anything
						if (Resources.greaterThan()){
							super.allocateResource(clusterResource, assignedToChild.getResource(),nodeLabels);
							Resources.addTo(assignment.getResource(), assignedToChild.getResource());
						}else{
							break;
						}
						
					}
					
					
				}
			}
		}
	}

	
2022-01-21 06:20:09,925 INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_1642707618461_0001_01_000014 Container 
Transitioned from ALLOCATED to ACQUIRED

	// LOG打印: Transitioned from ALLOCATED to ACQUIRED
	ApplicationMasterProtocolPBServiceImpl.allocate(){
		AllocateRequestPBImpl request = new AllocateRequestPBImpl(proto);
		AllocateResponse response = real.allocate(request);{//ApplicationMasterService.allocate()
			
		}
		return ((AllocateResponsePBImpl)response).getProto();
	}
	
	// 不断轮询; 
	ApplicationMasterService.allocate(){
		RMApp app =this.rmContext.getRMApps().get(applicationId);
		ApplicationSubmissionContext asc = app.getApplicationSubmissionContext();
		RMServerUtils.validateBlacklistRequest(blacklistRequest);
		Allocation allocation =this.rScheduler.allocate(appAttemptId, ask, release, blacklistRemovals);{//CapacityScheduler.allocate()
			return application.getAllocation(getResourceCalculator(),clusterResource, getMinimumResourceCapability());{//FiCaSchedulerApp.application()
				ContainersAndNMTokensAllocation allocation =pullNewlyAllocatedContainersAndNMTokens();{//SchedulerApplicationAttempt.
					List<Container> returnContainerList =new ArrayList<Container>(newlyAllocatedContainers.size());
					// FiCaSchedulerApp.newlyAllocatedContainers: List<RMContainer> 是在 CapacityScheduler.allocateContainersToNode() -> assignContainer() -> FiCaSchedulerApp.allocate() 添加;
					// LeafQueue.assignContainer() -> FiCaSchedulerApp.allocate() 中 newlyAllocatedContainers.add(rmContainer);
					for (Iterator<RMContainer> i = newlyAllocatedContainers.iterator(); i.hasNext();) {
						RMContainer rmContainer = i.next();
						NMToken nmToken =rmContext.getNMTokenSecretManager().createAndGetNMToken(getUser(), getApplicationAttemptId(), container);
						rmContainer.handle(new RMContainerEvent(RMContainerEventType.ACQUIRED));
						// LOG 打印: Transitioned from ALLOCATED to ACQUIRED
					}
					return new ContainersAndNMTokensAllocation(returnContainerList, nmTokens);
				}
			}
		}
		
	}


2022-01-21 06:20:10,927 INFO org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl: container_1642707618461_0001_01_000014 Container
 Transitioned from ACQUIRED to RUNNING
 Transitioned from ACQUIRED to LAUNCHED 
 Transitioned from RUNNING to COMPLETED

	// 处理每秒上送过来的  RMNodeEventType.STATUS_UPDATE 事件,并把 RUNNING状态的container 加到 nodeUpdateQueue队列中;
	ResourceManager$NodeEventDispatcher.handle(){
		RMNode node = this.rmContext.getRMNodes().get(nodeId);
		node.handle(event);{
			//  
			RMNodeImpl.handle(){
				stateMachine.doTransition(event.getType(), event);{
					
					StateMachineFactory.doTransition(){
						RMNodeImpl$StatusUpdateWhenHealthyTransition.transition(){
							RMNodeImpl.handleContainerStatus(List<ContainerStatus> containerStatuses){
								
								for (ContainerStatus remoteContainer : containerStatuses) {
									// 把传过来的 containerStatuses中 container 加到 newlyLaunchedContainers列表,下面把不空的 加到 nodeUpdateQueue中; 
									List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
									for (ContainerStatus remoteContainer : containerStatuses) {
										if (remoteContainer.getState() == ContainerState.RUNNING) {
											if (!launchedContainers.contains(containerId)) {
												launchedContainers.add(containerId);
												newlyLaunchedContainers.add(remoteContainer);
											}
										}
									}
									
									if (newlyLaunchedContainers.size() != 0 || completedContainers.size() != 0) {
										nodeUpdateQueue.add(new UpdatedContainerInfo(newlyLaunchedContainers,completedContainers));
									}
								}
							}
						}
					}
					
				}
			}
		}
	}
	

	// 循环从 RMNodeImpl.nodeUpdateQueue:Queue<UpdatedContainerInfo> 中读取消费
	CapacityScheduler.nodeUpdate(){
		FiCaSchedulerNode node = getNode(nm.getNodeID());
		List<UpdatedContainerInfo> containerInfoList = nm.pullContainerUpdates();{//RMNodeImpl.pullContainerUpdates
			while ((containerInfo = nodeUpdateQueue.poll()) != null) {
				latestContainerInfoList.add(containerInfo);
			}
			return latestContainerInfoList;
		}
		// Processing the newly launched containers
		for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
			containerLaunchedOnNode(launchedContainer.getContainerId(), node);{//AbstractYarnScheduler
				application.containerLaunchedOnNode(containerId, node.getNodeID());{//SchedulerApplicationAttempt.
					RMContainer rmContainer = getRMContainer(containerId);
					// LOG打印 Transitioned from ACQUIRED to LAUNCHED 
					rmContainer.handle(new RMContainerEvent(containerId,RMContainerEventType.LAUNCHED));
				}
			}
		}
		
		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			LOG.debug("Container FINISHED: " + containerId);
			completedContainer(getRMContainer(containerId), completedContainer, RMContainerEventType.FINISHED);{
				LeafQueue queue = (LeafQueue)application.getQueue();
				queue.completedContainer(clusterResource, application, node):{//LeafQueue.completedContainer
					Container container = rmContainer.getContainer();
					if (rmContainer.getState() == RMContainerState.RESERVED) {
						
					}else{
						removed =application.containerCompleted(rmContainer, containerStatus, event);{//FiCaSchedulerApp.containerCompleted
							newlyAllocatedContainers.remove(rmContainer);
							// LOG打印:  Transitioned from RUNNING to COMPLETED
							rmContainer.handle(new RMContainerFinishedEvent()):
							containersToPreempt.remove(rmContainer.getContainerId());
							
						}
						node.releaseContainer(container);
					}
					if (removed) {
						getParent().completedContainer(clusterResource, application, node);
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






