// cg-xx模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 


// linkis-cli -engineType flink-1.12.2 -code "select name, age from tb_user;"

java ${LINKIS_CLIENT_HEAP_OPTS} ${LINKIS_CLIENT_GC_OPTS} ${LINKIS_CLIENT_OPTS} \
-classpath ${LINKIS_CLIENT_CLASSPATH} \
-Dconf.root=${LINKIS_CLIENT_CONF_DIR} -Dconf.file=${LINKIS_CLIENT_CONF_FILE} ${LINKIS_CLIENT_LOG_OPTS}  \
com.webank.wedatasphere.linkis.cli.application.LinkisClientApplication \
"${input_args[@]}"


// linkis-cli模块: LinkisClientApplication 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 


// cg-entrance: 处理个中请求的线程:  其发送了 EngineAskRequest 请求; 
// 	fromTh: cg-entrance 交互服务
// 	nextTh: cg-engineplugin(先? cg-engineconnmanager?); 

EntranceRestfulApi.submit(){
	logger.info("Begin to get an execID");
	String execID = entranceServer.execute(json);{ // EntranceServer.execute
		var jobRequest = getEntranceContext.getOrCreateEntranceParser().parseToTask(params);
		//将map parse 成 jobRequest 之后，我们需要将它存储到数据库中，task可以获得唯一的taskID
		getEntranceContext.getOrCreatePersistenceManager().createPersistenceEngine().persist(jobRequest)
		val job:EntranceExecutionJob = getEntranceContext.getOrCreateEntranceParser().parseToJob(jobRequest)
		job.init();{// EntranceExecutionJob.init()
			
		}
		getEntranceContext.getOrCreateScheduler().submit(job);{// job.run(): EntranceExecutionJob.Job.run()
			Utils.tryAndWarn(transition(Running))
			executor.execute(jobToExecuteRequest);{//DefaultEntranceExecutor的父类 EntranceExecutor.execute()
				interceptors.foreach(in => request = in.apply(request, executeRequest));
				val engineReturn = callExecute(executeRequest);{//DefaultEntranceExecutor.callExecute()
					val entranceExecuteRequest: EntranceExecuteRequest = request match {case request: EntranceExecuteRequest => request}
					val compJobReq = requestToComputationJobReq(entranceExecuteRequest);
					val orchestration = EntranceOrchestrationFactory.getOrchestrationSession().orchestrate(compJobReq)
					val orchestratorFuture = orchestration.asyncExecute()
				}
				engineReturn
			}
			rs match {
				case r: CompletedExecuteResponse =>
					transitionCompleted(r)
				case r: IncompleteExecuteResponse =>
					transitionCompleted(ErrorExecuteResponse(if(StringUtils.isNotEmpty(r.message)) r.message else "incomplete code.", null))
				case r: AsynReturnExecuteResponse => 
				
			}
		}
		logger.info(s"Job with jobId : ${job.getId} and execID : ${job.getId()} submitted ")
	}
	message.setMethod("/api/entrance/submit");
	return Message.messageToResponse(message);
}


AsyncExecTaskRunnerImpl.run(){
	
	val response = task.execute();{
		
		GatherStrategyJobExecTask.execute();
		
		GatherStrategyStageInfoExecTask.execute();
		
		CodeLogicalUnitExecTask.execute();
		
		// 4. 
		GatherStrategyStageInfoExecTask.execute();
		
		// 5
		GatherStrategyJobExecTask.execute();
	}
	
	response match {
      case async: AsyncTaskResponse =>
        transientStatus(ExecutionNodeStatus.Running)
      case succeed: SucceedTaskResponse =>
        info(s"Succeed to execute ExecTask(${task.getIDInfo})")
        transientStatus(ExecutionNodeStatus.Succeed);{
			afterStatusChanged(oldStatus, status);{// AsyncExecTaskRunnerImpl.afterStatusChanged
				task.getPhysicalContext.broadcastSyncEvent(ExecTaskRunnerCompletedEvent(this));{//PhysicalContextImpl.
					syncListenerBus.postToAll(orchestratorSyncEvent)
				}
			}
		}
      case failedTaskResponse: FailedTaskResponse =>
        info(s"Failed to execute ExecTask(${task.getIDInfo})")
        transientStatus(ExecutionNodeStatus.Failed)
      case retry: RetryTaskResponse =>
        warn(s"ExecTask(${task.getIDInfo}) need to retry")
        transientStatus(ExecutionNodeStatus.WaitForRetry)
    }
}


// entrance 进程中 申请Executor,执行task的主逻辑: CodeLogicalUnitExecTask.execute() askExecutor() askEngineConnExecutor(): ComputationEngineConnManager.getEngineNodeAskManager()
// 先获取 NodeExecutor执行资源 engineNode: AMEngineNode: DefaultCodeExecTaskExecutorManager.askExecutor() -> ComputationEngineConnManager.getEngineNodeAskManager()

CodeLogicalUnitExecTask.execute(){
	
	// cg-linkismanager 模块 请求资源engineNode: AMEngineNode, linkismanager的 DefaultEngineAskEngineService.askEngine() 会从内存或新建1个 Executor; 
	executor = Utils.tryCatch(codeExecTaskExecutorManager.askExecutor(this));{//DefaultCodeExecTaskExecutorManager.askExecutor
		var executor: Option[CodeExecTaskExecutor] = None
		while (System.currentTimeMillis - startTime < wait.toMillis && executor.isEmpty){
			executor = askExecutor(execTask);{// DefaultCodeExecTaskExecutorManager.askExecutor
				val executor = createExecutor(execTask);{
					val engineConnManager = getEngineConnManager(execTask.getLabels)
					val markReq = createMarkReq(execTask)
					// getEngineConn Executor
					val engineConnExecutor = engineConnManager.getAvailableEngineConnExecutor(mark);{//ComputationEngineConnManager的父类AbstractEngineConnManager.getAvailableEngineConnExecutor()
						if (null != mark && getMarkCache().containsKey(mark)) {
							tryReuseEngineConnExecutor(mark) match {case Some(engineConnExecutor) => return engineConnExecutor}
							val engineConnExecutor = askEngineConnExecutor(mark.getMarkReq.createEngineConnAskReq(), mark);{//ComputationEngineConnManager.askEngineConnExecutor()
								var count = getEngineConnApplyAttempts()
								while (count >= 1) {
									count = count - 1;
									val engineNode = getEngineNodeAskManager(engineAskRequest, mark);{//ComputationEngineConnManager.getEngineNodeAskManager
										// 这里向谁请求? linkis-cg-engineconn ?
										val response = getManagerSender().ask(engineAskRequest);{// BaseRPCSender.ask()
											case protocol: Protocol if getRPCInterceptors.nonEmpty =>  rpcInterceptorChain.handle(createRPCInterceptorExchange(protocol, op));// linkis-cg-linkismanager
											{// engineAskRequest: EngineAskRequest 请求被 cg-linkismanager模块的 DefaultEngineAskEngineService.askEngine() 方法处理
												DefaultEngineAskEngineService.askEngine(){
													
												}
											}
										}
										response match {
											case engineNode: EngineNode => // 如果已经有引擎(如上次的SparkSubmit已存在)了, 这里直接返回; 
												engineNode: AMEngineNode
											case EngineAskAsyncResponse(id, serviceInstance) =>{ // 第一次,进入这里
												info(s"${mark.getMarkId()} received EngineAskAsyncResponse id: ${id} serviceInstance: $serviceInstance ")
												cacheMap.getAndRemove(id, Duration(engineAskRequest.getTimeOut + 100000, TimeUnit.MILLISECONDS)) match {
													case EngineCreateSuccess(id, engineNode) => engineNode
												}
											}
										}
									}
								}
							}
							saveToMarkCache(mark, engineConnExecutor);
							engineConnExecutor
						}
					}
					if (null == engineConnExecutor) {return null}
					val codeExecTaskExecutor = new CodeExecTaskExecutor(engineConnExecutor, execTask, mark);
					info(s"Finished to create Executor for execId ${execTask.getIDInfo()} mark id is ${mark.getMarkId()}, user ${mark.getMarkReq.getUser}")
					codeExecTaskExecutor
				}
			}
		}
		executor
	}
	
	// 这里向 cg-engineconn: SparkSubmit 进程 提交 requestTask: 
	if (executor.isDefined && !isCanceled) {
		val response = Utils.tryCatch(codeExecutor.getEngineConnExecutor.execute(requestTask));{// ComputationEngineConnExecutor.execute()
			requestTask.setLock(this.locker);
			getEngineConnSender.ask(requestTask) match { // ask(requestTask): 向 cg-engineconn: SparkSubmit 进程发 RequestTask的Rpc请求; 触发其 TaskExecutionServiceImpl.execute()方法
				case submitResponse: SubmitResponse =>{
					getRunningTasks.put(submitResponse.taskId, requestTask)
					// TaskExecutionServiceImpl.submitTask() 处理回复此消息 
					submitResponse
				}
				case successExecuteResponse: SuccessExecuteResponse => successExecuteResponse;
				
			}
		}
		
		response match {
			case SubmitResponse(engineConnExecId) =>{
				codeExecutor.setEngineConnTaskId(engineConnExecId)
				codeExecTaskExecutorManager.addEngineConnTaskID(codeExecutor)
				new AsyncTaskResponse {}
			}
		}
	} else if(null != retryException) {
		new DefaultFailedTaskResponse();
	}else {
		throw new OrchestratorRetryException();
	}
}

execute:74, ComputationEngineConnExecutor (com.webank.wedatasphere.linkis.orchestrator.ecm.service.impl)
apply:82, CodeLogicalUnitExecTask$$anonfun$1 (com.webank.wedatasphere.linkis.orchestrator.computation.physical)
apply:82, CodeLogicalUnitExecTask$$anonfun$1 (com.webank.wedatasphere.linkis.orchestrator.computation.physical)
tryCatch:39, Utils$ (com.webank.wedatasphere.linkis.common.utils)
execute:82, CodeLogicalUnitExecTask (com.webank.wedatasphere.linkis.orchestrator.computation.physical)




//1. 判断子task是否执行成功，如果执行失败，则调用context标记ExecutionTask为失败
// 2. 如果Task执行成功，则结果集汇总
GatherStrategyStageInfoExecTask.execute(){
	val execIdToResponse = getChildrenResultSet()
	if (null != execIdToResponse && execIdToResponse.nonEmpty) {
		val response = new DefaultResultSetTaskResponse(resultSets.toArray)
	}else{
		new SucceedTaskResponse() {}
	}
}

GatherStrategyJobExecTask.execute() = getTaskDesc() match{
	case _: StartJobTaskDesc =>
		super.execute();
	case _: EndJobTaskDesc =>{
		if (getPhysicalContext.isCompleted){
			
		}else{
			getPhysicalContext.markSucceed(response);
		}
	}
}




// cg-linkismanager: "ForkJoinPool-1-worker-7"线程: AskEngine申请执行flink执行引擎: 校验label, 
// fromTh: cg-entrance 交互服务
// nextTh: cg-engineplugin(先? cg-engineconnmanager?); 主要靠FlinkEngineConnResourceFactory.getRequestResource() 返回 UserNodeResource资源;


DefaultEngineAskEngineService.askEngine(){ //com.webank.wedatasphere.linkis.manager.am.service.engine
	if(! engineAskRequest.getLabels.containsKey(LabelKeyConstant.EXECUTE_ONCE_KEY)){ //一般会进入这里; 
		val engineReuseRequest = new EngineReuseRequest()
		val reuseNode = engineReuseService.reuseEngine(engineReuseRequest);{//DefaultEngineReuseService.reuseEngine
			info(s"Start to reuse Engine for request: $engineReuseRequest")
			//3. 执行Select 判断label分数、判断是否可用、判断负载
			choseNode = nodeSelector.choseNode(engineScoreList.toArray)
			// choseNode 未空还没准备好的话, 先报RetryEx错返回; 
			if (choseNode.isEmpty) {throw new LinkisRetryException();}
			//5. 调用EngineNodeManager 进行reuse 如果reuse失败，则去掉该engine进行重新reuse走3和4
			// 进入这里说明 choseNode:Option[Node] 不为空, 已经有 ExecutorNode了; 
			engine = Utils.tryAndWarn(getEngineNodeManager.reuseEngine(choseNode.get.asInstanceOf[EngineNode]));{
				
			}
			
			engine
		}
		if (null != reuseNode) {// 如果已存在, 则不未空; 否则 reuseNode == null;
			return reuseNode
		}
	}
	// 当相应引擎不存在时, 才会进入这里创建引擎; 
	engineAskRequest.getLabels.remove("engineInstance")
	val createNodeThread = Future {
		val createNode = engineCreateService.createEngine(engineCreateRequest, smc);{//DefaultEngineCreateService.createEngine
			// 1. 检查Label是否合法
			var labelList: util.List[Label[_]] = LabelUtils.distinctLabel(labelBuilderFactory.getLabels(engineCreateRequest.getLabels),userLabelService.getUserLabels(engineCreateRequest.getUser))
			//2. NodeLabelService getNodesByLabel  获取EMNodeList
			val emScoreNodeList = getEMService().getEMNodes(emLabelList.filter(!_.isInstanceOf[EngineTypeLabel]))
			//3. 执行Select  比如负载过高，返回没有负载低的EM，每个规则如果返回为空就抛出异常
			val choseNode = if (null == emScoreNodeList || emScoreNodeList.isEmpty) null else nodeSelector.choseNode(emScoreNodeList.toArray)
			val emNode = choseNode.get.asInstanceOf[EMNode]
			//4. 请求资源
			val (resourceTicketId, resource) = requestResource(engineCreateRequest, labelFilter.choseEngineLabel(labelList), emNode, timeout);{
				//读取管理台的的配置: 主要读取globalConfig + engineConfig(configuration_config_key表中)
				if(engineCreateRequest.getProperties == null) engineCreateRequest.setProperties(new util.HashMap[String,String]())
				val configProp = engineConnConfigurationService.getConsoleConfiguration(labelList);{//DefaultEngineConnConfigurationService.getConsoleConfiguration()
					val properties = new JMap[String, String];// 新建Props
					val userCreatorLabelOption = label.find(_.isInstanceOf[UserCreatorLabel])
					val engineTypeLabelOption = label.find(_.isInstanceOf[EngineTypeLabel])
					if (userCreatorLabelOption.isDefined) {
						// 从全局环境缓存对象ConfigurationMapCache: globalMapCache: RPCMapCache[UserCreatorLabel, String, String] 中获取 LINKISCLI-flink的UserCreatorLabel对应参数;
						val globalConfig = Utils.tryAndWarn(ConfigurationMapCache.globalMapCache.getCacheMap(userCreatorLabel));{// RPCMapCache.getCacheMap()
							// sender=linkis-ps-publicservice
							val result: ResponseQueryConfig =sender.ask(createRequest(key));
							createMap(result);
						}
						properties.putAll(engineConfig);// 这个加载7个全局配置(wds.linkis.rm.yarnqueue.memory.max等)
						if (engineTypeLabelOption.isDefined) {
							// 读取从linkis_ps_configuration_config_key表中加载的 flink-1.12.2对应的5个flink配置:  flink.test.jobmanager.memory
							val engineConfig = Utils.tryAndWarn(ConfigurationMapCache.engineMapCache.getCacheMap((userCreatorLabel, engineTypeLabel)))
							properties.putAll(engineConfig)
						}
					}
				}
				configProp.foreach(keyValue => {
					if (! props.containsKey(keyValue._1)) {// 只对非用户定义的 config 用环境参数覆盖; 
						props.put(keyValue._1, keyValue._2)
					}
				});
				val timeoutEngineResourceRequest = TimeoutEngineResourceRequest(timeout, engineCreateRequest.getUser, labelList, engineCreateRequest.getProperties)
				// 向 engineplugin(engineconnmanager)发Rpc请求获取flink资源申请:UserNodeResource;
				val resource = engineConnPluginPointer.createEngineResource(timeoutEngineResourceRequest);{
					getEngineConnPluginSender().ask(engineResourceRequest){// BaseRPCSender.execute(message)()
						execute(message);{// BaseRPCSender.execute()
							case protocol: Protocol if getRPCInterceptors.nonEmpty => 
								val rpcInterceptorChain = createRPCInterceptorChain();{
									new BaseRPCInterceptorExchange(protocol, () => op)
								}
								// 实际上,该请求会最终被 cg-engineplugin进程的 DefaultEngineConnResourceFactoryService.createEngineResource()处理;
								rpcInterceptorChain.handle(createRPCInterceptorExchange(protocol, op)); //rpc: RPCSender(linkis-cg-engineconnmanager, bdnode111.hjq.com:9102)
								{// DefaultEngineConnResourceFactoryService.createEngineResource()
									DefaultEngineConnResourceFactoryService.getResourceFactoryBy(engineTypeLabel)
									DefaultEngineConnResourceFactoryService.createEngineResource(engineResourceRequest)
									FlinkEngineConnResourceFactory.getRequestResource();{
											val containers = 2 = wds.linkis.engineconn.flink.app.parallelism(4) / flink.taskmanager.numberOfTaskSlots(2);
											val yarnMemory = 10G = flink.taskmanager.memory(4) * containers(2) + flink.jobmanager.memory(2) +"G"
											val yarnCores = 5 = flink.taskmanager.cpu.cores(2) * containers(2) +1 
											new DriverAndYarnResource(loadResource, new YarnResource(yarnMemory, yarnCores, 0, LINKIS_QUEUE_NAME.getValue(properties)));
									}
								}
							
							case _ => op
						}
						val response = getRPC.receiveAndReply(msg)
					} match {
						// 向engineconnmanager发Rpc请求 节点资源对象: nodeResource:UserNodeResource
						case nodeResource: NodeResource => nodeResource
						 case _ => throw new AMErrorException(AMConstant.ENGINE_ERROR_CODE, s"Failed to create engineResource")
					}
				}
				
				resourceManager.requestResource(LabelUtils.distinctLabel(labelList, emNode.getLabels), resource, timeout){//DefaultResourceManager.requestResource
					val requestResourceService = getRequestResourceService(resource.getResourceType)
					labelContainer.getResourceLabels.foreach{ label => 
						labelContainer.setCurrentLabel(label)
						var canRequest:Boolean = requestResourceService.canRequest(labelContainer, resource);{//RequestResourceService.canRequest
							val requestedDriverAndYarnResource = resource.getMaxResource.asInstanceOf[DriverAndYarnResource]
							val requestedYarnResource = requestedDriverAndYarnResource.yarnResource
							
							val providedYarnResource = externalResourceService.getResource(ResourceType.Yarn, labelContainer, yarnIdentifier)
							val (maxCapacity, usedCapacity) = (providedYarnResource.getMaxResource, providedYarnResource.getUsedResource)
							
							val queueLeftResource = maxCapacity  - usedCapacity
						}
						if (!canRequest){
							return NotEnoughResource(s"Labels：$labels not enough resource")
						}
					}
				} match {
				  case AvailableResource(ticketId) =>
					(ticketId, resource)
				  case NotEnoughResource(reason) =>
					warn(s"资源不足，请重试: $reason")
					throw new LinkisRetryException(AMConstant.EM_ERROR_CODE, s"资源不足，请重试: $reason")
				}
			}
			
			//5. 封装engineBuildRequest对象,并发送给EM进行执行
			val engineBuildRequest = EngineConnBuildRequestImpl(resourceTicketId,resource,EngineConnCreationDescImpl());
		}
		val createEngineNode = getEngineNodeManager.useEngine(createNode, timeout)
		createEngineNode
	}
	
	createNodeThread.onComplete {
		case Success(engineNode) =>
			smc.getSender.send(EngineCreateSuccess(engineAskAsyncId, engineNode))
		case Failure(exception) => 	
	}
	// 上面异步创建Engine, 这里先回复1个 EngineAskAsyncResponse 消息,
	EngineAskAsyncResponse(engineAskAsyncId, Sender.getThisServiceInstance)
}


resourceManager.requestResource(labels, resource: NodeResource, waitTimeout: Long){//DefaultResourceManager.requestResource()
	val requestResourceService = getRequestResourceService(resource.getResourceType);
	labelContainer.getResourceLabels.foreach{ label => 
		labelContainer.setCurrentLabel(label)
		var canRequest:Boolean = requestResourceService.canRequest(labelContainer, resource);{//RequestResourceService.canRequest
			val superCanReq = super.canRequest(labelContainer, resource);{// RequestResourceService.canRequest()
				var labelResource = labelResourceService.getLabelResource(labelContainer.getCurrentLabel);{//LabelResourceServiceImpl.getLabelResource()
					resourceLabelService.getResourceByLabel(label);{//DefaultResourceLabelService.
						val persistenceResource = label match {
							case p: PersistenceLabel => resourceLabelPersistence.getResourceByLabel(p)
							// EMInstanceLabel ,进入这里; 
							case _ =>  resourceLabelPersistence.getResourceByLabel(LabelManagerUtils.convertPersistenceLabel(label));{//DefaultResourceLabelPersistence.
								if (label.getId() != null && label.getId() > 0) {
									return labelManagerMapper.listResourceByLaBelId(label.getId());
								}else{
									PersistenceLabel dbLabel = labelManagerMapper.getLabelByKeyValue(label.getLabelKey(), label.getStringValue());
									if (null == dbLabel) {
										return Collections.emptyList();
									} else {
										// SELECT r.* FROM linkis_cg_manager_label_resource lr, linkis_cg_manager_linkis_resources r, linkis_cg_manager_label l WHERE lr.resource_id = r.id and lr.label_id = #{labelId} and l.id = #{labelId}
										// 通过labelId获取到resource, 从linkis_cg_manager_linkis_resources 表读取max_resource,left_resource等资源结果; 
										return labelManagerMapper.listResourceByLaBelId(dbLabel.getId());
									}
								}
							}
						}
						if(persistenceResource.isEmpty){
							null
						}else {
							ResourceUtils.fromPersistenceResource(persistenceResource.get(0))
						}
					}
				}
				
				labelResource.setMinResource(Resource.initResource(labelResource.getResourceType))
				// 这里重新基于max - userd - locked = left剩余资源,并赋值给 LeftResource变量
				labelResource.setLeftResource(labelResource.getMaxResource - labelResource.getUsedResource - labelResource.getLockedResource)
				if(labelResource != null){
					val labelAvailableResource = labelResource.getLeftResource// CommonNodeResource.DriverAndYarnResource
					if(labelAvailableResource < resource.getMinResource && enableRequest){
						info(s"Failed check: ${labelContainer.getUserCreatorLabel.getUser} want to use label [${labelContainer.getCurrentLabel}] resource[${resource.getMinResource}] > label available resource[${labelAvailableResource}]")
						val notEnoughMessage = generateNotEnoughMessage(aggregateResource(labelResource.getUsedResource, labelResource.getLockedResource), labelAvailableResource)
						throw new RMWarnException(notEnoughMessage._1, notEnoughMessage._2)
					}
					// 资源校验通过,就会打印 Passed日志; 
					info(s"Passed check: ${labelContainer.getUserCreatorLabel.getUser} want to use label [${labelContainer.getCurrentLabel}] resource[${resource.getMinResource}] <= label available resource[${labelAvailableResource}]")
				}
			}
			if(! superCanReq) {
				return false
			}
			val requestedDriverAndYarnResource = resource.getMaxResource.asInstanceOf[DriverAndYarnResource]
			val requestedYarnResource = requestedDriverAndYarnResource.yarnResource
			
			val providedYarnResource = externalResourceService.getResource(ResourceType.Yarn, labelContainer, yarnIdentifier)
			val (maxCapacity, usedCapacity) = (providedYarnResource.getMaxResource, providedYarnResource.getUsedResource)
			
			val queueLeftResource = maxCapacity  - usedCapacity
		}
		if (!canRequest){
			return NotEnoughResource(s"Labels：$labels not enough resource")
		}
	}
}
				



// cg-engineplugin:  "" 线程: 通过各engine的ResourceFactory获取默认资源参数,并返回 DriverAndYarnResource;
// fromTh: cg-linkismanager 
// nextTh: cg-linkismanager 
// 处理逻辑: DefaultEngineConnResourceFactoryService.getEngineConnPlugin() -> FlinkEngineConnResourceFactory.createEngineResource()

DefaultEngineConnResourceFactoryService.createEngineResource(engineResourceRequest: EngineResourceRequest){
	val engineTypeOption = engineResourceRequest.labels.find(_.isInstanceOf[EngineTypeLabel]);// 解析成有TypeLabel的 
	if (engineTypeOption.isDefined) {
		val engineTypeLabel = engineTypeOption.get.asInstanceOf[EngineTypeLabel]
		getResourceFactoryBy(engineTypeLabel){
			val engineConnPluginInstance = EngineConnPluginsLoader.getEngineConnPluginsLoader().getEngineConnPlugin(engineType)
			engineConnPluginInstance.plugin.getEngineResourceFactory
		}
			.createEngineResource(engineResourceRequest);{
				// 不同的引擎,管理的 XXEngineResource资源情况不一样,实现类不同
				GenericEngineResourceFactory.createEngineResource(){}
				
				AbstractEngineResourceFactory.createEngineResource(){
					val engineResource = new UserNodeResource
					val minResource = getMinRequestResource(engineResourceRequest);{
						getRequestResource(engineResourceRequest.properties)
					}
					val maxResource = getMaxRequestResource(engineResourceRequest);{
						// 由子类: Flink/Spark**ResourceFactory的getRequestResource()实现; 
						getRequestResource(engineResourceRequest.properties);
					}
					engineResource.setMaxResource(maxResource)
					engineResource
				}
				
				SparkEngineConnResourceFactory.createEngineResource(){}
				
				FlinkEngineConnResourceFactory.createEngineResource(){// linkis-engineconn-plugins 下面的flink包
					// 由 AbstractEngineResourceFactory.createEngineResource() 实现
					val minResource = getMinRequestResource().getRequestResource();{
						// 默认=2, 可由 flink.container.num,wds.linkis.engineconn.flink.app.parallelism,flink.taskmanager.numberOfTaskSlots 指定;
						val containers = if(properties.containsKey(LINKIS_FLINK_CONTAINERS)) {//flink.container.num==2(默认); 没配置,跳过;
						  val containers = LINKIS_FLINK_CONTAINERS.getValue(properties)
						  properties.put(FLINK_APP_DEFAULT_PARALLELISM.key, String.valueOf(containers * LINKIS_FLINK_TASK_SLOTS.getValue(properties)))
						  containers
						} else{
							// 2 = wds.linkis.engineconn.flink.app.parallelism(4) / flink.taskmanager.numberOfTaskSlots(2); 
							math.round(FLINK_APP_DEFAULT_PARALLELISM.getValue(properties) * 1.0f / LINKIS_FLINK_TASK_SLOTS.getValue(properties))
						}
						
						// 10G= flink.taskmanager.memory(4) * containers(2) + flink.jobmanager.memory(2) +"G"
						val yarnMemory = ByteTimeUtils.byteStringAsBytes(LINKIS_FLINK_TASK_MANAGER_MEMORY.getValue(properties) * containers + "G")
								+ ByteTimeUtils.byteStringAsBytes(LINKIS_FLINK_JOB_MANAGER_MEMORY.getValue(properties) + "G")
						// 5 yarnCores= flink.taskmanager.cpu.cores(2) * containers(2) +1
						val yarnCores = LINKIS_FLINK_TASK_MANAGER_CPU_CORES.getValue(properties) * containers + 1
						// loadResource: flink.client.memory(4),1,1 
						val loadResource= new LoadInstanceResource(LINKIS_FLINK_CLIENT_MEMORY.getValue(properties),LINKIS_FLINK_CLIENT_CORES,1 )
						
						new DriverAndYarnResource(loadResource, new YarnResource(yarnMemory, yarnCores, 0, LINKIS_QUEUE_NAME.getValue(properties));
					}
				}
				}
			}
	} else {throw new EngineConnPluginErrorException(10001, "EngineTypeLabel are requested")}
}

// 定义拼接 commands 参数: 接受响应 cg-engineconnmanager:LinuxProcessEngineConnLaunchService.launchEngineConn()中的 engineConnBuildRequest请求
DefaultEngineConnLaunchService.createEngineConnLaunchRequest(engineBuildRequest: EngineConnBuildRequest){
	val engineTypeOption = engineBuildRequest.labels.find(_.isInstanceOf[EngineTypeLabel])
	if (engineTypeOption.isDefined) {
		getEngineLaunchBuilder(engineTypeLabel).buildEngineConn(engineBuildRequest);{//FlinkEngineConnLaunchBuilder 父类ProcessEngineConnLaunchBuilder.buildEngineConn()
			engineConnBuildRequest match { case richer: RicherEngineConnBuildRequest => bmlResources.addAll(util.Arrays.asList(richer.getBmlResources: _*))}
			bmlResources.addAll(getBmlResources);{//JavaProcessEngineConnLaunchBuilder.getBmlResources()
				val engineConnResource = engineConnResourceGenerator.getEngineConnBMLResources(engineType);{//EngineConnResourceService.
					val engineConnBMLResourceRequest = new GetEngineConnResourceRequest
					getEngineConnBMLResources(engineConnBMLResourceRequest);{//DefaultEngineConnResourceService.
						val engineConnType = engineConnBMLResourceRequest.getEngineConnType
						val engineConnBmlResources = asScalaBuffer(engineConnBmlResourceDao.getAllEngineConnBmlResource(engineConnType, "v" + version));{
							// SELECT * FROM linkis_cg_engine_conn_plugin_bml_resources WHERE engine_conn_type=#{engineConnType}
							// 就是获取 flink-1.12.2的 conf+lib的资源情况和ID;
							EngineConnBmlResourceDao.getAllEngineConnBmlResource();
						}
						
						val confBmlResource = engineConnBmlResources.find(_.getFileName == LaunchConstants.ENGINE_CONN_CONF_DIR_NAME + ".zip").map(parseToBmlResource).get
						val libBmlResource = engineConnBmlResources.find(_.getFileName == LaunchConstants.ENGINE_CONN_LIB_DIR_NAME + ".zip").map(parseToBmlResource).get
						val otherBmlResources = engineConnBmlResources.filterNot(r => r.getFileName == LaunchConstants.ENGINE_CONN_CONF_DIR_NAME + ".zip" || r.getFileName == LaunchConstants.ENGINE_CONN_LIB_DIR_NAME + ".zip").map(parseToBmlResource).toArray
						new EngineConnResource
					}
				}
				Array(engineConnResource.getConfBmlResource, engineConnResource.getLibBmlResource) ++: engineConnResource.getOtherBmlResources.toList
			}
			engineConnBuildRequest.labels.find(_.isInstanceOf[UserCreatorLabel]).map {case label: UserCreatorLabel =>{
				val commands:  = getCommands();{// FlinkEngineConnLaunchBuilder.
					val properties = engineConnBuildRequest.engineConnCreationDesc.properties
					properties.put(EnvConfiguration.ENGINE_CONN_MEMORY.key, FlinkResourceConfiguration.LINKIS_FLINK_CLIENT_MEMORY.getValue(properties) + "G")
					super.getCommands();{//JavaProcessEngineConnLaunchBuilder.getCommands():Array[String]
						commandLine += (variable(JAVA_HOME) + "/bin/java")
						commandLine += "-server"
						val engineConnMemory = EngineConnPluginConf.JAVA_ENGINE_REQUEST_MEMORY.getValue.toString //从 wds.linkis.engineconn.java.driver.memory 读取;
						commandLine += ("-Xmx" + engineConnMemory)
						val javaOPTS = getExtractJavaOpts //从wds.linkis.engineConn.java.extraOpts 读取
						if (StringUtils.isNotEmpty(EnvConfiguration.ENGINE_CONN_DEFAULT_JAVA_OPTS.getValue)){//若参数 wds.linkis.engineConn.javaOpts.default 不为空, 加到commandLine
							EnvConfiguration.ENGINE_CONN_DEFAULT_JAVA_OPTS.getValue.format(getGcLogDir(engineConnBuildRequest)).split("\\s+").foreach(commandLine += _)
						}
						if (StringUtils.isNotEmpty(javaOPTS)){
							javaOPTS.split("\\s+").foreach(commandLine += _)
						} 
						getLogDir(engineConnBuildRequest).trim.split(" ").foreach(commandLine += _)
						commandLine += ("-Djava.io.tmpdir=" + variable(TEMP_DIRS));
						// 通过wds.linkis.engineconn.debug.enable 设置Debug参数; 
						if (EnvConfiguration.ENGINE_CONN_DEBUG_ENABLE.getValue) {
						  commandLine += s"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=${variable(RANDOM_PORT)}"
						}
						commandLine += "-cp"
						commandLine += variable(CLASSPATH)
						commandLine += getMainClass
					}
				}
				CommonProcessEngineConnLaunchRequest(engineConnBuildRequest.ticketId, getEngineStartUser(label),
					engineConnBuildRequest.labels, engineConnBuildRequest.engineResource, bmlResources, environment, getNecessaryEnvironment,
					engineConnBuildRequest.engineConnCreationDesc, getEngineConnManagerHooks, getCommands, getMaxRetries );
			}}
			
		}
	}
}

"CLASSPATH":{
	"${HADOOP_CONF_DIR}:
	${HIVE_CONF_DIR}:
	${PWD}/conf:		wordDir.conf=plugin/flink/dist/conf
	"${PWD}/lib/*":		wordDir.lib =plugin/flink/dist/lib(308)	包含308个依赖,其中flink 31个包, hadoop相关5个包; hive 0个包,linkis 31个包;
	"${LINKIS_HOME}/lib/linkis-commons/public-module/*":	linkis的283个public依赖,其中8个linkis公共包;大部分spring,jetty,json,common包;
	${PWD}",
}




// cg-engineconnmanager 模块: LinkisECMApplication 进程: "" 线程 LinuxProcess脚本引擎执行 sudo su - bigdata -c sh engineConnExec.sh 脚本
// fromTh:  
// nextTh: cg-engineconn 模块: launch.EngineConnServer 进程, 运行Flink Table 
// 处理逻辑: LinuxProcessEngineConnLaunchService.launchEngineConn():runner.run().launch().exec.execute(): ProcessBuilder.start()
// logDir: $LINKIS_HOME/logs/linkis-cg-engineconnmanager.log
// 关键变量: export CLASSPATH="${HADOOP_CONF_DIR}:${HIVE_CONF_DIR}:${PWD}/conf:${PWD}/lib/*:/opt/dsslinkisall/dsslinkisall-release/linkis/lib/linkis-commons/public-module/*:${PWD}"


LinuxProcessEngineConnLaunchService.launchEngineConn(){
	// 向 cg-engineplugin 发送EngineConnBuildRequest请求,如果Build成功则接收 EngineConnLaunchRequest事件,并launch启动 EngineConnServer进程;  
	Sender.getSender(ENGINECONN_PLUGIN_SPRING_NAME).ask(engineConnBuildRequest) match {
		case request: EngineConnLaunchRequest if ENGINECONN_CREATE_DURATION._1 != 0L =>
			// 从 cg-engineplugin接受到成功build,核心是 EngineConnLaunchRequest.commands:String[] 包含了java EngineConnServer启动命令; 
			launchEngineConn(request, ENGINECONN_CREATE_DURATION._1);{//AbstractEngineConnLaunchService.
				//1.创建engineConn和runner,launch 并设置基础属性
				val conn = createEngineConn
				val runner = createEngineConnLaunchRunner
				val launch = createEngineConnLaunch
				//2.资源本地化，并且设置ecm的env环境信息
				getResourceLocalizationServie.handleInitEngineConnResources(request, conn);{//BmlResourceLocalizationService.
					request match {case request: ProcessEngineConnLaunchRequest =>{
						val files = request.bmlResources
						val ticketId = request.ticketId
						// 创建 workDir,logDir,tmpDir,
						val workDir = createDirIfNotExit(localDirsHandleService.getEngineConnWorkDir(user, ticketId));
						val logDirs = createDirIfNotExit(localDirsHandleService.getEngineConnLogDir(user, ticketId))
						// 下载
						files.foreach(downloadBmlResource(request, linkDirsP, _, workDir));{//BmlResourceLocalizationService.downloadBmlResource()
							resource.getVisibility match {case BmlResource.BmlResourceVisibility.Public =>{
								val publicDir = localDirsHandleService.getEngineConnPublicDir ///tmp/dsslinkisall/engConn/engineConnPublickDir
								val bmlResourceDir = schema + Paths.get(publicDir, resourceId, version).toFile.getPath //file:///tmp/dsslinkisall/engConn/engineConnPublickDir/08385d14-ac1f-4543-8d0a-1aa90d954174/v000002
								val fsPath = new FsPath(bmlResourceDir)
								if (!fs.exists(fsPath)) {
									ECMUtils.downLoadBmlResourceToLocal(resource, user, fsPath.getPath)
									FileSystemUtils.mkdirs(fs, new FsPath(unzipDir), Utils.getJvmUser)
									ZipUtils.unzip(bmlResourceDir + File.separator + resource.getFileName, unzipDir)
									fs.delete(new FsPath(bmlResourceDir + File.separator + resource.getFileName))
								}
								
								//2.软连，并且添加到map
								val dirAndFileList = fs.listPathWithError(fsPath)
								linkDirs.put(path.getPath, workDir + seperator + name);
							}}
						}
						engineConn.getEngineConnLaunchRunner.getEngineConnLaunch.setEngineConnManagerEnv(new EngineConnManagerEnv {});
					}}
				}
				//3.添加到list
				LinkisECMApplication.getContext.getECMSyncListenerBus.postToAll(EngineConnAddEvent(conn));{// ECMSyncListenerBus.postToAll()
					while (iter.hasNext) {
						val listener = iter.next()
						doPostEvent(listener, event);{// ECMSyncListenerBus.doPostEvent()
							listener.onEvent(event);{// DefaultEngineConnListService.onEvent()
								info(s"Deal event $event")
								event match {
								  case event: ECMClosedEvent => shutdownEngineConns(event)
								  case event: YarnAppIdCallbackEvent => updateYarnAppId(event)
								  case event: YarnInfoCallbackEvent => updateYarnInfo(event)
								  case event: EngineConnPidCallbackEvent => updatePid(event)
								  case EngineConnAddEvent(engineConn) => addEngineConn(engineConn);{// DefaultEngineConnListService.addEngineConn()
									if (LinkisECMApplication.isReady){engineConnMap.put(engineConn.getTickedId, engineConn);}
								  }
								  case EngineConnStatusChangeEvent(tickedId, updateStatus) => updateEngineConnStatus(tickedId, updateStatus)
								  case _ =>
								}
								
							}
						}
					}
				}
				//4.run
				beforeLaunch(request, conn, duration)
				runner.run();{//EngineConnLaunchRunnerImpl.run()
					launch.launch();{//ProcessEngineConnLaunch[LinuxProcessEngineConnLaunch].launch()
						prepareCommand();{//ProcessEngineConnLaunch.prepareCommand()
							processBuilder: UnixProcessEngineCommandBuilder = newProcessEngineConnCommandBuilder();// 构建 /bin/bash
							initializeEnv();{//ProcessEngineConnLaunch.initializeEnv
								//加载Request中5个环境变量: $HADOOP_CONF_DIR,HIVE_CONF_DIR,FLINK_HOME,FLINK_CONF_DIR, CLASSPATH
								val environment: HashMap = request.environment
								Environment.values foreach {
									case USER => environment.put(USER.toString, request.user)
									case ECM_HOME => environment.put(ECM_HOME.toString, engineConnManagerEnv.engineConnManagerHomeDir)
									case PWD => environment.put(PWD.toString, engineConnManagerEnv.engineConnWorkDir)
									case LOG_DIRS => environment.put(LOG_DIRS.toString, engineConnManagerEnv.engineConnLogDirs)
									case TEMP_DIRS => environment.put(TEMP_DIRS.toString, engineConnManagerEnv.engineConnTempDirs)
									case ECM_HOST => environment.put(ECM_HOST.toString, engineConnManagerEnv.engineConnManagerHost)
									case ECM_PORT => environment.put(ECM_PORT.toString, engineConnManagerEnv.engineConnManagerPort)
									case HADOOP_HOME => putIfExists(HADOOP_HOME)
									case HADOOP_CONF_DIR => putIfExists(HADOOP_CONF_DIR)
									case HIVE_CONF_DIR => putIfExists(HIVE_CONF_DIR)
									case RANDOM_PORT => environment.put(RANDOM_PORT.toString, findAvailPort().toString);// new ServerSocket(0), 随机;
									case _ =>
								}
							}
							// 移除了CLASSPATH变量,重新定义? <<LHADOOP_CONF_DIRR>><<CPS>><<LHIVE_CONF_DIRR>><<CPS>><<LPWDR>>/conf<<CPS>><<LPWDR>>/lib/*<<CPS>>/opt/dsslinkisall/dsslinkisall-release/linkis/lib/linkis-commons/public-module/*<<CPS>><<LPWDR>>
							val classPath = request.environment.remove(CLASSPATH.toString);// env需要考虑顺序问题
							
							processBuilder.setEnv(CLASSPATH.toString, processBuilder.replaceExpansionMarker(classPath.replaceAll(CLASS_PATH_SEPARATOR, File.pathSeparator)));{
								setEnv("CLASSPATH","${HADOOP_CONF_DIR}:${HIVE_CONF_DIR}:${PWD}/conf:${PWD}/lib/*:/opt/dsslinkisall/dsslinkisall-release/linkis/lib/linkis-commons/public-module/*:${PWD}");
							}
							// 构建执行命令: execCommand: List<String> = request.commands(25个) + engineArgs(19个);
							val execCommand = request.commands.map(processBuilder.replaceExpansionMarker(_)) ++ getCommandArgs
							processBuilder.setCommand(execCommand)
							
							// 吧内容赋值给engineConnExec.sh;
							preparedExecFile = new File(engineConnManagerEnv.engineConnWorkDir, "engineConnExec.sh").getPath
							processBuilder.writeTo(output);{//ShellProcessEngineCommandBuilder.writeTo()
								IOUtils.write(sb, output)
							}
						}
						val exec = newProcessEngineConnCommandExec(sudoCommand(request.user, execFile.mkString(" ")), engineConnManagerEnv.engineConnWorkDir)
						exec.execute();{// ShellProcessEngineCommandExec.execute()
							info(s"Invoke subProcess, base dir ${this.baseDir} cmd is: ${command.mkString(" ")}")
							val builder = new ProcessBuilder(command: _*)
							if (environment != null) builder.environment.putAll(this.environment)
							if (baseDir != null) builder.directory(new File(this.baseDir));
							//执行脚本命令: sudo su - bigdata -c sh /tmp/dsslinkisall/engConn/bigdata/workDir/350dcb50-521e-41bf-afb2-bfb99aa74156/engineConnExec.sh
							process = builder.start();
						}
						process = exec.getProcess
					}
				}
				launch match {case pro: ProcessEngineConnLaunch =>{
					val serviceInstance = ServiceInstance(GovernanceCommonConf.ENGINE_CONN_SPRING_NAME.getValue, ECMUtils.getInstanceByPort(pro.getEngineConnPort))
					conn.setServiceInstance(serviceInstance)
				}}
				val engineNode = new AMEngineNode()
				engineNode
			}
		case request: EngineConnLaunchRequest =>
			launchEngineConn(request, smc.getAttribute(DURATION_KEY).asInstanceOf[Duration]._1)
    }
}




/usr/java/jdk-release/bin/java \
-server -Xmx1g -Xms1g -XX:+UseG1GC -XX:MaxPermSize=250m -XX:PermSize=128m \
-Xloggc:/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps \
-Dwds.linkis.configuration=linkis-engineconn.properties -Dwds.linkis.gateway.url=http://192.168.51.111:9001 -Dlogging.file=log4j2-engineconn.xml \
-DTICKET_ID=a689125c-dc7e-4f0f-8343-6fa6e93c4f48 -Djava.io.tmpdir=/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/tmp \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=38767 \
-cp /opt/hadoop/etc/hadoop:/opt/hive/conf:/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/conf:/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/lib/*:/opt/dsslinkisall/dsslinkisall-release/linkis/lib/linkis-commons/public-module/*:/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48 \
com.webank.wedatasphere.linkis.engineconn.launch.EngineConnServer \
--engineconn-conf wds.linkis.rm.instance=1 --engineconn-conf label.userCreator=bigdata-LINKISCLI \
--engineconn-conf ticketId=a689125c-dc7e-4f0f-8343-6fa6e93c4f48 --engineconn-conf wds.linkis.rm.yarnqueue.memory.max=300G \
--engineconn-conf label.engineType=flink-1.12.2 --engineconn-conf wds.linkis.rm.yarnqueue.instance.max=30 --engineconn-conf flink.container.num=1 \
--engineconn-conf flink.taskmanager.memory=1 --engineconn-conf wds.linkis.rm.client.memory.max=20G --engineconn-conf wds.linkis.rm.client.core.max=10 \
--engineconn-conf wds.linkis.engineConn.memory=4G --engineconn-conf wds.linkis.rm.yarnqueue.cores.max=150 --engineconn-conf user=bigdata \
--engineconn-conf wds.linkis.rm.yarnqueue=default --spring-conf eureka.client.serviceUrl.defaultZone=http://192.168.51.111:20303/eureka/ \
--spring-conf logging.config=classpath:log4j2-engineconn.xml --spring-conf spring.profiles.active=engineconn --spring-conf server.port=42018 \
--spring-conf spring.application.name=linkis-cg-engineconn

[bigdata@bdnode111 a689125c-dc7e-4f0f-8343-6fa6e93c4f48]$ pwd
/tmp/dsslinkisall/engConn/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48
[bigdata@bdnode111 a689125c-dc7e-4f0f-8343-6fa6e93c4f48]$ ls
conf  engineConnExec.sh  lib  logs  tmp
[bigdata@bdnode111 a689125c-dc7e-4f0f-8343-6fa6e93c4f48]$ ls conf/
flink-sql-defaults.yaml  linkis-engineconn.properties  log4j2-engineconn.xml



// cg-engineconn 模块: launch.EngineConnServer 进程: 构建Flink执行命令 ?
// fromTh:  
// nextTh: 
// 处理逻辑: 
// # 构建AM启动命令amContext: createEngineConn() createEngineConnSession(): 
// #. executeEngineConn() ->execute()->createLabelExecutor()->labelExecutor.init(): 调Yarn Api: YarnClusterDescriptor.deploySessionCluster() 启动TM: YarnSessionClusterEntrypoint;

// logDir: $ENGINECONN_ROOT_PATH(wds.linkis.engineconn.root.dir)/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/logs


EngineConnServer.main(){
	init(args);
	//2. 创建EngineConn: 包括利用factory创建 FlinkEnv等初始化环境; 
	val engineConn = getEngineConnManager.createEngineConn(getEngineCreationContext);{//DefaultEngineConnManager.createEngineConn()
		if(engineConn != null) return engineConn // engineConn:AbstractEngineConnFactory 是单例对象;
		this.engineConn = EngineConnObject.getEngineConnPlugin.getEngineConnFactory.createEngineConn(engineCreationContext);{// AbstractEngineConnFactory.createEngineConn()
			val engineConn = new DefaultEngineConn(engineCreationContext)
			val engineConnSession = createEngineConnSession(engineCreationContext);{
				// MultiExecutorEngineConnFactory 包括 spark,flink的 ConnFactory子类;
				FlinkEngineConnFactory.createEngineConnSession(){
					val environmentContext = createEnvironmentContext(engineCreationContext);{
						// 从 插件目录flink/conf/flink-sql-defaults.yaml 中,解析 env:Properties;
						val defaultEnv:Environment = Environment.parse(this.getClass.getClassLoader.getResource("flink-sql-defaults.yaml"));{
							return new ConfigUtil.LowerCaseYamlMapper().readValue(url, Environment.class);
						}
						val hadoopConfDir = EnvConfiguration.HADOOP_CONF_DIR.getValue(options)
						val flinkHome = FLINK_HOME.getValue(options)
						val flinkConfDir = FLINK_CONF_DIR.getValue(options)
						val flinkLibRemotePath = FLINK_LIB_REMOTE_PATH.getValue(options)
						val flinkDistJarPath = FLINK_DIST_JAR_PATH.getValue(options)
						val providedLibDirsArray = FLINK_LIB_LOCAL_PATH.getValue(options).split(",")
						val shipDirsArray = FLINK_SHIP_DIRECTORIES.getValue(options).split(",")
						val context = new EnvironmentContext(defaultEnv, new Configuration, hadoopConfDir, flinkConfDir, flinkHome,flinkDistJarPath, flinkLibRemotePath, providedLibDirsArray, shipDirsArray, null)
						
						val flinkMainClassJar = FLINK_APPLICATION_MAIN_CLASS_JAR.getValue(options)
						
					}
					val flinkEngineConnContext = createFlinkEngineConnContext(environmentContext) = new FlinkEngineConnContext(environmentContext);
					val executionContext = createExecutionContext(options, environmentContext);{//FlinkEngineConnFactory.createExecutionContext()
						// 这个即Session Env;
						val environment = environmentContext.getDeploymentTarget match {
							case YarnDeploymentTarget.PER_JOB | YarnDeploymentTarget.SESSION =>{
								val planner = FlinkEnvConfiguration.FLINK_SQL_PLANNER.getValue(options)
								val properties = new util.HashMap[String, String]
								properties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_PLANNER, planner)
								properties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_TYPE, executionType)
								//把上述 properties 和defaultEnv的默认配置/tabel/catalog等资源 合并一起;
								Environment.enrich(environmentContext.getDefaultEnv, properties, Collections.emptyMap());{
									final Environment enrichedEnv = new Environment();
									enrichedEnv.catalogs = new LinkedHashMap<>(env.getCatalogs());
									// execution:props 执行环境,conf配置,deploy 需要合并;
									enrichedEnv.execution = ExecutionEntry.enrich(env.execution, properties);
									enrichedEnv.configuration = ConfigurationEntry.enrich(env.configuration, properties);
									enrichedEnv.deployment = DeploymentEntry.enrich(env.deployment, properties);
									return enrichedEnv;
								}
							}
							case YarnDeploymentTarget.APPLICATION => null
						}
						ExecutionContext.builder(environmentContext.getDefaultEnv, environment, environmentContext.getDependencies,environmentContext.getFlinkConfig).build();{
							if(sessionEnv == null){
								this.currentEnv = defaultEnv;
							}
							Environment environment = this.currentEnv == null ? Environment.merge(defaultEnv, sessionEnv) : this.currentEnv;{//Environment.merge()
								if(null==env2){
									return env1;
								}
								modules.putAll(env2.getModules());
								catalogs.putAll(env2.getCatalogs());
								mergedEnv.execution = ExecutionEntry.merge(env1.getExecution(), env2.getExecution());{//ExecutionEntry.merge()
									Map<String, String> mergedProperties = new HashMap(execution1.asMap());
									mergedProperties.putAll(execution2.asMap());
								}
								mergedEnv.configuration = ConfigurationEntry.merge(env1.getConfiguration(), env2.getConfiguration());
								
							}
							
							return new ExecutionContext(environment,this.sessionState,this.dependencies,this.configuration);
						}
					}
					flinkEngineConnContext.setExecutionContext(executionContext);
				}
				SparkEngineConnFactory.createEngineConnSession(){}
				
				// MultiExecutorEngineConnFactory[SingleExecutorEngineConnFactory] 包括hive,python,shell,jdbc等引擎
				HiveEngineConnFactory.createEngineConnSession()
				PythonEngineConnFactory.createEngineConnSession()
			}
			engineConn.setEngineConnSession(engineConnSession)
			engineConn
		}
		this.engineConn
	}
	info("Finished to execute all hooks of beforeExecutionExecute.")
	//3. 注册的executions 执行
	executeEngineConn(engineConn);{
		EngineConnExecution.getEngineConnExecutions.foreach{
			case execution: AbstractEngineConnExecution =>{}
			// flink的进入这里: AccessibleEngineConnExecution
			case execution =>{execution.execute(getEngineCreationContext, engineConn)};{//ComputationExecutorManagerEngineConnExecution.
				AccessibleEngineConnExecution.execute(){
					init(engineCreationContext)
					val executor = findReportExecutor(engineCreationContext, engineConn);{= ExecutorManager.getInstance.getReportExecutor(){//ExecutorManager.getReportExecutor()
						LabelExecutorManagerImpl.getReportExecutor(){createExecutor(engineConn.getEngineCreationContext, labels){//LabelExecutorManagerImpl.
							if (null == labels || labels.isEmpty){
								defaultFactory.createExecutor(engineCreationContext, engineConn)
							}else{// flink的LabelExecutor对应 YarnSessionClusterEndpoint 进程? 没有就新建;
								createLabelExecutor(engineCreationContext, labels);{//LabelExecutorManagerImpl.createLabelExecutor
									val labelKey = getLabelKey(labels) // ="sql"
									val executor = tryCreateExecutor(engineCreationContext, labels);{//LabelExecutorManagerImpl.
										// defaultFactory.createExecutor()
										val labelExecutor = if (null == labels || labels.isEmpty){
											defaultFactory.createExecutor(engineCreationContext, engineConn)
										}else{
											factories.find {case labelExecutorFactory: LabelExecutorFactory => labelExecutorFactory.canCreate(labels)}
												.map(case labelExecutorFactory: LabelExecutorFactory =>{labelExecutorFactory.createExecutor(engineCreationContext, engineConn, labels)})
												.getOrElse(defaultFactory.createExecutor(engineCreationContext, engineConn));
										}
										labelExecutor.init();{//不同引擎,这里init不一样;
											// flink-yarn-session 这里是链接yarn-RM,并提交 SubmitApplication
											FlinkSQLComputationExecutor.init(){
												ClusterDescriptorAdapterFactory.create(flinkEngineConnContext.getExecutionContext) match {
													case adapter: YarnSessionClusterDescriptorAdapter => clusterDescriptor = adapter
												}
												clusterDescriptor.deployCluster();{//YarnSessionClusterDescriptorAdapter.
													// 这里调用flink Yarn Api: YarnClusterDescriptor.deploySessionCluster()
													val clusterClientProvider = clusterDescriptor.deploySessionCluster(clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
														return deployInternal();
													}
												}
											}
											
										}
										info(s"Finished to init ${labelExecutor.getClass.getSimpleName}(${labelExecutor.getId}).")
										labelExecutor
									}
								}
							}
						}}
					}}
					beforeReportToLinkisManager(executor, engineCreationContext, engineConn)
					reportUsedResource(executor, engineCreationContext)
					reportLabel(executor)
					executorStatusChecker
					afterReportToLinkisManager(executor, engineCreationContext, engineConn)
				}
			}
		}
	}
	info("Finished to execute executions.")
	
}




submitApplication:238, YarnClientImpl (org.apache.hadoop.yarn.client.api.impl)
startAppMaster:1177, YarnClusterDescriptor (org.apache.flink.yarn)
deployInternal:592, YarnClusterDescriptor (org.apache.flink.yarn)
deploySessionCluster:418, YarnClusterDescriptor (org.apache.flink.yarn)
deployCluster:30, YarnSessionClusterDescriptorAdapter (com.webank.wedatasphere.linkis.engineconnplugin.flink.client.deployment)
init:60, FlinkSQLComputationExecutor (com.webank.wedatasphere.linkis.engineconnplugin.flink.executor)
tryCreateExecutor:100, LabelExecutorManagerImpl (com.webank.wedatasphere.linkis.engineconn.core.executor)
createLabelExecutor:136, LabelExecutorManagerImpl (com.webank.wedatasphere.linkis.engineconn.core.executor)
createExecutor:121, LabelExecutorManagerImpl (com.webank.wedatasphere.linkis.engineconn.core.executor)
getReportExecutor:178, LabelExecutorManagerImpl (com.webank.wedatasphere.linkis.engineconn.core.executor)
findReportExecutor:44, AccessibleEngineConnExecution (com.webank.wedatasphere.linkis.engineconn.acessible.executor.execution)
execute:56, AccessibleEngineConnExecution (com.webank.wedatasphere.linkis.engineconn.acessible.executor.execution)
apply:111, EngineConnServer$$anonfun$com$webank$wedatasphere$linkis$engineconn$launch$EngineConnServer$$executeEngineConn$1 (com.webank.wedatasphere.linkis.engineconn.launch)
apply:104, EngineConnServer$$anonfun$com$webank$wedatasphere$linkis$engineconn$launch$EngineConnServer$$executeEngineConn$1 (com.webank.wedatasphere.linkis.engineconn.launch)
foreach:33, IndexedSeqOptimized$class (scala.collection)
foreach:186, ArrayOps$ofRef (scala.collection.mutable)
com$webank$wedatasphere$linkis$engineconn$launch$EngineConnServer$$executeEngineConn:104, EngineConnServer$ (com.webank.wedatasphere.linkis.engineconn.launch)
apply$mcV$sp:53, EngineConnServer$$anonfun$main$1 (com.webank.wedatasphere.linkis.engineconn.launch)
apply:53, EngineConnServer$$anonfun$main$1 (com.webank.wedatasphere.linkis.engineconn.launch)
apply:53, EngineConnServer$$anonfun$main$1 (com.webank.wedatasphere.linkis.engineconn.launch)
tryCatch:39, Utils$ (com.webank.wedatasphere.linkis.common.utils)
tryThrow:56, Utils$ (com.webank.wedatasphere.linkis.common.utils)
main:53, EngineConnServer$ (com.webank.wedatasphere.linkis.engineconn.launch)
main:-1, EngineConnServer (com.webank.wedatasphere.linkis.engineconn.launch)






// SparkSubmit 进程 
// 执行请求来自 entrace的 
TaskExecutionServiceImpl.execute(requestTask: RequestTask, smc){
	info("Received a new task, task content is " + requestTask)
	if (StringUtils.isBlank(requestTask.getLock)) {
		return ErrorExecuteResponse();
	}
	
	val taskId: Int = taskExecutedNum.incrementAndGet();
	
	val task = new CommonEngineConnTask(String.valueOf(taskId), retryAble);
	val entranceServerInstance = RPCUtils.getServiceInstanceFromSender(smc.getSender);
	val executor = executorManager.getExecutorByLabels(labels);{//LabelExecutorManagerImpl.getExecutorByLabels()
		val labelKey = getLabelKey(labels);
		if (!executors.containsKey(labelKey)){// 如果不存在,则创建并存到map内存中; 
			createExecutor(engineConn.getEngineCreationContext, labels);{
				createLabelExecutor(engineCreationContext, labels)
			}
		}
		executors.get(labelKey);
	}
	executor match {
		case computationExecutor: ComputationExecutor => {
			taskIdCache.put(task.getTaskId, computationExecutor)
			submitTask(task, computationExecutor);{// TaskExecutionServiceImpl.submitTask()
				computationExecutor match {
					case concurrentComputationExecutor: ConcurrentComputationExecutor => submitConcurrentTask(task, concurrentComputationExecutor)
					case _ => submitSyncTask(task, computationExecutor);{
						val runTask = new Runnable {
							override def run(){executeTask(task, computationExecutor){
								val response = executor.execute(task);// 执行spark flink;
								response match {case ErrorExecuteResponse(message, throwable) => sendToEntrance(task, ResponseTaskError(task.getTaskId, message))}
								clearCache(task.getTaskId);
							};}
						}
						lastTaskFuture = Utils.defaultScheduler.submit(runTask);
						SubmitResponse(task.getTaskId);// 回复SubmitResponse消息,并entrance 进程中 CodeLogicalUnitExecTask.execute()中的 ComputationEngineConnExecutor.execute()处理; 
					}
				}
			}
		}
		case o => ErrorExecuteResponse();
	}
	
}



/* SparkSubmit 进程 是spark EngineConnServer 
java \
-cp /tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/conf/:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/lib/*:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/:/opt/spark/conf/:/opt/spark/spark-release/jars/*:/opt/hadoop/etc/hadoop/ \
-Xmx1g -server -XX:+UseG1GC -XX:MaxPermSize=250m -XX:PermSize=128m \
-Xloggc:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps \
-Dwds.linkis.configuration=linkis-engineconn.properties -Dwds.linkis.gateway.url=http://192.168.51.111:9001 -Dlogging.file=log4j2-engineconn.xml -DTICKET_ID=761c1216-cb44-4e04-9b5c-90bef11b09a5 -Djava.io.tmpdir=/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/tmp \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=42955 \
org.apache.spark.deploy.SparkSubmit \
--master yarn --deploy-mode client \
--conf spark.driver.memory=1g --conf spark.driver.extraClassPath=:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/conf:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/lib/*:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5 --conf spark.driver.extraJavaOptions= \
-server -XX:+UseG1GC -XX:MaxPermSize=250m -XX:PermSize=128m -Xloggc:/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/logs/gc.log \
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Dwds.linkis.configuration=linkis-engineconn.properties \
-Dwds.linkis.gateway.url=http://192.168.51.111:9001 -Dlogging.file=log4j2-engineconn.xml  -DTICKET_ID=761c1216-cb44-4e04-9b5c-90bef11b09a5 \
-Djava.io.tmpdir=/tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/tmp -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=42955 \
--class com.webank.wedatasphere.linkis.engineconn.launch.EngineConnServer --name Linkis-EngineConn-Spark_LINKISCLI --driver-cores 1 \
--executor-memory 1g --executor-cores 2 --num-executors 3 \
--queue default /tmp/dsslinkisall/engConn/bigdata/workDir/761c1216-cb44-4e04-9b5c-90bef11b09a5/lib/linkis-engineplugin-spark-1.0.2.jar \
--engineconn-conf wds.linkis.rm.instance=10 --engineconn-conf label.userCreator=bigdata-LINKISCLI --engineconn-conf ticketId=761c1216-cb44-4e04-9b5c-90bef11b09a5 \
--engineconn-conf wds.linkis.rm.yarnqueue.memory.max=300G --engineconn-conf label.engineType=spark-2.4.3 --engineconn-conf wds.linkis.rm.yarnqueue.instance.max=30 \
--engineconn-conf wds.linkis.rm.client.memory.max=20G --engineconn-conf wds.linkis.rm.client.core.max=10 --engineconn-conf wds.linkis.rm.yarnqueue.cores.max=150 \
--engineconn-conf user=bigdata --engineconn-conf wds.linkis.rm.yarnqueue=default --spring-conf eureka.client.serviceUrl.defaultZone=http://192.168.51.111:20303/eureka/ \
--spring-conf logging.config=classpath:log4j2-engineconn.xml --spring-conf spring.profiles.active=engineconn --spring-conf server.port=34072 \
--spring-conf spring.application.name=linkis-cg-engineconn

*/




// linkis.engine flink executor: EngineConnServer

TaskExecutionServiceImpl.executeTask(){
	val response = executor.execute(task);{//ComputationExecutor.execute
		info(s"start to execute task ${engineConnTask.getTaskId}")
		beforeExecute(engineConnTask)
		val response = ensureOp {toExecuteTask(engineConnTask)};{//ComputationExecutor.toExecuteTask
			runningTasks.increase()
			val codes = Utils.tryCatch(getCodeParser.map(_.parse(hookedCode)).getOrElse(Array(hookedCode))) 
			codes.indices.foreach({index =>{
				val code = codes(index)
				if (incomplete.nonEmpty){
					executeCompletely(engineExecutionContext, code, incomplete.toString())
				}else {
					executeLine(engineExecutionContext, code);{// 由子类 FlinkSQLComputationExecutor.executeLine()实现;
						val callOpt = SqlCommandParser.getSqlCommandParser.parse(code.trim, true);
						val operation = OperationFactory.getOperationFactory.createOperation(callSQL, flinkEngineConnContext)
						val resultSet = operation.execute (){//SelectOperation的父类 AbstractJobOperation.execute()
							// 对于 show tables; 其算子是 ShowTablesOperation
							ShowTablesOperation.execute(){
								final TableEnvironment tableEnv = context.getTableEnvironment();{
									if(tableEnv == null) {
										initializeTableEnvironment(sessionState);{//ExecutionContext.initializeTableEnvironment()
											final EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
											createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);{
												if (environment.getExecution().isStreamingPlanner()) {
													streamExecEnv = createStreamExecutionEnvironment();{//ExecutionContext.createStreamExecutionEnvironment()
														// 我朝,这一行报错了; 竟然是因为properties里面没有 parallelism字段; 
														env.setParallelism(environment.getExecution().getParallelism().get());{
															return this.properties.getOptionalInt("parallelism");
														}
													}
												}
											}
										}
									}
								}
								for (String table : context.wrapClassLoader(() -> Arrays.asList(tableEnv.listTables()))) {
									rows.add(Row.of(table));
								}
								return ResultSet.builder().columns(ColumnInfo.create(ConstantNames.SHOW_TABLES_RESULT, new VarCharType(false, maxNameLength))).data(rows).build();
							}
							
							
							jobId = submitJob();{//SelectOperation.submitJob()
								JobID jobId = executeQueryInternal(context.getExecutionContext(), query);{
									// 构建table, 校验catalog等; 
									final Table table = createTable(executionContext, executionContext.getTableEnvironment(), query);{
										return context.wrapClassLoader(() -> tableEnv.sqlQuery(selectQuery));{
											// 进入 Flink Table的Api
											flink.table.api.internal.TableEnvironmentImpl.sqlQuery();// 代码详解下面和flink源码; 
										}
									}
									boolean isChangelogResult = executionContext.getEnvironment().getExecution().inStreamingMode();
									tableResult = executionContext.wrapClassLoader(tableEnv -> {
										tableEnv.registerTableSinkInternal(tableName, result.getTableSink());
										return table.executeInsert(tableName);
									});
									return tableResult.getJobClient().map(jobClient -> {result.startRetrieval(jobClient);});
								}
							}
							return ResultSet.builder()
								.resultKind(ResultKind.SUCCESS_WITH_CONTENT)
								.columns(ColumnInfo.create("jobId", new VarCharType(false, strJobId.length()))).data(Row.of(strJobId))
								.build();
						}
						resultSet.getResultKind match {case ResultKind.SUCCESS => new SuccessExecuteResponse;}
					}
				}
			}});
		}
		afterExecute(engineConnTask, response);
	}
	response match {case ErrorExecuteResponse(message, throwable) => sendToEntrance(task, ResponseTaskError(task.getTaskId, message))}
	clearCache(task.getTaskId);
}




// linkis.engine Flink 环境创建

ExecutionContext.getTableEnvironment(){//ExecutionContext.
	if(tableEnv == null) {
		initializeTableEnvironment(sessionState);{//ExecutionContext.initializeTableEnvironment()
			final EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
			
			createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);{
				if (environment.getExecution().isStreamingPlanner()) {
					streamExecEnv = createStreamExecutionEnvironment();{//ExecutionContext.createStreamExecutionEnvironment()
						// 我朝,这一行报错了; 竟然是因为properties里面没有 parallelism字段; 
						env.setParallelism(environment.getExecution().getParallelism().get());{
							return this.properties.getOptionalInt("parallelism");
						}
					}
				}
			}
			
			initializeCatalogs();{// ExecutionContext.
				environment.getCatalogs().forEach((name, entry) -> {
					Catalog catalog = createCatalog(name, entry.asMap(), classLoader);{// ExecutionContext.createCatalog
						CatalogFactory factory =TableFactoryService.find(CatalogFactory.class, catalogProperties, classLoader);
						return factory.createCatalog(name, catalogProperties);{
							// hive factory的实现
							HiveCatalogFactory.createCatalog(String name, Map<String, String> properties){
								DescriptorProperties descriptorProperties = getValidatedProperties(properties);
								Optional<String> hiveConfDir = descriptorProperties.getOptionalString(CATALOG_HIVE_CONF_DIR);
								final Optional<String> hadoopConfDir =descriptorProperties.getOptionalString(CATALOG_HADOOP_CONF_DIR);
								final String version = descriptorProperties.getOptionalString(CATALOG_HIVE_VERSION).orElse(HiveShimLoader.getHiveVersion());{//flink.table.catalog.hive.client.HiveShimLoader.getHiveVersion()
									//确认hive-common-xx.jar包是否存在;
									return hive.common.util.HiveVersionInfo.getVersion();
								}
								return new HiveCatalog(version);
							}
							HiveShimLoader
						}
					}
					tableEnv.registerCatalog(name, catalog);
				});
				
				for(Entry<String, TableEntry> keyValue: environment.getTables().entrySet()) {
					if (entry instanceof SourceTableEntry || entry instanceof SourceSinkTableEntry) {
						tableSources.put(name, createTableSource(environment.getExecution(), entry.asMap(), classLoader));
					}
					if (entry instanceof SinkTableEntry || entry instanceof SourceSinkTableEntry) {
						tableSinks.put(name, createTableSink(environment.getExecution(), entry.asMap(), classLoader));
					}
				}
				
				// Switch to the current database. 把加载到的 current catalog, database 设置成 curDB
				Optional<String> catalog = environment.getExecution().getCurrentCatalog();
				catalog.ifPresent(tableEnv::useCatalog);
				Optional<String> database = environment.getExecution().getCurrentDatabase();
				database.ifPresent(tableEnv::useDatabase);
		
			}
			
			
		}
	}
}


ERROR [Linkis-Default-Scheduler-Thread-3] com.webank.wedatasphere.linkis.common.utils.Utils$ 57 error - Throw error java.lang.NoClassDefFoundError: org/apache/hive/common/util/HiveVersionInfo
	at org.apache.flink.table.catalog.hive.client.HiveShimLoader.getHiveVersion(HiveShimLoader.java:137) ~[flink-connector-hive_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory.createCatalog(HiveCatalogFactory.java:91) ~[flink-connector-hive_2.11-1.12.2.jar:1.12.2]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.createCatalog(ExecutionContext.java:249) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.lambda$null$4(ExecutionContext.java:431) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at java.util.HashMap.forEach(HashMap.java:1289) ~[?:1.8.0_261]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.lambda$initializeCatalogs$5(ExecutionContext.java:430) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.wrapClassLoader(ExecutionContext.java:190) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.initializeCatalogs(ExecutionContext.java:430) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.initializeTableEnvironment(ExecutionContext.java:378) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.context.ExecutionContext.getTableEnvironment(ExecutionContext.java:199) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.client.sql.operation.impl.ShowTablesOperation.execute(ShowTablesOperation.java:51) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconnplugin.flink.executor.FlinkSQLComputationExecutor.executeLine(FlinkSQLComputationExecutor.scala:91) ~[linkis-engineconn-plugin-flink-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$toExecuteTask$2$$anonfun$apply$10$$anonfun$apply$11.apply(ComputationExecutor.scala:179) ~[linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$toExecuteTask$2$$anonfun$apply$10$$anonfun$apply$11.apply(ComputationExecutor.scala:178) ~[linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.common.utils.Utils$.tryCatch(Utils.scala:39) [linkis-common-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$toExecuteTask$2$$anonfun$apply$10.apply(ComputationExecutor.scala:180) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$toExecuteTask$2$$anonfun$apply$10.apply(ComputationExecutor.scala:174) [linkis-computation-engineconn-1.0.2.jar:?]
	at scala.collection.immutable.Range.foreach(Range.scala:160) [scala-library-2.11.12.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$toExecuteTask$2.apply(ComputationExecutor.scala:173) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$toExecuteTask$2.apply(ComputationExecutor.scala:149) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.common.utils.Utils$.tryFinally(Utils.scala:60) [linkis-common-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor.toExecuteTask(ComputationExecutor.scala:222) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$3.apply(ComputationExecutor.scala:237) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor$$anonfun$3.apply(ComputationExecutor.scala:237) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.common.utils.Utils$.tryFinally(Utils.scala:60) [linkis-common-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.acessible.executor.entity.AccessibleExecutor.ensureIdle(AccessibleExecutor.scala:54) [linkis-accessible-executor-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.acessible.executor.entity.AccessibleExecutor.ensureIdle(AccessibleExecutor.scala:48) [linkis-accessible-executor-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor.ensureOp(ComputationExecutor.scala:133) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.execute.ComputationExecutor.execute(ComputationExecutor.scala:236) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.service.TaskExecutionServiceImpl.com$webank$wedatasphere$linkis$engineconn$computation$executor$service$TaskExecutionServiceImpl$$executeTask(TaskExecutionServiceImpl.scala:239) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.service.TaskExecutionServiceImpl$$anon$1$$anonfun$run$1.apply$mcV$sp(TaskExecutionServiceImpl.scala:172) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.service.TaskExecutionServiceImpl$$anon$1$$anonfun$run$1.apply(TaskExecutionServiceImpl.scala:170) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.service.TaskExecutionServiceImpl$$anon$1$$anonfun$run$1.apply(TaskExecutionServiceImpl.scala:170) [linkis-computation-engineconn-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.common.utils.Utils$.tryCatch(Utils.scala:39) [linkis-common-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.common.utils.Utils$.tryAndWarn(Utils.scala:68) [linkis-common-1.0.2.jar:?]
	at com.webank.wedatasphere.linkis.engineconn.computation.executor.service.TaskExecutionServiceImpl$$anon$1.run(TaskExecutionServiceImpl.scala:170) [linkis-computation-engineconn-1.0.2.jar:?]
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [?:1.8.0_261]
	at java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_261]
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180) [?:1.8.0_261]
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
Caused by: java.lang.ClassNotFoundException: org.apache.hive.common.util.HiveVersionInfo
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418) ~[?:1.8.0_261]
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:355) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351) ~[?:1.8.0_261]
	... 43 more


