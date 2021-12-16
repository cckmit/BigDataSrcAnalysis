// cg-xx模块: xx 进程:  
// fromTh:  
// nextTh: 
// 处理逻辑: 


// cg-entrance: 处理个中请求的线程:  其发送了 EngineAskRequest 请求; 
// 	fromTh: cg-entrance 交互服务
// 	nextTh: cg-engineplugin(先? cg-engineconnmanager?); 


"_sender_" -> "RPCSender(linkis-cg-entrance, bdnode111.hjq.com:9104)"
	AsyncExecTaskRunnerImpl
	AsyncTaskManager
	ComputationEngineConnManager


// cg-linkismanager: "ForkJoinPool-1-worker-7"线程: AskEngine申请执行flink执行引擎: 校验label, 
// fromTh: cg-entrance 交互服务
// nextTh: cg-engineplugin(先? cg-engineconnmanager?); 主要靠FlinkEngineConnResourceFactory.getRequestResource() 返回 UserNodeResource资源;

DefaultEngineAskEngineService.askEngine(){ //com.webank.wedatasphere.linkis.manager.am.service.engine
	if(! engineAskRequest.getLabels.containsKey(LabelKeyConstant.EXECUTE_ONCE_KEY)){ //一般会进入这里; 
		val engineReuseRequest = new EngineReuseRequest()
		val reuseNode = engineReuseService.reuseEngine(engineReuseRequest)
	}
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
				//读取管理台的的配置
				val configProp = engineConnConfigurationService.getConsoleConfiguration(labelList)
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
									- DefaultEngineConnResourceFactoryService.getResourceFactoryBy(engineTypeLabel)
									- DefaultEngineConnResourceFactoryService.createEngineResource(engineResourceRequest)
									- FlinkEngineConnResourceFactory.getRequestResource(){
											val containers = 2 = wds.linkis.engineconn.flink.app.parallelism(4) / flink.taskmanager.numberOfTaskSlots(2);
											val yarnMemory = 10G = flink.taskmanager.memory(4) * containers(2) + flink.jobmanager.memory(2) +"G"
											val yarnCores = 5 = flink.taskmanager.cpu.cores(2) * containers(2) +1
											new DriverAndYarnResource(loadResource, new YarnResource(yarnMemory, yarnCores, 0, LINKIS_QUEUE_NAME.getValue(properties));
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
						var canRequest:Boolean = requestResourceService.canRequest(labelContainer, resource);{
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
			val engineBuildRequest = EngineConnBuildRequestImpl(resourceTicketId,resource,EngineConnCreationDescImpl())
			
		
		}
		val createEngineNode = getEngineNodeManager.useEngine(createNode, timeout)
	}
	
	createNodeThread.onComplete {
		case Success(engineNode) =>
			smc.getSender.send(EngineCreateSuccess(engineAskAsyncId, engineNode))
		case Failure(exception) => 	
	}
	EngineAskAsyncResponse(engineAskAsyncId, Sender.getThisServiceInstance)
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


// cg-engineconnmanager ?



2021-12-15 00:42:08.737 [INFO ] [message-executor_263                    ] c.w.w.l.e.s.s.i.DefaultEngineConnListService (40) [info] - 
	Deal event EngineConnAddEvent(DefaultEngineConn(Starting, 350dcb50-521e-41bf-afb2-bfb99aa74156, com.webank.wedatasphere.linkis.manager.engineplugin.common.resource.
		UserNodeResource@136ae8ea, [[key: userCreator, value: {"creator":"LINKISCLI","user":"bigdata"}, str: bigdata-LINKISCLI], [key: engineType, value: {"engineType":"flink","version":"1.12.2"}, str: flink-1.12.2]], 
		EngineConnCreationDescImpl(null,null,{wds.linkis.rm.yarnqueue.memory.max=300G, wds.linkis.rm.client.core.max=10, wds.linkis.engineConn.memory=4G, wds.linkis.rm.instance=1, wds.linkis.rm.yarnqueue.instance.max=30, wds.linkis.rm.yarnqueue.cores.max=150, flink.container.num=1, wds.linkis.rm.client.memory.max=20G, flink.taskmanager.memory=1, wds.linkis.rm.yarnqueue=default}), com.webank.wedatasphere.linkis.ecm.core.engineconn.EngineConnInfo@566864e3, 
		null, com.webank.wedatasphere.linkis.ecm.core.launch.EngineConnLaunchRunnerImpl@5bebb3e7, null, null))




// cg-engineconn 模块: launch.EngineConnServer 进程: "message-executor" 线程 LinuxProcess脚本引擎执行 sudo su - bigdata -c sh engineConnExec.sh脚本
// fromTh:  
// 处理逻辑: LinuxProcessEngineConnLaunchService.launchEngineConn():runner.run().launch().exec.execute(): ProcessBuilder.start()
// logDir: $LINKIS_HOME/logs/linkis-cg-engineconnmanager.log
// 关键变量: export CLASSPATH="${HADOOP_CONF_DIR}:${HIVE_CONF_DIR}:${PWD}/conf:${PWD}/lib/*:/opt/dsslinkisall/dsslinkisall-release/linkis/lib/linkis-commons/public-module/*:${PWD}"


LinuxProcessEngineConnLaunchService.launchEngineConn(){
	Sender.getSender(ENGINECONN_PLUGIN_SPRING_NAME).ask(engineConnBuildRequest) match {
		case request: EngineConnLaunchRequest if ENGINECONN_CREATE_DURATION._1 != 0L =>
			launchEngineConn(request, ENGINECONN_CREATE_DURATION._1);{//AbstractEngineConnLaunchService.
				val conn = createEngineConn
				val runner = createEngineConnLaunchRunner
				val launch = createEngineConnLaunch
				//2.资源本地化，并且设置ecm的env环境信息
				getResourceLocalizationServie.handleInitEngineConnResources(request, conn)
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
							processBuilder = newProcessEngineConnCommandBuilder();// 构建 /bin/bash
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






// cg-engineconn 模块: launch.EngineConnServer 进程: 构建Flink执行命令 ?
// fromTh:  
// nextTh: 
// 处理逻辑: 
// logDir: $ENGINECONN_ROOT_PATH(wds.linkis.engineconn.root.dir)/bigdata/workDir/a689125c-dc7e-4f0f-8343-6fa6e93c4f48/logs

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

