问题: 
1. 在KafakaSource.init()方法中启动20ms定时线程执行Consumer.poll()拉去数据; 如果拉去不到数据, 就一直阻塞

2. KafkaConsumer客户端进程包括哪些线程?
	- main:
	- kafka-coordinator-heartbeat-thread
	- Attach Listener ?

相关注意实现:
* 每个FetchRequest获取获取返回的FetchResponse.PartitionData中封装的是Kafka分区某一整段数据,并不一定等于maxPollNum,比如4031个; 那是多少有谁控制的?
	

/** "main"线程:
*
*/

// KafkaConsumer client端的重要类和结构;

// KafakConsumer初始化的重要代码?

new KafkaConsumer(){
	String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
	if (clientId.length() <= 0)
		// 该计数器CONSUMER_CLIENT_ID_SEQUENCE为static修饰, 若在同一进程中的话是能被不同KafakConsumer实例累计的;
		clientId = "consumer-" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement(); //static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
	this.clientId = clientId;
	reporters.add(new JmxReporter(JMX_PREFIX));
	AppInfoParser.registerAppInfo(JMX_PREFIX, clientId);{//这里将该clientId注册到
        try {
			// 解析平均后,里面name== kafka.consumer:id=clientId,type=app-info
            ObjectName name = new ObjectName(prefix + ":type=app-info,id=" + id);
            AppInfo mBean = new AppInfo();
            ManagementFactory.getPlatformMBeanServer().registerMBean(mBean, name);{//JmxMBeanServer.registerMBean()
				return mbsInterceptor.registerMBean(object, cloneObjectName(name));{//DefaultMBeanServerInterceptor.registerMBean
					return registerObject(infoClassName, object, name);{
						Repository.addMBean(){
							String cstr = name.getCanonicalKeyPropertyListString();
								NamedObject elmt= moiTb.get(cstr);//cstr== "id=consumer-1,type=app-info"
								if (elmt != null) {
									throw new InstanceAlreadyExistsException(name.toString());
								} else {
									nbElements++;
									addMoiToTb(object,name,cstr,moiTb,context);
								}
						}
					}
				}
			}//以该name(kafka.consumer:id=clientId)注册一个mBean;
        } catch (JMException e) {//注意, 这个InstanceAlreadyExistsException是JMException子类,会被catch而没有向上抛; 后面的Consumer初始化会正常运行;
            log.warn("Error registering AppInfo mbean", e);
        }
	} 
	
}



// KafkaConsumer.poll(timeout)的完整代码;

KafkaConsumer.poll(timeout);{
	acquire();
	try {
		if (timeout < 0) throw new IllegalArgumentException("Timeout must not be negative");
		if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
		// poll for new data until the timeout expires
		long start = time.milliseconds();
		long remaining = timeout;
		do {
			// 核心: 执行一次poll操作: 
			Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollOnce(remaining);{ //KafkaConsumer.pollOnce(long timeout)
				client.maybeTriggerWakeup();{//ConsumerNetworkClient.maybeTriggerWakeup() :先看看NetworkClient是不是要 先唤醒;
					if (!wakeupDisabled.get() && wakeup.get()) {
						log.trace("Raising wakeup exception in response to user wakeup");
						wakeup.set(false);
						throw new WakeupException();
					}
				}
				
				// 判断和确保coordinator:Node不为空; 保证成功JoinGroup; 若为空先FindGroup所在节点,再JoinGroup,并SyncGroup获取元数据和绑定的分区数据Assignment;
				coordinator.poll(time.milliseconds(), timeout){//ConsumerCoordinator.poll(long now, long remainingMs)
					invokeCompletedOffsetCommitCallbacks(){//ConsumerCoordinator.invokeCompletedOffsetCommitCallbacks()
						while (true) {
							OffsetCommitCompletion completion = completedOffsetCommits.poll();
							if (completion == null){break;}
							completion.invoke();
						}
					}
					
					boolean isAutoAssigned = subscriptions.partitionsAutoAssigned(){
						// 有4种订阅模式: None, Auto_Topic,Auto_Pattern, User_Assigned; 
						return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
					}
					if (isAutoAssigned){//当自动订阅Topic(AutoTopics)和 基于Topics过滤的自动识别时: 一般(AutoTopics)进入这里;
						// 1. 第一步:查找并确保映射GroupCoordinator的AbstractCoordinator.coordinator不为空; 若为空, 会在while()循环执行 lookupCoordinator()直到coordinator不为空;
						if (coordinatorUnknown(){//AbstractCoordinator.coordinatorUnknown(): 查看是否确定G
							return coordinator() == null;{//AbstractCoordinator.coordinator()
								// coordinator:Node == Node(host,port,id): 指向该TopicPartiton所在的节点?
								if (coordinator != null && client.connectionFailed(coordinator)) {
									coordinatorDead();
									return null;
								}
								return this.coordinator;
							}
						}) {//进入这里,代表是初次连接Server或上次会话已经结束了; 需要重新加入 GroupCoordinator;
							// Block until the coordinator for this group is known and is ready to receive requests.
							ensureCoordinatorReady(){//AbstractCoordinator.ensureCoordinatorReady(): 阻塞在此,直到成功加入其ConsumerGroup;
								ensureCoordinatorReady(0, Long.MAX_VALUE);{//AbstractCoordinator.ensureCoordinatorReady(long startTimeMs, long timeoutMs)
									long remainingMs = timeoutMs;
									while (coordinatorUnknown()) {//只要 AbstractCoordinator.coordinator==null,即还没加入ConsumerGroup,会循环阻塞在此;
										//异步发送 GroupCoordinator 消息; 寻找该consumerGroup所对应GroupCoordinator所在节点;
										RequestFuture<Void> future = lookupCoordinator();{//AbstractCoordinator.lookupCoordinator()
											if (findCoordinatorFuture == null) {
												// find a node to ask about the coordinator
												Node node = this.client.leastLoadedNode();
												if (node == null) {
													// TODO: If there are no brokers left, perhaps we should use the bootstrap set
													// from configuration?
													log.debug("No broker available to send GroupCoordinator request for group {}", groupId);
													return RequestFuture.noBrokersAvailable();
												} else{// 进入这里; 因为正常至少会有1个Node以上; 
													findCoordinatorFuture = sendGroupCoordinatorRequest(node);{//AbstractCoordinator.sendGroupCoordinatorRequest(Node node)
														// initiate the group metadata request ; 发GroupCoordinator请求,寻找该TP的Group,并加入;
														log.debug("Sending GroupCoordinator request for group {} to broker {}", groupId, node);
														FindCoordinatorRequest.Builder requestBuilder = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, this.groupId);
														return client.send(node, requestBuilder).compose(new GroupCoordinatorResponseHandler());
														
														{
															// 1. Server端接到FindCoordinatorRequest后的主要处理逻辑:
															KafkaApis.handleFindCoordinatorRequest(){
																val (partition, topicMetadata) = findCoordinatorRequest.coordinatorType match {
																	case FindCoordinatorRequest.CoordinatorType.GROUP => {
																		val partition = groupCoordinator.partitionFor(findCoordinatorRequest.coordinatorKey) //根据groupId的哈希值,算出其在_cosumer_offsets属于哪个分区;
																		val metadata = getOrCreateInternalTopic(GROUP_METADATA_TOPIC_NAME, request.listenerName) //_cosumer_offsets的元数据,里面主要是存了 分区位置的哈希表: 分区TP-> 所在节点Node
																		(partition, metadata)
																	}
																	
																	val coordinatorEndpoint = topicMetadata.partitionMetadata.asScala
																		.find(_.partition == partition) // 过滤该_consumer_offsets分区所在位置;
																		.map(_.leader) 
																	// 将coordinatorEndpoint其实是一个Node对象,包含了其分区所在的节点位置;
																	coordinatorEndpoint match {case Some(endpoint) if !endpoint.isEmpty => new FindCoordinatorResponse(requestThrottleMs, Errors.NONE, endpoint)}
																}
															}
															
															// 2. 成功完成Server的请求后, client端的处理逻辑;
															GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){
																log.debug("Received GroupCoordinator response {} for group {}", resp, groupId);
																FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
																Errors error = findCoordinatorResponse.error();
																clearFindCoordinatorFuture();
																if (error == Errors.NONE) {//正常返回时Errors.None; 
																	synchronized (AbstractCoordinator.this) {
																		// 将该consumerGroup在Server端 _consumer_offsets的分区所在Broker节点信息,封装进Node对象作为coordinator;
																		AbstractCoordinator.this.coordinator = new Node(
																				Integer.MAX_VALUE - findCoordinatorResponse.node().id(),
																				findCoordinatorResponse.node().host(),
																				findCoordinatorResponse.node().port());
																		log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
																		
																		// 尝试ConsumerNetworkClient连接到 GroupCoordinator所在节点上;
																		client.tryConnect(coordinator);
																		
																		heartbeat.resetTimeouts(time.milliseconds());// 将Heartbeat的resetTime 更新下;
																	}
																	future.complete(null);
																} else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
																	future.raise(new GroupAuthorizationException(groupId));
																} else {
																	log.debug("Group coordinator lookup for group {} failed: {}", groupId, error.message());
																	future.raise(error);
																}
															}	
														}
														
													}
												}
											}
											return findCoordinatorFuture;
										}
										
										// 阻塞在此循环处理pendingCompletion:Queue队列中所有已返回请求; 直到当前futrue.isDone或者超timeout时间了;
										client.poll(future, remainingMs);//代码详解后面;
										
										if (future.failed()) {// 当上面的查找GroupCoordinator失败时: 
											if (future.isRetriable()) {
												remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
												if (remainingMs <= 0) break;
												log.debug("Coordinator discovery failed for group {}, refreshing metadata", groupId);
												client.awaitMetadataUpdate(remainingMs);
											} else
												throw future.exception();
										} else if (coordinator != null && client.connectionFailed(coordinator)) {
											// we found the coordinator, but the connection has failed, so mark
											// it dead and backoff before retrying discovery
											coordinatorDead();
											time.sleep(retryBackoffMs);
										}
										remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
										if (remainingMs <= 0)
											break;
									}
									return !coordinatorUnknown();
								}
								lookupCoordinator(){
									sendGroupCoordinatorRequest();// 发送 GroupCoordinatorRequest 给Server
								}
							}
							now = time.milliseconds();
						}
						
						// 2. 第二步: 当订阅有变动或初始化rejoinNeeded==true时,需要重新发JoinGroupRequest已加入Group并Rebalance;
						boolean needRejoin = needRejoin();{//ConsumerCoordinator.needRejoin()
							if (!subscriptions.partitionsAutoAssigned())//当None or USER_ASSIGNED类型订阅时,不需要再JoinGroup
								return false;
							// we need to rejoin if we performed the assignment and metadata has changed
							if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot)) //当关联的节点(assignment?) 或元数据被(主动)改变了后;当然需要 Re JoinGroup
								return true;
							// we need to join if our subscription has changed since the last join
							if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription())) //当订阅的主题(topics)和SubscriptionState.subscription数据不一样时,也需要重新加入以Rebalace;
								return true;
							return super.needRejoin();{//AbstractCoordinator.needRejoin()
								return rejoinNeeded;//默认rejoinNeeded=true,只有当 JoinGroupRequest.onSuccess()时才会置为false;
							}
						}
						if (needRejoin){//  
							if (subscriptions.hasPatternSubscription()){//return thisType == AUTO_PATTERN; 当基于主题过滤(自动识别)的订阅方式时,需要等待元数据的更新;
								client.ensureFreshMetadata();
							}
							
							// 
							ensureActiveGroup(){// AbstractCoordinator.ensureActiveGroup()
								ensureCoordinatorReady();//确保AbstractCoordinator.coordinator不为空; 代码详解上面 poll().if(coordinatorUnknown(){}中;
								startHeartbeatThreadIfNeeded(){//初次启动HeartbeatThread==null时, new HeartbeatThread()并start()启动"kafka-coordinator-heartbeat-thread"线程定时检查心跳;
									if (heartbeatThread == null) {
										heartbeatThread = new HeartbeatThread[extends KafkaThread[extends Thread]](){
											super("kafka-coordinator-heartbeat-thread" + (groupId.isEmpty() ? "" : " | " + groupId), true);
										}
										heartbeatThread.start(){
											HeartbeatThread.run(){
												while (true) {
													synchronized (AbstractCoordinator.this) {
														if (closed)return;

														if (!enabled) {
															AbstractCoordinator.this.wait();
															continue;
														}


														client.pollNoWakeup();
														long now = time.milliseconds();

														if (coordinatorUnknown()) {
															if (findCoordinatorFuture != null || lookupCoordinator().failed())
																AbstractCoordinator.this.wait(retryBackoffMs);
														} else if (heartbeat.sessionTimeoutExpired(now)) {
															coordinatorDead();
														} else if (heartbeat.pollTimeoutExpired(now)) {
															maybeLeaveGroup();
														} else if (!heartbeat.shouldHeartbeat(now)) {
															AbstractCoordinator.this.wait(retryBackoffMs);
														} else {
															heartbeat.sentHeartbeat(now);
															sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
																@Override
																public void onSuccess(Void value) {
																	synchronized (AbstractCoordinator.this) {
																		heartbeat.receiveHeartbeat(time.milliseconds());
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
								
								joinGroupIfNeeded(){
									// 当
									while (needRejoin() || rejoinIncomplete(){return joinFuture != null;}) { //当订阅有变动 |初始化时rejoinNeeded==true | 有未完成JoinFuture 时,会循环调initiateJoinGroup()不断JoinGroup;
										ensureCoordinatorReady();
										
										if (needsJoinPrepare) {
											onJoinPrepare(generation.generationId, generation.memberId);{
												maybeAutoCommitOffsetsSync(rebalanceTimeoutMs);
												ConsumerRebalanceListener listener = subscriptions.listener();
												try {
													Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
													
													//这里执行用户定义的ConsumerRebalanceListener接口的onPartitionsRevoked()
													listener.onPartitionsRevoked(revoked);
												} catch (WakeupException | InterruptException e) {
													throw e;
												} catch (Exception e) {
													log.error("User provided listener {} for group {} failed on partition revocation",, groupId, e);
												}
												isLeader = false;
												subscriptions.resetGroupSubscription();{
													this.groupSubscription.retainAll(subscription);
												}
											}
											needsJoinPrepare = false;
										}
										
										RequestFuture<ByteBuffer> future = initiateJoinGroup(){//向Server发 一个JoinGroupRequest请求;
											if (joinFuture == null) {//判断joinFutrue状态,避免重复发请求;
												disableHeartbeatThread();
												joinFuture = sendJoinGroupRequest();// 发送 JoinGroupRequest加入该C
												joinFuture.addListener(new RequestFutureListener<ByteBuffer>());
											}
										}
										
										// 阻塞在此,依次处理pendingCompletion中任务, 直到当前(JoinGroupRequest)futrue.isDone(); 不设置超时;
										client.poll(future);// 实现代码参见后面;
										
										resetJoinGroupFuture();{//将joinFuture置为空,以此作为 JoinGroupRequest请求是否完成的状态;
											this.joinFuture = null;
										}
										
										if (future.succeeded(){return isDone() && !failed();}) {//进入这里,应该就代表
											needsJoinPrepare = true;
											
											onJoinComplete(generation.generationId, generation.memberId, generation.protocol, future.value());{//ConsumerCoordinator.onJoinComplete()
												// only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
												if (!isLeader) assignmentSnapshot = null;//不是Leader,不需要监控metadata,将assignmentSnapshot置为null;
												
												// 指定Rebalacing时的分区分配器: 默认RangeAssignor,另有RoundRobinAssignor和StickyAssignor两种; 可由partition.assignment.strategy参数设置;
												PartitionAssignor assignor = lookupAssignor(assignmentStrategy);{//根据分配策略获取 分配器Assignor; 分配器用于Leader-ConsumerCoordinator在Rebalacne时候的分区策略;
													for (PartitionAssignor assignor : this.assignors) {
														if (assignor.name().equals(name))
															return assignor;
													}
													return null;//如果构造出的assignors中没有该 assignmentStrategy指定的分配器,后面会抛异常;
												}
												if (assignor == null) throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
												
												Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);//将从Server中获取的可用分区(partitions:List<TopicPartition>) 字节数据,反序列化并封装进Assignment中;
												// set the flag to refresh last committed offsets
												subscriptions.needRefreshCommits();{//标记 要重新提交;
													this.needsFetchCommittedOffsets = true;
												}
												// update partition assignment; 将从Server新获取的绑定分区信息(Assignment)更新到SubscriptionState.assignment中,并清空里面原来的;
												subscriptions.assignFromSubscribed(assignment.partitions());{//SubscriptionState.assignFromSubscribed(Collection<TopicPartition> assignments)
													if (!this.partitionsAutoAssigned()) throw new IllegalArgumentException("Attempt to dynamically assign partitions while");//当None,UserAssign类型订阅时, 就不能更改 绑定的分区了;
													
													Map<TopicPartition, TopicPartitionState> assignedPartitionStates = partitionToStateMap(assignments);//为每一个TP新建一个状态管理对象:TopicPartitionState,并装进Map中;
													fireOnAssignment(assignedPartitionStates.keySet());{//添加到Listener实现类Fetcher.sensors.assignedPartitions中;
														for (Listener listener : listeners){// List<Listener> listeners;接口Listener只有1个实现类:Fetcher,
															listener.onAssignment(assignment);{//Fetcher.onAssignment(Set<TopicPartition> assignment);
																sensors.updatePartitionLagSensors(assignment);{//Fetcher.FetchManagerMetrics.updatePartitionLagSensors(Set<TopicPartition> assignedPartitions)
																	if (this.assignedPartitions != null) {
																		for (TopicPartition tp : this.assignedPartitions) {
																			if (!assignedPartitions.contains(tp)) //原Metrics中缓存的TPs,若不新传进来的里面, 就从metrics中移除;
																				metrics.removeSensor(partitionLagMetricName(tp));
																		}
																	}
																	this.assignedPartitions = assignedPartitions;//初始化时,由null变为assignedPartitions;
																}
															}
														}
													}
													
													if (this.subscribedPattern != null) {//AutoPattern过滤Topic的订阅模式;
														for (TopicPartition tp : assignments) {
															if (!this.subscribedPattern.matcher(tp.topic()).matches())
																throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic regex pattern; subscription pattern is " + this.subscribedPattern);
														}
													} else {//AutoTopic订阅模式,进入这里; 判断若Server获取的TPs中包含非订阅的Topic(那就是Server分配错误了) 就抛异常;
														for (TopicPartition tp : assignments)
															if (!this.subscription.contains(tp.topic()))
																throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic; subscription is " + this.subscription);
													}
													// 每次Rebalance,这里都要更新; after rebalancing, we always reinitialize the assignment value
													this.assignment.set(assignedPartitionStates);{//PartitionStates.set() 将最新的分区即State信息更新到SubsccriptionState.assignment[PartitionStates]中; 只是HashMap->LinkedHashMap而已;
														map.clear();//先清空;
														update(partitionToState);{//
															// 向将相同Topic的分区都聚合在同一Topic(key)下面;
															LinkedHashMap<String, List<TopicPartition>> topicToPartitions = new LinkedHashMap<>();
															for (TopicPartition tp : partitionToState.keySet()) {
																List<TopicPartition> partitions = topicToPartitions.get(tp.topic());
																if (partitions == null) {
																	partitions = new ArrayList<>();
																	topicToPartitions.put(tp.topic(), partitions);
																}
																partitions.add(tp);
															}
															
															for (Map.Entry<String, List<TopicPartition>> entry : topicToPartitions.entrySet()) {
																for (TopicPartition tp : entry.getValue()) {
																	S state = partitionToState.get(tp);
																	map.put(tp, state);
																}
															}
														}
													}
													this.needsFetchCommittedOffsets = true;//要重新更新commit了;
												}
												
												// 当有新增Topic时,[ 只有AutoPattern订阅模式才会新增);将新增的Topic信息添加到subscriptions中;
												Set<String> addedTopics = new HashSet<>();
												for (TopicPartition tp : subscriptions.assignedPartitions()) {
													if (!joinedSubscription.contains(tp.topic())) addedTopics.add(tp.topic());
												}
												if (!addedTopics.isEmpty()) {
													Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
													Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
													newSubscription.addAll(addedTopics);
													newJoinedSubscription.addAll(addedTopics);
													this.subscriptions.subscribeFromPattern(newSubscription);
													this.joinedSubscription = newJoinedSubscription;
												}
												
												// update the metadata and enforce a refresh to make sure the fetcher can start
												// fetching data in the next iteration
												this.metadata.setTopics(subscriptions.groupSubscription());{//将SubscriptionState.groupSubscription[Set<String>]所有订阅Topics 更新到元数据Metadata.topics[Map<String,Long>]中,并设过期都为-1;
													if (!this.topics.keySet().containsAll(topics)) {
														requestUpdateForNewTopics();
													}
													this.topics.clear();//清空元数据总原来的Topic信息;
													for (String topic : topics)
														this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);// 给所有新增/更新的Topic都设置过期-1,不过期;
												}
												client.ensureFreshMetadata();{
													if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(time.milliseconds()) == 0){
														awaitMetadataUpdate();
													}
												}
												
												
												// give the assignor a chance to update internal state based on the received assignment
												assignor.onAssignment(assignment);//向分配器中 更新最新绑定的分区数据;
												// reschedule the auto commit starting from now
												this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs; //从当前开始安排 auto.commit的时间;
												// execute the user's callback after rebalance
												ConsumerRebalanceListener listener = subscriptions.listener();
												log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
												try {
													Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
													
													/* 这里执行用户定义的ConsumerRebalanceListener接口 在rebalance前执行方法: onPartitionsAssigned()
													*	listener位于内存 KafkaConsumer.SubscriptionState.listener[ConsumerRebalanceListener]中;
													*/
													listener.onPartitionsAssigned(assigned);{//用户定义的onPartitionsAssigned()实现; 默认实现没代码;
														
													}
												} catch (WakeupException | InterruptException e) {
													throw e;
												} catch (Exception e) {
													log.error("User provided listener {} for group {} failed on partition assignment",listener.getClass().getName(), groupId, e);
												}
												
											}
											
										} else {
											RuntimeException exception = future.exception();
											if (exception instanceof UnknownMemberIdException ||exception instanceof RebalanceInProgressException ||exception instanceof IllegalGenerationException)
												continue;
											else if (!future.isRetriable()) throw exception;
											time.sleep(retryBackoffMs);
										}
									}
								}
							}
							now = time.milliseconds();
						}
					}else{
						if (metadata.updateRequested() && !client.hasReadyNodes()) {
							boolean metadataUpdated = client.awaitMetadataUpdate(remainingMs);
							if (!metadataUpdated && !client.hasReadyNodes()) return;
							now = time.milliseconds();
						}
					}
					
					// 先触发一次心跳检测?
					pollHeartbeat(now){//AbstractCoordinator.pollHeartbeat(long now)
						if (heartbeatThread != null){
							if (heartbeatThread.hasFailed()){
								RuntimeException cause = heartbeatThread.failureCause();
								heartbeatThread = null;
								throw cause;
							}
							
							heartbeat.poll(now){//Heartbeat.poll(long now)
								this.lastPoll = now;
							}
						}
					}
					
					maybeAutoCommitOffsetsAsync(now){// ConsumerCoordinator.maybeAutoCommitOffsetsAsync()
						if (autoCommitEnabled) { // auto.commit.enable =true时;
							
						}
					}
				}
				
				if (!subscriptions.hasAllFetchPositions(){//SubscriptionState.hasAllFetchPositions()
					return hasAllFetchPositions(this.assignedPartitions());{// hasAllFetchPositions(Collection<TopicPartition> partitions)
						for (TopicPartition partition : partitions)
							// 需要所有的TP都满足 SubscriptionState.assignment.contains(tp) && 且该TP.TopicPartitionState.position !=null; 才能通过校验 返回true;
							if (!hasValidPosition(partition){return isAssigned(tp) && assignedState(tp).hasValidPosition();}){
								return false;//
							}
								
						return true;
					}
				}){//当存在某TP.TopicPartitionState.position 为空时(初次或rebalace时,都为null); 
					
					/* 将SubscriptionState.assignment中tpState.position==null的分区fetcher位置重置为: committed位置,或者按reset policy重置;
					*	
					*
					*/
					Set<TopicPartition> missing = this.subscriptions.missingFetchPositions();{
						Set<TopicPartition> missing = new HashSet<>();
						for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
							if (!state.value().hasValidPosition())
								missing.add(state.topicPartition());
						}
						return missing;//获取position==null的 TPs 为missing;
					}
					updateFetchPositions(missing);{//KafkaConsumer.updateFetchPositions(Set<TopicPartition> partitions)
						
						// 重要?
						fetcher.resetOffsetsIfNeeded(partitions);{//Fetcher.resetOffsetsIfNeeded()
							final Set<TopicPartition> needsOffsetReset = new HashSet<>();
							for (TopicPartition tp : partitions) {
								if (subscriptions.isAssigned(tp){return assignment.contains(tp);} //保证SubscriptionState.assignment[PartitionStates].map[TopicPartition,TopicPartitionState]这个LinkedMap中包括此 tp的Key;
										&& subscriptions.isOffsetResetNeeded(tp){return assignedState(partition){return map.get(tp);}.awaitingReset(){return resetStrategy != null;};}//判断该tp.TopicPartitionState.resetStrategy 不为空;
									)// 判断哪些partitons要reset: 
									needsOffsetReset.add(tp);
							}
							// 初始化时,因为TPState.resetStrategy==null, 所以不会触发 resetOffsets()操作;
							if (!needsOffsetReset.isEmpty()) {//当有TPState.postion==null但其resetStrategy!=null的时候(即needsOffsetReset里的tp), 需要对此TPs进行reset操作;  
								resetOffsets(needsOffsetReset);{//Fetcher.resetOffsets(Set<TopicPartition> partitions): 将指定tps按照给定的 Reset Policy(Earliest/Latest/Node)重置position?
									final Map<TopicPartition, Long> offsetResets = new HashMap<>();
									final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
									for (final TopicPartition partition : partitions) {
										offsetResetStrategyTimestamp(partition, offsetResets, partitionsWithNoOffsets);
									}
									final Map<TopicPartition, OffsetData> offsetsByTimes = retrieveOffsetsByTimes(offsetResets, Long.MAX_VALUE, false);
									for (final TopicPartition partition : partitions) {
										final OffsetData offsetData = offsetsByTimes.get(partition);
										if (offsetData == null) {
											partitionsWithNoOffsets.add(partition);
											continue;
										}
										// we might lose the assignment while fetching the offset, so check it is still active
										if (subscriptions.isAssigned(partition)) {
											log.debug("Resetting offset for partition {} to offset {}.", partition, offsetData.offset);
											// 这里就是reset offset的操作?
											this.subscriptions.seek(partition, offsetData.offset);
										}
									}
									if (!partitionsWithNoOffsets.isEmpty()) {
										throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
									}
								}
							}
						}
						
						if (!subscriptions.hasAllFetchPositions(partitions)) {//所有TPState.position都有,hasAllFetchPositions()才为true; 当还有TPState.position为空时,进入; 初始化时这里会进入;
							coordinator.refreshCommittedOffsetsIfNeeded();{//ConsumerCoordinator.refreshCommittedOffsetsIfNeeded()
								if (subscriptions.refreshCommitsNeeded(){return this.needsFetchCommittedOffsets;}) {//前面(Rebalance后)onJoinComplete()中会将 needsFetchCommittedOffsets置为true;
									// 阻塞在while(true)中不断发 OffsetFetchRequest 和poll()直到(某一个)请求成功;
									Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());{
										while (true) {
											ensureCoordinatorReady();
											// contact coordinator to fetch committed offsets
											RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);{//构建OffsetFetchRequest请求到unset待发送队列;
												Node coordinator = coordinator();
												if (coordinator == null)
													return RequestFuture.coordinatorNotAvailable();
												log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
												// construct the request
												OffsetFetchRequest.Builder requestBuilder =new OffsetFetchRequest.Builder(this.groupId, new ArrayList<>(partitions));
												// send the request with a callback
												return client.send(coordinator, requestBuilder).compose(new OffsetFetchResponseHandler());
											}
											//阻塞在ConsumerNetworkClient.poll()中循环 send()和handle()返回的Response,直到该OffsetFetchRequest返回并处理完;
											client.poll(future);// 代码详解后面; 注意,在该future.onSuccess()后,会执行相关
											{//OffsetFetchResponseHandler.handle()
												
											}
											
											if (future.succeeded())return future.value();//
											if (!future.isRetriable()) throw future.exception();
											time.sleep(retryBackoffMs);
										}
										
										
										
									}
									
									for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
										TopicPartition tp = entry.getKey();
										// verify assignment is still active
										if (subscriptions.isAssigned(tp))
											this.subscriptions.committed(tp, entry.getValue());
									}
									this.subscriptions.commitsRefreshed();
								}
							}
							
							// then do any offset lookups in case some positions are not known
							fetcher.updateFetchPositions(partitions);{//Fetcher.updateFetchPositions(tp)
								// 什么情况下, 要先resetOffset()?  其SubscriptionState.resetStrategy !=null(非空)时,
								if (subscriptions.isOffsetResetNeeded(tp)) {
									resetOffset(tp);{//Fetcher.resetOffset(tp)
										// 更新之前设置的resetStrategy(重置策略:Latest/Earliest/None) 重置其offset
										OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
										final long timestamp;
										if (strategy == OffsetResetStrategy.EARLIEST)
											timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
										else if (strategy == OffsetResetStrategy.LATEST)
											timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
										else
											throw new NoOffsetForPartitionException(partition);

										log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
										// 真正完成网络获取offset位置的请求: 发异步请求,并循环等待future.succeeded
										long offset = listOffset(partition, timestamp);{//Fetcher.listOffset(tp,time)
											while(true){
												RequestFuture<Long> future = sendListOffsetRequest(partition, timestamp);
												client.poll(future);
												if (future.succeeded()){//查询成功,就中断循环并返回;
													return future.value();
												}
												if (future.exception() instanceof InvalidMetadataException){
													client.awaitMetadataUpdate();
												}else{
													time.sleep(retryBackoffMs);//重试期间,随眠等待 n毫秒
												}
											}
										}
										// 将本consumer订阅的TP.消费offset位置
										if (subscriptions.isAssigned(partition)){
											this.subscriptions.seek(partition, offset);{//SubscriptionState.seek()
												//class TopicPartitionState(Long position, OffsetAndMetadata committed, boolean paused, OffsetResetStrategy resetStrategy)
												assignedState(tp).seek(offset);{//TopicPartitionState.seek(long offset)
													this.position = offset;
													this.resetStrategy = null;
												}
											}
										}
									}
								}
							}
						}
					}
					
				}
				
				// if data is available already, return it immediately
				// 重要操作, 先进行一次获取数据;从哪里获取? client-buff? server端的buff?
				Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();{//Fetcher.fetchedRecords()
					Map<TopicPartition, List<ConsumerRecord<K, V>>> fetched = new HashMap<>();
					int recordsRemaining = maxPollRecords;//recordsRemaining表示:剩余还要拉取的数据; maxPollRecords有参数max.poll.records设定;
					try {
						while (recordsRemaining > 0) {
							if (nextInLineRecords == null || nextInLineRecords.isFetched) {//?
								// completedFetches:LinkedQueue<CompletedFetch> 用于存放FetchRequest请求成功后获取的FetchData 存于该完成队列中;
								// 位于 FetchRequest.RequestFutureListener.onSuccess()方法中 completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData));
								CompletedFetch completedFetch = completedFetches.peek();//先从队列的头部读取(但并不移除)头元素;peek()若为空反馈null;
								if (completedFetch == null) break;// 若没有元素,终止本循环; ==null,表示所有请求还没完成; 
								
								// 处理各种已完成Batch的结果;
								nextInLineRecords = parseCompletedFetch(completedFetch);{//Fetcher.parseCompletedFetch(CompletedFetch completedFetch)
									TopicPartition tp = completedFetch.partition;
									FetchResponse.PartitionData partition = completedFetch.partitionData;
									long fetchOffset = completedFetch.fetchedOffset;
									PartitionRecords partitionRecords = null;
									Errors error = partition.error;
									try {
										if (!subscriptions.isFetchable(tp)) {
											log.debug("Ignoring fetched records for partition {} since it is no longer fetchable", tp);
										} else if (error == Errors.NONE) {//正常没错误,进入这里;
											Long position = subscriptions.position(tp);{//SubscriptionState.position(tp)
												return assignedState(tp){//SubscriptionState.assignedState(TopicPartition tp)
													TopicPartitionState state = this.assignment.stateValue(tp);
													if (state == null)
														throw new IllegalStateException("No current assignment for partition " + tp);
													return state;
												}.position;
											}
											if (position == null || position != fetchOffset) {
												log.debug("Discarding stale fetch response for partition {} since its offset {} does not match {}", tp, fetchOffset, position);
												return null;
											}
											
											// 进入这里说明position==fetchOffset;
											log.trace("Preparing to read {} bytes of data for partition {} with offset {}",partition.records.sizeInBytes(), tp, position);
											Iterator<? extends RecordBatch> batches = partition.records.batches().iterator();
											partitionRecords = new PartitionRecords(tp, completedFetch, batches);
											if (!batches.hasNext() && partition.records.sizeInBytes() > 0) {
												if (completedFetch.responseVersion < 3) {
													// Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
													Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
													throw new RecordTooLargeException("There are some messages at [Partition=Offset]: ")", recordTooLargePartitions);
												} else {
													// This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
													throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
														fetchOffset + ". Received a non-empty fetch response from the server, but no " +
														"complete records were found.");
												}
											}
											
											if (partition.highWatermark >= 0) {
												log.trace("Updating high watermark for partition {} to {}", tp, partition.highWatermark);
												subscriptions.updateHighWatermark(tp, partition.highWatermark);
											}
											
											if (partition.lastStableOffset >= 0) {
												log.trace("Updating last stable offset for partition {} to {}", tp, partition.lastStableOffset);
												subscriptions.updateLastStableOffset(tp, partition.lastStableOffset);
											}
										} else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
											log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
											this.metadata.requestUpdate();
										} else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
											log.warn("Received unknown topic or partition error in fetch for partition {}. The topic/partition " +
													"may not exist or the user may not have Describe access to it", tp);
											this.metadata.requestUpdate();
										} else if (error == Errors.OFFSET_OUT_OF_RANGE) {
											if (fetchOffset != subscriptions.position(tp)) {
												log.debug("Discarding stale fetch response for partition {} since the fetched offset {}" +
														"does not match the current offset {}", tp, fetchOffset, subscriptions.position(tp));
											} else if (subscriptions.hasDefaultOffsetResetPolicy()) {
												log.info("Fetch offset {} is out of range for partition {}, resetting offset", fetchOffset, tp);
												subscriptions.needOffsetReset(tp);
											} else {
												throw new OffsetOutOfRangeException(Collections.singletonMap(tp, fetchOffset));
											}
										} else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
											log.warn("Not authorized to read from topic {}.", tp.topic());
											throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
										} else if (error == Errors.UNKNOWN) {
											log.warn("Unknown error fetching data for topic-partition {}", tp);
										} else {
											throw new IllegalStateException("Unexpected error code " + error.code() + " while fetching data");
										}
									} finally {
										if (partitionRecords == null)
											completedFetch.metricAggregator.record(tp, 0, 0);
										if (error != Errors.NONE)
											subscriptions.movePartitionToEnd(tp);
									}
									
									return partitionRecords;
								}
								completedFetches.poll();
							} else {
								List<ConsumerRecord<K, V>> records = fetchRecords(nextInLineRecords, recordsRemaining);{//Fetcher.fetchRecords(): 
									if (!subscriptions.isAssigned(partitionRecords.partition)) {
										// this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
										log.debug("Not returning fetched records for partition {} since it is no longer assigned", partitionRecords.partition);
									} else {
										// note that the consumed position should always be available as long as the partition is still assigned
										long position = subscriptions.position(partitionRecords.partition);
										if (!subscriptions.isFetchable(partitionRecords.partition)) {
											// this can happen when a partition is paused before fetched records are returned to the consumer's poll call
											log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", partitionRecords.partition);
										} else if (partitionRecords.nextFetchOffset == position) {
											List<ConsumerRecord<K, V>> partRecords = partitionRecords.fetchRecords(maxRecords);
											long nextOffset = partitionRecords.nextFetchOffset;
											log.trace("Returning fetched records at offset {} for assigned partition {} and update " + "position to {}", position, partitionRecords.partition, nextOffset);
											subscriptions.position(partitionRecords.partition, nextOffset);
											Long partitionLag = subscriptions.partitionLag(partitionRecords.partition, isolationLevel);
											if (partitionLag != null)
												this.sensors.recordPartitionLag(partitionRecords.partition, partitionLag);

											return partRecords;
										} else {
											log.debug("Ignoring fetched records for {} at offset {} since the current position is {}", partitionRecords.partition, partitionRecords.nextFetchOffset, position);
										}
									}
									
									// 从缓存的数据中直接拉取;
									partitionRecords.drain();{//Fetcher.PartitionRecords.drain()
										if (!isFetched) {
											maybeCloseRecordStream();
											hasExceptionInLastFetch = false;
											this.isFetched = true;
											this.completedFetch.metricAggregator.record(partition, bytesRead, recordsRead);
											if (bytesRead > 0)
												subscriptions.movePartitionToEnd(partition);
										}
									}
									
									return emptyList();
								}
								
								TopicPartition partition = nextInLineRecords.partition;
								// 当该分区获取到数据时, 将其存于其TP对应在fetched:Map<TopicPartition, List<ConsumerRecord<K, V>>> 的list列表中;
								if (!records.isEmpty()) {
									List<ConsumerRecord<K, V>> currentRecords = fetched.get(partition);
									if (currentRecords == null) {//若该TP不存在 就put()新增;
										fetched.put(partition, records);
									} else {//若该TP存在说明上需要添加到其List中;(一般是 maxPollRecords > 刚fetchRecords()获取的数据;
										List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
										newRecords.addAll(currentRecords);
										newRecords.addAll(records);
										fetched.put(partition, newRecords);
									}
									recordsRemaining -= records.size();
								}
							}
						}
					} catch (KafkaException e) {
						if (fetched.isEmpty())
							throw e;
					}
					return fetched;
				}
				if (!records.isEmpty())
					return records;
				
				// send any new fetches (won't resend pending fetches)
				fetcher.sendFetches();{//Fetcher.sendFetches() ; 若执行到此,说明上面 completedFetches 和records为空,没有数据; 就再准备FetchRequest请求,存于ConsumerNetworkClient.unsent 待发队列中;
					Map<Node, FetchRequest.Builder> fetchRequestMap = createFetchRequests();{
						// create the fetch info: 
						Cluster cluster = metadata.fetch();
						Map<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> fetchable = new LinkedHashMap<>();
						List<TopicPartition> fetchableTPs = fetchablePartitions();{
							Set<TopicPartition> exclude = new HashSet<>();
							List<TopicPartition> fetchable = subscriptions.fetchablePartitions();{//判断SubscriptionState.PartitionState.map[TP,TPState]中哪些TPState是可用的(TPState.paused==false & TPState.position !=null);
								List<TopicPartition> fetchable = new ArrayList<>(assignment.size());
								for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
									if (state.value().isFetchable(){return !paused && hasValidPosition();}) // 可fetchable的条件2个: 没有被暂停(!paused) 且 position不为空(position !=null)
										fetchable.add(state.topicPartition());
								}
								return fetchable;
							}
							// 判断如果Fetcher.nextInLineRecords或 Fetcher.completedFetches中,还有哪些TP的是[已拉取过来但未Fetched]的, 将这些已有缓存的TPs排除在本次SendFetch请求之外;
							if (nextInLineRecords != null && !nextInLineRecords.isFetched) {//排除已缓存在nextInLineRecords中的TPs;
								exclude.add(nextInLineRecords.partition);
							}
							for (CompletedFetch completedFetch : completedFetches) {// 排除已缓存在completedFetchers中的TPs;
								exclude.add(completedFetch.partition);
							}
							fetchable.removeAll(exclude);
							return fetchable;// 将PartitionState.map中可
						}
						for (TopicPartition partition : fetchableTPs) {
							Node node = cluster.leaderFor(partition);
							if (node == null) {
								metadata.requestUpdate();
							} else if (!this.client.hasPendingRequests(node){//ConsumerNetworkClient.hasPendingRequests(node);判断NetworkClient与该Node直接是否有正在传输(In-Flight)的网络请求尚未完成; 若没有了,才进入这里;
								//能进这里说明本client与目标Node的所有请求都已完成,
								if (unsent.hasRequests(node)) return true;//若consumerNetClient.unset()待发队列中有该node且其对应queue中不为空,表示该node还有pending的请求有待发送: return ! unsent.get(node).isEmpty();
								synchronized (this) {
									return client.hasInFlightRequests(node.idString());{//NetworkClient.hasInFlightRequests():若NetworkClient.inFlightRequests.get(node)的Queue正在飞行队列中有数据,返回ture,
										return this.inFlightRequests.isEmpty(node);{return !requests.get(node).isEmpty()}
									}
								}
							}) {
								//  InFlightRequests是对已经被发送或正在被发送但是均未接收到响应的客户端请求集合的一个封装，在它内部，有两个重要的变量
								// maxInFlightRequestsPerConnection不消多说，它是限制每个连接，即每个node对应客户端请求队列大小的一个阈值。
								// if there is a leader and no in-flight requests, issue a new fetch
								LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
								if (fetch == null) {
									fetch = new LinkedHashMap<>();
									fetchable.put(node, fetch);
								}
								long position = this.subscriptions.position(partition);
								fetch.put(partition, new FetchRequest.PartitionData(position, FetchRequest.INVALID_LOG_START_OFFSET,this.fetchSize));
								log.debug("Added {} fetch request for partition {} at offset {} to node {}", isolationLevel,partition, position, node);
							} else {//进入这里;说明上面并没有创建新 FetchRequest请求,因为NetworkClient.inFlightRequests中,还有向相同Node发出的 尚未完成的(正在飞行)的请求;
								log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);//这里打印过多,说明 server端响应结果太慢;
							}
						}
						// create the fetches
						Map<Node, FetchRequest.Builder> requests = new HashMap<>();
						for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
							Node node = entry.getKey();
							FetchRequest.Builder fetch = FetchRequest.Builder.forConsumer(this.maxWaitMs, this.minBytes,
									entry.getValue(), isolationLevel)
									.setMaxBytes(this.maxBytes);
							requests.put(node, fetch);
						}
						return requests;
					}
					for (Map.Entry<Node, FetchRequest.Builder> fetchEntry : fetchRequestMap.entrySet()) {
						client.send(fetchEntry.getKey(), request)
							.addListener(new RequestFutureListener<ClientResponse>() {});
					}
				}
				long now = time.milliseconds();
				long pollTimeout = Math.min(coordinator.timeToNextPoll(now), timeout);
				client.poll(pollTimeout, now, new PollCondition() {
					@Override
					public boolean shouldBlock() {
						// since a fetch might be completed by the background thread, we need this poll condition
						// to ensure that we do not block unnecessarily in poll()
						return !fetcher.hasCompletedFetches();
					}
				});
				if (coordinator.needRejoin())
					return Collections.emptyMap();
				return fetcher.fetchedRecords();
			}
			
			if (!records.isEmpty()) {
				
				// sendFetches() 向可用Node准备好 ;向client发送 FetchRequest.Builder 构建请求;
				int sendFetchesNum = fetcher.sendFetches();{//Fetcher.sendFetches()
					Map<Node, FetchRequest.Builder> fetchRequestMap = createFetchRequests();
					for (Map.Entry<Node, FetchRequest.Builder> fetchEntry : fetchRequestMap.entrySet()) {
						final FetchRequest.Builder request = fetchEntry.getValue();
						final Node fetchTarget = fetchEntry.getKey();

						log.debug("Sending {} fetch for partitions {} to broker {}", isolationLevel, request.fetchData().keySet(),
								fetchTarget);
						client.send(fetchTarget, request)
								.addListener(new RequestFutureListener<ClientResponse>() {
									@Override
									public void onSuccess(ClientResponse resp) {
										FetchResponse response = (FetchResponse) resp.responseBody();
										if (!matchesRequestedPartitions(request, response)) {
											log.warn("Ignoring fetch response containing partitions {} since it does not match " +
													"the requested partitions {}", response.responseData().keySet(),
													request.fetchData().keySet());
											return;
										}

										Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
										FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

										for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
											TopicPartition partition = entry.getKey();
											long fetchOffset = request.fetchData().get(partition).fetchOffset;
											FetchResponse.PartitionData fetchData = entry.getValue();

											log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
													isolationLevel, fetchOffset, partition, fetchData);
											completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
													resp.requestHeader().apiVersion()));
										}

										sensors.fetchLatency.record(resp.requestLatencyMs());
									}

									@Override
									public void onFailure(RuntimeException e) {
										log.debug("Fetch request {} to {} failed", request.fetchData(), fetchTarget, e);
									}
								});
					}
					return fetchRequestMap.size();
				}
				
				if (sendFetchesNum > 0 || client.hasPendingRequests()){//hasPendingRequests()判断unset中是否有未发送请求或hasInFlightRequests();
					// 把unset队列中的请求发送完,把pendingCompletion队列的结果处理完, 且不唤醒client
					client.pollNoWakeup();// ConsumerNetworkClient.pollNoWakeup(): 
				}
				
				
				if (this.interceptors == null)
					return new ConsumerRecords<>(records);
				else
					return this.interceptors.onConsume(new ConsumerRecords<>(records));
			}

			long elapsed = time.milliseconds() - start;
			remaining = timeout - elapsed;
		} while (remaining > 0);

		return ConsumerRecords.empty();
	} finally {
		release();
	}
}


// Kafka的poll()逻辑之:高度总结
	KafkaConsumer.poll(timeout){
		do {
			Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollOnce(remaining);{//若Fetcher.completedFetches中有直接取出返回; 若没有数据就进行一次发送FetchRequests的操作;
				Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();{//1. 若Fetcher.completedFetches:LinkedQueue<CompletedFetch> 中有数据,就将其取出封装到PartitionRecords对象中,并进一步装入Map<TP,List<ConsumerRecord>>中返回;
					int recordsRemaining = maxPollRecords;
					while (recordsRemaining > 0) {
						if (nextInLineRecords == null || nextInLineRecords.isFetched) {//nextInLineRecords:PartitionRecords 若为空或已被Fetched标记,则从completedFetches队列中重新拉取元素PartitionData并赋值给nextInLineRecords 
							// completedFetches:LinkedQueue<CompletedFetch> 用于存放FetchRequest请求成功后获取的FetchData 存于该完成队列中;位于 FetchRequest.RequestFutureListener.onSuccess()方法中 completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData));
							CompletedFetch completedFetch = completedFetches.peek();//这里就是从completedFetches中 一个一个的取出PartitonData元素并赋值给 nextInLineRecords,用于下面else被解析;
							if (completedFetch == null) break;// 若没有元素,终止本循环; ==null,表示所有请求还没完成; 
							nextInLineRecords = parseCompletedFetch(completedFetch);{//这里就仅仅是将completedFetch.partitionData.records 取出并封装进 PartitionRecords对象中;
								TopicPartition tp = completedFetch.partition;
								FetchResponse.PartitionData partition = completedFetch.partitionData;
								else if (error == Errors.NONE) {//正常没错误,进入这里;
									Long position = subscriptions.position(tp);
									if (position == null || position != completedFetch.fetchedOffset;) return null; //若subscriptions想要的offset与 completedFetch返回的offset不一致; 则返回空;
									// 进入这里说明position==fetchOffset; 
									Iterator<? extends RecordBatch> batches = partition.records.batches().iterator();//将completedFetch.partitionData.records 以迭代器返回;
									partitionRecords = new PartitionRecords(tp, completedFetch, batches);//将TP和 返回数据batches 封装进 PartitionRecords中返回;
								}
							}
							completedFetches.poll();
						} else{//说明nextInLineRecords里面有数据; 这里就仅是将nextInLineRecords里 PartitionRecords
							List<ConsumerRecord<K, V>> records = fetchRecords(nextInLineRecords, recordsRemaining);{//fetchRecords(PartitionRecords partitionRecords, int maxRecords) 
								if (!subscriptions.isAssigned(partitionRecords.partition)) {
									log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable");
								} else{ //该分区(TopicPartition)是绑定在SubscriptionState.assignment.map中的; 进入这里获取数据,并更新SubscriptionState
									else if (partitionRecords.nextFetchOffset == position) {
										List<ConsumerRecord<K, V>> partRecords = partitionRecords.fetchRecords(maxRecords);//partitionRecords即上面上层方法的Fetcher.nextInLineRecords[PartitionRecords]
										subscriptions.position(partitionRecords.partition, nextOffset);
										long nextOffset = partitionRecords.nextFetchOffset;
										subscriptions.position(partitionRecords.partition, nextOffset);//将该TP的下个offset值(partitionRecords.nextFetchOffset) 更新为subscriptions.position值;
									}
								}
							}
						}
					}
				}
				if (!records.isEmpty()) return records; //若读取到任一分区有返回数据,则结束方法,立马返回records;
				fetcher.sendFetches();{//Fetcher.sendFetches() ; 若执行到此,说明上面 completedFetches 和records为空,没有数据; 就再准备FetchRequest请求,存于ConsumerNetworkClient.unsent 待发队列中;
					Map<Node, FetchRequest.Builder> fetchRequestMap = createFetchRequests();{//计算PartitionState.map中可fetchable的,且没有被已缓存的nextInLineRecords/completedFetches中的分区TPs
						List<TopicPartition> fetchableTPs = fetchablePartitions();{//计算可fetchable的分区: PartitionState.map中可fetchable的, 且不再已缓存的nextInLineRecords/completedFetches中的分区;
							List<TopicPartition> fetchable = subscriptions.fetchablePartitions();{//判断SubscriptionState.PartitionState.map[TP,TPState]中哪些TPState是可用的(TPState.paused==false & TPState.position !=null);
								for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
									if (state.value().isFetchable(){return !paused && hasValidPosition();}) // 可fetchable的条件2个: 没有被暂停(!paused) 且 position不为空(position !=null)
										fetchable.add(state.topicPartition());
								}
							}
							if (nextInLineRecords != null && !nextInLineRecords.isFetched) {//排除已缓存在nextInLineRecords中的TPs;
								exclude.add(nextInLineRecords.partition);
							}
							for (CompletedFetch completedFetch : completedFetches) {// 排除已缓存在completedFetchers中的TPs;
								exclude.add(completedFetch.partition);
							}
							fetchable.removeAll(exclude);// 判断如果Fetcher.nextInLineRecords或 Fetcher.completedFetches中,还有哪些TP的是[已拉取过来但未Fetched]的, 将这些已有缓存的TPs排除在本次SendFetch请求之外;
							return fetchable;// 将PartitionState.map中可fetchable的,且不再已缓存的nextInLineRecords/completedFetches中的分区TPs,返回;
						}
						for (TopicPartition partition : fetchableTPs) {
							else if (!this.client.hasPendingRequests(node)) {//ConsumerNetworkClient.hasPendingRequests(node);判断NetworkClient与该Node直接是否有正在传输(In-Flight)的网络请求尚未完成; 若没有(inFlight)了,才进入这里;
								LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);//能进这里说明本client与目标Node的所有请求都已完成,
								long position = this.subscriptions.position(partition);//查看PartitonState.map.get(tp)该分区的position作为要获取数据的位移;
								//在这正式构建一次Fetch数据的参数: TPState.position作为fetchOffset,max.partition.fetch.bytes作为maxBytes;
								fetch.put(partition, new FetchRequest.PartitionData(position, FetchRequest.INVALID_LOG_START_OFFSET,this.fetchSize));//将该 position,fetchSize一起封装到PartitonData,用于传给Server做请求做Fetch的参数;
							}
						}
						for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {//讲该TP这次的请求数据PartitionData(及entry.getValue())封装到 FetchRequest.Builder对象中;
							FetchRequest.Builder fetch = FetchRequest.Builder.forConsumer(this.maxWaitMs, this.minBytes,entry.getValue(), isolationLevel).setMaxBytes(this.maxBytes);
							requests.put(node, fetch);//client通信是以Node为单位的,
						}
					}
					
					for (Map.Entry<Node, FetchRequest.Builder> fetchEntry : fetchRequestMap.entrySet()) {
						client.send(fetchEntry.getKey(), request)
							.addListener(new RequestFutureListener<ClientResponse>() {});
					}
				}
				client.poll(pollTimeout, now, new PollCondition() {});{//将其unsent待发队列中的请求发出去; 先把pendingCompletion队列的Futures处理完; 在把unsent待发队列中请求都通过selector发出去; 再处理一遍pendingCompletion中返回Futures;
					poll(0, time.milliseconds(), null, true);{//ConsumerNetworkClient.poll()终极执行: 先把pendingCompletion队列的Futures处理完; 在把unsent待发队列中请求都通过selector发出去; 再处理一遍pendingCompletion中返回Futures;
						// 1. 先把 pendingCompletion队列中 未处理的RequestFutures都处理完;
						firePendingCompletedRequests(){//ConsumerNetworkClient.firePendingCompletedRequests(); 若Queue中还有CompletionRequest,会阻塞在此,直到循环处理完所有CompletionRequest;
							for (;;) {//循环从已completion任务的队列中拿任务; 当 pendingCompletion:LinkedQueue队列中还有待处理任务时,会一直循环处理,否则break结束,进入 client.wakeup();
								RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();//从等待处理的已完成请求队列中,取一个: pendingCompletion:Queue<RequestFutureCompletionHandler>
								if (completionHandler == null) break; // 当pending队列为空时, 即终止for(;;)循环并进入后面的client.wakeup()操作; 表示所有成功返回的请求,都已被处理完了; 
								completionHandler.fireCompletion(){//ConsumerNetworkClient.RequestFutureCompletionHandler.fireCompletion()
									if(e == null && !response.wasDisconnected()) future.complete(response){//RequestFuture.complete(T value) 若请求没有错误和断开连接, 则进入future.complete()处理
										fireSuccess();{//RequestFuture.fireSuccess(): 阻塞在此,一直处理各种成功消息?
											while (true) {//当 异步结果队列listeners中还有未处理完的Future时,会一直阻塞在此循环处理;
												RequestFutureListener<T> listener = listeners.poll();//listeners= LinkedQueue<RequestFutureListener<T>>, 异步请求结果的future队列,
												if (listener == null) break;// 若队列为空,则结束 while()循环,直接返回;
												listener.onSuccess(value);{//RequestFuture.onSuccess(): 当队列中有 未处理的Future结果时,调其onSuccess()处理;
													GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){}
												}
											}
										}
									}
								}
							}
						}
						// 2. 将 unsent:Queue<ClientRequest> 队列中所有未发请求,都向Server发送;
						trySend(now);{//ConsumerNetworkClient.trySend()
							for (Node node : unsent.nodes()) {// return unsent.keySet();遍历各节点;
								Iterator<ClientRequest> iterator = unsent.requestIterator(node);//return unsent.get(node).iterator();将对该节点的所有未发请求队列,以迭代器返回;
								while (iterator.hasNext()) {// 将对该Node的所有请求,都发送出去;
									ClientRequest request = iterator.next();
									if (client.ready(node, now)) {
										client.send(request, now);{//NetworkClient.send(ClientRequest request, long now): 真正开始发送一个网络请求
											doSend(request, false, now);{//NetworkClient.send(ClientRequest clientRequest, boolean isInternalRequest, long now)
												AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
												doSend(clientRequest, isInternalRequest, now, builder.build(version));{
													InFlightRequest inFlightRequest = new InFlightRequest();
													selector.send(inFlightRequest.send);{
														KafkaChannel channel = channelOrFail(send.destination(), false);
														channel.setSend(send);{//KafkaChannel.setSend()
															this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);//
														}
													}
												}
											}
										}
									}
								}
							}
						}
						// 3. 这期间可能 pendingCompletion队列中又有一些已成功返回的结果, 对这些从Server返回的RequestFutures依次处理,阻塞在此循环处理,直到pendingCompletion为空; 放外面避免死锁;
						firePendingCompletedRequests(){//ConsumerNetworkClient.firePendingCompletedRequests(); 若Queue中还有CompletionRequest,会阻塞在此,直到循环处理完所有CompletionRequest;
							for (;;) {//循环从已completion任务的队列中拿任务; 当 pendingCompletion:LinkedQueue队列中还有待处理任务时,会一直循环处理,否则break结束,进入 client.wakeup();
								RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();//从等待处理的已完成请求队列中,取一个: pendingCompletion:Queue<RequestFutureCompletionHandler>
								if (completionHandler == null) break; // 当pending队列为空时, 即终止for(;;)循环并进入后面的client.wakeup()操作; 表示所有成功返回的请求,都已被处理完了; 
								completionHandler.fireCompletion(){//ConsumerNetworkClient.RequestFutureCompletionHandler.fireCompletion()
									if(e == null && !response.wasDisconnected()) future.complete(response){//RequestFuture.complete(T value) 若请求没有错误和断开连接, 则进入future.complete()处理
										fireSuccess();{//RequestFuture.fireSuccess(): 阻塞在此,一直处理各种成功消息?
											while (true) {//当 异步结果队列listeners中还有未处理完的Future时,会一直阻塞在此循环处理;
												RequestFutureListener<T> listener = listeners.poll();//listeners= LinkedQueue<RequestFutureListener<T>>, 异步请求结果的future队列,
												if (listener == null) break;// 若队列为空,则结束 while()循环,直接返回;
												listener.onSuccess(value);{//RequestFuture.onSuccess(): 当队列中有 未处理的Future结果时,调其onSuccess()处理;
													GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){}
													Fetcher.FetchRequest.RequestFutureListener.$onSuccess(){
														FetchResponse response = (FetchResponse) resp.responseBody();
														for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
															long fetchOffset = request.fetchData().get(partition).fetchOffset;
															FetchResponse.PartitionData fetchData = entry.getValue();//结果数据存于 FetchResponse.responseData: Map<TopicPartition, PartitionData> 字段中;
															//将各分区的返回数据 PartitionData,与fetchOffset一起封装进CompletedFetch(tp,PartitonData)中,并缓存到Fetcher.completedFetches队列中; 
															completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator, resp.requestHeader().apiVersion()));
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
				
			}
			if (!records.isEmpty()) {//若上面pollOnce()从completedFetches中读取数据,则封装到ConsumerRecords对象中,结束方法返回;
				if (fetcher.sendFetches() > 0 || client.hasPendingRequests()){ //判断上面pollOnce()中有没有向 ConsumerNetworkClient.unset新增过请求并刷新下返回结果;
					client.pollNoWakeup();{//ConsumerNetworkClient.pollNoWakeup()
						poll(0, time.milliseconds(), null, true);{//ConsumerNetworkClient.poll()终极执行: 先把pendingCompletion队列的Futures处理完; 在把unsent待发队列中请求都通过selector发出去; 再处理一遍pendingCompletion中返回Futures;
							// 1. 先把 pendingCompletion队列中 未处理的RequestFutures都处理完;
							firePendingCompletedRequests(){//ConsumerNetworkClient.firePendingCompletedRequests(); 若Queue中还有CompletionRequest,会阻塞在此,直到循环处理完所有CompletionRequest;
								for (;;) {//循环从已completion任务的队列中拿任务; 当 pendingCompletion:LinkedQueue队列中还有待处理任务时,会一直循环处理,否则break结束,进入 client.wakeup();
									RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();//从等待处理的已完成请求队列中,取一个: pendingCompletion:Queue<RequestFutureCompletionHandler>
									if (completionHandler == null) break; // 当pending队列为空时, 即终止for(;;)循环并进入后面的client.wakeup()操作; 表示所有成功返回的请求,都已被处理完了; 
									completionHandler.fireCompletion(){//ConsumerNetworkClient.RequestFutureCompletionHandler.fireCompletion()
										if(e == null && !response.wasDisconnected()) future.complete(response){//RequestFuture.complete(T value) 若请求没有错误和断开连接, 则进入future.complete()处理
											fireSuccess();{//RequestFuture.fireSuccess(): 阻塞在此,一直处理各种成功消息?
												while (true) {//当 异步结果队列listeners中还有未处理完的Future时,会一直阻塞在此循环处理;
													RequestFutureListener<T> listener = listeners.poll();//listeners= LinkedQueue<RequestFutureListener<T>>, 异步请求结果的future队列,
													if (listener == null) break;// 若队列为空,则结束 while()循环,直接返回;
													listener.onSuccess(value);{//RequestFuture.onSuccess(): 当队列中有 未处理的Future结果时,调其onSuccess()处理;
														GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){}
													}
												}
											}
										}
									}
								}
							}
							// 2. 将 unsent:Queue<ClientRequest> 队列中所有未发请求,都向Server发送;
							trySend(now);{//ConsumerNetworkClient.trySend()
								for (Node node : unsent.nodes()) {// return unsent.keySet();遍历各节点;
									Iterator<ClientRequest> iterator = unsent.requestIterator(node);//return unsent.get(node).iterator();将对该节点的所有未发请求队列,以迭代器返回;
									while (iterator.hasNext()) {// 将对该Node的所有请求,都发送出去;
										ClientRequest request = iterator.next();
										if (client.ready(node, now)) {
											client.send(request, now);{//NetworkClient.send(ClientRequest request, long now): 真正开始发送一个网络请求
												doSend(request, false, now);{//NetworkClient.send(ClientRequest clientRequest, boolean isInternalRequest, long now)
													AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
													doSend(clientRequest, isInternalRequest, now, builder.build(version));{
														InFlightRequest inFlightRequest = new InFlightRequest();
														selector.send(inFlightRequest.send);{
															KafkaChannel channel = channelOrFail(send.destination(), false);
															channel.setSend(send);{//KafkaChannel.setSend()
																this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);//
															}
														}
													}
												}
											}
										}
									}
								}
							}
							// 3. 这期间可能 pendingCompletion队列中又有一些已成功返回的结果, 对这些从Server返回的RequestFutures依次处理,阻塞在此循环处理,直到pendingCompletion为空; 放外面避免死锁;
							firePendingCompletedRequests(){//ConsumerNetworkClient.firePendingCompletedRequests(); 若Queue中还有CompletionRequest,会阻塞在此,直到循环处理完所有CompletionRequest;
								for (;;) {//循环从已completion任务的队列中拿任务; 当 pendingCompletion:LinkedQueue队列中还有待处理任务时,会一直循环处理,否则break结束,进入 client.wakeup();
									RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();//从等待处理的已完成请求队列中,取一个: pendingCompletion:Queue<RequestFutureCompletionHandler>
									if (completionHandler == null) break; // 当pending队列为空时, 即终止for(;;)循环并进入后面的client.wakeup()操作; 表示所有成功返回的请求,都已被处理完了; 
									completionHandler.fireCompletion(){//ConsumerNetworkClient.RequestFutureCompletionHandler.fireCompletion()
										if(e == null && !response.wasDisconnected()) future.complete(response){//RequestFuture.complete(T value) 若请求没有错误和断开连接, 则进入future.complete()处理
											fireSuccess();{//RequestFuture.fireSuccess(): 阻塞在此,一直处理各种成功消息?
												while (true) {//当 异步结果队列listeners中还有未处理完的Future时,会一直阻塞在此循环处理;
													RequestFutureListener<T> listener = listeners.poll();//listeners= LinkedQueue<RequestFutureListener<T>>, 异步请求结果的future队列,
													if (listener == null) break;// 若队列为空,则结束 while()循环,直接返回;
													listener.onSuccess(value);{//RequestFuture.onSuccess(): 当队列中有 未处理的Future结果时,调其onSuccess()处理;
														GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){}
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
				if (this.interceptors == null){
					return new ConsumerRecords<>(records);
				} else{
					return this.interceptors.onConsume(new ConsumerRecords<>(records));
				}
			}
			
		} while (remaining > 0);
	}




// 还是在"main"线程中, 解释ConsumerNetworkClient.poll()相关几个重载方法;
ConsumerNetworkClient.poll(){
	
	// 1. 阻塞等待unset队列中的请求发送和pendingCompletion队列的结果处理, 没处理完超时结束返回;
	poll(timeout);{//ConsumerNetworkClient.poll(long timeout)
		poll(timeout, time.milliseconds(), null);{
			poll(timeout, now, pollCondition, false);// 实现代码参见如下[最终实现]
		}
	}
	
	
	// 2. 阻塞在此,依次处理pendingCompletion中任务, 直到当前futrue.isDone(); 不设置超时;
	client.poll(future);{//ConsumerNetworkClient.poll(RequestFuture<?> future)
		while (!future.isDone()){//只要该future还没处理完,会一直阻塞在此,依次处理pendingCompletion中任务;直到当前futrue.isDone();
			poll(MAX_POLL_TIMEOUT_MS, time.milliseconds(), future);{
				poll(timeout, now, pollCondition, false);// 实现代码参见如下[最终实现]
			}
		}
	}
	
	//3. 阻塞等待future的结果返回; 实现同步效果;
	// client.poll(future, remainingMs) 的实现代码: 阻塞在此循环处理pendingCompletion:Queue队列中所有已返回请求; 直到当前futrue.isDone或者超timeout时间了;
	ConsumerNetworkClient.poll(RequestFuture<?> future, long timeout){
		long begin = time.milliseconds();
		long remaining = timeout;
		long now = begin;
		do {
			poll(remaining, now, future);{
				poll(timeout, now, pollCondition, false);//实现代码参见如下[最终实现]
			}
			now = time.milliseconds();
			long elapsed = now - begin;
			remaining = timeout - elapsed;
		} while (!future.isDone() && remaining > 0);
		return future.isDone();
	}
	
	// 4. 把unset队列中的请求发送完,把pendingCompletion队列的结果处理完, 且不唤醒client?
	client.pollNoWakeup();{//ConsumerNetworkClient.pollNoWakeup()
		poll(0, time.milliseconds(), null, true);// 实现代码参见如下[最终实现]
	}
	
	
	
	
	// 最终实现:  阻塞等待unset队列中的请求发送 和pendingCompletion队列的结果处理完;
	poll(timeout, now, pollCondition, false);{//poll最终实现: ConsumerNetworkClient.poll(long timeout, long now, PollCondition pollCondition, boolean disableWakeup)
		// 1. 先把 pendingCompletion队列中 未处理的RequestFutures都处理完;
		firePendingCompletedRequests(){//ConsumerNetworkClient.firePendingCompletedRequests()
			boolean completedRequestsFired = false;
			for (;;) {//循环从已completion任务的队列中拿任务; 当pendingCompletion:LinkedQueue队列中还有待处理任务时,会一直循环处理,否则break结束,进入 client.wakeup();
				RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();//从等待处理的已完成请求队列中,取一个: pendingCompletion:Queue<RequestFutureCompletionHandler>
				if (completionHandler == null) break; // 当pending队列为空时, 即终止for(;;)循环并进入后面的client.wakeup()操作; 表示所有成功返回的请求,都已被处理完了; 
				
				// 进入这里,表示有RequestFuture返回了但还没来得及去处理; 会进入调RequestFuture.complete()进行对相应请求进行onSuccess() or onFailure()的处理;
				completionHandler.fireCompletion(){//ConsumerNetworkClient.RequestFutureCompletionHandler.fireCompletion()
					if (e != null) {//有错误,抛出一个异常;
						future.raise(e);
					}else if (response.wasDisconnected()) {// 若断开了连接,抛出 DisconnectException异常;
						log.debug("Cancelled {} request {} with correlation id {} due to node {} being disconnected", api, requestHeader, correlation, response.destination());
						future.raise(DisconnectException.INSTANCE);
					}else { // 进入这里则表示请求成功完成; 
						future.complete(response){//RequestFuture.complete(T value)
							try {
								// 若有RuntimeEx异常 或 INCOMPLETE_SENTINEL锁已被覆盖, 则为状态异常; 抛出;
								if (value instanceof RuntimeException) throw new IllegalArgumentException("The argument to complete can not be an instance of RuntimeException");
								if (!result.compareAndSet(INCOMPLETE_SENTINEL, value))throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
								fireSuccess();{//RequestFuture.fireSuccess(): 阻塞在此,一直处理各种成功消息?
									T value = value();
									while (true) {
										RequestFutureListener<T> listener = listeners.poll();
										if (listener == null)
											break;
										listener.onSuccess(value);{//RequestFuture.onSuccess()
											// 括: GroupCoordinatorResponseHandler, 
											/*不同的请求类,adapter: RequestFutureAdapter<ClientResponse, T>的实现类不同; 包括: 
											* 	- 直接实现类: GroupCoordinatorResponseHandler
											*	- 简介实现类CoordinatorResponseHandler的子类: 
													* AbstractCoordinator.HeartbeatResponseHandler
													* AbstractCoordinator.JoinGroupResponseHandler
													* AbstractCoordinator.LeaveGroupResponseHandler
													* AbstractCoordinator.SyncGroupResponseHandler
													* ConsumerCoordinator.OffsetCommitResponseHandler
													* ConsumerCoordinator.OffsetFetchResponseHandler
											*	- 匿名内部类: Fetcher.sendListOffsetRequest()方法中: client.send().compose(new RequestFutureAdapter());
											*/
											adapter.onSuccess(value, adapted);{//不同实现类的onSuccess()方法
												// 1. 处理向Server端寻找GroupCoordinator请求 
												GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){
													log.debug("Received GroupCoordinator response {} for group {}", resp, groupId);
													FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
													Errors error = findCoordinatorResponse.error();
													clearFindCoordinatorFuture();
													if (error == Errors.NONE) {
														synchronized (AbstractCoordinator.this) {
															AbstractCoordinator.this.coordinator = new Node(
																	Integer.MAX_VALUE - findCoordinatorResponse.node().id(),
																	findCoordinatorResponse.node().host(), findCoordinatorResponse.node().port());
															log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
															client.tryConnect(coordinator);
															heartbeat.resetTimeouts(time.milliseconds());
														}
														future.complete(null);
													} else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
														future.raise(new GroupAuthorizationException(groupId));
													} else {
														log.debug("Group coordinator lookup for group {} failed: {}", groupId, error.message());
														future.raise(error);
													}
												}
												
												// 2. 抽象父类,转到个子类的handle()的; 
												CoordinatorResponseHandler.onSuccess(ClientResponse clientResponse, RequestFuture<T> future){
													this.response = clientResponse;
													R responseObj = (R) clientResponse.responseBody();
													handle(responseObj, future);{//handle()是抽象类, 有子类实现
														// 2.1 当本consumerCliet成功加入到 该Group后;
														JoinGroupResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){
															Errors error = joinResponse.error();
															if (error == Errors.NONE) {//正常请求成功时,进入这里;
																log.debug("Received successful JoinGroup response for group {}: {}", groupId, joinResponse);
																sensors.joinLatency.record(response.requestLatencyMs());
																synchronized (AbstractCoordinator.this) {
																	if (state != MemberState.REBALANCING) {
																		// if the consumer was woken up before a rebalance completes, we may have already left
																		// the group. In this case, we do not want to continue with the sync group.
																		future.raise(new UnjoinedGroupException());
																	} else {// 正常,进入这里;
																		AbstractCoordinator.this.generation = new Generation(joinResponse.generationId(), joinResponse.memberId(), joinResponse.groupProtocol());
																		/* 若为Consumer-Group-leader,把Reblance结果返回给bocker,并发给各consumer;
																		*	JoinGroupResponse={leaderId,members:[{clientId,value},{client-2,value2}]}
																		*/
																		if (joinResponse.isLeader()) {
																			onJoinLeader(joinResponse){//AbstractCoordinator.onJoinLeader(JoinGroupResponse joinResponse)
																				// 这里默认用RangeAssignor.assign()方法执行分配算法: 其结果为 assignment: Map<clinetId,List<TopicPartition>> 
																				Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),joinResponse.members());
																				SyncGroupRequest.Builder requestBuilder =new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId, groupAssignment);
																				log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}",groupId, this.coordinator, requestBuilder);
																				return sendSyncGroupRequest(requestBuilder);
																			}
																				.chain(future);
																		} else {
																			onJoinFollower().chain(future);
																		}
																	}
																}
															} else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
																log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
																		coordinator());
																// backoff and retry
																future.raise(error);
															} else if (error == Errors.UNKNOWN_MEMBER_ID) {
																// reset the member id and retry immediately
																resetGeneration();
																log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
																future.raise(Errors.UNKNOWN_MEMBER_ID);
															} else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
																// re-discover the coordinator and retry with backoff
																coordinatorDead();
																log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
																future.raise(error);
															} else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL || error == Errors.INVALID_SESSION_TIMEOUT || error == Errors.INVALID_GROUP_ID) {
																// log the error and re-throw the exception
																log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
																future.raise(error);
															} else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
																future.raise(new GroupAuthorizationException(groupId));
															} else {
																// unexpected error, throw the exception
																future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
															}
														}
														// 2.2 同步加入Group相关信息;
														SyncGroupResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
														
														// 2.3 发送超时或close等后,停止
														LeaveGroupResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
														
														
														// 在fetchCommittedOffsets()方法中,发送OffsetFetchRequest并成功后,处理逻辑如下:
														OffsetFetchResponseHandler.handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future){
															if (response.hasError()) {// 没有错误, 不进;
																Errors error = response.error();
																log.debug("Offset fetch for group {} failed: {}", groupId, error.message());
																if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
																	// just retry
																	future.raise(error);
																} else if (error == Errors.NOT_COORDINATOR) {
																	// re-discover the coordinator and retry
																	coordinatorDead();
																	future.raise(error);
																} else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
																	future.raise(new GroupAuthorizationException(groupId));
																} else {
																	future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
																}
																return;
															}
															
															Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
															for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
																TopicPartition tp = entry.getKey();
																OffsetFetchResponse.PartitionData data = entry.getValue();
																if (data.hasError()) {//没有错误,不进;
																	Errors error = data.error;
																	log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());
																	if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
																		future.raise(new KafkaException("Partition " + tp + " may not exist or the user may not have " +
																				"Describe access to the topic"));
																	} else {
																		future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
																	}
																	return;
																} else if (data.offset >= 0) {//正常 进入这里;将每个tp的Offset位置和元数据封装进OffsetAndMetadata;并存于Map中;
																	// record the position with the offset (-1 indicates no committed offset to fetch)
																	offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
																} else {//若该 GroupId还没commit过,Server就没数据, 其data.offset == -1, 进入这里;
																	log.debug("Group {} has no committed offset for partition {}", groupId, tp);
																}
															}
															//若该groupId提交过,这里是存于_consumer_offsets的offsets信息;  若没有提交过, 此offsets为空;
															future.complete(offsets);{
																try {
																	if (value instanceof RuntimeException) throw new IllegalArgumentException("The argument to complete can not be an instance of RuntimeException");
																	if (!result.compareAndSet(INCOMPLETE_SENTINEL, value)) throw new IllegalStateException("Invalid attempt to complete a request future which is already complete");
																	fireSuccess();{
																		T value = value();
																		while (true) {
																			RequestFutureListener<T> listener = listeners.poll();
																			if (listener == null) break;
																			listener.onSuccess(value);
																		}
																	}
																} finally {
																	completedLatch.countDown();
																}
															}
														}
														
													}
												}
												
											}
										}
									}
								}
							} finally {//不管是该RequestFuture成功还是失败, 都要释放锁;
								completedLatch.countDown();
							}
							
						}
					}
				}
			}
			if (completedRequestsFired) client.wakeup();//当请求完成时, 换气client;
		}
		
		// 2. 将 unsent:Queue<ClientRequest> 队列中所有未发请求,都向Server发送;
		synchronized (this) {
			trySend(now);{//ConsumerNetworkClient.trySend(long now): 遍历unset:Queue<ClientRequest>队列中所有未发请求并发送完;
				// send any requests that can be sent now
				boolean requestsSent = false;
				/* 遍历尚未发送的请求: unsent:Map<Node,LinkedQueue<ClientRequest>> 
				*	Node边上一个Broker节点; 
				*	Queue<ClientRequest>: 表示对该节点的所有请求队列; 
				*/
				for (Node node : unsent.nodes()) {// return unsent.keySet();遍历各节点;
					Iterator<ClientRequest> iterator = unsent.requestIterator(node);//return unsent.get(node).iterator();将对该节点的所有未发请求队列,以迭代器返回;
					while (iterator.hasNext()) {// 将对该Node的所有请求,都发送出去;
						ClientRequest request = iterator.next();
						if (client.ready(node, now)) {
							// 开始发送请求;
							client.send(request, now);{//NetworkClient.send()
								doSend(request, false, now);{//NetworkClient.doSend(ClientRequest clientRequest, boolean isInternalRequest, long now)
									String nodeId = clientRequest.destination();
									if (!isInternalRequest) {
										if (!canSendRequest(nodeId))
											throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
									}
									AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
									try {
										NodeApiVersions versionInfo = apiVersions.get(nodeId);
										short version;
										if (versionInfo == null) {
											version = builder.desiredOrLatestVersion();
											if (discoverBrokerVersions && log.isTraceEnabled())
												log.trace("No version information found when sending message of type {} to node {}.", clientRequest.apiKey(), nodeId, version);
										} else {
											version = versionInfo.usableVersion(clientRequest.apiKey(), builder.desiredVersion());
										}
										
										AbstractRequest request = builder.build(version);{//FetchRequest.build(version)
											if (version < 3) {
												maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
											}
											return new FetchRequest(version, replicaId, maxWait, minBytes, maxBytes, fetchData, isolationLevel);
										}
										
										doSend(clientRequest, isInternalRequest, now, request);{//NetworkClient.doSend(ClientRequest clientRequest, boolean isInternalRequest)
											Send send = request.toSend(nodeId, header);
											InFlightRequest inFlightRequest = new InFlightRequest()
											selector.send(inFlightRequest.send);
										}
									} catch (UnsupportedVersionException e) {
										log.debug("Version mismatch when attempting to send {} to {}", clientRequest.toString(), clientRequest.destination(), e);
										ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.desiredOrLatestVersion()), clientRequest.callback(), clientRequest.destination(), now, now, false, e, null);
										abortedSends.add(clientResponse);
									}
								}
							}
							iterator.remove();//移除刚才的lastRat 元素;
							requestsSent = true;
						}
					}
				}
				return requestsSent;
			}
			
			if (pollCondition == null || pollCondition.shouldBlock()) {
				// if there are no requests in flight, do not block longer than the retry backoff
				if (client.inFlightRequestCount() == 0) timeout = Math.min(timeout, retryBackoffMs);
				client.poll(Math.min(MAX_POLL_TIMEOUT_MS, timeout), now);
				now = time.milliseconds();
			} else {
				client.poll(0, now);
			}
			checkDisconnects(now);//检查是否与Server端开了连接;因为后面还会提交数据;
			if (!disableWakeup) {//根据入参,唤醒client;
				// trigger wakeups after checking for disconnects so that the callbacks will be ready
				// to be fired on the next call to poll()
				maybeTriggerWakeup();
			}
			maybeThrowInterruptException();// throw InterruptException if this thread is interrupted
			trySend(now);//可能 unsent:Queue<ClientRequest>队列中又要要发的请求了, 再进行一次对 unsent中请求的发送;
			failExpiredRequests(now);// fail requests that couldn't be sent if they have expired
			// clean unsent requests collection to keep the map from growing indefinitely
			unsent.clean();//?
		}
		
		// 3. 这期间可能 pendingCompletion队列中又有一些已成功返回的结果, 对这些从Server返回的RequestFutures依次处理,阻塞在此循环处理,直到pendingCompletion为空; 放外面避免死锁;
		firePendingCompletedRequests(){//ConsumerNetworkClient.firePendingCompletedRequests(); 若Queue中还有CompletionRequest,会阻塞在此,直到循环处理完所有CompletionRequest;
			for (;;) {//循环从已completion任务的队列中拿任务; 当 pendingCompletion:LinkedQueue队列中还有待处理任务时,会一直循环处理,否则break结束,进入 client.wakeup();
				RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();//从等待处理的已完成请求队列中,取一个: pendingCompletion:Queue<RequestFutureCompletionHandler>
				if (completionHandler == null) break; // 当pending队列为空时, 即终止for(;;)循环并进入后面的client.wakeup()操作; 表示所有成功返回的请求,都已被处理完了; 
				completionHandler.fireCompletion(){//ConsumerNetworkClient.RequestFutureCompletionHandler.fireCompletion()
					if(e == null && !response.wasDisconnected()) future.complete(response){//RequestFuture.complete(T value) 若请求没有错误和断开连接, 则进入future.complete()处理
						fireSuccess();{//RequestFuture.fireSuccess(): 阻塞在此,一直处理各种成功消息?
							while (true) {//当 异步结果队列listeners中还有未处理完的Future时,会一直阻塞在此循环处理;
								RequestFutureListener<T> listener = listeners.poll();//listeners= LinkedQueue<RequestFutureListener<T>>, 异步请求结果的future队列,
								if (listener == null) break;// 若队列为空,则结束 while()循环,直接返回;
								listener.onSuccess(value);{//RequestFuture.onSuccess(): 当队列中有 未处理的Future结果时,调其onSuccess()处理;
									GroupCoordinatorResponseHandler.onSuccess(ClientResponse resp, RequestFuture<Void> future){}
									CoordinatorResponseHandler.onSuccess(ClientResponse clientResponse, RequestFuture<T> future){
										handle(responseObj, future);{//CoordinatorResponseHandler.handle()是抽象类, 有子类实现
											HeartbeatResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
											JoinGroupResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
											SyncGroupResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
											LeaveGroupResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
											OffsetCommitResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
											OffsetFetchResponseHandler.handle(ClientResponse resp, RequestFuture<Void> future){}
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




/** "kafka-coordinator-heartbeat-thread" 线程: 
* 
*/	

KafkaConsumer.pollOnce();{ConsumerCoordinator.poll();{
	AbstractCoordinator.ensureActiveGroup();{
		AbstractCoordinator.startHeartbeatThreadIfNeeded();{
			if (heartbeatThread == null) {
				heartbeatThread = new HeartbeatThread();{
					super("kafka-coordinator-heartbeat-thread" + (groupId.isEmpty() ? "" : " | " + groupId), true);{//new KafkaThread() extends Thread()
						super(name);//name= "kafka-coordinator-heartbeat-thread | consumerGroup"
						configureThread(name, daemon);//daemon==true;
					}
				}
				
				// 正式启动 "kafka-coordinator-heartbeat-thread" 心跳检测线程;
				heartbeatThread.start();{
					try {
						log.debug("Heartbeat thread for group {} started", groupId);
						while (true) {
							synchronized (AbstractCoordinator.this) {
								if (closed)
									return;
								if (!enabled) {
									AbstractCoordinator.this.wait();
									continue;
								}
								if (state != MemberState.STABLE) {//当还是Unjoined & Rebalancing状态时,置为disable状态,并循环等待;
									// MemberState包括:Unjoined, Rebalancing, Stable3中; 当initiateJoinGroup()成功后,会state置为Stable;
									disable();// 可能是消费者离开消费者组或者coordinator把我们踢了，所以需要disable heartbeats，等待主线程重新加入
									continue;
								}
								client.pollNoWakeup();
								long now = time.milliseconds();
								
								if (coordinatorUnknown()) {//return coordinator() == null 即为未知;
								// 检测GroupCoordinator是否已连接, 如果没有连接，则查找GroupCoordinator,并返回一个请求结果
									if (findCoordinatorFuture != null || lookupCoordinator().failed())
										// the immediate future check ensures that we backoff properly in the case that no
										// brokers are available to connect to.
										AbstractCoordinator.this.wait(retryBackoffMs);
								} else if (heartbeat.sessionTimeoutExpired(now)) {// 检测HeartbeatRespose是否超时
									Heartbeat.sessionTimeoutExpired(now){
										long latestSessionTime = Math.max(lastSessionReset, lastHeartbeatReceive);// 
										return now - latestSessionTime > sessionTimeout;//与最近的回话超过 sessionTimeout(默认10秒),则结束回话;
									}
									// 如果超时，则认为GroupCoordinator宕机，调用coordinatorDead方法清空unsent集合中的
									// 请求，将coordinator 设置为null,表示将重新选举GroupCoordinator
									// the session timeout has expired without seeing a successful heartbeat, so we should
									// probably make sure the coordinator is still healthy.
									coordinatorDead();
								} else if (heartbeat.pollTimeoutExpired(now){return now-lastPoll >maxPollInterval;}) {// 距离上一次poll若超过maxPollInterval(默认300秒/5分钟),client就主动发LeaveGroup到server进行Rebalance;
									maybeLeaveGroup();{//AbstractCoordinator.maybeLeaveGroup(): 
										// 	此时有coordinator(true)			state==STABLE (true)		  gId_13 != gId_-1 (true)
										if (!coordinatorUnknown() && state != MemberState.UNJOINED && generation != Generation.NO_GENERATION) {//这里进入,构建一个LeaveGroupRequest,后面发送;
											log.debug("Sending LeaveGroup request to coordinator {} for group {}", coordinator, groupId);
											LeaveGroupRequest.Builder request =new LeaveGroupRequest.Builder(groupId, generation.memberId);
											client.send(coordinator, request).compose(new LeaveGroupResponseHandler());
											client.pollNoWakeup();//这里触发Network的发送;
										}
										resetGeneration();{//将版本(年代)重置为初始化 No_Generation(-1);
											this.generation = Generation.NO_GENERATION;//其gId == -1;
											this.rejoinNeeded = true; //设置AbstractCoordinator.rejoinNeeded==true;//需该ConsuemrCoordinator重新加入Group;
											this.state = MemberState.UNJOINED; //设置AbstractCoordinator.state == UNJOINED;(正常为Stable);
										}
									}
								} else if (!heartbeat.shouldHeartbeat(now){//Heartbeat.shouldHeartbeat(long now)
									long timeToNext = timeToNextHeartbeat(now);{//Heartbeat.timeToNextHeartbeat(long now)
										//计算最近一次向Server端发消息时刻,离现在有多久;取fetch会话或发心跳的最近一次时间;
										long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);
										final long delayToNextHeartbeat;
										if (heartbeatFailed)
											delayToNextHeartbeat = retryBackoffMs;
										else //上次心跳正常,取 心跳间隔heartbeatInterval(heartbeat.interval.ms参数,默认3000ms), 作为判断满足发送的条件: 
											delayToNextHeartbeat = heartbeatInterval;
										if (timeSinceLastHeartbeat > delayToNextHeartbeat)
											return 0;
										else
											return delayToNextHeartbeat - timeSinceLastHeartbeat;
									}
									
									return timeToNext == 0;
								}) {// 没有到心跳请求的发送时间，等待
									AbstractCoordinator.this.wait(retryBackoffMs);//休眠retryBackoffMs时长, 默认100L毫秒;
								} else {
									heartbeat.sentHeartbeat(now); // 更新lastHeartbeatSend的时间,并且初始化heartbeatFailed

									// 构造HeartbeatRequest对象，通过ConsumerClientNetwork添加到unsent队列，
									// 等待发送，结果HeartbeatResponseHandler处理后返回一个RequestFuture
									// 添加RequestFutureListener监听器，如果成功更新lastHeartbeatReceive时间
									// 如果失败，则需要看情况：
									// # 如果是正处于rebalance过程还是更新lastHeartbeatReceive时间
									// # 标记heartbeat请求失败
									sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
										@Override
										public void onSuccess(Void value) {
											synchronized (AbstractCoordinator.this) {
												heartbeat.receiveHeartbeat(time.milliseconds());
											}
										}
										@Override
										public void onFailure(RuntimeException e) {
											synchronized (AbstractCoordinator.this) {
												if (e instanceof RebalanceInProgressException) {
													heartbeat.receiveHeartbeat(time.milliseconds());
												} else {
													heartbeat.failHeartbeat();
													// wake up the thread if it's sleeping to reschedule the heartbeat
													AbstractCoordinator.this.notify();
												}
											}
										}
									});
								}
							}
						}
					} catch (InterruptedException | InterruptException e) {
						Thread.interrupted();
						log.error("Unexpected interrupt received in heartbeat thread for group {}", groupId, e);
						this.failed.set(new RuntimeException(e));
					} catch (RuntimeException e) {
						log.error("Heartbeat thread for group {} failed due to unexpected error", groupId, e);
						this.failed.set(e);
					} finally {
						log.debug("Heartbeat thread for group {} has closed", groupId);
					}
				}
			}
		}
	}
}}



// Kafak基本Api的源码
KafkaConsumer.api.{
	
	assign(Collection<TopicPartition> partitions){//手动绑定到某(些)TPs
        acquire();//区别notClosed(),并确保没有多线程访问;
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {//传进来的绑定分区为0,则需要assigment,coordinator,元数据都置为0;
                this.unsubscribe();{//KafakConsumer.unsubscribe()
					acquire();
					try {
						log.debug("Unsubscribed all topics or patterns and assigned partitions");
						this.subscriptions.unsubscribe();{//SubscriptionState.unsubscribe()
							this.subscription = Collections.emptySet();
							this.assignment.clear();
							this.subscribedPattern = null;
							this.subscriptionType = SubscriptionType.NONE;//重要一步,将KafkaConsumer.SubscriptionState.subscriptionType 的值置为 None; 后面poll()时据此抛异常;
							fireOnAssignment(Collections.<TopicPartition>emptySet());
						}
						this.coordinator.maybeLeaveGroup();
						// 这里进去不用发 UnJoin的Request,因为第一个调节coordinator就没有; ?
						maybeLeaveGroup();{//AbstractCoordinator.maybeLeaveGroup(): 
							// 	此时有coordinator(true)			state==STABLE (true)		  gId_13 != gId_-1 (true)
							if (!coordinatorUnknown() && state != MemberState.UNJOINED && generation != Generation.NO_GENERATION) {//此处没有进入,
								client.send(coordinator, request).compose(new LeaveGroupResponseHandler());
								client.pollNoWakeup();//这里触发Network的发送;
							}
							resetGeneration();{//将版本(年代)重置为初始化 No_Generation(-1);
								this.generation = Generation.NO_GENERATION;//其gId == -1;
								this.rejoinNeeded = true; //设置AbstractCoordinator.rejoinNeeded==true;//需该ConsuemrCoordinator重新加入Group;
								this.state = MemberState.UNJOINED; //设置AbstractCoordinator.state == UNJOINED;(正常为Stable);
							}
						}
						
						this.metadata.needMetadataForAllTopics(false);
					} finally {
						release();
					}
				}
				
            } else {//正常绑定,进入这里;
				//从tps中解析出Topic,并存于此Set<String> topics 中;
                Set<String> topics = new HashSet<>();
                for (TopicPartition tp : partitions) {// 确保每个topic不为空;
                    String topic = (tp != null) ? tp.topic() : null;
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                    topics.add(topic);
                }
				
                this.coordinator.maybeAutoCommitOffsetsNow();{//若开启了自动的话, 这里要先 异步提交下offsets,以免重复读数据;
					if (autoCommitEnabled && !coordinatorUnknown())
						doAutoCommitOffsetsAsync();
				}
                log.debug("Subscribed to partition(s): {}", Utils.join(partitions, ", "));
                this.subscriptions.assignFromUser(new HashSet<>(partitions));{
					setSubscriptionType(SubscriptionType.USER_ASSIGNED);{//重要一步,将将subscriptionType设置目标UserAssigned,且检验不"mutually exclusive";
						if (this.subscriptionType == SubscriptionType.NONE){//初始化时为None,进入这里;
							this.subscriptionType = type;
						}
						else if (this.subscriptionType != type){//若初始化不为None,且SubscriptionState里原来的 subscriptionType 与这次传入的type不一样; 那说明同一Consumer里有多种订阅模式,这是不允许的; 抛IllegalStateException异常
							throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
						}
					}
					//若SubscriptionState.PartitionStates.map[Map<TopicPartition,TPState]里map的keys 与这次要绑定的partitions:Set<TopicPartition> 有任何不一样的,进入这里重置;
					if (!this.assignment.partitionSet().equals(partitions)) {//若内存中的assignment.map中tps与传入的不一样,就进行重试;
						fireOnAssignment(partitions);{//重置listeners(默认只有Fetcher这个Listener)中所有的tps缓存;
							for (Listener listener : listeners) listener.onAssignment(assignment);
						}
						
						Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
						for (TopicPartition partition : partitions) {
							TopicPartitionState state = assignment.stateValue(partition);{//读取该tp在map中的TPState值;若有的话;
								return map.get(topicPartition);
							}
							if (state == null)//新建时一般为Null,进入这里new 一个TPState对象,里面offset,position,hw等都为null;
								/* TopicPartitionState 对象用于保存本consumer对各个TopicPartiton消费情况的实时状态
									- position:			当前消费位置; 记录当前要消费的offset
									- highWatermark: 	the high watermark from last fetch: 上一次Fetch时的高水位? 在Fetcher.parseCompletedFetch()中更新;
									- lastStableOffset:	?
									- committed: 		last committed position 记录已经commit过的offset:OffsetAndMetadata
									- paused:			该分区当前是否暂停消费?
									- resetStrategy:	当重置position时,采取的end/begin/latest等策略?
								*/
								state = new TopicPartitionState();
							partitionToState.put(partition, state);//
						}
						this.assignment.set(partitionToState);
						this.needsFetchCommittedOffsets = true;//do we need to request the latest committed offsets from the coordinator? 
					}
				}
                metadata.setTopics(topics);
            }
        } finally {
            release();{//相关线程计数 -1, 且currentThread置为 -1 (No_Current_Thread);
				if (refcount.decrementAndGet() == 0) currentThread.set(NO_CURRENT_THREAD);
			}
        }
	}
}



每个Consumer对应offset存储的位置:
* 用 abc(GroupId.hashCode()) % numPartitions(50),即为该ConsumerGroup对应存储offset的地方;

什么是Consumer的 Coordinator Leader? broker
* 第一个加入该group的consumer就是该group的leader;
* 还会将当期组中所有的members信息返回给leader用于后面让leader来分配各个member要消费的partition
* 在JoinGroup请求中: broker会将该ConsumerGroup的所有members信息返回给consumer-leader, 
* Consumer(Group)-leader会负责分配各member要消费的partition;


KafkaConsumer的性能优化方法:
* 提高Partition 数量: 为什么?
	- Kafaka是按分区实现并发消费的,越多的分区,越多高的并发? 如何实现;
* kafka在消息为10K时吞吐量达到最大



K1. 初始化new KafkaConsumer()






K2. 从Server消费一次消息: KafkaConsumer.poll()






协调者Coordinator原理

在执行一次KafkaConsumer.poll()中 其pollOnce()-> coordinator.poll()
2.1 发送GCR请求寻找Coordinator： 	coordinator.poll() -> ensureCoordinatorReady() -> lookupCoordinator() -> sendGroupCoordinatorRequest(node): GroupCoordinatorRequest
	* 向集群中负载最小的broker发起请求,成功返回后，那么该Broker将作为 Coordinator，尝试连接该Coordinator

2.2 发送 JoinGroupRequest 请求加入该组：	coordinator.poll() -> ensureActiveGroup() -> joinGroupIfNeeded() -> AbstractCoordinator.sendJoinGroupRequest();
	* 发起加入group的请求，集群Coordinator会接收到该请求，会给集群分配一个Leader（通常是第一个），让其负责partition的分配; 表示该consumer是该Group的成员，

	
2.3 发送 SyncGroupRequest 请求： coordinator.poll() -> AbstractCoordinator.joinGroupIfNeeded() -> client.poll()-> fireCompletion()-> JoinGroupResponseHandler.handle() -> sendSyncGroupRequest(requestBuilder);
	* JOIN_GROUP只是加入并没有分配partiton, SYNC_GROUP请求用于获取该consumer要消费哪些partition。
	* 如果发现当前Consumer是leader，那么会进行partition的分配，并发起SGR请求将分配结果发送给Coordinator;如果不是leader，那么也会发起SGR请求，不过分配结果为空
	* 如果是leader，直接获取所有members然后使用PartitionAssignor的实现类(默认RangeAssignor)来为各Consumer分配要消费哪些partition; 配置partition.assignment.strategy指定分配算法;
	* 最后leader把分配好的信息{SyncGroupRequest}发送给broker, broker根据leader发来的 分配信息,在内存给每个member分配对应partiton，然后将这些信息返回给对应的consumer。
		- broker再将 reblance后的结果(对象?) 发给每个Consumer,
		- 各Consuemr结束其阻塞状态,开始从新的partition消费数据;



正常的Group.state变化路径:
- Empty: 			
- PreparingRebalance: 	JoinGroupRequest -> GroupCoordinator.doJoinGroup().addmemberAndReblance().maybePrepareRebalance -> prepareRebalance() 								=>[GroupMetadata.state=PreparingRebalance]
		* 心跳超时,若已分配则重新:	DelayHeartbeat.run().onExpiration()->GroupCoordinator.onExpireHeartbeat().onMemberFailure(){case Stable|AwaitingSync}.maybePrepareRebalance().prepareRebalance()	=>[GroupMetadata.state=PreparingRebalance]
- AwaitingSync: 		
		* 正常: InitialDelayedJoin.run() -> onComplete() -> DelayedJoin.onComplete() -> GroupCoordinator.onCompleteJoin() 													=>[GroupMetadata.state=AwaitingSync]
		* 心跳超时,若未分配则 DelayHeartbeat.run().onExpiration()->GroupCoordinator.onExpireHeartbeat().onMemberFailure(){case PreparingRebalance}->DelayedOperationPuragtory.checkAndComplete().tryCompleteWatched()->DelayedJoin.tryComplete().onComplete()-> GroupCoordinator.onCompleteJoin() 	=>[GroupMetadata.state=AwaitingSync]
		
- Stable: 	SyncGroupRequest->GroupCoordinator.doSyncGroup().GroupMetadataManager.storeGroup().appendForGroup()->ReplicaManager.appendRecords()->DelayedOperationPuragtory.tryCompleteElseWatch()->DelayedProduce.tryComplete().onComplete()->
				-> responseCallback().GroupMetadataManager.putCacheCallback().responseCallback(responseError)->doSyncGroup().anonfun$-> GroupCoordinator.setAndPropagateAssignment()		=>[GroupMetadata.state=Stable]
- Dead:		CoreUtils.runnable().run()->GroupMetadataManager.cleanupGroupMetadata() 					=>[GroupMetadata.state=Dead]


总结触发Reblance的方法: maybePrepareRebalance()
* doSyncGroup
* addMemberAndRebalance() 	增加新Member时;
* doJoinGroup()() -> 三种情况:updateMemberAndRebalance()	更新成员时? 啥时?
* onMemberFailure()		
	-> handleLeaveGroup()
	-> DelayedHeartbeat.onExpireHeartbeat() 

Server端,频繁触发: DelayedHeartbeat.run()->onExpiration();

对于Saver端的 Heartbeat的逻辑

在于CroupCoordinator.completeAndScheduleNextHeartbeatExpiration(){
	
	// 定义一个 检查心跳的 Heartbeat线程,
	val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
	
	heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
	
}
DelayedHeartbeat.onExpiration() 处理过期: 超时是的情况:

DelayedHeartbeat.onExpiration(){
	coordinator.onExpireHeartbeat(group, member, heartbeatDeadline){
		if (!shouldKeepMemberAlive(member, heartbeatDeadline){
			// 
			member.awaitingJoinCallback != null ||member.awaitingSyncCallback != null || member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline
		}){
			onMemberFailure(group, member){ 
				group.remove(member.memberId)
				group.currentState match {
				  case Dead | Empty =>
				  case Stable | AwaitingSync => maybePrepareRebalance(group){
					if (group.canRebalance){
						prepareRebalance(group){//GroupCoordinator.prepareRebalance()
							if (group.is(AwaitingSync)){
								resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)
							}
							
							val delayedRebalance = if (group.is(Empty)){
								new InitialDelayedJoin(this,joinPurgatory,group,groupConfig.groupInitialRebalanceDelayMs,groupConfig.groupInitialRebalanceDelayMs,
									max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
							}else{
								new DelayedJoin(this, group, group.rebalanceTimeoutMs)
							}
							
							group.transitionTo(PreparingRebalance)
							val groupKey = GroupKey(group.groupId)
							joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
							
						}
					}
				  }
				  
				  case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
				}
	
			}
		}
	}
}

//执行一个定时任务: 1. 先强制完成执行, 若完成则再expiration过期操作;
DelayedOperation.run(
	if (forceComplete(){
		if (completed.compareAndSet(false, true)) {
		  cancel()
		  onComplete()
		  true
		} else {
		  false
		}
	}){
		// 只有前面onComplete()方法执行完,才会执行onExpiration();
		onExpiration()
	}
      
)

昨天主要2个:
* 单个Stage的性能测试
* KafkaSource单独跑时,非常容易出现 阻塞在reblance这个环境, 频繁的发出 JoinGroup, 而这个加入过程触发了Rebalcne又进一步触发重新加入 Group的; 
	- 涉及KafkaConsuemr的原理, 还需要进一步看下. 




