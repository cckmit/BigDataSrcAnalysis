Server端重要线程的源码


/** 1. 启动线程"main" : 
*  KafkaServerStartable.startup() -> KafkaServer.startup():在其中创建并启动3大核心服务, 12大基本组件;
* 		启动相关处理线程: 
*		"kafka-request-handler-"线程: new KafakRequestHandlerPool() -> for(i <-0 until numThreads)循环创建n个请求处理线程:Utils.daemonThread("kafka-request-handler-" + i, runnables(i)).start();
*/
kafka.Kafka.main(){
	try {
		val serverProps = getPropsFromArgs(args)
		val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)
		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook") {
			override def run(): Unit = kafkaServerStartable.shutdown()
		})
		
		// 正式启动Kafak服务
		kafkaServerStartable.startup(){//KafkaServerStartable.startup()
			try {
				/*
					核心:  3个核心服务;
						LogManager
						SocketServer
						KafkaRequestHandlerPool(new KafakApi())
					
					相关组件: 12个基本组件;
						kafkaScheduler: 
						zkUtils:
						Metrics
						MetadataCache
						ReplicaManager
						KafkaController
						AdminManager
						GroupCoordinator
						TransactionCoordinator
						DynamicConfigManager
						KafkaHealthcheck
				*/
				server.startup(){//KafkaServer.startup(): 启动Kafak的 ?大Server
					try{
						info("starting")
						val canStartup = isStartingUp.compareAndSet(false, true)
						if (canStartup) {
							kafkaScheduler.startup()
							zkUtils = initZk()
							_clusterId = getOrGenerateClusterId(zkUtils)
							info(s"Cluster ID = $clusterId")
							
							// 启动数据服务,即logManager
							logManager = LogManager(config, zkUtils, brokerState, kafkaScheduler, time, brokerTopicStats)
							logManager.startup()
							
							socketServer = new SocketServer(config, metrics, time, credentialProvider)
							socketServer.startup()
							replicaManager = createReplicaManager(isShuttingDown)
							replicaManager.startup()
							kafkaController = new KafkaController(config, zkUtils, time, metrics, threadNamePrefix)
							kafkaController.startup()
							adminManager = new AdminManager(config, metrics, metadataCache, zkUtils)
							
							// 启动ConsumerGroup的协调者服务
							groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM)
							groupCoordinator.startup()
			
					
							// 创建KafakApi,并将之传入 KafkaRequestHandlerPool中,用于转进每个 KafkaRequestHandler的构造函数中;
							apis = new KafkaApis(socketServer.requestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,kafkaController, clusterId, time)
							requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, time, config.numIoThreads);{
								val runnables = new Array[KafkaRequestHandler](numThreads)//该数组用于 响应client各种Request Handler处理器,即Socker处理器; 
								// 每个KafkaRequestHandler应该是一样的功能, 只是多个numThreads能提供Server端的并发处理能力;
								for(i <- 0 until numThreads) {
									runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis, time)
									Utils.daemonThread("kafka-request-handler-" + i, runnables(i)).start();{
										def run(){while (true){ApiKeys.forId(request.requestId) match {
											case ApiKeys.PRODUCE => handleProduceRequest(request)
											// todo...
										}}}
									}
								}
							}
							
							kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, listeners, zkUtils, config.rack, config.interBrokerProtocolVersion)
							kafkaHealthcheck.startup()
							
							checkpointBrokerId(config.brokerId)
							AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString)
							info("started")
			
						}
					
					}catch {
					  case e: Throwable =>fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
						isStartingUp.set(false)
						shutdown()
						throw e
					}
				}
			}catch {
			  case _: Throwable =>fatal("Exiting Kafka.")
				Exit.exit(1)
			}
		}
		kafkaServerStartable.awaitShutdown()
    }
    catch {
      case e: Throwable => fatal(e)
        Exit.exit(1)
    }
    Exit.exit(0) // 正常退出,返回0; 
}


/** 2. "kafka-request-handler-n"线程: 响应client端各种Socker的请求;
*		触发位置: Kafak.main()->KafkaServer.startup()->new KafakRequestHandlerPool()->Utils.daemonThread("kafka-request-handler-"+i,runnables(i)).start()
*		基本运行逻辑:
*		下游线程: 
*/
new KafkaRequestHandlerPool(){
	private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)
	val runnables = new Array[KafkaRequestHandler](numThreads)
	for(i <- 0 until numThreads) {
		runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis, time)
		Utils.daemonThread("kafka-request-handler-" + i, runnables(i)).start(){//KafkaRequestHandler.start()
			def run(){// KafkaRequestHandler.run() 统一处理Api的请求步骤
				while (true) {
					try {
						var req : RequestChannel.Request = null
						while (req == null) {//循环阻塞300ms, 直到接受到一个请求,进入 apis.handle()处理
						  val startSelectTime = time.nanoseconds
						  req = requestChannel.receiveRequest(300)
						  val endTime = time.nanoseconds
						  if (req != null)
							req.requestDequeueTimeNanos = endTime
						  val idleTime = endTime - startSelectTime
						  aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
						}

						if (req eq RequestChannel.AllDone) {
						  latch.countDown()
						  return
						}
						trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
						
						apis.handle(req){//KafkaApis.handle(req:RequestChannel.Request)
							try {
								ApiKeys.forId(request.requestId) match {
									/** API.1	生产者存储数据的Api: ProducerRequest
									* KafkaApis.handleProduceRequest()-> ReplicaManager.appendRecords()->  Partition.appendRecordsToLeader()
									* 	-> Log.appendAsLeader() -> Log.append() -> LogSegment.append() -> FileRecords.append(MemoryRecords records)
									*/
									case ApiKeys.PRODUCE => handleProduceRequest(request){//KafkaApis.handleProduceRequest()	处理生产者发来的: 存储消息日志
										val produceRequest = request.body[ProduceRequest]
										val numBytesAppended = request.header.toStruct.sizeOf + request.bodyAndSize.size
										
										val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) =
										  produceRequest.partitionRecordsOrFail.asScala.partition { case (tp, _) =>
											authorize(request.session, Describe, new Resource(Topic, tp.topic)) && metadataCache.contains(tp.topic)
										  }
										
										if (authorizedRequestInfo.isEmpty){
											sendResponseCallback(Map.empty)
										}else {// 进入这里
											val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId
											  // call the replica manager to append messages to the replicas	将消息追加到该partition的主节点(leader replicas)中,并让其同步其他replicas
											replicaManager.appendRecords(timeout = produceRequest.timeout.toLong, requiredAcks = produceRequest.acks,internalTopicsAllowed = internalTopicsAllowed,
													isFromClient = true,entriesPerPartition = authorizedRequestInfo, responseCallback = sendResponseCallback){//ReplicaManager.appendRecords()
												if (isValidRequiredAcks(requiredAcks)) {
												  val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,isFromClient = isFromClient, entriesPerPartition, requiredAcks){
														entriesPerPartition.map { case (topicPartition, records) =>
														  brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
														  brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

														  // reject appending to internal topics if it is not allowed
														  if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
															(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo,Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
														  } else {// 正常,进入这里
															try {
															  val partitionOpt = getPartition(topicPartition)
															  val info = partitionOpt match {
																case Some(partition) =>//正常,进入这里,调用 Partition来完成具体的写入
																	partition.appendRecordsToLeader(records, isFromClient, requiredAcks){//Partition.appendRecordsToLeader()
																		val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
																			leaderReplicaIfLocal match {
																				case Some(leaderReplica) =>
																					val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient){
																						Log.append(records, isFromClient, assignOffsets = true, leaderEpoch){//Log.append()
																							
																							val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)
																							try {
																							  // they are valid, insert them in the log
																							  lock synchronized {
																								if(assignOffsets) {
																								  val validateAndOffsetAssignResult = try {
																									LogValidator.validateMessagesAndAssignOffsets(validRecords,offset,now)
																								  } catch {
																									case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
																								  }
																								}else{
																									if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
																										throw new IllegalArgumentException("Out of order offsets found in " + records.records.asScala.map(_.offset))
																								}

																								// update the epoch cache with the epoch stamped onto the message by the leader
																								validRecords.batches.asScala.foreach { batch =>
																								  if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
																									leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
																								}
																								// maybe roll the log if this segment is full
																								val segment = maybeRoll(messagesSize = validRecords.sizeInBytes, maxTimestampInMessages = appendInfo.maxTimestamp, maxOffsetInMessages = appendInfo.lastOffset)
																								val logOffsetMetadata = LogOffsetMetadata()

																								// 核心代码?
																								segment.append(firstOffset = appendInfo.firstOffset,largestOffset = appendInfo.lastOffset, largestTimestamp = appendInfo.maxTimestamp,
																								  shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,records = validRecords){//LogSegment.append()
																									LogSegment.append(){
																										if (records.sizeInBytes > 0) {
																											val appendedBytes = log.append(records){//FileRecords.append(MemoryRecords records)
																												int written = records.writeFullyTo(channel){
																													buffer.mark();
																													int written = 0;
																													while (written < sizeInBytes())
																														written += channel.write(buffer){
																															
																														}
																													buffer.reset();
																													return written;
																												}
																												size.getAndAdd(written);
																												return written;
																											}
																											
																											if(bytesSinceLastIndexEntry > indexIntervalBytes) {
																												index.append(firstOffset, physicalPosition)
																												timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
																												bytesSinceLastIndexEntry = 0
																											}
																											bytesSinceLastIndexEntry += records.sizeInBytes
																										}
																									}
																								  }

																								// update the producer state
																								for ((producerId, producerAppendInfo) <- updatedProducers) {
																								  producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
																								  producerStateManager.update(producerAppendInfo)
																								}
																								for (completedTxn <- completedTxns) {
																								  val lastStableOffset = producerStateManager.completeTxn(completedTxn)
																								  segment.updateTxnIndex(completedTxn, lastStableOffset)
																								}
																								
																								producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)
																								updateLogEndOffset(appendInfo.lastOffset + 1)
																								updateFirstUnstableOffset()
																								appendInfo
																							  }
																							} catch {
																							  case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
																							}
																						}
																					}
																					
																					replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
																					(info, maybeIncrementLeaderHW(leaderReplica))
																				
																				case None => throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d" .format(topicPartition, localBrokerId))
																			}
																			
																			if (leaderHWIncremented) tryCompleteDelayedRequests(){//Partition.tryCompleteDelayedRequests()
																				val requestKey = new TopicPartitionOperationKey(topicPartition)
																				replicaManager.tryCompleteDelayedFetch(requestKey)
																				replicaManager.tryCompleteDelayedProduce(requestKey)
																				replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
																			}
																			return info
																		}
																	}

																case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
																  .format(topicPartition, localBrokerId))
															  }

															  val numAppendedMessages =
																if (info.firstOffset == -1L || info.lastOffset == -1L)
																  0
																else
																  info.lastOffset - info.firstOffset + 1

															  // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
															  brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
															  brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
															  brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
															  brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

															  trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
																.format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
															  (topicPartition, LogAppendResult(info))
															} catch {
															  // NOTE: Failed produce requests metric is not incremented for known exceptions
															  // it is supposed to indicate un-expected failures of a broker in handling a produce request
															  case e: KafkaStorageException =>
																fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
																Exit.halt(1)
																(topicPartition, null)
															  case e@ (_: UnknownTopicOrPartitionException |
																	   _: NotLeaderForPartitionException |
																	   _: RecordTooLargeException |
																	   _: RecordBatchTooLargeException |
																	   _: CorruptRecordException |
																	   _: InvalidTimestampException) =>
																(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
															  case t: Throwable =>
																brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
																brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
																error("Error processing append operation on partition %s".format(topicPartition), t)
																(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
															}
														  }
    }
														
												  }

												  if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
													// create delayed produce operation
													val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)
													// ...
												  } else {
													// we can respond immediately
													val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
													responseCallback(produceResponseStatus)
												  }
												} else {
													val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
														topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,LogAppendInfo.UnknownLogAppendInfo.firstOffset, RecordBatch.NO_TIMESTAMP)
													}
													responseCallback(responseStatus){// KafkaApis.handleProduceRequest().sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse])
														val mergedResponseStatus = responseStatus ++ 
															unauthorizedForWriteRequestInfo.mapValues(_ => new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)) ++
															nonExistingOrUnauthorizedForDescribeTopics.mapValues(_ => new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
													  
														// When this callback is triggered, the remote API call has completed
														request.apiRemoteCompleteTimeNanos = time.nanoseconds

														quotas.produce.recordAndMaybeThrottle(request.session.sanitizedUser,request.header.clientId, numBytesAppended,produceResponseCallback){//clientQuotaManager.recordAndMaybeThrottle()
															val clientSensors = getOrCreateQuotaSensors(sanitizedUser, clientId);
															
															recordAndThrottleOnQuotaViolation(clientSensors, value, callback){
																clientSensors.quotaSensor.record(value)
																callback(0)
																return throttleTimeMs;
															}
														}
													}
												}
											}
											// hence we clear its data here inorder to let GC re-claim its memory since it is already appended to log
											produceRequest.clearPartitionRecords()
										}
										
									}
									
									/** API.2 消费者获取数据	FetchRequest
									*
									*/
									case ApiKeys.FETCH => handleFetchRequest(request){//KafkaApis.handleFetchRequest()
										val fetchRequest = request.body[FetchRequest]
										//..
										if (authorizedRequestInfo.isEmpty){
											processResponseCallback(Seq.empty)
										}else {
											// call the replica manager to fetch messages from the local replica
											replicaManager.fetchMessages(fetchRequest.maxWait.toLong, fetchRequest.replicaId, fetchRequest.minBytes,fetchRequest.maxBytes, versionId <= 2, authorizedRequestInfo,
													replicationQuota(fetchRequest), processResponseCallback, fetchRequest.isolationLevel){// ReplicaManager.fetchMessages()
												
												val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)
												val logReadResults = readFromLocalLog(){
													val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
													var minOneMessage = !hardMaxBytesLimit
													readPartitionInfo.foreach { case (tp, fetchInfo) =>
															val readResult = read(tp, fetchInfo, limitBytes, minOneMessage){//ReplicaManager.readFromLocalLog().read()
																brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
																brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
																
																val localReplica = if (fetchOnlyFromLeader){
																	getLeaderReplicaIfLocal(tp)
																}else{
																	getReplicaOrException(tp)
																}
																
																val logReadInfo = localReplica.log match {
																	case Some(log) =>
																		// 在max.partiton.fetch.bytes(1M) 和 fetch.max.bytes(50M) 之间取最小值;
																		val adjustedFetchSize = math.min(partitionFetchSize, limitBytes); 
																		
																		val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel){// Log.read()
																			
																			
																			
																		}
																		
																		if (shouldLeaderThrottle(quota, tp, replicaId)){
																			return FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
																		}else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete){
																			return FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
																		}else{return fetch}
																	
																	case None => 
																		return FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
																}
																
															}
															val messageSetSize = readResult.info.records.sizeInBytes
															if (messageSetSize > 0) minOneMessage = false
															limitBytes = math.max(0, limitBytes - messageSetSize)
															result += (tp -> readResult)
													}
													result
												}
											}
											
										}
									}
									
									case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
									case ApiKeys.METADATA => handleTopicMetadataRequest(request)
									case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
									case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
									case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
									case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
									case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
									case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
									case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
									case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
									case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
									case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
									case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
									case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
									case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
									case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
									case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
									case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request){//		创建Topic
										val createTopicsRequest = request.body[CreateTopicsRequest]
										// createTopicsRequest.topics== Map<Topic, TopicDetails> {case TopicDetail(int numPartitions,short replicationFactor,Map<Integer, List<Integer>> replicasAssignments,Map<String, String> configs}
										
										if (!controller.isActive) {
										  val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
											(topic, new ApiError(Errors.NOT_CONTROLLER, null))
										  }
										  sendResponseCallback(results)
										} else if (!authorize(request.session, Create, Resource.ClusterResource)) {
										  val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
											(topic, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null))
										  }
										  sendResponseCallback(results)
										} else { // 正常的话,直接进入这里
										  val (validTopics, duplicateTopics) = createTopicsRequest.topics.asScala.partition { case (topic, _) =>
											!createTopicsRequest.duplicateTopics.contains(topic)
										  }
										  
											adminManager.createTopics(createTopicsRequest.timeout,createTopicsRequest.validateOnly, validTopics,sendResponseWithDuplicatesCallback ){//AdminManager.createTopics()
												// 1. map over topics creating assignment and calling zookeeper
												val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
													// 向/$KAFKA/config/topics/{topic} 和/$KAFKA/brokers/topics/{topic} 两个ZK路径写入partition信息;
												val metadata = createInfo.map { case (topic, arguments) =>
												  try {
													LogConfig.validate(configs)
													val assignments = {
													  if ((arguments.numPartitions != NO_NUM_PARTITIONS || arguments.replicationFactor != NO_REPLICATION_FACTOR)&& !arguments.replicasAssignments.isEmpty)
														throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. Both cannot be used at the same time.")
													  else if (!arguments.replicasAssignments.isEmpty) {
														arguments.replicasAssignments.asScala.map { case (partitionId, replicas) =>(partitionId.intValue, replicas.asScala.map(_.intValue))}
													  } else{//正常 进入这里
															AdminUtils.assignReplicasToBrokers(brokers, arguments.numPartitions, arguments.replicationFactor){
																if (brokerMetadatas.forall(_.rack.isEmpty))
																  assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex, startPartitionId){
																	  
																	val ret = mutable.Map[Int, Seq[Int]]()
																	val brokerArray = brokerList.toArray
																	val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
																	var currentPartitionId = math.max(0, startPartitionId)
																	var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
																	for (_ <- 0 until nPartitions) {
																	  if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
																		nextReplicaShift += 1
																	  val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
																	  val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
																	  for (j <- 0 until replicationFactor - 1)
																		replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
																	  ret.put(currentPartitionId, replicaBuffer)
																	  currentPartitionId += 1
																	}
																	return ret;// ret=Map<Integer[Partition?],ArrayBuffer[Broker?]>
																	  
																  }
																else {
																  if (brokerMetadatas.exists(_.rack.isEmpty)){
																	  throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment")
																  }
																  assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,startPartitionId)
																}
															}
													  }
														
													}
													trace(s"Assignments for topic $topic are $assignments ")

													createTopicPolicy match {
													  case Some(policy) =>
														AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
													  case None =>
														if (validateOnly)
														  AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
														else
														  AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false){
															validateCreateOrUpdateTopic(zkUtils, topic, partitionReplicaAssignment, config, update)
															if (!update) {
															  writeEntityConfig(zkUtils, getEntityConfigPath(ConfigType.Topic, topic), config){
																//entityPath = /config/topics/testCreate01: /config/topics/{topic}
																zkUtils.updatePersistentPath(entityPath, Json.encode(map)){
																	val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
																	// 向/config/topics/{topic} 路径,写入 data={version:1,config:{}}
																	zkClient.writeData(path, data)
																}
															  }
															}
															
															writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update){
																val zkPath = getTopicPath(topic) // 获取的是topic正式位置 /brokers/topics/{topic}路径
																
																// 获取该Topic的分区和分区所在节点信息: partitons== Map<String(分区),Seq[Int](主副节点?)]
																val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => e._1.toString -> e._2))

																if (!update) {
																	info("Topic creation " + jsonPartitionData.toString)
																	zkUtils.createPersistentPath(zkPath, jsonPartitionData)
																} else {
																	info("Topic update " + jsonPartitionData.toString)
																		* [2020-03-14 00:52:11,159] INFO Topic creation {"version":1,"partitions":{"1":[0],"0":[0]}} (kafka.admin.AdminUtils$)
																	// 向 /$KAFKA/brokers/topics/{topic}路径文件,写入字符串: {"version":1,"partitions":{"1":[0],"0":[0]}}
																	zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
																}
																
															}
															
														  }
													}
													
													return CreateTopicMetadata(topic, assignments, ApiError.NONE)
												  } catch {
													// Log client errors at a lower level than unexpected exceptions
													case e@ (_: PolicyViolationException | _: ApiException) =>
													  info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
													  CreateTopicMetadata(topic, Map(), ApiError.fromThrowable(e))
													case e: Throwable =>
													  error(s"Error processing create topic request for topic $topic with arguments $arguments", e)
													  CreateTopicMetadata(topic, Map(), ApiError.fromThrowable(e))
												  }
												}

												// 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
												if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
												  val results = metadata.map { createTopicMetadata =>
													if (createTopicMetadata.error.isSuccess() && !validateOnly) {
													  (createTopicMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
													} else {
													  (createTopicMetadata.topic, createTopicMetadata.error)
													}
												  }.toMap
												  responseCallback(results)
												} else {// 正常,进入这里
													// 3. else pass the assignments and errors to the delayed operation and set the keys
													val delayedCreate = new DelayedCreateTopics(timeout, metadata.toSeq, this, responseCallback)
													val delayedCreateKeys = createInfo.keys.map(new TopicKey(_)).toSeq
													// try to complete the request immediately, otherwise put it into the purgatory
													topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys){//DelayedOperation.tryCompleteElseWatch()
														var isCompletedByMe = operation.safeTryComplete(){
															synchronized{
																tryComplete(){//抽象方法, 由DelayedCreateTopics.tryComplete()实现
																	val leaderlessPartitionCount = createMetadata.filter(_.error.isSuccess)
																	  .foldLeft(0) { case (topicCounter, metadata) =>
																		topicCounter + missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet)
																	  }
																	  
																	if (leaderlessPartitionCount == 0) {
																	  trace("All partitions have a leader, completing the delayed operation")
																	  forceComplete()
																	} else {
																	  trace(s"$leaderlessPartitionCount partitions do not have a leader, not completing the delayed operation")
																	  false
																	}
																	
																	
																}
															}
														}
														if (isCompletedByMe){return true;}
														
														var watchCreated = false
														for(key <- watchKeys) {
														  // If the operation is already completed, stop adding it to the rest of the watcher list.
														  if (operation.isCompleted)
															return false
														  watchForOperation(key, operation)

														  if (!watchCreated) {
															watchCreated = true
															estimatedTotalOperations.incrementAndGet()
														  }
														}

														isCompletedByMe = operation.safeTryComplete()
														if (isCompletedByMe) return true

														// if it cannot be completed by now and hence is watched, add to the expire queue also
														if (!operation.isCompleted) {
														  if (timerEnabled)
															timeoutTimer.add(operation){//kafka.Time.SystemTime.add()
																readLock.lock()
																try {
																	addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs)){
																		if (!timingWheel.add(timerTaskEntry)) {
																			if (!timerTaskEntry.cancelled){taskExecutor.submit(timerTaskEntry.timerTask){
																				
																				DelayedCreateTopics.run(){
																					val isCompleted= forceComplete(){
																						if (completed.compareAndSet(false, true)) {
																							cancel()
																							onComplete(){// 抽象类,由继承类实现
																								//1. 当 timeTask==DelayedFetch时, 
																								DelayedFetch.onComplete(){
																									val logReadResults = replicaManager.readFromLocalLog()
																									//..
																								}
																								
																								// 2. timeTask==DelayedCreateTopics时
																								DelayedCreateTopics.onComplete(){
																									val results = createMetadata.map { metadata =>
																									  if (metadata.error.isSuccess && missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet) > 0)
																										(metadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
																									  else
																										(metadata.topic, metadata.error)
																									}.toMap
																									
																									responseCallback(results)
																								}
																							}
																							true
																						}else { false}
																					}
																					if(isCompleted){onExpiration()}
																				}
																				
																			}}
																		}
																																			
																	}
																} finally {
																	readLock.unlock()
																}
															}
														  if (operation.isCompleted) {
															// cancel the timer task
															operation.cancel()
														  }
														}
														return false
													}
												}
												
											}
										}
									}
									case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
									case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
									case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request)
									case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
									case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request)
									case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request)
									case ApiKeys.END_TXN => handleEndTxnRequest(request)
									case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request)
									case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request)
									case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
									case ApiKeys.CREATE_ACLS => handleCreateAcls(request)
									case ApiKeys.DELETE_ACLS => handleDeleteAcls(request)
									case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
									case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
							  }
							} catch {
								case e: FatalExitError => throw e
								case e: Throwable => handleError(request, e)
							} finally {
								request.apiLocalCompleteTimeNanos = time.nanoseconds
							}
							
						}
						
					} catch {
						case e: FatalExitError =>latch.countDown()
						  Exit.exit(e.statusCode)
						case e: Throwable => error("Exception when handling request", e)
					}
				}
				
				
			}
		}
	}
	
}


// 隶属上面"kafka-request-handler-n"线程, 解释其各个Request的处理类:Handler
ApiKeys.forId(request.requestId) match {
	/** API.1	生产者存储数据的Api: ProducerRequest
	* KafkaApis.handleProduceRequest()-> ReplicaManager.appendRecords()->  Partition.appendRecordsToLeader()
	* 	-> Log.appendAsLeader() -> Log.append() -> LogSegment.append() -> FileRecords.append(MemoryRecords records)
	*/
	case ApiKeys.PRODUCE => handleProduceRequest(request){//KafkaApis.handleProduceRequest()	处理生产者发来的: 存储消息日志
		val produceRequest = request.body[ProduceRequest]
		val numBytesAppended = request.header.toStruct.sizeOf + request.bodyAndSize.size
		
		val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) =
		  produceRequest.partitionRecordsOrFail.asScala.partition { case (tp, _) =>
			authorize(request.session, Describe, new Resource(Topic, tp.topic)) && metadataCache.contains(tp.topic)
		  }
		
		if (authorizedRequestInfo.isEmpty){
			sendResponseCallback(Map.empty)
		}else {// 进入这里
			val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId
			  // call the replica manager to append messages to the replicas	将消息追加到该partition的主节点(leader replicas)中,并让其同步其他replicas
			replicaManager.appendRecords(timeout = produceRequest.timeout.toLong, requiredAcks = produceRequest.acks,internalTopicsAllowed = internalTopicsAllowed,
					isFromClient = true,entriesPerPartition = authorizedRequestInfo, responseCallback = sendResponseCallback){//ReplicaManager.appendRecords()
				if (isValidRequiredAcks(requiredAcks)) {
				  val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,isFromClient = isFromClient, entriesPerPartition, requiredAcks){
						entriesPerPartition.map { case (topicPartition, records) =>
						  brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
						  brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

						  // reject appending to internal topics if it is not allowed
						  if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
							(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo,Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
						  } else {// 正常,进入这里
							try {
							  val partitionOpt = getPartition(topicPartition)
							  val info = partitionOpt match {
								case Some(partition) =>//正常,进入这里,调用 Partition来完成具体的写入
									partition.appendRecordsToLeader(records, isFromClient, requiredAcks){//Partition.appendRecordsToLeader()
										val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
											leaderReplicaIfLocal match {
												case Some(leaderReplica) =>
													val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient){
														Log.append(records, isFromClient, assignOffsets = true, leaderEpoch){//Log.append()
															
															val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)
															try {
															  // they are valid, insert them in the log
															  lock synchronized {
																if(assignOffsets) {
																  val validateAndOffsetAssignResult = try {
																	LogValidator.validateMessagesAndAssignOffsets(validRecords,offset,now)
																  } catch {
																	case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
																  }
																}else{
																	if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
																		throw new IllegalArgumentException("Out of order offsets found in " + records.records.asScala.map(_.offset))
																}

																// update the epoch cache with the epoch stamped onto the message by the leader
																validRecords.batches.asScala.foreach { batch =>
																  if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
																	leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
																}
																// maybe roll the log if this segment is full
																val segment = maybeRoll(messagesSize = validRecords.sizeInBytes, maxTimestampInMessages = appendInfo.maxTimestamp, maxOffsetInMessages = appendInfo.lastOffset)
																val logOffsetMetadata = LogOffsetMetadata()

																// 核心代码?
																segment.append(firstOffset = appendInfo.firstOffset,largestOffset = appendInfo.lastOffset, largestTimestamp = appendInfo.maxTimestamp,
																  shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,records = validRecords){//LogSegment.append()
																	LogSegment.append(){
																		if (records.sizeInBytes > 0) {
																			val appendedBytes = log.append(records){//FileRecords.append(MemoryRecords records)
																				int written = records.writeFullyTo(channel){
																					buffer.mark();
																					int written = 0;
																					while (written < sizeInBytes())
																						written += channel.write(buffer){
																							
																						}
																					buffer.reset();
																					return written;
																				}
																				size.getAndAdd(written);
																				return written;
																			}
																			
																			if(bytesSinceLastIndexEntry > indexIntervalBytes) {
																				index.append(firstOffset, physicalPosition)
																				timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
																				bytesSinceLastIndexEntry = 0
																			}
																			bytesSinceLastIndexEntry += records.sizeInBytes
																		}
																	}
																  }

																// update the producer state
																for ((producerId, producerAppendInfo) <- updatedProducers) {
																  producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
																  producerStateManager.update(producerAppendInfo)
																}
																for (completedTxn <- completedTxns) {
																  val lastStableOffset = producerStateManager.completeTxn(completedTxn)
																  segment.updateTxnIndex(completedTxn, lastStableOffset)
																}
																
																producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)
																updateLogEndOffset(appendInfo.lastOffset + 1)
																updateFirstUnstableOffset()
																appendInfo
															  }
															} catch {
															  case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
															}
														}
													}
													
													replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
													(info, maybeIncrementLeaderHW(leaderReplica))
												
												case None => throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d" .format(topicPartition, localBrokerId))
											}
											
											if (leaderHWIncremented) tryCompleteDelayedRequests(){//Partition.tryCompleteDelayedRequests()
												val requestKey = new TopicPartitionOperationKey(topicPartition)
												replicaManager.tryCompleteDelayedFetch(requestKey)
												replicaManager.tryCompleteDelayedProduce(requestKey)
												replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
											}
											return info
										}
									}

								case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
								  .format(topicPartition, localBrokerId))
							  }

							  val numAppendedMessages =
								if (info.firstOffset == -1L || info.lastOffset == -1L)
								  0
								else
								  info.lastOffset - info.firstOffset + 1

							  // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
							  brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
							  brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
							  brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
							  brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

							  trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
								.format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
							  (topicPartition, LogAppendResult(info))
							} catch {
							  // NOTE: Failed produce requests metric is not incremented for known exceptions
							  // it is supposed to indicate un-expected failures of a broker in handling a produce request
							  case e: KafkaStorageException =>
								fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
								Exit.halt(1)
								(topicPartition, null)
							  case e@ (_: UnknownTopicOrPartitionException |
									   _: NotLeaderForPartitionException |
									   _: RecordTooLargeException |
									   _: RecordBatchTooLargeException |
									   _: CorruptRecordException |
									   _: InvalidTimestampException) =>
								(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
							  case t: Throwable =>
								brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
								brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
								error("Error processing append operation on partition %s".format(topicPartition), t)
								(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
							}
						  }
}
						
				  }

				  if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
					// create delayed produce operation
					val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)
					// ...
				  } else {
					// we can respond immediately
					val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
					responseCallback(produceResponseStatus)
				  }
				} else {
					val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
						topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,LogAppendInfo.UnknownLogAppendInfo.firstOffset, RecordBatch.NO_TIMESTAMP)
					}
					responseCallback(responseStatus){// KafkaApis.handleProduceRequest().sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse])
						val mergedResponseStatus = responseStatus ++ 
							unauthorizedForWriteRequestInfo.mapValues(_ => new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)) ++
							nonExistingOrUnauthorizedForDescribeTopics.mapValues(_ => new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
					  
						// When this callback is triggered, the remote API call has completed
						request.apiRemoteCompleteTimeNanos = time.nanoseconds

						quotas.produce.recordAndMaybeThrottle(request.session.sanitizedUser,request.header.clientId, numBytesAppended,produceResponseCallback){//clientQuotaManager.recordAndMaybeThrottle()
							val clientSensors = getOrCreateQuotaSensors(sanitizedUser, clientId);
							
							recordAndThrottleOnQuotaViolation(clientSensors, value, callback){
								clientSensors.quotaSensor.record(value)
								callback(0)
								return throttleTimeMs;
							}
						}
					}
				}
			}
			// hence we clear its data here inorder to let GC re-claim its memory since it is already appended to log
			produceRequest.clearPartitionRecords()
		}
		
	}
	
	/** API.2 消费者获取数据	FetchRequest
	* 
	*/
	case ApiKeys.FETCH => handleFetchRequest(request){//KafkaApis.handleFetchRequest()
		// Server端: "kafka-request-handler-1"线程: 接受FetchRequest请求,并转有ReplicaManager处理,创建并启动DelayedFetch任务
		KafkaApis.handleFetchRequest(){
			ReplicaManager.fetchMessages(){
				val logReadResults = readFromLocalLog();{//ReplicaManager.readFromLocalLog()
					readPartitionInfo.foreach { case (tp, fetchInfo) => 
						val readResult = read(tp, fetchInfo, limitBytes, minOneMessage);{//ReplicaManager.readFromLocalLog()$read()
							val localReplica = if (fetchOnlyFromLeader) 
									getLeaderReplicaIfLocal(tp)
								else
									getReplicaOrException(tp)
							val logReadInfo = localReplica.log match {
								case Some(log) =>{
									val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)
									val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel);{//Log.read()
										val fetchInfo = segment.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage);{//LogSegment.read()
											val startOffsetAndSize = translateOffset(startOffset);{//LogSegment.translateOffset()
												val mapping = index.lookup(offset)
												log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition));{
													for (FileChannelRecordBatch batch : batchesFrom(startingPosition)){
														long offset = batch.lastOffset();{//DefaultRecordBatch.$DefaultFileChannelRecordBatch.lastOffset()
															return loadBatchHeader(){//FileLogInputStream.$FileChannelRecordBatch.loadBatchHeader()
																if (batchHeader == null) batchHeader = loadBatchWithSize(headerSize(), "record batch header");{//FileChannelRecordBatch.loadBatchHeader()
																	ByteBuffer buffer = ByteBuffer.allocate(size);//定义要读取数据大小的空buffer;
																	Utils.readFullyOrFail(channel, buffer, position, description);{//从channel中读取size大小的数据到buffer中?
																		int expectedReadBytes = destinationBuffer.remaining();
																		readFully(channel, destinationBuffer, position);{//Utils.readFully(FileChannel channel, ByteBuffer destinationBuffer, long position)
																			long currentPosition = position;
																			int bytesRead;
																			do {
																				bytesRead = channel.read(destinationBuffer, currentPosition);
																				currentPosition += bytesRead;
																			} while (bytesRead != -1 && destinationBuffer.hasRemaining());
																		}
																	}
																	buffer.rewind();
																	return toMemoryRecordBatch(buffer);{
																		return new DefaultRecordBatch(buffer);
																	}
																}
																return batchHeader;
															}.lastOffset();
														}
														if (offset >= targetOffset) return new LogOffsetPosition(offset, batch.position(), batch.sizeInBytes());
													}
												}
											}
											val fileRecords:FileRecords= log.read(startPosition, fetchSize);{//FileRecords.read()
												return new FileRecords(file, channel, this.start + position, end, true);
											}
											FetchDataInfo(offsetMetadata, fileRecords, firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
										}
									}
									FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
								}
							}
						}
						result += (tp -> readResult)
					}
				}
				if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
					val fetchPartitionData = logReadResults.map { case (tp, result)=> tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records, result.lastStableOffset, result.info.abortedTransactions)}
					responseCallback(fetchPartitionData)
				} else{
					val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
					val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, isolationLevel, responseCallback)
					delayedFetchPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys){
						var isCompletedByMe = operation.safeTryComplete()//默认调父类DelayedOperation.safeTryComplete(),有些如Heartbeat有自己实现的方法;
						if (isCompletedByMe){return true;}//会进行2次的 safeTryComplete(),如果完成了直接结束返回;
						if (!operation.isCompleted) {//该DelayedOperation.completed 还是false;
							timeoutTimer.add(operation){//SystemTime.add()
								addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs));{//根据expiration时刻判断是:立马submit() or delayQueue.offser() 本轮执行 or 放入overflowWheel下一轮执行;
									val addedForWaiting = timingWheel.add(timerTaskEntry);{//TimingWheel.add()若逾期执行时间expiration,在下个刻度(tickMs)内返回false立马执行; 在一个轮长(200ms)内 则添加到queue并返回true; 若在本轮以后,则添加到overflowWheel等待并返回true;
										val expiration = timerTaskEntry.expirationMs
										if (timerTaskEntry.cancelled) {
											false
										} else if (expiration < currentTime + tickMs) {//tickMs两个Entry间执行间隔(默认1 ms?(有时20,40,400,8000)?),若小于 表示该taskEntry已经到了(或过了)执行时间;
											false // expiration < 下一个执行刻度, 添加到timingWheel失败,返回false需要后面立马taskExecutor.submit()提交 或取消;
										} else if (expiration < currentTime + interval) {
											bucket.add(buckets((expiration / tickMs % wheelSize.toLong).toInt))
											queue.offer(bucket) //添加到SystemTimer.delayQueue队列,在"ExpirationReaper-0-{}"线程中被循环poll()出被执行或继续addTimerTaskEntry()
											true //若预期执行时间(expiration) 就在本轮interval(200毫秒)以内,则将该taskEntry加到delayQueue队里等待执行;
										} else {
										  if (overflowWheel == null) addOverflowWheel()
										  overflowWheel.add(timerTaskEntry)
										}
									}
									if(!addedForWaiting){//进入这里代表expiration <currentTime+tickMs 下个刻度前需要立马执行了;(或者cancelled)
										taskExecutor.submit(timerTaskEntry.timerTask);//启动一个名为"executor-{executorName}"的线程,执行其 onComplete(), onExpiration()方法;
									}
								}
							}
						}
					}
					
				}
			}
		}
	}
	
	case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
	case ApiKeys.METADATA => handleTopicMetadataRequest(request)
	case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
	case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
	case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
	case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
	case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
	case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
	case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
	
	case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request);{
		
	}
	
	case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request){//KafkaApis.handleHeartbeatRequest()
		/** 接收client端的心跳检测请求;
		 * 
			KafkaApis.handleHeartbeatRequest(){
				GroupCoordinator.handleHeartbeat();{
					heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey));{
						timeoutTimer.add(operation);{//SystemTimer.add()
							addTimerTaskEntry(new TimerTaskEntry(timerTask));{//SystemTimer.addTimerTaskEntry()
								val addedForWaiting = timingWheel.add(timerTaskEntry);{//若逾期执行时间expiration,在下个刻度(tickMs)内返回false立马执行; 在一个轮长(200ms)内 则添加到queue并返回true; 若在本轮以后,则添加到overflowWheel等待并返回true;
									val expiration = timerTaskEntry.expirationMs
									if (timerTaskEntry.cancelled) {
										false
									} else if (expiration < currentTime + tickMs) {//tickMs两个Entry间执行间隔(默认1 ms?(有时20,40,400,8000)?),若小于 表示该taskEntry已经到了(或过了)执行时间;
										false // expiration < 下一个执行刻度, 添加到timingWheel失败,返回false需要后面立马taskExecutor.submit()提交 或取消;
									} else if (expiration < currentTime + interval) {
										bucket.add(buckets((expiration / tickMs % wheelSize.toLong).toInt))
										queue.offer(bucket) //添加到SystemTimer.delayQueue队列,在"ExpirationReaper-0-{}"线程中被循环poll()出被执行或继续addTimerTaskEntry()
										true //若预期执行时间(expiration) 就在本轮interval(200毫秒)以内,则将该taskEntry加到delayQueue队里等待执行;
									} else {
									  if (overflowWheel == null) addOverflowWheel()
									  overflowWheel.add(timerTaskEntry)
									}
								}
								if(!addedForWaiting){//进入这里代表expiration <currentTime+tickMs 下个刻度前需要立马执行了;(或者cancelled)
									taskExecutor.submit(timerTaskEntry.timerTask);//启动一个名为"executor-{executorName}"的线程,执行其 onComplete(), onExpiration()方法;
								}
							}
						}
					}
				}
			}
		*/
		
		groupCoordinator.handleHeartbeat();{//GroupCoordinator.handleHeartbeat()
			completeAndScheduleNextHeartbeatExpiration();{//GroupCoordinator.:
				member.latestHeartbeat = time.milliseconds(); //System.currentTimeMillis(): 返回系统当前时间戳;
				//将该memberKey(groupId+clientId)对应Watchers.operations[LinkedQueue[DelayedOperation实现类]]中所有的DelayedOperation.completed设置为true (compareAndSet(false, true));
				val memberKey = MemberKey(member.groupId, member.memberId)
				heartbeatPurgatory.checkAndComplete(memberKey){//DelayOperationPurgatory.checkAndComplete(): 若Watchers.operations中DelayOperation已经completed==true,则将其从队列移除; 否则尝试safeTryComplete()成功后再移除;
					if(watchers != null) watchers.tryCompleteWatched();{//Watchers.tryCompleteWatched(operation,watchKeys)
						while (operations.iterator().hasNext) {
							if(curr.isCompleted) {//若该DelayOperation.completed ==true,表示操作已经执行外;这里直接从该Watchers.operations中移除该实例;
								iter.remove()//从watchers.operations中删除该DelayOperation实例;
							} else if (curr.safeTryComplete(){//DelayedOperation.safeTryComplete()
								synchronized {tryComplete();{//抽象类,由子类实现:
									DelayedHeartbeat.tryComplete();{
										coordinator.tryCompleteHeartbeat(group, member, heartbeatDeadline, forceComplete _);{
											group synchronized {
												if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
													forceComplete();{//DelayedOperation.forceComplete
														if (completed.compareAndSet(false, true)) {
															cancel();{//
																if (timerTaskEntry != null) timerTaskEntry.remove() //将自己this从 TimerTaskList中移除;
																timerTaskEntry = null // 将引用值为空;
															}
															onComplete();{//DelayedHeartbeat.onComplete()
																coordinator.onCompleteHeartbeat();{//GroupCoordinator.onCompleteHeartbeat()
																	//TODO: add metrics for complete heartbeats 空代码;
																}
															}
															true
														} else {
															false
														}
													}
												else false
											}
										}
									}
								}}
							}){
								iter.remove();//若调用其safeTryComplete()方法也正常执行,也将该实例从operations中移除;
							}
						}
					}
				}
	
				
				// 创建新DelayedHeartbeat对象来覆盖 对应Watchers.operations中的原对象: reschedule the next heartbeat expiration deadline: 安排下一次心跳过期的deadline;
				val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs 
				//会话超时配置位于: GroupCoordinator.groupManager[GroupMetadataManager].groupMetadataCache[Pool[String, GroupMetadata]].GroupMetadata.members[MemberMetadata].sessionTimeoutMs == JoinGroup时client端传来的joinGroupRequest.sessionTimeout==Consumer端的session.timeout.ms;
				val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
				// 添加定时任务到时间轮;
				heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey));{//先safeTryComplete()两次,若还未completed就addTimerTaskEntry()添加到timingWheel或立马submit();
					var isCompletedByMe = operation.safeTryComplete()//默认调父类DelayedOperation.safeTryComplete(),有些如Heartbeat有自己实现的方法;
					if (isCompletedByMe){return true;}//会进行2次的 safeTryComplete(),如果完成了直接结束返回;
					if (!operation.isCompleted) {//该DelayedOperation.completed 还是false;
						//正常情况, 会将该Heartbeat的消息执行时间
						timeoutTimer.add(operation){//SystemTime.add():
							addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs));//根据expiration时刻判断是:立马submit() or delayQueue.offser() 本轮执行 or 放入overflowWheel下一轮执行;
						}
					}
				}
			}
		}
	}
	case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
	case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
	case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
	case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
	case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
	case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
	case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request){//		创建Topic
		val createTopicsRequest = request.body[CreateTopicsRequest]
		// createTopicsRequest.topics== Map<Topic, TopicDetails> {case TopicDetail(int numPartitions,short replicationFactor,Map<Integer, List<Integer>> replicasAssignments,Map<String, String> configs}
		
		if (!controller.isActive) {
		  val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
			(topic, new ApiError(Errors.NOT_CONTROLLER, null))
		  }
		  sendResponseCallback(results)
		} else if (!authorize(request.session, Create, Resource.ClusterResource)) {
		  val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
			(topic, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null))
		  }
		  sendResponseCallback(results)
		} else { // 正常的话,直接进入这里
		  val (validTopics, duplicateTopics) = createTopicsRequest.topics.asScala.partition { case (topic, _) =>
			!createTopicsRequest.duplicateTopics.contains(topic)
		  }
		  
			adminManager.createTopics(createTopicsRequest.timeout,createTopicsRequest.validateOnly, validTopics,sendResponseWithDuplicatesCallback ){//AdminManager.createTopics()
				// 1. map over topics creating assignment and calling zookeeper
				val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
					// 向/$KAFKA/config/topics/{topic} 和/$KAFKA/brokers/topics/{topic} 两个ZK路径写入partition信息;
				val metadata = createInfo.map { case (topic, arguments) =>
				  try {
					LogConfig.validate(configs)
					val assignments = {
					  if ((arguments.numPartitions != NO_NUM_PARTITIONS || arguments.replicationFactor != NO_REPLICATION_FACTOR)&& !arguments.replicasAssignments.isEmpty)
						throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. Both cannot be used at the same time.")
					  else if (!arguments.replicasAssignments.isEmpty) {
						arguments.replicasAssignments.asScala.map { case (partitionId, replicas) =>(partitionId.intValue, replicas.asScala.map(_.intValue))}
					  } else{//正常 进入这里
							AdminUtils.assignReplicasToBrokers(brokers, arguments.numPartitions, arguments.replicationFactor){
								if (brokerMetadatas.forall(_.rack.isEmpty))
								  assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex, startPartitionId){
									  
									val ret = mutable.Map[Int, Seq[Int]]()
									val brokerArray = brokerList.toArray
									val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
									var currentPartitionId = math.max(0, startPartitionId)
									var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
									for (_ <- 0 until nPartitions) {
									  if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
										nextReplicaShift += 1
									  val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
									  val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
									  for (j <- 0 until replicationFactor - 1)
										replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
									  ret.put(currentPartitionId, replicaBuffer)
									  currentPartitionId += 1
									}
									return ret;// ret=Map<Integer[Partition?],ArrayBuffer[Broker?]>
									  
								  }
								else {
								  if (brokerMetadatas.exists(_.rack.isEmpty)){
									  throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment")
								  }
								  assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,startPartitionId)
								}
							}
					  }
						
					}
					trace(s"Assignments for topic $topic are $assignments ")

					createTopicPolicy match {
					  case Some(policy) =>
						AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
					  case None =>
						if (validateOnly)
						  AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
						else
						  AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false){
							validateCreateOrUpdateTopic(zkUtils, topic, partitionReplicaAssignment, config, update)
							if (!update) {
							  writeEntityConfig(zkUtils, getEntityConfigPath(ConfigType.Topic, topic), config){
								//entityPath = /config/topics/testCreate01: /config/topics/{topic}
								zkUtils.updatePersistentPath(entityPath, Json.encode(map)){
									val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
									// 向/config/topics/{topic} 路径,写入 data={version:1,config:{}}
									zkClient.writeData(path, data)
								}
							  }
							}
							
							writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update){
								val zkPath = getTopicPath(topic) // 获取的是topic正式位置 /brokers/topics/{topic}路径
								
								// 获取该Topic的分区和分区所在节点信息: partitons== Map<String(分区),Seq[Int](主副节点?)]
								val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => e._1.toString -> e._2))

								if (!update) {
									info("Topic creation " + jsonPartitionData.toString)
									zkUtils.createPersistentPath(zkPath, jsonPartitionData)
								} else {
									info("Topic update " + jsonPartitionData.toString)
										* [2020-03-14 00:52:11,159] INFO Topic creation {"version":1,"partitions":{"1":[0],"0":[0]}} (kafka.admin.AdminUtils$)
									// 向 /$KAFKA/brokers/topics/{topic}路径文件,写入字符串: {"version":1,"partitions":{"1":[0],"0":[0]}}
									zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
								}
								
							}
							
						  }
					}
					
					return CreateTopicMetadata(topic, assignments, ApiError.NONE)
				  } catch {
					// Log client errors at a lower level than unexpected exceptions
					case e@ (_: PolicyViolationException | _: ApiException) =>
					  info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
					  CreateTopicMetadata(topic, Map(), ApiError.fromThrowable(e))
					case e: Throwable =>
					  error(s"Error processing create topic request for topic $topic with arguments $arguments", e)
					  CreateTopicMetadata(topic, Map(), ApiError.fromThrowable(e))
				  }
				}

				// 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
				if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
				  val results = metadata.map { createTopicMetadata =>
					if (createTopicMetadata.error.isSuccess() && !validateOnly) {
					  (createTopicMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
					} else {
					  (createTopicMetadata.topic, createTopicMetadata.error)
					}
				  }.toMap
				  responseCallback(results)
				} else {// 正常,进入这里
					// 3. else pass the assignments and errors to the delayed operation and set the keys
					val delayedCreate = new DelayedCreateTopics(timeout, metadata.toSeq, this, responseCallback)
					val delayedCreateKeys = createInfo.keys.map(new TopicKey(_)).toSeq
					// try to complete the request immediately, otherwise put it into the purgatory
					topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys){//DelayedOperation.tryCompleteElseWatch()
						var isCompletedByMe = operation.safeTryComplete(){
							synchronized{
								tryComplete(){//抽象方法, 由DelayedCreateTopics.tryComplete()实现
									val leaderlessPartitionCount = createMetadata.filter(_.error.isSuccess)
									  .foldLeft(0) { case (topicCounter, metadata) =>
										topicCounter + missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet)
									  }
									  
									if (leaderlessPartitionCount == 0) {
									  trace("All partitions have a leader, completing the delayed operation")
									  forceComplete()
									} else {
									  trace(s"$leaderlessPartitionCount partitions do not have a leader, not completing the delayed operation")
									  false
									}
									
									
								}
							}
						}
						if (isCompletedByMe){return true;}
						
						var watchCreated = false
						for(key <- watchKeys) {
						  // If the operation is already completed, stop adding it to the rest of the watcher list.
						  if (operation.isCompleted)
							return false
						  watchForOperation(key, operation)

						  if (!watchCreated) {
							watchCreated = true
							estimatedTotalOperations.incrementAndGet()
						  }
						}

						isCompletedByMe = operation.safeTryComplete()
						if (isCompletedByMe) return true

						// if it cannot be completed by now and hence is watched, add to the expire queue also
						if (!operation.isCompleted) {
						  if (timerEnabled)
							timeoutTimer.add(operation){//kafka.Time.SystemTime.add()
								readLock.lock()
								try {
									addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs)){
										if (!timingWheel.add(timerTaskEntry)) {
											if (!timerTaskEntry.cancelled){taskExecutor.submit(timerTaskEntry.timerTask){
												
												DelayedCreateTopics.run(){
													val isCompleted= forceComplete(){
														if (completed.compareAndSet(false, true)) {
															cancel()
															onComplete(){// 抽象类,由继承类实现
																//1. 当 timeTask==DelayedFetch时, 
																DelayedFetch.onComplete(){
																	val logReadResults = replicaManager.readFromLocalLog()
																	//..
																}
																
																// 2. timeTask==DelayedCreateTopics时
																DelayedCreateTopics.onComplete(){
																	val results = createMetadata.map { metadata =>
																	  if (metadata.error.isSuccess && missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet) > 0)
																		(metadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
																	  else
																		(metadata.topic, metadata.error)
																	}.toMap
																	
																	responseCallback(results)
																}
															}
															true
														}else { false}
													}
													if(isCompleted){onExpiration()}
												}
												
											}}
										}
																											
									}
								} finally {
									readLock.unlock()
								}
							}
						  if (operation.isCompleted) {
							// cancel the timer task
							operation.cancel()
						  }
						}
						return false
					}
				}
				
			}
		}
	}
	case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
	case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
	case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request)
	case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
	case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request)
	case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request)
	case ApiKeys.END_TXN => handleEndTxnRequest(request)
	case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request)
	case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request)
	case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
	case ApiKeys.CREATE_ACLS => handleCreateAcls(request)
	case ApiKeys.DELETE_ACLS => handleDeleteAcls(request)
	case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
	case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
}





// 线程"ExpirationReaper-0-Heartbeat" 的启动源码;
// 线程"ExpirationReaper-0-Rebalance" 的启动源码;

"main"线程启动GroupCoordinator及相关"ExpirationReaper-{brokerId}-{modelName}"线程
Kafka.main(){KafkaServer.startup(){
	groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM);{//GroupCoordinator.apply()
		val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId);{//DelayedOperationPurgatory.apply(purgeInterval: Int = 1000,reaperEnabled: Boolean = true,timerEnabled: Boolean = true)
			val timer = new SystemTimer(purgatoryName);{
				val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
					def newThread(runnable: Runnable): Thread = Utils.newThread("executor-"+executorName, runnable, false)//
				});
				val delayQueue = new DelayQueue[TimerTaskList]();// 任务队列,用户顺序缓存任务;
				val timingWheel = new TimingWheel(tickMs = tickMs,wheelSize = wheelSize,startMs = startMs,taskCounter = taskCounter,delayQueue);//时间轮;用于调度定时执行?
			}
			
			new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled);{
				private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)));// 各Operations操作的keys;
				val expirationReaper = new ExpiredOperationReaper();// 启动"ExpirationReaper-{brokerId}-{modelName}"线程, 用于后台执行过期的任务;
				/*正式启动"ExpirationReaper-0-Heartbeat"线程: 循环delayQueue[DelayQueue[TimerTaskList]].poll()拉取taskList,对并其中所有元素执行 addTimerTaskEntry()方法;
				* */
					ExpiredOperationReaper.run(){
						while (isRunning.get){
							ExpiredOperationReaper.doWork(){advanceClock(200L){//DelayedOperationPurgatory.advanceClock()
								timeoutTimer.advanceClock(timeoutMs);{//SystemTimer.advanceClock()
									var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
									if (bucket != null) {
										while (bucket != null) {//循环取出delayQueue队列中的任务Bucket,更新时间轮,并调用bucket.flush()刷新;
											timingWheel.advanceClock(bucket.getExpiration())
											bucket.flush(reinsert);{//TimerTaskList.flush(f: (TimerTaskEntry)=>Unit): 将bucket:TimeTaskList中所有的元素遍历执行其reinsert()=addTimerTaskEntry(timerTaskEntry)这个方法,并移除; 
												var head = root.next
												while (head ne root) {// head表示下个元素,还不是根元素,说明这个Linked不知一个元素, 还需要遍历;
													remove(head)
													f(head);{//f==reinsert==addTimerTaskEntry()方法: addTimerTaskEntry(timerTaskEntry)
														SystemTimer.addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs));//代码详解BaseApi源码;
													}
													head = root.next
												}
												expiration.set(-1L)
											}
											bucket = delayQueue.poll()
										}
									}
								}
								if (estimatedTotalOperations.get - delayed > purgeInterval) {
									estimatedTotalOperations.getAndSet(delayed)
									val purged = allWatchers.map(_.purgeCompleted()).sum 
								}
							}}
						}
					}
				
				if (reaperEnabled){//传进的reaperEnabled默认==true;
					expirationReaper.start();{//ExpiredOperationReaper的父类ShutdownableThread重写了run()方法:
						ShutdownableThread.run(){//ExpiredOperationReaper.run()
							info("Starting")
							try {
								while (isRunning.get)//循环执行 doWork()(抽象方法),直到进程终止或抛异常;
									doWork();{//抽象方法,由ShutdownableThread不同实现子类实现; 这里是DelayedOperation.ExpiredOperationReaper类来实现;
										ExpiredOperationReaper.doWork(){
											advanceClock(200L);{//DelayedOperationPurgatory.advanceClock()
												// 1. 
												timeoutTimer.advanceClock(timeoutMs);{//SystemTimer.advanceClock()
													
													var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS) //delayQueue队列是在SystemTimer.addTimerTaskEntry() ->timingWheel.add(timerTaskEntry)时,将该offert(taskEntry)到队里的;
													{
														SystemTimer.addTimerTaskEntry(){
															if (!timingWheel.add(timerTaskEntry);{//TimingWheel.add(timerTaskEntry: TimerTaskEntry)
																val expiration = timerTaskEntry.expirationMs
																if(){
																	//..
																}else if (expiration < currentTime + interval) {
																	val bucket = buckets((virtualId % wheelSize.toLong).toInt)
																	bucket.add(timerTaskEntry)
																	if (bucket.setExpiration(virtualId * tickMs)) {
																		queue.offer(bucket)
																	}
																	true //若预期执行时间(expiration) 就在本轮interval(200毫秒)以内,则将该taskEntry加到delayQueue队里等待执行;
																}else{
																	if (overflowWheel == null) addOverflowWheel()
																	overflowWheel.add(timerTaskEntry)	
																}
															})
														}
													}
													
													if (bucket != null) {
													  writeLock.lock()
													  try {
														while (bucket != null) {//循环取出delayQueue队列中的任务Bucket,更新时间轮,并调用bucket.flush()刷新;
															timingWheel.advanceClock(bucket.getExpiration())
															// Remove all task entries and apply the supplied function to each of them
															bucket.flush(reinsert);{//TimerTaskList.flush(f: (TimerTaskEntry)=>Unit): 将bucket:TimeTaskList中所有的元素遍历执行其reinsert()=addTimerTaskEntry(timerTaskEntry)这个方法,并移除; 
																synchronized {
																  var head = root.next
																  while (head ne root) {// ne 是not equalt !=的含义? head表示下个元素,还不是根元素,说明这个Linked不知一个元素, 还需要遍历;
																	remove(head)
																	f(head);{//f==reinsert==addTimerTaskEntry()方法: addTimerTaskEntry(timerTaskEntry)
																		SystemTimer.addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs));//代码详解BaseApi源码;
																	}
																	head = root.next
																  }
																  expiration.set(-1L)
																}
															}
															bucket = delayQueue.poll()
														}
													  } finally {
														writeLock.unlock()
													  }
													  true
													} else {
													  false
													}
												}
												
												/* 2. 判断当 积压数量>1000时,
												*		estimatedTotalOperations= AtomicInteger(0): estimated total operations所有要执行的任务? : 每执行tryCompleteElseWatch()方法(成功后?)会进行+1;
												*		delayed==timeoutTimer.size==SystemTimer.taskCounter.get : 对add(TimerTaskEntry)的计数;
												*		purgeInterval: 清除间隔(数),在DelayedOperationPurgatory.apply()传入默认值 1000;
												*/
												if (estimatedTotalOperations.get - delayed > purgeInterval) {//当 总调度任务 - 过期任务数 超过1000个;
													estimatedTotalOperations.getAndSet(delayed)//delayed() = timeoutTimer.size == SystemTimer.taskCounter.get
													debug("Begin purging watch lists")//purging:清理,净化的含义; 
													// allWatchers== watchersForKey.values, 返回所有当前正在watcher的list:List<Watchers>, 并执行每个Watchers.purgeCompleted()
													val purged = allWatchers.map(_.purgeCompleted()).sum ;{
														allWatchers = watchersForKey.values
														val purgedNums:List[Int]= allWatchers.map(_.purgeCompleted(){//Watchers.purgeCompleted()
														  var purged = 0
														  val iter = operations.iterator()
														  while (iter.hasNext) {
															val curr = iter.next()
															if (curr.isCompleted) {//若该任务已经完成, 则从列表中移除;
															  iter.remove()
															  purged += 1
															}
														  }
														  if (operations.isEmpty){
															  removeKeyIfEmpty(key, this)
														  }
														  
														  purged
														});
														
														purgedNums.sum()
													}
												}
											}
										}
									}
							} catch {
								case e: FatalExitError =>{
									isRunning.set(false)
									shutdownLatch.countDown()
									info("Stopped")
									Exit.exit(e.statusCode())
								}
								case e: Throwable => if (isRunning.get())error("Error due to", e)
							}
							shutdownLatch.countDown()
							info("Stopped")
						}
					}
					
				}
			}
		}
		// 同上,里面启动"ExpirationReaper-0-Rebalance"线程,循环进行定时执行和过期清理工作;
		val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId);{
			
		}
		
		apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
	}
}}




//线程"executor-Heartbeat"线程
// 还有个"executor-Rebalance"线程;

DelayedOperation.run(){
	if (forceComplete())
		onExpiration();{//DelayedHeartbeat.onExpiration()
			coordinator.onExpireHeartbeat(group, member, heartbeatDeadline);{//GroupCoordinator.onExpireHeartbeat()
				group synchronized {
					if (!shouldKeepMemberAlive(member, heartbeatDeadline))
						onMemberFailure(group, member);{//GroupCoordinator.onMemberFailure()
							debug(s"Member ${member.memberId} in group ${group.groupId} has failed")
							group.remove(member.memberId)//从内存中移除该成员(的clientId);
							group.currentState match {
								case Dead | Empty =>
								case Stable | AwaitingSync => maybePrepareRebalance(group)
								case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
							}
						}
				}
			}
		}
}


// 线程"executor-Fetch": 完成Fetch的请求并构建FetchResponse并返回;
DelayedOperation.run(){
	val isForceCompleted = forceComplete(){//DelayedOperation.forceComplete()
		//该DelayedOperation完成的标志,就是将其DelayedOperation.completed从false成功置为true;
		if (completed.compareAndSet(false, true)) {//重要步骤:标记完成状态为true;
			cancel(){//DelayedOperation.cancel()
				synchronized {
				  if (timerTaskEntry != null) timerTaskEntry.remove()
				  timerTaskEntry = null
				}
			}
			onComplete();{//抽象方法,由子类实现;
				DelayedFetch.onComplete(){
					val logReadResults = replicaManager.readFromLocalLog(replicaId = fetchMetadata.replicaId,fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
					  readOnlyCommitted = fetchMetadata.fetchOnlyCommitted,fetchMaxBytes = fetchMetadata.fetchMaxBytes,
					  hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
					  readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
					  quota = quota,isolationLevel = isolationLevel)
					
					val fetchPartitionData = logReadResults.map { case (tp, result) =>
					  tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
						result.lastStableOffset, result.info.abortedTransactions)
					}
					//responseCallback为 new DelayedFetch(responseCallback)构造参数, 其实现为KafkaApi.handleFetchRequest()中的def processResponseCallback()
					responseCallback(fetchPartitionData);{//KafkaApi.handleFetchRequest()$processResponseCallback()
					  val partitionData = {
						responsePartitionData.map { case (tp, data) =>
						  val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
						  val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
						  tp -> new FetchResponse.PartitionData(data.error, data.highWatermark, lastStableOffset,
							data.logStartOffset, abortedTransactions, data.records)
						}
					  }
					  val mergedPartitionData = partitionData ++ unauthorizedForReadPartitionData ++ nonExistingOrUnauthorizedForDescribePartitionData
					  val fetchedPartitionData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]()
					  mergedPartitionData.foreach { case (topicPartition, data) =>
						fetchedPartitionData.put(topicPartition, data)
					  }
					  
					  // fetch response callback invoked after any throttling
					  def fetchResponseCallback(bandwidthThrottleTimeMs: Int) {
						def createResponse(requestThrottleTimeMs: Int): RequestChannel.Response = {
						  val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]
						  fetchedPartitionData.asScala.foreach { case (tp, partitionData) =>
							convertedData.put(tp, convertedPartitionData(tp, partitionData))
						  }
						  val response = new FetchResponse(convertedData, 0)
						  val responseStruct = response.toStruct(versionId)
						  trace(s"Sending fetch response to client $clientId of ${responseStruct.sizeOf} bytes.")
						  response.responseData.asScala.foreach { case (topicPartition, data) =>
							// record the bytes out metrics only when the response is being sent
							brokerTopicStats.updateBytesOut(topicPartition.topic, fetchRequest.isFromFollower, data.records.sizeInBytes)
						  }
						  val responseSend = response.toSend(responseStruct, bandwidthThrottleTimeMs + requestThrottleTimeMs,request.connectionId, request.header)
						  RequestChannel.Response(request, responseSend)
						}
						if (fetchRequest.isFromFollower)
						  sendResponseExemptThrottle(createResponse(0))
						else
						  sendResponseMaybeThrottle(request, request.header.clientId, requestThrottleMs =>
							requestChannel.sendResponse(createResponse(requestThrottleMs)))
					  }
					  // When this callback is triggered, the remote API call has completed.
					  // Record time before any byte-rate throttling.
					  request.apiRemoteCompleteTimeNanos = time.nanoseconds
					  if (fetchRequest.isFromFollower) {
						// We've already evaluated against the quota and are good to go. Just need to record it now.
						val responseSize = sizeOfThrottledPartitions(versionId, fetchRequest, mergedPartitionData, quotas.leader)
						quotas.leader.record(responseSize)
						fetchResponseCallback(bandwidthThrottleTimeMs = 0)
					  } else {
						val response = new FetchResponse(fetchedPartitionData, 0)
						val responseStruct = response.toStruct(versionId)
						quotas.fetch.recordAndMaybeThrottle(request.session.sanitizedUser, clientId, responseStruct.sizeOf,
						  fetchResponseCallback)
					  }
					}
				}
			}
			true
		} else {
			false
		}
	}
	if (isForceCompleted)
		onExpiration();{//DelayedHeartbeat的抽象方法,有子类实现; 
			
		}
}




prepareRebalance(group: GroupMetadata)){
    // if any members are awaiting sync, cancel their request and have them rejoin
    if (group.is(AwaitingSync))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} with old generation ${group.generationId} " +
      s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
}



// 初始化恢复本地文件和加载各种状态文件时:
{
	
	
	
	// 报此错误时的源码;
	// ERROR There was an error in one of the threads during logs loading: org.apache.kafka.common.protocol.types.SchemaException: Error reading field 'version': java.nio.BufferUnderflowException (kafka.log.LogManager)
	LogManager.loadLogs(){
		val jobsForDir = for {
			dirContent <- Option(dir.listFiles).toList
			logDir <- dirContent if logDir.isDirectory
		  } yield {
			CoreUtils.runnable {
				val current = Log.apply(){
					val topicPartition = Log.parseTopicPartitionName(dir)
					val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
					new Log(){
						private val lastflushedTime = new AtomicLong(time.milliseconds)
						val leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()
						loadSegments()
						loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile){//读取Producer的状态信息;
							if (producerStateManager.latestSnapshotOffset.isEmpty && (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 || reloadFromCleanShutdown)) {
								//
							} else {
								producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds());{//重新加载?
									if (logEndOffset != mapEndOffset) {
										producers.clear()
										ongoingTxns.clear()
										unreplicatedTxns.clear()
										loadFromSnapshot(logStartOffset, currentTimeMs);{//从快照中读取;
											while (true) {
												latestSnapshotFile match {
													case Some(file) =>{//读取到最新的快照后;
														info(s"Loading producer state from snapshot file ${file.getName} for partition $topicPartition")
														val loadedProducers = readSnapshot(file){//ProducerStateManager.&.readSnapshot()
															val buffer = Files.readAllBytes(file.toPath)
															val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer));{//Read a struct from the buffer: 从缓存buff中读取一个Struct对象;
																Object[] objects = new Object[fields.length];
																for (int i = 0; i < fields.length; i++) {
																	try {
																		objects[i] = fields[i].type.read(buffer);
																	} catch (Exception e) {// 是下面这里抛异常;
																		throw new SchemaException("Error reading field '" + fields[i].name + "': " +(e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
																	}
																}
															}
														}
															.filter { producerEntry => isProducerRetained(producerEntry, logStartOffset) && !isProducerExpired(currentTime, producerEntry)}
														loadedProducers.foreach(loadProducerEntry)
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
	}
	
}


val current = Log(
            dir = logDir,
            config = config,
            logStartOffset = logStartOffset,
            recoveryPoint = logRecoveryPoint,
            maxProducerIdExpirationMs = maxPidExpirationMs,
            scheduler = scheduler,
            time = time,
            brokerTopicStats = brokerTopicStats)


// 报错分析1: 
LogManager.loadLogs(){
	
	for (dir <- this.logDirs) {
		
		val jobsForDir = for {
			dirContent <- Option(dir.listFiles).toList
			logDir <- dirContent if logDir.isDirectory
		  } yield {
			CoreUtils.runnable {
			  debug("Loading log '" + logDir.getName + "'")
			  val topicPartition = Log.parseTopicPartitionName(logDir)
			  val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
			  val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
			  val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)
			  
			  val current = Log(
				dir = logDir,
				config = config,
				logStartOffset = logStartOffset,
				recoveryPoint = logRecoveryPoint,
				maxProducerIdExpirationMs = maxPidExpirationMs,
				scheduler = scheduler,
				time = time,
				brokerTopicStats = brokerTopicStats);{//Log.apply()
					new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
						producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager);{//new Log()
							loadSegments();{//Log.loadSegments()
								loadSegmentFiles();{//Log.loadSegmentFiles()
									
			at java.nio.HeapByteBuffer.<init>(HeapByteBuffer.java:57)
	at java.nio.ByteBuffer.allocate(ByteBuffer.java:335)
	at org.apache.kafka.common.record.FileLogInputStream$FileChannelRecordBatch.loadBatchWithSize(FileLogInputStream.java:209)
	at org.apache.kafka.common.record.FileLogInputStream$FileChannelRecordBatch.loadFullBatch(FileLogInputStream.java:192)
	at org.apache.kafka.common.record.FileLogInputStream$FileChannelRecordBatch.ensureValid(FileLogInputStream.java:164)
	at kafka.log.LogSegment$$anonfun$recover$1.apply(LogSegment.scala:265)
	at kafka.log.LogSegment$$anonfun$recover$1.apply(LogSegment.scala:264)
	at scala.collection.Iterator$class.foreach(Iterator.scala:891)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1334)
	at scala.collection.IterableLike$class.foreach(IterableLike.scala:72)
	at scala.collection.AbstractIterable.foreach(Iterable.scala:54)
	at kafka.log.LogSegment.recover(LogSegment.scala:264)
	at kafka.log.Log.kafka$log$Log$$recoverSegment(Log.scala:335)
	at kafka.log.Log$$anonfun$loadSegmentFiles$3.apply(Log.scala:314)
	at kafka.log.Log$$anonfun$loadSegmentFiles$3.apply(Log.scala:272)
	at scala.collection.TraversableLike$WithFilter$$anonfun$foreach$1.apply(TraversableLike.scala:733)
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
	at scala.collection.TraversableLike$WithFilter.foreach(TraversableLike.scala:732)
	at kafka.log.Log.loadSegmentFiles(Log.scala:272)
	at kafka.log.Log.loadSegments(Log.scala:376)
									
									// 
									FileChannelRecordBatch.loadBatchWithSize(){
										
									}
									
								}
							}
						}
				}
				
			  if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
				this.logsToBeDeleted.add(current)
			  } else {
			  }
			}
		  }
		
	}
	
    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        cleanShutdownFile.delete()
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
	}
}



