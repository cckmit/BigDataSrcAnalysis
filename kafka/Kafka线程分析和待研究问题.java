1. kafkaServer包括哪些线程?



main: Kafka$.main(Kafka.scala:66)
executor-Heartbeat: 
Controller-0-to-broker-0-send-thread: 
kafka-scheduler-n: 

TxnMarkerSenderThread-0: SelectorImpl.lockAndDoSelect(SelectorImpl.java:86); Selector.select(Selector.java:527)
transaction-log-manager-0: 
group-metadata-manager-0
controller-event-thread: ControllerEventManager$ControllerEventThread.doWork(ControllerEventManager.scala:45)

kafka-socket-acceptor-ListenerName(PLAINTEXT)-PLAINTEXT-9092
kafka-network-thread-0-ListenerName(PLAINTEXT)-PLAINTEXT-n: SelectorImpl.select(SelectorImpl.java:97)

kafka-log-cleaner-thread-0: LogCleaner$CleanerThread.cleanOrSleep(LogCleaner.scala:260)
metrics-meter-tick-thread-n

SensorExpiryThread: 
main-EventThread
main-SendThread(ldsver51:2181): SelectorImpl.select(SelectorImpl.java:97)
ZkClient-EventThread-20-ldsver51:2181/kafka : ZkEventThread.run(ZkEventThread.java:68): Send-thread用于与zk发送心跳，接收zk事件响应、main-eventThread用于发布事件

ExpirationReaper-0-Rebalance: 
ExpirationReaper-0-Heartbeat: 
ExpirationReaper-0-topic: DelayedOperationPurgatory.advanceClock
ExpirationReaper-0-DeleteRecords: DelayedOperationPurgatory.advanceClock(DelayedOperation.scala:383)
ExpirationReaper-0-Fetch: DelayQueue.poll(DelayQueue.java:259)
ExpirationReaper-0-Produce: DelayedOperationPurgatory.advanceClock(DelayedOperation.scala:383)


ThrottledRequestReaper-Request: ClientQuotaManager$ThrottledRequestReaper.doWork(ClientQuotaManager.scala:164)
ThrottledRequestReaper-Produce: ClientQuotaManager$ThrottledRequestReaper.doWork(ClientQuotaManager.scala:164)
ThrottledRequestReaper-Fetch: 

文章:
* https://blog.csdn.net/qq_22929803/article/details/51154865







kafka-request-handler-n: RequestChannel.receiveRequest(RequestChannel.scala:269)

/** "kafka-request-handler-n"线程: 接收处理Client端 Api请求的处理线程: 
*
*/

KafkaApis.handle(request){
	{// 启动
		RequestChannel.receiveRequest(RequestChannel.scala:269)
	}
	
	ApiKeys.forId(request.requestId) match {
        case ApiKeys.PRODUCE => handleProduceRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
        case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
        case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
        case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request);
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request)
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
	
}








