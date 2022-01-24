2022-01-22 14:43:31,153 INFO  org.apache.hudi.common.table.view.AbstractTableFileSystemView [] - addFilesToView: NumFiles=0, NumFileGroups=0, FileGroupsCreationTime=0, StoreTimeTaken=0
2022-01-22 14:43:31,155 INFO  org.apache.hudi.io.HoodieAppendHandle                        [] - New AppendHandle for partition :2022-01-22/00

hdfs DFSClient.callAppend

2022-01-22 14:43:31,157 WARN  org.apache.hudi.common.table.log.HoodieLogFormatWriter       [] - Remote Exception, attempting to handle or recover lease
org.apache.hadoop.ipc.RemoteException: Failed to APPEND_FILE /hudi/lakehouse2_dwd_order_hudi/2022-01-17/16/.0480d22a-f42b-49a2-90df-a3edc9dc2545_20220121160252.log.1_0-2-11 for DFSClient_NONMAPREDUCE_80015576_68 on 192.168.51.102 because this file lease is currently owned by DFSClient_NONMAPREDUCE_186736270_67 on 192.168.51.102
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.recoverLeaseInternal(FSNamesystem.java:2931)
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.appendFileInternal(FSNamesystem.java:2683)
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.appendFileInt(FSNamesystem.java:2982)
	at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.appendFile(FSNamesystem.java:2950)
	at org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer.append(NameNodeRpcServer.java:654)
	at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB.append(ClientNamenodeProtocolServerSideTranslatorPB.java:421)
	at org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos$ClientNamenodeProtocol$2.callBlockingMethod(ClientNamenodeProtocolProtos.java)
	at org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.call(ProtobufRpcEngine.java:616)
	at org.apache.hadoop.ipc.RPC$Server.call(RPC.java:969)
	at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2049)
	at org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2045)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657)
	at org.apache.hadoop.ipc.Server$Handler.run(Server.java:2043)

	at org.apache.hadoop.ipc.Client.call(Client.java:1475) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.ipc.Client.call(Client.java:1412) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.ipc.ProtobufRpcEngine$Invoker.invoke(ProtobufRpcEngine.java:229) ~[hadoop-common-2.7.2.jar:?]
	at com.sun.proxy.$Proxy34.append(Unknown Source) ~[?:?]
	at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB.append(ClientNamenodeProtocolTranslatorPB.java:328) ~[hadoop-hdfs-2.7.2.jar:?]
	at sun.reflect.GeneratedMethodAccessor46.invoke(Unknown Source) ~[?:?]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:191) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:102) ~[hadoop-common-2.7.2.jar:?]
	at com.sun.proxy.$Proxy35.append(Unknown Source) ~[?:?]
	at org.apache.hadoop.hdfs.DFSClient.callAppend(DFSClient.java:1808) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DFSClient.append(DFSClient.java:1877) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DFSClient.append(DFSClient.java:1847) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem$4.doCall(DistributedFileSystem.java:340) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem$4.doCall(DistributedFileSystem.java:336) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.fs.FileSystemLinkResolver.resolve(FileSystemLinkResolver.java:81) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem.append(DistributedFileSystem.java:336) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem.append(DistributedFileSystem.java:318) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.fs.FileSystem.append(FileSystem.java:1174) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hudi.common.fs.HoodieWrapperFileSystem.append(HoodieWrapperFileSystem.java:506) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.common.table.log.HoodieLogFormatWriter.getOutputStream(HoodieLogFormatWriter.java:108) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.common.table.log.HoodieLogFormatWriter.appendBlocks(HoodieLogFormatWriter.java:151) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.io.HoodieAppendHandle.appendDataAndDeleteBlocks(HoodieAppendHandle.java:370) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.io.HoodieAppendHandle.doAppend(HoodieAppendHandle.java:353) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.action.commit.delta.BaseFlinkDeltaCommitActionExecutor.handleUpdate(BaseFlinkDeltaCommitActionExecutor.java:52) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.action.commit.BaseFlinkCommitActionExecutor.handleUpsertPartition(BaseFlinkCommitActionExecutor.java:190) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.action.commit.BaseFlinkCommitActionExecutor.execute(BaseFlinkCommitActionExecutor.java:108) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.action.commit.BaseFlinkCommitActionExecutor.execute(BaseFlinkCommitActionExecutor.java:70) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.action.commit.FlinkWriteHelper.write(FlinkWriteHelper.java:71) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.action.commit.delta.FlinkUpsertDeltaCommitActionExecutor.execute(FlinkUpsertDeltaCommitActionExecutor.java:49) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.table.HoodieFlinkMergeOnReadTable.upsert(HoodieFlinkMergeOnReadTable.java:64) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.HoodieFlinkWriteClient.upsert(HoodieFlinkWriteClient.java:151) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.lambda$initWriteFunction$1(StreamWriteFunction.java:297) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.lambda$flushRemaining$7(StreamWriteFunction.java:642) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.LinkedHashMap$LinkedValues.forEach(LinkedHashMap.java:608) ~[?:1.8.0_261]
	at org.apache.hudi.sink.StreamWriteFunction.flushRemaining(StreamWriteFunction.java:635) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.snapshotState(StreamWriteFunction.java:226) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.trySnapshotFunctionState(StreamingFunctionUtils.java:118) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.snapshotFunctionState(StreamingFunctionUtils.java:99) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.snapshotState(AbstractUdfStreamOperator.java:89) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:205) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:162) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.AbstractStreamOperator.snapshotState(AbstractStreamOperator.java:371) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.checkpointStreamOperator(SubtaskCheckpointCoordinatorImpl.java:686) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.buildOperatorSnapshotFutures(SubtaskCheckpointCoordinatorImpl.java:607) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.takeSnapshotSync(SubtaskCheckpointCoordinatorImpl.java:572) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.checkpointState(SubtaskCheckpointCoordinatorImpl.java:298) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$performCheckpoint$9(StreamTask.java:1004) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$1.runThrowing(StreamTaskActionExecutor.java:50) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.performCheckpoint(StreamTask.java:988) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.triggerCheckpointOnBarrier(StreamTask.java:947) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler.notifyCheckpoint(CheckpointBarrierHandler.java:115) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.SingleCheckpointBarrierHandler.processBarrier(SingleCheckpointBarrierHandler.java:156) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointedInputGate.handleEvent(CheckpointedInputGate.java:180) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointedInputGate.pollNext(CheckpointedInputGate.java:157) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:179) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
2022-01-22 14:43:31,165 WARN  org.apache.hudi.common.table.log.HoodieLogFormatWriter       [] - Another task executor writing to the same log file(HoodieLogFile{pathStr='hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/2022-01-17/16/.0480d22a-f42b-49a2-90df-a3edc9dc2545_20220121160252.log.1_0-2-11', fileLen=0}. Rolling over
2022-01-22 14:43:31,175 INFO  org.apache.hudi.table.marker.DirectWriteMarkers              [] - Creating Marker Path=hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/.temp/20220122144312/2022-01-22/00/a803378d-9abe-4f43-ac86-89f5175736fa_1-2-12_20220122144312.parquet.marker.APPEND
2022-01-22 14:43:31,176 INFO  org.apache.hudi.io.HoodieAppendHandle                        [] - AppendHandle for partitionPath 2022-01-17/16 filePath 2022-01-17/16/.0480d22a-f42b-49a2-90df-a3edc9dc2545_20220121160252.log.2_0-2-12, took 119 ms.


 