container_1642039450749_0002_01_000004
 compact_commit (1/1) (a9d7498cb86b7a668e15132364b91187) 
 recover the failed task e25f6e1ab1c17525b429dd155d96a0dd_0.
 dataPort=43822
 
 
// taskExecutor 中抛出的 异常
	CheckpointBarrierHandler.notifyCheckpoint()
	StreamOperatorStateHandler.snapshotState()


2022-01-13 17:02:51,753 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-13 17:02:51,753 INFO  org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl [] - Could not complete snapshot 255 for operator Sink: compact_commit (1/1)#0. Failure reason: Checkpoint was declined.
org.apache.flink.runtime.checkpoint.CheckpointException: Could not complete snapshot 255 for operator Sink: compact_commit (1/1)#0. Failure reason: Checkpoint was declined.
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:241) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
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
	
	
	
	
Caused by: org.apache.flink.util.SerializedThrowable:
 Task java.util.concurrent.FutureTask@7042bcc9 rejected from java.util.concurrent.ThreadPoolExecutor@6acee30f
 [Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
	at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2063) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.reject(ThreadPoolExecutor.java:830) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1379) ~[?:1.8.0_261]
	at java.util.concurrent.AbstractExecutorService.submit(AbstractExecutorService.java:112) ~[?:1.8.0_261]
	at java.util.concurrent.Executors$DelegatedExecutorService.submit(Executors.java:678) ~[?:1.8.0_261]
	at org.apache.hudi.async.HoodieAsyncService.monitorThreads(HoodieAsyncService.java:154) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.async.HoodieAsyncService.start(HoodieAsyncService.java:133) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.AsyncCleanerService.startAsyncCleaningIfEnabled(AsyncCleanerService.java:62) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.HoodieFlinkWriteClient.startAsyncCleaning(HoodieFlinkWriteClient.java:272) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.CleanFunction.snapshotState(CleanFunction.java:84) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.trySnapshotFunctionState(StreamingFunctionUtils.java:118) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.snapshotFunctionState(StreamingFunctionUtils.java:99) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.snapshotState(AbstractUdfStreamOperator.java:89) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:205) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	... 23 more
2022-01-13 17:02:51,753 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi



2022-01-13 17:02:51,754 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Loaded instants upto : Option{val=[==>20220113170153__deltacommit__INFLIGHT]}
2022-01-13 17:02:51,754 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading HoodieTableMetaClient from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,755 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-13 17:02:51,755 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-13 17:02:51,756 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,756 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading Active commit timeline for hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,756 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Loaded instants upto : Option{val=[==>20220113170153__deltacommit__INFLIGHT]}
2022-01-13 17:02:51,757 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading HoodieTableMetaClient from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,757 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,757 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading Active commit timeline for hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,768 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,768 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading Active commit timeline for hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-13 17:02:51,771 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-13 17:02:51,771 WARN  org.apache.flink.runtime.taskmanager.Task                    [] - Sink: compact_commit (1/1)#0 (5101c3d103f67902f46584c885645d6c) switched from RUNNING to FAILED.
java.io.IOException: Could not perform checkpoint 255 for operator Sink: compact_commit (1/1)#0.
	at org.apache.flink.streaming.runtime.tasks.StreamTask.triggerCheckpointOnBarrier(StreamTask.java:963) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler.notifyCheckpoint(CheckpointBarrierHandler.java:115) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.SingleCheckpointBarrierHandler.processBarrier(SingleCheckpointBarrierHandler.java:156) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointedInputGate.handleEvent(CheckpointedInputGate.java:180) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointedInputGate.pollNext(CheckpointedInputGate.java:157) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:179) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
Caused by: org.apache.flink.runtime.checkpoint.CheckpointException: Could not complete snapshot 255 for operator Sink: compact_commit (1/1)#0. Failure reason: Checkpoint was declined.
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:241) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
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
	... 13 more
Caused by: org.apache.flink.util.SerializedThrowable: Task java.util.concurrent.FutureTask@7042bcc9 rejected from java.util.concurrent.ThreadPoolExecutor@6acee30f[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]
	at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2063) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.reject(ThreadPoolExecutor.java:830) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1379) ~[?:1.8.0_261]
	at java.util.concurrent.AbstractExecutorService.submit(AbstractExecutorService.java:112) ~[?:1.8.0_261]
	at java.util.concurrent.Executors$DelegatedExecutorService.submit(Executors.java:678) ~[?:1.8.0_261]
	at org.apache.hudi.async.HoodieAsyncService.monitorThreads(HoodieAsyncService.java:154) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.async.HoodieAsyncService.start(HoodieAsyncService.java:133) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.AsyncCleanerService.startAsyncCleaningIfEnabled(AsyncCleanerService.java:62) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.HoodieFlinkWriteClient.startAsyncCleaning(HoodieFlinkWriteClient.java:272) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.CleanFunction.snapshotState(CleanFunction.java:84) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
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
	... 13 more
2022-01-13 17:02:51,772 INFO  org.apache.flink.runtime.taskmanager.Task                    [] - Freeing task resources for Sink: compact_commit (1/1)#0 (5101c3d103f67902f46584c885645d6c).
2022-01-13 17:02:51,775 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutor           [] - Un-registering task and sending final execution state FAILED to JobManager for task Sink: compact_commit (1/1)#0 5101c3d103f67902f46584c885645d6c.




// JobMgr中 接受和 打印的 ckp 日志



2022-01-13 20:48:53,867 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       
[] - Sink: compact_commit (1/1) (a9d7498cb86b7a668e15132364b91187) 

switched from RUNNING to FAILED on container_1642039450749_0002_01_000004 @ bdnode102.hjq.com (dataPort=43822).
java.io.IOException: Could not perform checkpoint 482 for operator Sink: compact_commit (1/1)#1.
	at org.apache.flink.streaming.runtime.tasks.StreamTask.triggerCheckpointOnBarrier(StreamTask.java:963) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler.notifyCheckpoint(CheckpointBarrierHandler.java:115) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.SingleCheckpointBarrierHandler.processBarrier(SingleCheckpointBarrierHandler.java:156) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointedInputGate.handleEvent(CheckpointedInputGate.java:180) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.CheckpointedInputGate.pollNext(CheckpointedInputGate.java:157) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:179) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.lang.Thread.run(Thread.java:748) ~[?:1.8.0_261]

Caused by: org.apache.flink.runtime.checkpoint.CheckpointException: Could not complete snapshot 482 
for operator Sink: compact_commit (1/1)#1. Failure reason: Checkpoint was declined.
	at org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.snapshotState(StreamOperatorStateHandler.java:241) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
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
	... 13 more

Caused by: org.apache.flink.util.SerializedThrowable: 
Task java.util.concurrent.FutureTask@2e6727ac rejected from java.util.concurrent.ThreadPoolExecutor@1f049fa7
[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 0]

	at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2063) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.reject(ThreadPoolExecutor.java:830) ~[?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1379) ~[?:1.8.0_261]
	at java.util.concurrent.AbstractExecutorService.submit(AbstractExecutorService.java:112) ~[?:1.8.0_261]
	at java.util.concurrent.Executors$DelegatedExecutorService.submit(Executors.java:678) ~[?:1.8.0_261]
	at org.apache.hudi.async.HoodieAsyncService.monitorThreads(HoodieAsyncService.java:154) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.async.HoodieAsyncService.start(HoodieAsyncService.java:133) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.AsyncCleanerService.startAsyncCleaningIfEnabled(AsyncCleanerService.java:62) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.client.HoodieFlinkWriteClient.startAsyncCleaning(HoodieFlinkWriteClient.java:272) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.CleanFunction.snapshotState(CleanFunction.java:84) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
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
	... 13 more
	
	
2022-01-13 20:48:53,870 INFO  org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy [] - 
Calculating tasks to restart to recover the failed task e25f6e1ab1c17525b429dd155d96a0dd_0.
2022-01-13 20:48:53,870 INFO  org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy [] - 
18 tasks should be restarted to recover the failed task e25f6e1ab1c17525b429dd155d96a0dd_0. 
2022-01-13 20:48:53,870 I













