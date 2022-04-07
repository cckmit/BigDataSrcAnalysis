

// 1.  FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold
  mysql 大表查询 超长时间; 

Caused by: org.apache.flink.util.FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold.
    at org.apache.flink.runtime.checkpoint.CheckpointFailureManager.handleCheckpointException(CheckpointFailureManager.java:98)
    at org.apache.flink.runtime.checkpoint.CheckpointFailureManager.handleJobLevelCheckpointException(CheckpointFailureManager.java:67)
    at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.abortPendingCheckpoint(CheckpointCoordinator.java:1915)
    at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.abortPendingCheckpoint(CheckpointCoordinator.java:1888)
    at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.access$600(CheckpointCoordinator.java:94)
    at org.apache.flink.runtime.checkpoint.CheckpointCoordinator$CheckpointCanceller.run(CheckpointCoordinator.java:2029)
    at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
    at java.util.concurrent.FutureTask.run(FutureTask.java:266)
    at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
    at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)


	// 因 ckp expired过期导致的 ckp取消; 
	CheckpointCoordinator.CheckpointCanceller.run(){
		if (!pendingCheckpoint.isDisposed()) {
			LOG.info("Checkpoint {} of job {} expired before completing.",pendingCheckpoint.getCheckpointId(),job);
			// CHECKPOINT_EXPIRED: "Checkpoint expired before completing."
			abortPendingCheckpoint(pendingCheckpoint,new CheckpointException(CHECKPOINT_EXPIRED));{
				// CheckpointCoordinator.abortPendingCheckpoint(pendingCheckpoint,exception) 详解如下: 
			}
		}
	}

	CheckpointCoordinator.abortPendingCheckpoint(pendingCheckpoint,exception){
		abortPendingCheckpoint(pendingCheckpoint, exception, null);{
			if (!pendingCheckpoint.isDisposed()) {
				try {
					pendingCheckpoint.abort();
					if (pendingCheckpoint.getProps().isSavepoint() && pendingCheckpoint.getProps().isSynchronous()) {
						failureManager.handleSynchronousSavepointFailure(exception);
					} else if (executionAttemptID != null) {
						failureManager.handleTaskLevelCheckpointException(exception, checkpointId(), executionAttemptID);
					} else {// 
						failureManager.handleJobLevelCheckpointException(exception, checkpointId()){//CheckpointFailureManager.handleJobLevelCheckpointException()
							handleCheckpointException(exception, checkpointId, failureCallback::failJob);{
								if (checkpointId > lastSucceededCheckpointId) {
									checkFailureCounter(exception, checkpointId);
									if (continuousFailureCounter.get() > tolerableCpFailureNumber) {
										clearCount();
										// 即此异常 超出ckp可容易上限: FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold
										errorHandler.accept(new FlinkRuntimeException(EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE));
									}
								}
							}
						}
					}
				}finally {
					sendAbortedMessages();
					pendingCheckpoints.remove(pendingCheckpoint.getCheckpointId());
					scheduleTriggerRequest();
				}
			}
		}
	}




2021-06-16 15:07:26,168 WARN  org.apache.flink.runtime.taskmanager.Task                    [] - Task 'Interval Join (1/1)#0' did not react to cancelling signal for 30 seconds, but is stuck in method:
 java.net.SocketInputStream.socketRead0(Native Method)
java.net.SocketInputStream.socketRead(SocketInputStream.java:116)
java.net.SocketInputStream.read(SocketInputStream.java:171)
java.net.SocketInputStream.read(SocketInputStream.java:141)
com.mysql.jdbc.util.ReadAheadInputStream.fill(ReadAheadInputStream.java:101)
com.mysql.jdbc.util.ReadAheadInputStream.readFromUnderlyingStreamIfNecessary(ReadAheadInputStream.java:144)
com.mysql.jdbc.util.ReadAheadInputStream.read(ReadAheadInputStream.java:174)
com.mysql.jdbc.MysqlIO.readFully(MysqlIO.java:3008)
com.mysql.jdbc.MysqlIO.nextRowFast(MysqlIO.java:2064)
com.mysql.jdbc.MysqlIO.nextRow(MysqlIO.java:1989)
com.mysql.jdbc.MysqlIO.readSingleRowSet(MysqlIO.java:3410)
com.mysql.jdbc.MysqlIO.getResultSet(MysqlIO.java:470)
com.mysql.jdbc.MysqlIO.readResultsForQueryOrUpdate(MysqlIO.java:3112)
com.mysql.jdbc.MysqlIO.readAllResults(MysqlIO.java:2341)
com.mysql.jdbc.MysqlIO.sqlQueryDirect(MysqlIO.java:2736)
com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2487)
com.mysql.jdbc.PreparedStatement.executeInternal(PreparedStatement.java:1858)
com.mysql.jdbc.PreparedStatement.executeQuery(PreparedStatement.java:1966)
com.report.util.DbUtil.queryKv(DbUtil.java:193)
com.report.function.HuiChatWindowFunction.processElement(HuiChatWindowFunction.java:495)
com.report.function.HuiChatWindowFunction.processElement(HuiChatWindowFunction.java:38)
org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.collect(IntervalJoinOperator.java:274)
org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.processElement(IntervalJoinOperator.java:248)
org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.processElement2(IntervalJoinOperator.java:208)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.processRecord2(StreamTwoInputProcessorFactory.java:207)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.lambda$create$1(StreamTwoInputProcessorFactory.java:176)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory$$Lambda$760/267817329.accept(Unknown Source)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory$StreamTaskNetworkOutput.emitRecord(StreamTwoInputProcessorFactory.java:277)
org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.processElement(StreamTaskNetworkInput.java:204)
org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:174)
org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor.processInput(StreamTwoInputProcessor.java:97)
org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396)
org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$568/407559166.runDefaultAction(Unknown Source)
org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191)
org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617)
org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581)
org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755)
org.apache.flink.runtime.taskmanager.Task.run(Task.java:570)
java.lang.Thread.run(Thread.java:748)

2021-06-16 15:07:56,172 WARN  org.apache.flink.runtime.taskmanager.Task                    
[] - Task 'Interval Join (1/1)#0' did not react to cancelling signal for 30 seconds, but is stuck in method:
 java.net.SocketInputStream.socketRead0(Native Method)
java.net.SocketInputStream.socketRead(SocketInputStream.java:116)
java.net.SocketInputStream.read(SocketInputStream.java:171)
java.net.SocketInputStream.read(SocketInputStream.java:141)
com.mysql.jdbc.util.ReadAheadInputStream.fill(ReadAheadInputStream.java:101)
com.mysql.jdbc.util.ReadAheadInputStream.readFromUnderlyingStreamIfNecessary(ReadAheadInputStream.java:144)
com.mysql.jdbc.util.ReadAheadInputStream.read(ReadAheadInputStream.java:174)
com.mysql.jdbc.MysqlIO.readFully(MysqlIO.java:3008)
com.mysql.jdbc.MysqlIO.nextRowFast(MysqlIO.java:2064)
com.mysql.jdbc.MysqlIO.nextRow(MysqlIO.java:1989)
com.mysql.jdbc.MysqlIO.readSingleRowSet(MysqlIO.java:3410)
com.mysql.jdbc.MysqlIO.getResultSet(MysqlIO.java:470)
com.mysql.jdbc.MysqlIO.readResultsForQueryOrUpdate(MysqlIO.java:3112)
com.mysql.jdbc.MysqlIO.readAllResults(MysqlIO.java:2341)
com.mysql.jdbc.MysqlIO.sqlQueryDirect(MysqlIO.java:2736)
com.mysql.jdbc.ConnectionImpl.execSQL(ConnectionImpl.java:2487)
com.mysql.jdbc.PreparedStatement.executeInternal(PreparedStatement.java:1858)
com.mysql.jdbc.PreparedStatement.executeQuery(PreparedStatement.java:1966)
com.report.util.DbUtil.queryKv(DbUtil.java:193)
com.report.function.HuiChatWindowFunction.processElement(HuiChatWindowFunction.java:495)
com.report.function.HuiChatWindowFunction.processElement(HuiChatWindowFunction.java:38)
org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.collect(IntervalJoinOperator.java:274)
org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.processElement(IntervalJoinOperator.java:248)
org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.processElement2(IntervalJoinOperator.java:208)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.processRecord2(StreamTwoInputProcessorFactory.java:207)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory.lambda$create$1(StreamTwoInputProcessorFactory.java:176)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory$$Lambda$760/267817329.accept(Unknown Source)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessorFactory$StreamTaskNetworkOutput.emitRecord(StreamTwoInputProcessorFactory.java:277)
org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.processElement(StreamTaskNetworkInput.java:204)
org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:174)
org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor.processInput(StreamTwoInputProcessor.java:97)
org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396)
org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$568/407559166.runDefaultAction(Unknown Source)
org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191)
org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617)
org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581)
org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755)
org.apache.flink.runtime.taskmanager.Task.run(Task.java:570)
java.lang.Thread.run(Thread.java:748)






// 2. 

 2020-06-10 12:02:49,083 INFO org.apache.flink.runtime.checkpoint.CheckpointCoordinator - Triggering checkpoint 1 @ 1591761769060 for job c41f4811262db1c4c270b136571c8201. 
 2020-06-10 12:04:47,898 INFO org.apache.flink.runtime.checkpoint.CheckpointCoordinator - Decline checkpoint 1 by task 0cb03590fdf18027206ef628b3ef5863 of job c41f4811262db1c4c270b136571c8201 at container_e27_1591466310139_21670_01_000006 @ hdp1-hadoop-datanode-4.novalocal (dataPort=44778). 
 2020-06-10 12:04:47,899 INFO org.apache.flink.runtime.checkpoint.CheckpointCoordinator - Discarding checkpoint 1 of job c41f4811262db1c4c270b136571c8201. org.apache.flink.runtime.checkpoint.CheckpointException: Could not complete snapshot 1 for operator Source: Custom Source -> Map -> Source_Map -> Empty_Filer -> Field_Filter -> Type_Filter -> Value_Filter -> Map -> Map -> Map -> Sink: Unnamed (7/12). 
 Failure reason: Checkpoint was declined. at org.apache.flink.streaming.api.operators.AbstractStreamOperator.snapshotState(AbstractStreamOperator.java:434) 
 at org.apache.flink.streaming.runtime.tasks.StreamTask$CheckpointingOperation.checkpointStreamOperator(StreamTask.java:1420) 
 at org.apache.flink.streaming.runtime.tasks.StreamTask$CheckpointingOperation.executeCheckpointing(StreamTask.java:1354) at org.apache.flink.streaming.runtime.tasks.StreamTask.checkpointState(StreamTask.java:991) at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$performCheckpoint$5(StreamTask.java:887) 
 at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.runThrowing(StreamTaskActionExecutor.java:94) at org.apache.flink.streaming.runtime.tasks.StreamTask.performCheckpoint(StreamTask.java:860) at org.apache.flink.streaming.runtime.tasks.StreamTask.triggerCheckpoint(StreamTask.java:793) 
 at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$triggerCheckpointAsync$3(StreamTask.java:777) at java.util.concurrent.FutureTask.run(FutureTask.java:266) at org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor$SynchronizedStreamTaskActionExecutor.run(StreamTaskActionExecutor.java:87) at org.apache.flink.streaming.runtime.tasks.mailbox.Mail.run(Mail.java:78) at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.processMail(MailboxProcessor.java:261) 
 at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:186) at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:487) at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:470) at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:707) 
 at org.apache.flink.runtime.taskmanager.Task.run(Task.java:532) at java.lang.Thread.run(Thread.java:748) Caused by: org.apache.flink.streaming.connectors.kafka.FlinkKafkaException: Failed to send data to Kafka: The server disconnected before a response was received. at org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.checkErroneous(FlinkKafkaProducer.java:1218) at org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.flush(FlinkKafkaProducer.java:973) at org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.preCommit(FlinkKafkaProducer.java:892) at org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.preCommit(FlinkKafkaProducer.java:98) 
 at org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.snapshotState(TwoPhaseCommitSinkFunction.java:317) at org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.snapshotState(FlinkKafkaProducer.java:978) at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.trySnapshotFunctionState(StreamingFunctionUtils.java:118) at org.apache.flink.streaming.util.functions.StreamingFunctionUtils.snapshotFunctionState(StreamingFunctionUtils.java:99) 
 at org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator.snapshotState(AbstractUdfStreamOperator.java:90) at org.apache.flink.streaming.api.operators.AbstractStreamOperator.snapshotStat(AbstractStreamOperator.java:402) ... 18 more Caused by: org.apache.kafka.common.errors.NetworkException: The server disconnected before a response was received. 
 
 2020-06-10 12:04:47,913 INFO org.apache.flink.runtime.jobmaster.JobMaster - Trying to recover from a global failure. org.apache.flink.util.FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold. 
 at org.apache.flink.runtime.checkpoint.CheckpointFailureManager.handleTaskLevelCheckpointException(CheckpointFailureManager.java:87) at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.failPendingCheckpointDueToTaskFailure(CheckpointCoordinator.java:1467) 
 at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.discardCheckpoint(CheckpointCoordinator.java:1377) at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.receiveDeclineMessage(CheckpointCoordinator.java:719) at org.apache.flink.runtime.scheduler.SchedulerBase.lambda$declineCheckpoint$5(SchedulerBase.java:807) at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) at java.util.concurrent.FutureTask.run(FutureTask.java:266) 
 at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180) at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293) at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) 
 at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) at java.lang.Thread.run(Thread.java:748)




// 3. 缺乏 hdfs in class path 
Could not find a file system implementation for scheme 'hdfs'. The scheme is not directly support
ed by Flink and no Hadoop file system to support this scheme could be loaded

Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Hadoop is not in the class

Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Hadoop is not in the classpath/dependencies.
	at org.apache.flink.core.fs.UnsupportedSchemeFactory.create(UnsupportedSchemeFactory.java:58)
	at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:446)
	... 29 more
2020-10-18 22:25:31,370 ERROR org.apache.flink.runtime.webmonitor.handlers.JarRunHandler    - Unhandled exception.
org.apache.flink.runtime.client.JobSubmissionException: Failed to submit job.
	at org.apache.flink.runtime.dispatcher.Dispatcher.lambda$internalSubmitJob$3(Dispatcher.java:336)
	at java.util.concurrent.CompletableFuture.uniHandle(CompletableFuture.java:822)
	at java.util.concurrent.CompletableFuture$UniHandle.tryFire(CompletableFuture.java:797)
	at java.util.concurrent.CompletableFuture$Completion.run(CompletableFuture.java:442)
	at akka.dispatch.TaskInvocation.run(AbstractDispatcher.scala:40)
	at akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(ForkJoinExecutorConfigurator.scala:44)
	at akka.dispatch.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
	at akka.dispatch.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)
	at akka.dispatch.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)
	at akka.dispatch.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)
Caused by: java.lang.RuntimeException: org.apache.flink.runtime.client.JobExecutionException: Could not set up JobManager
	at org.apache.flink.util.function.CheckedSupplier.lambda$unchecked$0(CheckedSupplier.java:36)
	at java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1590)
	... 6 more
Caused by: org.apache.flink.runtime.client.JobExecutionException: Could not set up JobManager
	at org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl.<init>(JobManagerRunnerImpl.java:152)
	at org.apache.flink.runtime.dispatcher.DefaultJobManagerRunnerFactory.createJobManagerRunner(DefaultJobManagerRunnerFactory.java:84)
	at org.apache.flink.runtime.dispatcher.Dispatcher.lambda$createJobManagerRunner$6(Dispatcher.java:379)
	at org.apache.flink.util.function.CheckedSupplier.lambda$unchecked$0(CheckedSupplier.java:34)
	... 7 more
Caused by: org.apache.flink.util.FlinkRuntimeException: Failed to create checkpoint storage at checkpoint coordinator side.
	at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.<init>(CheckpointCoordinator.java:282)
	at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.<init>(CheckpointCoordinator.java:205)
	at org.apache.flink.runtime.executiongraph.ExecutionGraph.enableCheckpointing(ExecutionGraph.java:486)
	at org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder.buildGraph(ExecutionGraphBuilder.java:338)
	at org.apache.flink.runtime.scheduler.SchedulerBase.createExecutionGraph(SchedulerBase.java:255)
	at org.apache.flink.runtime.scheduler.SchedulerBase.createAndRestoreExecutionGraph(SchedulerBase.java:227)
	at org.apache.flink.runtime.scheduler.SchedulerBase.<init>(SchedulerBase.java:215)
	at org.apache.flink.runtime.scheduler.DefaultScheduler.<init>(DefaultScheduler.java:120)
	at org.apache.flink.runtime.scheduler.DefaultSchedulerFactory.createInstance(DefaultSchedulerFactory.java:105)
	at org.apache.flink.runtime.jobmaster.JobMaster.createScheduler(JobMaster.java:278)
	at org.apache.flink.runtime.jobmaster.JobMaster.<init>(JobMaster.java:266)
	at org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory.createJobMasterService(DefaultJobMasterServiceFactory.java:98)
	at org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory.createJobMasterService(DefaultJobMasterServiceFactory.java:40)
	at org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl.<init>(JobManagerRunnerImpl.java:146)
	... 10 more
Caused by: org.apache.flink.core.fs.UnsupportedFileSystemSchemeException: Could not find a file system implementation for scheme 'hdfs'. The scheme is not directly support
ed by Flink and no Hadoop file system to support this scheme could be loaded.
	at org.apache.flink.core.fs.FileSystem.getUnguardedFileSystem(FileSystem.java:450)
	at org.apache.flink.core.fs.FileSystem.get(FileSystem.java:362)
	at org.apache.flink.core.fs.Path.getFileSystem(Path.java:298)
	at org.apache.flink.runtime.state.filesystem.FsCheckpointStorage.<init>(FsCheckpointStorage.java:64)
	at org.apache.flink.runtime.state.filesystem.FsStateBackend.createCheckpointStorage(FsStateBackend.java:490)
	at org.apache.flink.contrib.streaming.state.RocksDBStateBackend.createCheckpointStorage(RocksDBStateBackend.java:477)
	at org.apache.flink.runtime.checkpoint.CheckpointCoordinator.<init>(CheckpointCoordinator.java:279)
	... 23 more





// 4. Received late message for now expired checkpoint 	FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold



2021-08-18 18:49:35,383 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 8e8ad4a5e66753df9bacb838c4a8c195 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000016 @ xxxxxxxxx (dataPort=26682).

2021-08-18 18:28:40,502 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 1 (type=CHECKPOINT) @ 1629282520408 for job 1cba827ad8a8d68521605157b77a6191.
2021-08-18 18:38:40,502 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Checkpoint 1 of job 1cba827ad8a8d68521605157b77a6191 expired before completing.
2021-08-18 18:38:40,527 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 2 (type=CHECKPOINT) @ 1629283120507 for job 1cba827ad8a8d68521605157b77a6191.
2021-08-18 18:48:40,528 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Checkpoint 2 of job 1cba827ad8a8d68521605157b77a6191 expired before completing.
2021-08-18 18:48:40,581 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 3 (type=CHECKPOINT) @ 1629283720528 for job 1cba827ad8a8d68521605157b77a6191.
2021-08-18 18:49:35,383 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 8e8ad4a5e66753df9bacb838c4a8c195 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000016 @ (dataPort=26682).
2021-08-18 18:49:41,220 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 6c425a7bf2cdb6f9eaff230e657e7b71 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000028 @(dataPort=26329).
2021-08-18 18:51:23,142 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 4aca7ff37807d7c53dab4af4d7317a92 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000015 @ (dataPort=28244).
2021-08-18 18:51:43,449 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task f410376b8a4c4482529b47d615d0ef7d of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000027 @ (dataPort=35241).
2021-08-18 18:52:14,054 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task b0b4e5bc960fd38f09ed136eabc92207 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000011 @  (dataPort=17858).
2021-08-18 18:53:15,172 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 254800e15801981e9a5fcaf8f8374f80 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000019 @ (dataPort=28753).
2021-08-18 18:54:23,649 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task bda7204e335e5818502024d14b9cf515 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000032 @ (dataPort=37476).
2021-08-18 18:54:53,507 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 9839eef45f9224ddbbd3383cf962ea75 of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000026 @ (dataPort=31847).
2021-08-18 18:57:07,707 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 2 from task 6b91c567ac5eb48d6d8c4c692327c95d of job 1cba827ad8a8d68521605157b77a6191 at container_e150_1628460895322_672063_01_000028 @  (dataPort=26329).
2021-08-18 18:58:40,581 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Checkpoint 3 of job 1cba827ad8a8d68521605157b77a6191 expired before completing.
2021-08-18 18:58:40,604 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 4 (type=CHECKPOINT) @ 1629284320583 for job 1cba827ad8a8d68521605157b77a6191.
2021-08-18 18:58:40,590 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Trying to recover from a global failure.
org.apache.flink.util.FlinkRuntimeException: Exceeded checkpoint tolerable failure threshold.

jobmanager定时trigger checkpoint，给source处发送trigger信号，同时会启动一个异步线程，在checkpoint timeout时长之后停止本轮 checkpoint，cancel动作执行之后本轮的checkpoint就为超时，如果在超时之前收到了最后一个sink算子的ack信号，那么checkpoint就是成功的。

超时原因一般有两种：
        1. barrier对齐超时

        2. 异步状态遍历和写HDFS超时（比如State太大）

解决：     

        这个job 的sink是写入ES中，由于ES集群扩容了一些机器，因为跨机房导致ES端有问题，从而导致消费端有问题从而出现反压，从而导致barrier对齐时间过长 而导致checkpoint失



// ckp oom 失败: 


Checkpoint Coordinator is suspending
Triggered: 6514In Progress: 0Completed: 6486Failed: 28Restored: 35

2022-01-25 22:57:35
java.lang.OutOfMemoryError: Java heap space
	at java.util.ArrayDeque.allocateElements(ArrayDeque.java:147)
	at java.util.ArrayDeque.<init>(ArrayDeque.java:203)
	at org.apache.hudi.common.util.ObjectSizeCalculator.<init>(ObjectSizeCalculator.java:93)
	at org.apache.hudi.common.util.ObjectSizeCalculator.getObjectSize(ObjectSizeCalculator.java:74)
	at org.apache.hudi.sink.StreamWriteFunction$BufferSizeDetector.detect(StreamWriteFunction.java:456)
	at org.apache.hudi.sink.StreamWriteFunction.bufferRecord(StreamWriteFunction.java:537)
	at org.apache.hudi.sink.StreamWriteFunction.processElement(StreamWriteFunction.java:233)
	at org.apache.flink.streaming.api.operators.KeyedProcessOperator.processElement(KeyedProcessOperator.java:83)
	at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask$StreamTaskNetworkOutput.emitRecord(OneInputStreamTask.java:191)
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.processElement(StreamTaskNetworkInput.java:204)
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:174)
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396)
	at org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$195/35229817.runDefaultAction(Unknown Source)
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581)
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570)
	at java.lang.Thread.run(Thread.java:748)

Latest Restore	ID: 3264Restore Time: 2022-01-25 22:57:39Type: CheckpointPath: hdfs://bdnode102:9000/flink/flink-checkpoints/61562e236aa6e8b51fa2c6e66023472a/chk-3264
3264Restore. 2022-01-25 22:57, chk-3264, 


// JobManger中日志

2022-01-25 22:56:58,333 INFO  hive.metastore                                               [] - Closed a connection to metastore, current connections: 0
2022-01-25 22:56:58,333 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220125225658] error
java.lang.NoSuchMethodError: org.apache.hadoop.hive.serde2.thrift.Type.getType(Lorg/apache/hudi/org/apache/hive/service/rpc/thrift/TTypeId;)Lorg/apache/hadoop/hive/serde2/thrift/Type;
	at org.apache.hudi.org.apache.hive.service.cli.TypeDescriptor.<init>(TypeDescriptor.java:47) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.service.cli.ColumnDescriptor.<init>(ColumnDescriptor.java:46) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.service.cli.TableSchema.<init>(TableSchema.java:46) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveQueryResultSet.retrieveSchema(HiveQueryResultSet.java:264) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveQueryResultSet.<init>(HiveQueryResultSet.java:198) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveQueryResultSet$Builder.build(HiveQueryResultSet.java:179) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveDatabaseMetaData.getColumns(HiveDatabaseMetaData.java:232) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.getTableSchema(JDBCExecutor.java:121) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.getTableSchema(HoodieHiveClient.java:203) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncSchema(HiveSyncTool.java:241) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:182) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.doSync(HiveSyncTool.java:135) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:117) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.syncHive(StreamWriteOperatorCoordinator.java:279) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:67) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]

2022-01-25 22:57:12,282 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 3265 (type=CHECKPOINT) @ 1643122632241 for job 61562e236aa6e8b51fa2c6e66023472a.
2022-01-25 22:57:12,282 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [taking checkpoint 3265] success!
2022-01-25 22:57:35,731 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       
	[] - hoodie_stream_write (1/2) (bf4bf8d733e875e93ee686e3d09c973f) switched from RUNNING to FAILED on container_1642990216503_0004_01_000012 @ bdnode102.hjq.com (dataPort=40090).
java.lang.OutOfMemoryError: Java heap space
	at java.util.ArrayDeque.allocateElements(ArrayDeque.java:147) ~[?:1.8.0_261]
	at java.util.ArrayDeque.<init>(ArrayDeque.java:203) ~[?:1.8.0_261]
	at org.apache.hudi.common.util.ObjectSizeCalculator.<init>(ObjectSizeCalculator.java:93) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.common.util.ObjectSizeCalculator.getObjectSize(ObjectSizeCalculator.java:74) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction$BufferSizeDetector.detect(StreamWriteFunction.java:456) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.bufferRecord(StreamWriteFunction.java:537) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.processElement(StreamWriteFunction.java:233) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.flink.streaming.api.operators.KeyedProcessOperator.processElement(KeyedProcessOperator.java:83) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask$StreamTaskNetworkOutput.emitRecord(OneInputStreamTask.java:191) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.processElement(StreamTaskNetworkInput.java:204) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:174) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$195/35229817.runDefaultAction(Unknown Source) ~[?:?]
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.lang.Thread.run(Thread.java:748) ~[?:1.8.0_261]
	
	


	
	
2022-01-25 22:57:35,748 WARN  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Reset the event for task [0]
java.lang.OutOfMemoryError: Java heap space
	at java.util.ArrayDeque.allocateElements(ArrayDeque.java:147) ~[?:1.8.0_261]
	at java.util.ArrayDeque.<init>(ArrayDeque.java:203) ~[?:1.8.0_261]
	at org.apache.hudi.common.util.ObjectSizeCalculator.<init>(ObjectSizeCalculator.java:93) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.common.util.ObjectSizeCalculator.getObjectSize(ObjectSizeCalculator.java:74) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction$BufferSizeDetector.detect(StreamWriteFunction.java:456) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.bufferRecord(StreamWriteFunction.java:537) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteFunction.processElement(StreamWriteFunction.java:233) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.flink.streaming.api.operators.KeyedProcessOperator.processElement(KeyedProcessOperator.java:83) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask$StreamTaskNetworkOutput.emitRecord(OneInputStreamTask.java:191) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.processElement(StreamTaskNetworkInput.java:204) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.emitNext(StreamTaskNetworkInput.java:174) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:396) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$195/35229817.runDefaultAction(Unknown Source) ~[?:?]
	at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:191) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:617) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:581) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:755) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:570) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.lang.Thread.run(Thread.java:748) ~[?:1.8.0_261]
2022-01-25 22:57:35,748 INFO  org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy [] - Calculating tasks to restart to recover the failed task b343aa6873ffe27b3e05ab523d3dbace_0.
2022-01-25 22:57:35,749 INFO  org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy [] - 8 tasks should be restarted to recover the failed task b343aa6873ffe27b3e05ab523d3dbace_0. 
2022-01-25 22:57:35,749 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job default: INSERT INTO lakehouse2_dwd_order_hudi SELECT DATE_FORMAT(ods.gmt_create, 'yyyy-MM-dd') AS dt, DATE_FORMAT(ods.gmt_create, 'HH') AS hr, ods.order_id, ods.cate_id, dim.parent_cate_id, ods.trans_amount, ods.gmt_create FROM lakehouse2_ods_order_cdc ods LEFT JOIN lakehouse2_dim_order_category FOR SYSTEM_TIME AS OF ods.ptime AS dim ON dim.cate_id = ods.cate_id (61562e236aa6e8b51fa2c6e66023472a) switched from state RUNNING to RESTARTING.
2022-01-25 22:57:35,749 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Sink: compact_commit (1/1) (a7da93962d52d2824efc548496c8f504) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,750 WARN  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Reset the event for task [1]
2022-01-25 22:57:35,751 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (2/2) (0c6519dba2d9c2d05f64b6a99a5a0c83) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,751 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - bucket_assigner (1/1) (6c2c7ea3d444315e0d3b771bc3033d74) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,751 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_plan_generate (1/1) (e15745277b73b53b419501a25fbcbecf) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,751 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (2/2) (3aca211bbc65ff8dc5f64062d23d4b9d) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,751 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (1/2) (dff16e6bb0c3442347f7835aeaa1cc24) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,751 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) (43414dc156ef1c99f2885a4793f5ba51) switched from RUNNING to CANCELING.
2022-01-25 22:57:35,944 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    
[] - Received late message for now expired checkpoint attempt 3265 from task 43414dc156ef1c99f2885a4793f5ba51 of job 61562e236aa6e8b51fa2c6e66023472a 
at container_1642990216503_0004_01_000012 @ bdnode102.hjq.com (dataPort=40090).
2022-01-25 22:57:35,991 WARN  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Received late message for now expired checkpoint attempt 3265 from task 6c2c7ea3d444315e0d3b771bc3033d74 of job 61562e236aa6e8b51fa2c6e66023472a at container_1642990216503_0004_01_000012 @ bdnode102.hjq.com (dataPort=40090).
2022-01-25 22:57:36,496 WARN  akka.remote.ReliableDeliverySupervisor                       
[] - Association with remote system [akka.tcp://flink@bdnode102.hjq.com:46391] has failed, address is now gated for [50] ms. Reason: [Disassociated] 
2022-01-25 22:57:39,806 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker container_1642990216503_0004_01_000012 is terminated. Diagnostics: Exception from container-launch.
Container id: container_1642990216503_0004_01_000012
Exit code: 239
Stack trace: ExitCodeException exitCode=239: 
	at org.apache.hadoop.util.Shell.runCommand(Shell.java:545)
	at org.apache.hadoop.util.Shell.run(Shell.java:456)
	at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:722)
	at org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor.launchContainer(DefaultContainerExecutor.java:212)
	at org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.call(ContainerLaunch.java:302)
	at org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.call(ContainerLaunch.java:82)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)


Container exited with a non-zero exit code 239

2022-01-25 22:57:39,806 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Closing TaskExecutor connection container_1642990216503_0004_01_000012 
because: Exception from container-launch.
Container id: container_1642990216503_0004_01_000012
Exit code: 239
Stack trace: ExitCodeException exitCode=239: 
	at org.apache.hadoop.util.Shell.runCommand(Shell.java:545)
	at org.apache.hadoop.util.Shell.run(Shell.java:456)
	at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:722)
	at org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor.launchContainer(DefaultContainerExecutor.java:212)
	at org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.call(ContainerLaunch.java:302)
	at org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.call(ContainerLaunch.java:82)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)


Container exited with a non-zero exit code 239

2022-01-25 22:57:39,808 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (2/2) (0c6519dba2d9c2d05f64b6a99a5a0c83) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,809 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 0c6519dba2d9c2d05f64b6a99a5a0c83.
2022-01-25 22:57:39,813 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 0c6519dba2d9c2d05f64b6a99a5a0c83.
2022-01-25 22:57:39,813 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (2/2) (3aca211bbc65ff8dc5f64062d23d4b9d) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,814 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 3aca211bbc65ff8dc5f64062d23d4b9d.
2022-01-25 22:57:39,814 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 3aca211bbc65ff8dc5f64062d23d4b9d.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Sink: compact_commit (1/1) (a7da93962d52d2824efc548496c8f504) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,815 WARN  akka.remote.transport.netty.NettyTransport                   [] - Remote connection to [null] failed with java.net.ConnectException: 拒绝连接: bdnode102.hjq.com/192.168.51.102:46391
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution a7da93962d52d2824efc548496c8f504.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - bucket_assigner (1/1) (6c2c7ea3d444315e0d3b771bc3033d74) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 6c2c7ea3d444315e0d3b771bc3033d74.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 6c2c7ea3d444315e0d3b771bc3033d74.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_plan_generate (1/1) (e15745277b73b53b419501a25fbcbecf) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution e15745277b73b53b419501a25fbcbecf.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution e15745277b73b53b419501a25fbcbecf.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (1/2) (dff16e6bb0c3442347f7835aeaa1cc24) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution dff16e6bb0c3442347f7835aeaa1cc24.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution dff16e6bb0c3442347f7835aeaa1cc24.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) (43414dc156ef1c99f2885a4793f5ba51) switched from CANCELING to CANCELED.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 43414dc156ef1c99f2885a4793f5ba51.
2022-01-25 22:57:39,815 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Discarding the results produced by task execution 43414dc156ef1c99f2885a4793f5ba51.
2022-01-25 22:57:39,820 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job default: INSERT INTO lakehouse2_dwd_order_hudi SELECT DATE_FORMAT(ods.gmt_create, 'yyyy-MM-dd') AS dt, DATE_FORMAT(ods.gmt_create, 'HH') AS hr, ods.order_id, ods.cate_id, dim.parent_cate_id, ods.trans_amount, ods.gmt_create FROM lakehouse2_ods_order_cdc ods LEFT JOIN lakehouse2_dim_order_category FOR SYSTEM_TIME AS OF ods.ptime AS dim ON dim.cate_id = ods.cate_id (61562e236aa6e8b51fa2c6e66023472a) switched from state RESTARTING to RUNNING.
2022-01-25 22:57:39,820 DEBUG org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Status of the shared state registry of job 61562e236aa6e8b51fa2c6e66023472a after restore: SharedStateRegistry{registeredStates={}}.
2022-01-25 22:57:39,821 WARN  akka.remote.ReliableDeliverySupervisor                       [] - Association with remote system [akka.tcp://flink@bdnode102.hjq.com:46391] has failed, address is now gated for [50] ms. Reason: [Association failed with [akka.tcp://flink@bdnode102.hjq.com:46391]] Caused by: [java.net.ConnectException: 拒绝连接: bdnode102.hjq.com/192.168.51.102:46391]
2022-01-25 22:57:39,821 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Restoring job 61562e236aa6e8b51fa2c6e66023472a from Checkpoint 3264 @ 1643122612242 for 61562e236aa6e8b51fa2c6e66023472a located at hdfs://bdnode102:9000/flink/flink-checkpoints/61562e236aa6e8b51fa2c6e66023472a/chk-3264.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - No master state to restore
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) (12fafe11b8ea3b0a8e0070fc67859947) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - bucket_assigner (1/1) (06e4f45b24d299089679240153b946ad) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (1/2) (4dbba20843c6f2ead1b205ec89419728) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (2/2) (cd95b57d918ce84af51471b09b1127c1) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_plan_generate (1/1) (2a8b5312f775661b0ada517112b9c384) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (1/2) (6f4a7274bc88d1aca2470b685f548792) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (2/2) (746a01accbfa0f0d7cb0664daf156992) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Sink: compact_commit (1/1) (204d560ded439aaca6643dbaaa326741) switched from CREATED to SCHEDULED.
2022-01-25 22:57:39,822 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl     [] - Requesting new slot [SlotRequestId{ae06d1601ac920586e73a149ff7b6437}] and profile ResourceProfile{UNKNOWN} with allocation id 595b21a3c3ee8fa689ffabc498cf8d87 from resource manager.
2022-01-25 22:57:39,823 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Request slot with profile ResourceProfile{UNKNOWN} for job 61562e236aa6e8b51fa2c6e66023472a with allocation id 595b21a3c3ee8fa689ffabc498cf8d87.
2022-01-25 22:57:39,823 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl     [] - Requesting new slot [SlotRequestId{2bb0f55499a17276e0f44e3f17c94bf3}] and profile ResourceProfile{UNKNOWN} with allocation id 4d3d549992597a379362ef571d3829f6 from resource manager.
2022-01-25 22:57:39,823 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Requesting new worker with resource spec WorkerResourceSpec {cpuCores=8.0, taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=230.400mb (241591914 bytes)}, current pending count: 1.
2022-01-25 22:57:39,823 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Requesting new TaskExecutor container with resource TaskExecutorProcessSpec {cpuCores=8.0, frameworkHeapSize=128.000mb (134217728 bytes), frameworkOffHeapSize=128.000mb (134217728 bytes), taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemorySize=230.400mb (241591914 bytes), jvmMetaspaceSize=256.000mb (268435456 bytes), jvmOverheadSize=192.000mb (201326592 bytes)}, priority 1.
2022-01-25 22:57:39,824 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Request slot with profile ResourceProfile{UNKNOWN} for job 61562e236aa6e8b51fa2c6e66023472a with allocation id 4d3d549992597a379362ef571d3829f6.
2022-01-25 22:57:44,617 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Checkpoint triggering task Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) of job 61562e236aa6e8b51fa2c6e66023472a is not in state RUNNING but SCHEDULED instead. Aborting checkpoint.
2022-01-25 22:57:45,318 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Received 1 containers.
2022-01-25 22:57:45,318 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Received 1 containers with priority 1, 1 pending container requests.
2022-01-25 22:57:45,319 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Removing container request Capability[<memory:1024, vCores:8>]Priority[1].
2022-01-25 22:57:45,319 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Accepted 1 requested containers, returned 0 excess containers, 0 pending container requests of resource <memory:1024, vCores:8>.
2022-01-25 22:57:45,320 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - TaskExecutor container_1642990216503_0004_01_000013(bdnode102.hjq.com:46324) will be started on bdnode102.hjq.com with TaskExecutorProcessSpec {cpuCores=8.0, frameworkHeapSize=128.000mb (134217728 bytes), frameworkOffHeapSize=128.000mb (134217728 bytes), taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemorySize=230.400mb (241591914 bytes), jvmMetaspaceSize=256.000mb (268435456 bytes), jvmOverheadSize=192.000mb (201326592 bytes)}.
2022-01-25 22:57:45,322 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Creating container launch context for TaskManagers
2022-01-25 22:57:45,323 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Starting TaskManagers
2022-01-25 22:57:45,324 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Requested worker container_1642990216503_0004_01_000013(bdnode102.hjq.com:46324) with resource spec WorkerResourceSpec {cpuCores=8.0, taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=230.400mb (241591914 bytes)}.
2022-01-25 22:57:45,325 INFO  org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl [] - Processing Event EventType: START_CONTAINER for Container container_1642990216503_0004_01_000013
2022-01-25 22:57:45,325 INFO  org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy [] - Opening proxy : bdnode102.hjq.com:46324
2022-01-25 22:57:45,771 WARN  akka.remote.transport.netty.NettyTransport                   [] - Remote connection to [null] failed with java.net.ConnectException: 拒绝连接: bdnode102.hjq.com/192.168.51.102:46391
2022-01-25 22:57:45,771 WARN  akka.remote.ReliableDeliverySupervisor                       [] - Association with remote system [akka.tcp://flink@bdnode102.hjq.com:46391] has failed, address is now gated for [50] ms. Reason: [Association failed with [akka.tcp://flink@bdnode102.hjq.com:46391]] Caused by: [java.net.ConnectException: 拒绝连接: bdnode102.hjq.com/192.168.51.102:46391]
2022-01-25 22:57:48,250 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Registering TaskManager with ResourceID container_1642990216503_0004_01_000013(bdnode102.hjq.com:46324) (akka.tcp://flink@bdnode102.hjq.com:40342/user/rpc/taskmanager_0) at ResourceManager
2022-01-25 22:57:48,265 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker container_1642990216503_0004_01_000013(bdnode102.hjq.com:46324) is registered.
2022-01-25 22:57:48,265 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker container_1642990216503_0004_01_000013(bdnode102.hjq.com:46324) with resource spec WorkerResourceSpec {cpuCores=8.0, taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=230.400mb (241591914 bytes)} was requested in current attempt. Current pending count after registering: 0.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) (12fafe11b8ea3b0a8e0070fc67859947) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) (attempt #35) with attempt id 12fafe11b8ea3b0a8e0070fc67859947 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 595b21a3c3ee8fa689ffabc498cf8d87
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - bucket_assigner (1/1) (06e4f45b24d299089679240153b946ad) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying bucket_assigner (1/1) (attempt #35) with attempt id 06e4f45b24d299089679240153b946ad to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 595b21a3c3ee8fa689ffabc498cf8d87
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (1/2) (4dbba20843c6f2ead1b205ec89419728) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying hoodie_stream_write (1/2) (attempt #35) with attempt id 4dbba20843c6f2ead1b205ec89419728 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 595b21a3c3ee8fa689ffabc498cf8d87
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (2/2) (cd95b57d918ce84af51471b09b1127c1) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying hoodie_stream_write (2/2) (attempt #35) with attempt id cd95b57d918ce84af51471b09b1127c1 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 4d3d549992597a379362ef571d3829f6
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_plan_generate (1/1) (2a8b5312f775661b0ada517112b9c384) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying compact_plan_generate (1/1) (attempt #35) with attempt id 2a8b5312f775661b0ada517112b9c384 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 595b21a3c3ee8fa689ffabc498cf8d87
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (1/2) (6f4a7274bc88d1aca2470b685f548792) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying compact_task (1/2) (attempt #35) with attempt id 6f4a7274bc88d1aca2470b685f548792 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 595b21a3c3ee8fa689ffabc498cf8d87
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (2/2) (746a01accbfa0f0d7cb0664daf156992) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying compact_task (2/2) (attempt #35) with attempt id 746a01accbfa0f0d7cb0664daf156992 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 4d3d549992597a379362ef571d3829f6
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Sink: compact_commit (1/1) (204d560ded439aaca6643dbaaa326741) switched from SCHEDULED to DEPLOYING.
2022-01-25 22:57:48,304 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying Sink: compact_commit (1/1) (attempt #35) with attempt id 204d560ded439aaca6643dbaaa326741 to container_1642990216503_0004_01_000013 @ bdnode102.hjq.com (dataPort=40277) with allocation id 595b21a3c3ee8fa689ffabc498cf8d87
2022-01-25 22:57:48,930 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: TableSourceScan(table=[[myhive, default, lakehouse2_ods_order_cdc]], fields=[order_id, cate_id, trans_amount, gmt_create]) -> DropUpdateBefore -> LookupJoin(table=[myhive.default.lakehouse2_dim_order_category], joinType=[LeftOuterJoin], async=[false], lookup=[cate_id=cate_id], select=[order_id, cate_id, trans_amount, gmt_create, cate_id0, parent_cate_id]) -> Calc(select=[(gmt_create DATE_FORMAT _UTF-16LE'yyyy-MM-dd') AS dt, (gmt_create DATE_FORMAT _UTF-16LE'HH') AS hr, order_id, cate_id, parent_cate_id, trans_amount, gmt_create]) -> Filter -> Map (1/1) (12fafe11b8ea3b0a8e0070fc67859947) switched from DEPLOYING to RUNNING.
2022-01-25 22:57:48,930 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_plan_generate (1/1) (2a8b5312f775661b0ada517112b9c384) switched from DEPLOYING to RUNNING.
2022-01-25 22:57:48,930 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (2/2) (cd95b57d918ce84af51471b09b1127c1) switched from DEPLOYING to RUNNING.
2022-01-25 22:57:48,930 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - hoodie_stream_write (1/2) (4dbba20843c6f2ead1b205ec89419728) switched from DEPLOYING to RUNNING.
2022-01-25 22:57:48,930 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - compact_task (2/2) (746a01accbfa0f0d7cb0664daf156992) switched from DEPLOYING to RUNNING.
2022-01-25 22:57:48,930 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Sink: compact_commit (1/1) (204d560ded439aaca6643dbaaa326741) switched from DEPLOYING to RUNNING.














