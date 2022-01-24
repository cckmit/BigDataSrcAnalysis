

org.apache.flink.util.FlinkException: JobManager responsible for 2acdca0805ce0eb4fee330a41e303466 lost the leadership.
	at org.apache.flink.runtime.taskexecutor.TaskExecutor.disconnectJobManagerConnection(TaskExecutor.java:1593) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskexecutor.TaskExecutor.disconnectAndTryReconnectToJobManager(TaskExecutor.java:1162) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskexecutor.TaskExecutor.access$3400(TaskExecutor.java:175) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.taskexecutor.TaskExecutor$JobManagerHeartbeatListener.lambda$notifyHeartbeatTimeout$0(TaskExecutor.java:2206) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.util.Optional.ifPresent(Optional.java:159) ~[?:1.8.0_261]
	at org.apache.flink.runtime.taskexecutor.TaskExecutor$JobManagerHeartbeatListener.notifyHeartbeatTimeout(TaskExecutor.java:2204) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.heartbeat.HeartbeatMonitorImpl.run(HeartbeatMonitorImpl.java:111) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) ~[?:1.8.0_261]
	at java.util.concurrent.FutureTask.run(FutureTask.java:266) ~[?:1.8.0_261]
	at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleRunAsync(AkkaRpcActor.java:440) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleRpcMessage(AkkaRpcActor.java:208) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at org.apache.flink.runtime.rpc.akka.AkkaRpcActor.handleMessage(AkkaRpcActor.java:158) ~[flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:26) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.japi.pf.UnitCaseStatement.apply(CaseStatements.scala:21) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at scala.PartialFunction$class.applyOrElse(PartialFunction.scala:123) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.japi.pf.UnitCaseStatement.applyOrElse(CaseStatements.scala:21) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:170) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:171) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at scala.PartialFunction$OrElse.applyOrElse(PartialFunction.scala:171) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.actor.Actor$class.aroundReceive(Actor.scala:517) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.actor.AbstractActor.aroundReceive(AbstractActor.scala:225) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.actor.ActorCell.receiveMessage(ActorCell.scala:592) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.actor.ActorCell.invoke(ActorCell.scala:561) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.Mailbox.processMailbox(Mailbox.scala:258) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.Mailbox.run(Mailbox.scala:225) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.Mailbox.exec(Mailbox.scala:235) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979) [flink-dist_2.11-1.12.2.jar:1.12.2]
	at akka.dispatch.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107) [flink-dist_2.11-1.12.2.jar:1.12.2]
Caused by: java.util.concurrent.TimeoutException: The heartbeat of JobManager with id 30ce784be01cac42c6f5b8ac551e8cc9 timed out.
	... 27 more
2022-01-24 19:29:36,563 INFO  org.apache.flink.runtime.taskmanager.Task                    [] - Triggering cancellation of task code hoodie_stream_write (1/2)#1 (d654d4d863caaa1d52da9560c484ef53).
2022-01-24 19:29:36,564 INFO  io.debezium.connector.mysql.BinlogReader                     [] - Discarding 2 unsent record(s) due to the connector shutting down
2022-01-24 19:29:36,566 INFO  org.apache.flink.runtime.taskmanager.Task                    [] - Attempting to fail task externally Sink: compact_commit (1/1)#1 (e3f63bcc23593a8fae608dad33b9e137).
2022-01-24 19:29:36,566 WARN  org.apache.flink.runtime.taskmanager.Task                    [] - Sink: compact_commit (1/1)#1 (e3f63bcc23593a8fae608dad33b9e137) switched from RUNNING to FAILED.



