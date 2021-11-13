java.lang.RuntimeException: org.apache.thrift.transport.TTransportException: Socket is closed by peer.
	at org.apache.zeppelin.interpreter.remote.PooledRemoteClient.callRemoteFunction(PooledRemoteClient.java:86)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess.callRemoteFunction(RemoteInterpreterProcess.java:88)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.interpret(RemoteInterpreter.java:216)
	at org.apache.zeppelin.notebook.Paragraph.jobRun(Paragraph.java:458)
	at org.apache.zeppelin.notebook.Paragraph.jobRun(Paragraph.java:72)
	at org.apache.zeppelin.scheduler.Job.run(Job.java:172)
	at org.apache.zeppelin.scheduler.AbstractScheduler.runJob(AbstractScheduler.java:130)
	at org.apache.zeppelin.scheduler.RemoteScheduler$JobRunner.run(RemoteScheduler.java:180)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
Caused by: org.apache.thrift.transport.TTransportException: Socket is closed by peer.
	at org.apache.thrift.transport.TIOStreamTransport.read(TIOStreamTransport.java:130)
	at org.apache.thrift.transport.TTransport.readAll(TTransport.java:86)
	at org.apache.thrift.protocol.TBinaryProtocol.readAll(TBinaryProtocol.java:455)
	at org.apache.thrift.protocol.TBinaryProtocol.readI32(TBinaryProtocol.java:354)
	at org.apache.thrift.protocol.TBinaryProtocol.readMessageBegin(TBinaryProtocol.java:243)
	at org.apache.thrift.TServiceClient.receiveBase(TServiceClient.java:77)
	at org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService$Client.recv_interpret(RemoteInterpreterService.java:252)
	at org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService$Client.interpret(RemoteInterpreterService.java:236)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreter.lambda$interpret$3(RemoteInterpreter.java:217)
	at org.apache.zeppelin.interpreter.remote.PooledRemoteClient.callRemoteFunction(PooledRemoteClient.java:82)
	... 14 more
    
// flink stremaing sql的 窗口函数

select 
TUMBLE_START(rowtime, INTERVAL '5' SECOND) AS start_time,
url,
count(1) as pv
from log 
group by TUMBLE(rowtime,INTERVAL '5' SECOND), url; 









// log文件
INFO [2020-08-07 18:04:02,082] ({SchedulerFactory2} RemoteInterpreter.java[lambda$open$0]:139) - Open RemoteInterpreter org.apache.zeppelin.flink.FlinkInterpreter
 INFO [2020-08-07 18:04:02,083] ({SchedulerFactory2} RemoteInterpreter.java[pushAngularObjectRegistryToRemote]:408) - Push local angular object registry from ZeppelinServer to remote interpreter group flink-shared_process
 INFO [2020-08-07 18:04:02,441] ({JobStatusPoller-paragraph_1580998080340_1531975932} NotebookServer.java[onStatusChange]:1907) - Job paragraph_1580998080340_1531975932 starts to RUNNING
 INFO [2020-08-07 18:04:02,442] ({JobStatusPoller-paragraph_1580998080340_1531975932} VFSNotebookRepo.java[save]:145) - Saving note 2F2YS7PCE to Flink Tutorial/1. Flink Basics_2F2YS7PCE.zpln
 WARN [2020-08-07 18:04:19,689] ({JobProgressPoller, jobId=paragraph_1580998080340_1531975932} PooledRemoteClient.java[releaseBrokenClient]:69) - release broken client
 WARN [2020-08-07 18:04:19,689] ({SchedulerFactory2} PooledRemoteClient.java[releaseBrokenClient]:69) - release broken client
ERROR [2020-08-07 18:04:19,707] ({JobProgressPoller, jobId=paragraph_1580998080340_1531975932} JobProgressPoller.java[run]:58) - Can not get or update progress
java.lang.RuntimeException: org.apache.thrift.transport.TTransportException: Socket is closed by peer.

 INFO [2020-08-07 18:04:19,740] ({SchedulerFactory2} VFSNotebookRepo.java[save]:145) - Saving note 2F2YS7PCE to Flink Tutorial/1. Flink Basics_2F2YS7PCE.zpln
 INFO [2020-08-07 18:04:19,754] ({SchedulerFactory2} AbstractScheduler.java[runJob]:152) - Job paragraph_1580998080340_1531975932 finished by scheduler RemoteInterpreter-flink-shared_process-shared_session
ERROR [2020-08-07 18:04:19,810] ({qtp837764579-69} HeliumRestApi.java[suggest]:142) - java.io.IOException: org.apache.thrift.transport.TTransportException: java.net.ConnectException: 拒绝连接 (Connection refused)
java.lang.RuntimeException: java.io.IOException: org.apache.thrift.transport.TTransportException: java.net.ConnectException: 拒绝连接 (Connection refused)
	at org.apache.zeppelin.interpreter.remote.PooledRemoteClient.callRemoteFunction(PooledRemoteClient.java:88)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess.callRemoteFunction(RemoteInterpreterProcess.java:88)
	at org.apache.zeppelin.interpreter.InterpreterSettingManager.getAllResourcesExcept(InterpreterSettingManager.java:664)
	at org.apache.zeppelin.interpreter.InterpreterSettingManager.getAllResources(InterpreterSettingManager.java:646)
	at org.apache.zeppelin.helium.Helium.suggestApp(Helium.java:398)
	at org.apache.zeppelin.rest.HeliumRestApi.suggest(HeliumRestApi.java:140)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.glassfish.jersey.server.model.internal.ResourceMethodInvocationHandlerFactory.lambda$static$0(ResourceMethodInvocationHandlerFactory.java:76)
	at org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher$1.run(AbstractJavaResourceMethodDispatcher.java:148)
	at org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.invoke(AbstractJavaResourceMethodDispatcher.java:191)
	at org.glassfish.jersey.server.model.internal.JavaResourceMethodDispatcherProvider$ResponseOutInvoker.doDispatch(JavaResourceMethodDispatcherProvider.java:200)
	at org.glassfish.jersey.server.model.internal.AbstractJavaResourceMethodDispatcher.dispatch(AbstractJavaResourceMethodDispatcher.java:103)
	at org.glassfish.jersey.server.model.ResourceMethodInvoker.invoke(ResourceMethodInvoker.java:493)
	at org.glassfish.jersey.server.model.ResourceMethodInvoker.apply(ResourceMethodInvoker.java:415)
	at org.glassfish.jersey.server.model.ResourceMethodInvoker.apply(ResourceMethodInvoker.java:104)
	at org.glassfish.jersey.server.ServerRuntime$1.run(ServerRuntime.java:277)
	at org.glassfish.jersey.internal.Errors$1.call(Errors.java:272)
	at org.glassfish.jersey.internal.Errors$1.call(Errors.java:268)
	at org.glassfish.jersey.internal.Errors.process(Errors.java:316)
	at org.glassfish.jersey.internal.Errors.process(Errors.java:298)
	at org.glassfish.jersey.internal.Errors.process(Errors.java:268)
	at org.glassfish.jersey.process.internal.RequestScope.runInScope(RequestScope.java:289)
	at org.glassfish.jersey.server.ServerRuntime.process(ServerRuntime.java:256)
	at org.glassfish.jersey.server.ApplicationHandler.handle(ApplicationHandler.java:703)
	at org.glassfish.jersey.servlet.WebComponent.serviceImpl(WebComponent.java:416)
	at org.glassfish.jersey.servlet.WebComponent.service(WebComponent.java:370)
	at org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:389)
	at org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:342)
	at org.glassfish.jersey.servlet.ServletContainer.service(ServletContainer.java:229)
	at org.eclipse.jetty.servlet.ServletHolder.handle(ServletHolder.java:755)
	at org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1617)
	at org.apache.zeppelin.server.CorsFilter.doFilter(CorsFilter.java:64)
	at org.eclipse.jetty.servlet.ServletHandler$CachedChain.doFilter(ServletHandler.java:1604)
	at org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:545)
	at org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:143)
	at org.eclipse.jetty.security.SecurityHandler.handle(SecurityHandler.java:590)
	at org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:127)
	at org.eclipse.jetty.server.handler.ScopedHandler.nextHandle(ScopedHandler.java:235)
	at org.eclipse.jetty.server.session.SessionHandler.doHandle(SessionHandler.java:1610)
	at org.eclipse.jetty.server.handler.ScopedHandler.nextHandle(ScopedHandler.java:233)
	at org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:1300)
	at org.eclipse.jetty.server.handler.ScopedHandler.nextScope(ScopedHandler.java:188)
	at org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:485)
	at org.eclipse.jetty.server.session.SessionHandler.doScope(SessionHandler.java:1580)
	at org.eclipse.jetty.server.handler.ScopedHandler.nextScope(ScopedHandler.java:186)
	at org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:1215)
	at org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:141)
	at org.eclipse.jetty.server.handler.ContextHandlerCollection.handle(ContextHandlerCollection.java:221)
	at org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:127)
	at org.eclipse.jetty.server.Server.handle(Server.java:500)
	at org.eclipse.jetty.server.HttpChannel.lambda$handle$1(HttpChannel.java:383)
	at org.eclipse.jetty.server.HttpChannel.dispatch(HttpChannel.java:547)
	at org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:375)
	at org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:273)
	at org.eclipse.jetty.io.AbstractConnection$ReadCallback.succeeded(AbstractConnection.java:311)
	at org.eclipse.jetty.io.FillInterest.fillable(FillInterest.java:103)
	at org.eclipse.jetty.io.ChannelEndPoint$2.run(ChannelEndPoint.java:117)
	at org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:806)
	at org.eclipse.jetty.util.thread.QueuedThreadPool$Runner.run(QueuedThreadPool.java:938)
	at java.lang.Thread.run(Thread.java:748)
Caused by: java.io.IOException: org.apache.thrift.transport.TTransportException: java.net.ConnectException: 拒绝连接 (Connection refused)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess.lambda$new$0(RemoteInterpreterProcess.java:54)
	at org.apache.zeppelin.interpreter.remote.RemoteClientFactory.create(RemoteClientFactory.java:54)
	at org.apache.zeppelin.interpreter.remote.RemoteClientFactory.create(RemoteClientFactory.java:34)
	at org.apache.commons.pool2.BasePooledObjectFactory.makeObject(BasePooledObjectFactory.java:60)
	at org.apache.commons.pool2.impl.GenericObjectPool.create(GenericObjectPool.java:861)
	at org.apache.commons.pool2.impl.GenericObjectPool.borrowObject(GenericObjectPool.java:435)
	at org.apache.zeppelin.interpreter.remote.PooledRemoteClient.getClient(PooledRemoteClient.java:45)
	at org.apache.zeppelin.interpreter.remote.PooledRemoteClient.callRemoteFunction(PooledRemoteClient.java:80)
	... 62 more
Caused by: org.apache.thrift.transport.TTransportException: java.net.ConnectException: 拒绝连接 (Connection refused)
	at org.apache.thrift.transport.TSocket.open(TSocket.java:226)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess.lambda$new$0(RemoteInterpreterProcess.java:52)
	... 69 more
Caused by: java.net.ConnectException: 拒绝连接 (Connection refused)
	at java.net.PlainSocketImpl.socketConnect(Native Method)
	at java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:476)
	at java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:218)
	at java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:200)
	at java.net.SocksSocketImpl.connect(SocksSocketImpl.java:394)
	at java.net.Socket.connect(Socket.java:606)
	at org.apache.thrift.transport.TSocket.open(TSocket.java:221)
	... 70 more
 WARN [2020-08-07 18:04:19,903] ({Exec Default Executor} RemoteInterpreterManagedProcess.java[onProcessComplete]:251) - Process is exited with exit value 0
 INFO [2020-08-07 18:04:19,904] ({Exec Default Executor} ProcessLauncher.java[transition]:109) - Process state is transitioned to COMPLETED
 
 
 
 
 
// Kafka 分析

 INFO [2020-08-08 14:14:49,960] ({SchedulerFactory6} VFSNotebookRepo.java[save]:145) - Saving note 2FFB4J7EN to FlinkStreamingSQL_Demo01_2FFB4J7EN.zpln
 INFO [2020-08-08 14:14:49,976] ({SchedulerFactory6} AbstractScheduler.java[runJob]:152) - Job paragraph_1596863904616_175529513 finished by scheduler RemoteInterpreter-flink-shared_process-shared_session
 INFO [2020-08-08 14:25:32,077] ({qtp1047460013-84} VFSNotebookRepo.java[save]:145) - Saving note 2FFB4J7EN to FlinkStreamingSQL_Demo01_2FFB4J7EN.zpln
 INFO [2020-08-08 14:25:32,281] ({qtp1047460013-19} NotebookService.java[runParagraph]:312) - Start to run paragraph: paragraph_1596863904616_175529513 of note: 2FFB4J7EN
 INFO [2020-08-08 14:25:32,281] ({qtp1047460013-19} VFSNotebookRepo.java[save]:145) - Saving note 2FFB4J7EN to FlinkStreamingSQL_Demo01_2FFB4J7EN.zpln
 INFO [2020-08-08 14:25:32,332] ({SchedulerFactory10} AbstractScheduler.java[runJob]:125) - Job paragraph_1596863904616_175529513 started by scheduler RemoteInterpreter-flink-shared_process-shared_session
 INFO [2020-08-08 14:25:32,351] ({SchedulerFactory10} Paragraph.java[jobRun]:388) - Run paragraph [paragraph_id: paragraph_1596863904616_175529513, interpreter: org.apache.zeppelin.flink.FlinkStreamSqlInterpreter, note_id: 2FFB4J7EN, user: anonymous]
 INFO [2020-08-08 14:25:32,457] ({JobStatusPoller-paragraph_1596863904616_175529513} NotebookServer.java[onStatusChange]:1907) - Job paragraph_1596863904616_175529513 starts to RUNNING
 INFO [2020-08-08 14:25:32,458] ({JobStatusPoller-paragraph_1596863904616_175529513} VFSNotebookRepo.java[save]:145) - Saving note 2FFB4J7EN to FlinkStreamingSQL_Demo01_2FFB4J7EN.zpln
 WARN [2020-08-08 14:25:33,214] ({SchedulerFactory10} NotebookServer.java[onStatusChange]:1904) - Job paragraph_1596863904616_175529513 is finished, status: ERROR, exception: null, result: %text Fail to run sql command: insert into sink_kafka select orgId, pointId, `value` as kafka_ts, cast(`time`/1000000000 as timestamp(3)) as time_ts from source_kafka where kafka_val = 4.5
java.io.IOException: org.apache.flink.table.api.ValidationException: SQL validation failed. From line 1, column 143 to line 1, column 151: Column 'kafka_val' not found in any table
	at org.apache.zeppelin.flink.FlinkSqlInterrpeter.callInsertInto(FlinkSqlInterrpeter.java:536)
	at org.apache.zeppelin.flink.FlinkStreamSqlInterpreter.callInsertInto(FlinkStreamSqlInterpreter.java:100)
	at org.apache.zeppelin.flink.FlinkSqlInterrpeter.callCommand(FlinkSqlInterrpeter.java:272)
	at org.apache.zeppelin.flink.FlinkSqlInterrpeter.runSqlList(FlinkSqlInterrpeter.java:159)
	at org.apache.zeppelin.flink.FlinkSqlInterrpeter.interpret(FlinkSqlInterrpeter.java:124)
	at org.apache.zeppelin.interpreter.LazyOpenInterpreter.interpret(LazyOpenInterpreter.java:110)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer$InterpretJob.jobRun(RemoteInterpreterServer.java:776)
	at org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer$InterpretJob.jobRun(RemoteInterpreterServer.java:668)
	at org.apache.zeppelin.scheduler.Job.run(Job.java:172)
	at org.apache.zeppelin.scheduler.AbstractScheduler.runJob(AbstractScheduler.java:130)
	at org.apache.zeppelin.scheduler.ParallelScheduler.lambda$runJobInScheduler$0(ParallelScheduler.java:39)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
Caused by: org.apache.flink.table.api.ValidationException: SQL validation failed. From line 1, column 143 to line 1, column 151: Column 'kafka_val' not found in any table
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.org$apache$flink$table$planner$calcite$FlinkPlannerImpl$$validate(FlinkPlannerImpl.scala:130)
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.validate(FlinkPlannerImpl.scala:105)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convert(SqlToOperationConverter.java:130)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convertSqlInsert(SqlToOperationConverter.java:345)
	at org.apache.flink.table.planner.operations.SqlToOperationConverter.convert(SqlToOperationConverter.java:145)
	at org.apache.flink.table.planner.delegation.ParserImpl.parse(ParserImpl.java:66)
	at org.apache.flink.table.api.internal.TableEnvironmentImpl.sqlUpdate(TableEnvironmentImpl.java:484)
	at org.apache.zeppelin.flink.FlinkSqlInterrpeter.callInsertInto(FlinkSqlInterrpeter.java:528)
	... 13 more
Caused by: org.apache.calcite.runtime.CalciteContextException: From line 1, column 143 to line 1, column 151: Column 'kafka_val' not found in any table
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at org.apache.calcite.runtime.Resources$ExInstWithCause.ex(Resources.java:463)
	at org.apache.calcite.sql.SqlUtil.newContextException(SqlUtil.java:834)
	at org.apache.calcite.sql.SqlUtil.newContextException(SqlUtil.java:819)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.newValidationError(SqlValidatorImpl.java:4840)
	at org.apache.calcite.sql.validate.DelegatingScope.fullyQualify(DelegatingScope.java:259)
	at org.apache.calcite.sql.validate.SqlValidatorImpl$Expander.visit(SqlValidatorImpl.java:5732)
	at org.apache.calcite.sql.validate.SqlValidatorImpl$Expander.visit(SqlValidatorImpl.java:5717)
	at org.apache.calcite.sql.SqlIdentifier.accept(SqlIdentifier.java:317)
	at org.apache.calcite.sql.util.SqlShuttle$CallCopyingArgHandler.visitChild(SqlShuttle.java:134)
	at org.apache.calcite.sql.util.SqlShuttle$CallCopyingArgHandler.visitChild(SqlShuttle.java:101)
	at org.apache.calcite.sql.SqlOperator.acceptCall(SqlOperator.java:868)
	at org.apache.calcite.sql.validate.SqlValidatorImpl$Expander.visitScoped(SqlValidatorImpl.java:5750)
	at org.apache.calcite.sql.validate.SqlScopedShuttle.visit(SqlScopedShuttle.java:50)
	at org.apache.calcite.sql.validate.SqlScopedShuttle.visit(SqlScopedShuttle.java:33)
	at org.apache.calcite.sql.SqlCall.accept(SqlCall.java:139)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.expand(SqlValidatorImpl.java:5324)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateWhereClause(SqlValidatorImpl.java:4029)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateSelect(SqlValidatorImpl.java:3378)
	at org.apache.calcite.sql.validate.SelectNamespace.validateImpl(SelectNamespace.java:60)
	at org.apache.calcite.sql.validate.AbstractNamespace.validate(AbstractNamespace.java:84)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateNamespace(SqlValidatorImpl.java:1007)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateQuery(SqlValidatorImpl.java:967)
	at org.apache.calcite.sql.SqlSelect.validate(SqlSelect.java:216)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validateScopedExpression(SqlValidatorImpl.java:942)
	at org.apache.calcite.sql.validate.SqlValidatorImpl.validate(SqlValidatorImpl.java:649)
	at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.org$apache$flink$table$planner$calcite$FlinkPlannerImpl$$validate(FlinkPlannerImpl.scala:126)
	... 20 more
Caused by: org.apache.calcite.sql.validate.SqlValidatorException: Column 'kafka_val' not found in any table
	at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
	at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
	at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
	at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
	at org.apache.calcite.runtime.Resources$ExInstWithCause.ex(Resources.java:463)
	at org.apache.calcite.runtime.Resources$ExInst.ex(Resources.java:572)
	... 45 more
 
 
 
 