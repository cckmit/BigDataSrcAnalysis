2022-01-16 04:04:06,557 WARN  org.apache.hudi.org.apache.hive.jdbc.HiveConnection          [] - Failed to connect to localhost:10000
2022-01-16 04:04:06,558 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220116040405] error
org.apache.hudi.hive.HoodieHiveSyncException: Got runtime exception when hive syncing
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:85) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.HiveSyncContext.hiveSyncTool(HiveSyncContext.java:51) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.syncHive(StreamWriteOperatorCoordinator.java:279) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:67) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Failed to create HiveMetaStoreClient
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:91) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 6 more
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Cannot create hive connection jdbc:hive2://localhost:10000/
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:96) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 6 more
Caused by: java.sql.SQLException: Could not open client transport with JDBC Uri: jdbc:hive2://localhost:10000: Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hudi.org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:224) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
Caused by: org.apache.hudi.org.apache.hive.service.cli.HiveSQLException: Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hudi.org.apache.hive.jdbc.Utils.verifySuccess(Utils.java:267) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
Caused by: java.lang.RuntimeException: org.apache.hive.service.cli.HiveSQLException:Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hive.service.cli.session.SessionManager.createSession(SessionManager.java:419) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.openSession(SessionManager.java:362) ~[?:?]
Caused by: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException:User: bigdata is not allowed to impersonate hive
	at org.apache.hadoop.ipc.Client.call(Client.java:1475) ~[hadoop-common-2.7.2.jar:?]



2022-01-16 04:04:05,760 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [commits the instant 20220116040258] success!
2022-01-16 04:04:05,779 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading HoodieTableMetaClient from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-16 04:04:05,782 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-16 04:04:05,801 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-16 04:04:05,801 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading Active commit timeline for hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-16 04:04:05,809 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Loaded instants upto : Option{val=[20220116040405__rollback__COMPLETED]}
2022-01-16 04:04:05,888 INFO  org.apache.hudi.org.apache.hive.jdbc.Utils                   [] - Supplied authorities: localhost:10000
2022-01-16 04:04:05,888 INFO  org.apache.hudi.org.apache.hive.jdbc.Utils                   [] - Resolved authority: localhost:10000
2022-01-16 04:04:06,557 WARN  org.apache.hudi.org.apache.hive.jdbc.HiveConnection          [] - Failed to connect to localhost:10000
2022-01-16 04:04:06,558 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220116040405] error
org.apache.hudi.hive.HoodieHiveSyncException: Got runtime exception when hive syncing
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:85) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.HiveSyncContext.hiveSyncTool(HiveSyncContext.java:51) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.syncHive(StreamWriteOperatorCoordinator.java:279) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:67) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Failed to create HiveMetaStoreClient
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:91) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 6 more
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Cannot create hive connection jdbc:hive2://localhost:10000/
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:96) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:48) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:81) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 6 more
Caused by: java.sql.SQLException: Could not open client transport with JDBC Uri: jdbc:hive2://localhost:10000: Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hudi.org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:224) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.sql.DriverManager.getConnection(DriverManager.java:664) ~[?:1.8.0_261]
	at java.sql.DriverManager.getConnection(DriverManager.java:247) ~[?:1.8.0_261]
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:93) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:48) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:81) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 6 more
Caused by: org.apache.hudi.org.apache.hive.service.cli.HiveSQLException: Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hudi.org.apache.hive.jdbc.Utils.verifySuccess(Utils.java:267) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.Utils.verifySuccess(Utils.java:258) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveConnection.openSession(HiveConnection.java:683) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:200) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:107) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.sql.DriverManager.getConnection(DriverManager.java:664) ~[?:1.8.0_261]
	at java.sql.DriverManager.getConnection(DriverManager.java:247) ~[?:1.8.0_261]
	at org.apache.hudi.hive.ddl.JDBCExecutor.createHiveConnection(JDBCExecutor.java:93) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.<init>(JDBCExecutor.java:48) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.<init>(HoodieHiveClient.java:81) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.<init>(HiveSyncTool.java:80) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 6 more
Caused by: java.lang.RuntimeException: org.apache.hive.service.cli.HiveSQLException:Failed to open new session: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hive.service.cli.session.SessionManager.createSession(SessionManager.java:419) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.openSession(SessionManager.java:362) ~[?:?]
	at org.apache.hive.service.cli.CLIService.openSessionWithImpersonation(CLIService.java:193) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.getSessionHandle(ThriftCLIService.java:440) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.OpenSession(ThriftCLIService.java:322) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1377) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1362) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
Caused by: java.lang.RuntimeException: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:89) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[?:?]
	at java.security.AccessController.doPrivileged(Native Method) ~[?:1.8.0_261]
	at javax.security.auth.Subject.doAs(Subject.java:422) ~[?:1.8.0_261]
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59) ~[?:?]
	at com.sun.proxy.$Proxy35.open(Unknown Source) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.createSession(SessionManager.java:410) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.openSession(SessionManager.java:362) ~[?:?]
	at org.apache.hive.service.cli.CLIService.openSessionWithImpersonation(CLIService.java:193) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.getSessionHandle(ThriftCLIService.java:440) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.OpenSession(ThriftCLIService.java:322) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1377) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1362) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
Caused by: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate hive
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:606) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:544) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.cli.session.HiveSessionImpl.open(HiveSessionImpl.java:164) ~[?:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[?:?]
	at java.security.AccessController.doPrivileged(Native Method) ~[?:1.8.0_261]
	at javax.security.auth.Subject.doAs(Subject.java:422) ~[?:1.8.0_261]
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59) ~[?:?]
	at com.sun.proxy.$Proxy35.open(Unknown Source) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.createSession(SessionManager.java:410) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.openSession(SessionManager.java:362) ~[?:?]
	at org.apache.hive.service.cli.CLIService.openSessionWithImpersonation(CLIService.java:193) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.getSessionHandle(ThriftCLIService.java:440) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.OpenSession(ThriftCLIService.java:322) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1377) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1362) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
Caused by: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException:User: bigdata is not allowed to impersonate hive
	at org.apache.hadoop.ipc.Client.call(Client.java:1475) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.ipc.Client.call(Client.java:1412) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.ipc.ProtobufRpcEngine$Invoker.invoke(ProtobufRpcEngine.java:229) ~[hadoop-common-2.7.2.jar:?]
	at com.sun.proxy.$Proxy29.getFileInfo(Unknown Source) ~[?:?]
	at org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB.getFileInfo(ClientNamenodeProtocolTranslatorPB.java:771) ~[hadoop-hdfs-2.7.2.jar:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hadoop.io.retry.RetryInvocationHandler.invokeMethod(RetryInvocationHandler.java:191) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.io.retry.RetryInvocationHandler.invoke(RetryInvocationHandler.java:102) ~[hadoop-common-2.7.2.jar:?]
	at com.sun.proxy.$Proxy30.getFileInfo(Unknown Source) ~[?:?]
	at org.apache.hadoop.hdfs.DFSClient.getFileInfo(DFSClient.java:2108) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem$22.doCall(DistributedFileSystem.java:1305) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem$22.doCall(DistributedFileSystem.java:1301) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.fs.FileSystemLinkResolver.resolve(FileSystemLinkResolver.java:81) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.hdfs.DistributedFileSystem.getFileStatus(DistributedFileSystem.java:1301) ~[hadoop-hdfs-2.7.2.jar:?]
	at org.apache.hadoop.fs.FileSystem.exists(FileSystem.java:1424) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hadoop.hive.ql.session.SessionState.createRootHDFSDir(SessionState.java:704) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.session.SessionState.createSessionDirs(SessionState.java:650) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:582) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:544) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.cli.session.HiveSessionImpl.open(HiveSessionImpl.java:164) ~[?:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[?:?]
	at java.security.AccessController.doPrivileged(Native Method) ~[?:1.8.0_261]
	at javax.security.auth.Subject.doAs(Subject.java:422) ~[?:1.8.0_261]
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59) ~[?:?]
	at com.sun.proxy.$Proxy35.open(Unknown Source) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.createSession(SessionManager.java:410) ~[?:?]
	at org.apache.hive.service.cli.session.SessionManager.openSession(SessionManager.java:362) ~[?:?]
	at org.apache.hive.service.cli.CLIService.openSessionWithImpersonation(CLIService.java:193) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.getSessionHandle(ThriftCLIService.java:440) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.OpenSession(ThriftCLIService.java:322) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1377) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$OpenSession.getResult(TCLIService.java:1362) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
2022-01-16 04:04:56,586 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 3 (type=CHECKPOINT) @ 1642277096575 for job ce83ef9694260f4eeb88ae007a266063.
2022-01-16 04:04:56,587 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [taking checkpoint 3] success!
2022-01-16 04:04:56,666 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220116040405] success!
2022-01-16 04:04:56,892 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220116040405] success!
2022-01-16 04:04:56,960 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220116040405] success!
2022-01-16 04:04:57,387 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220116040405] success!
2022-01-16 04:04:57,418 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Completed checkpoint 3 for job ce83ef9694260f4eeb88ae007a266063 (1645254 bytes in 835 ms).
2022-01-16 04:04:57,420 INFO  org.apache.hudi.client.AbstractHoodieWriteClient             [] - Committing 20220116040405 action deltacommit
2022-01-16 04:04:57,420 INFO  org.apache




// HiveServer2 抛出次异常







2022-01-21 10:45:57,449 INFO  hive.metastore                                               [] - Connected to metastore.
2022-01-21 10:45:57,450 INFO  org.apache.hudi.hive.HiveSyncTool                            [] - Trying to sync hoodie table unknown_ro with base path hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi of type MERGE_ON_READ
2022-01-21 10:45:57,562 INFO  org.apache.hudi.hive.HiveSyncTool                            [] - Hive table unknown_ro is not found. Creating it
2022-01-21 10:45:57,562 INFO  org.apache.hudi.hive.ddl.QueryBasedDDLExecutor               [] - Creating table with CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`unknown_ro`( `_hoodie_commit_time` string, `_hoodie_commit_seqno` string, `_hoodie_record_key` string, `_hoodie_partition_path` string, `_hoodie_file_name` string, `order_id` string, `cate_id` string, `parent_cate_id` string, `trans_amount` double, `gmt_create` string) PARTITIONED BY (`dt` string,`hr` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' WITH SERDEPROPERTIES ('hoodie.query.as.ro.table'='true','path'='hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi') STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.HoodieParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 'hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi' TBLPROPERTIES('spark.sql.sources.schema.numPartCols'='2','spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"order_id","type":"string","nullable":false,"metadata":{}},{"name":"cate_id","type":"string","nullable":true,"metadata":{}},{"name":"parent_cate_id","type":"string","nullable":true,"metadata":{}},{"name":"trans_amount","type":"double","nullable":true,"metadata":{}},{"name":"gmt_create","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"hr","type":"string","nullable":true,"metadata":{}}]}','spark.sql.sources.schema.partCol.0'='dt','spark.sql.sources.schema.partCol.1'='hr','spark.sql.sources.schema.numParts'='1','spark.sql.sources.provider'='hudi')
2022-01-21 10:45:57,562 INFO  org.apache.hudi.hive.ddl.QueryBasedDDLExecutor               [] - Executing SQL CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`unknown_ro`( `_hoodie_commit_time` string, `_hoodie_commit_seqno` string, `_hoodie_record_key` string, `_hoodie_partition_path` string, `_hoodie_file_name` string, `order_id` string, `cate_id` string, `parent_cate_id` string, `trans_amount` double, `gmt_create` string) PARTITIONED BY (`dt` string,`hr` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' WITH SERDEPROPERTIES ('hoodie.query.as.ro.table'='true','path'='hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi') STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.HoodieParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 'hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi' TBLPROPERTIES('spark.sql.sources.schema.numPartCols'='2','spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"order_id","type":"string","nullable":false,"metadata":{}},{"name":"cate_id","type":"string","nullable":true,"metadata":{}},{"name":"parent_cate_id","type":"string","nullable":true,"metadata":{}},{"name":"trans_amount","type":"double","nullable":true,"metadata":{}},{"name":"gmt_create","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"hr","type":"string","nullable":true,"metadata":{}}]}','spark.sql.sources.schema.partCol.0'='dt','spark.sql.sources.schema.partCol.1'='hr','spark.sql.sources.schema.numParts'='1','spark.sql.sources.provider'='hudi')
2022-01-21 10:45:57,634 INFO  hive.metastore                                               [] - Closed a connection to metastore, current connections: 0
2022-01-21 10:45:57,634 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220121104557] error
org.apache.hudi.exception.HoodieException: Got runtime exception when hive syncing unknown
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:120) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.StreamWriteOperatorCoordinator.syncHive(StreamWriteOperatorCoordinator.java:279) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.sink.utils.NonThrownExecutor.lambda$execute$0(NonThrownExecutor.java:67) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
Caused by: org.apache.hudi.hive.HoodieHiveSyncException: Failed in executing SQL CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`unknown_ro`( `_hoodie_commit_time` string, `_hoodie_commit_seqno` string, `_hoodie_record_key` string, `_hoodie_partition_path` string, `_hoodie_file_name` string, `order_id` string, `cate_id` string, `parent_cate_id` string, `trans_amount` double, `gmt_create` string) PARTITIONED BY (`dt` string,`hr` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' WITH SERDEPROPERTIES ('hoodie.query.as.ro.table'='true','path'='hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi') STORED AS INPUTFORMAT 'org.apache.hudi.hadoop.HoodieParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 'hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi' TBLPROPERTIES('spark.sql.sources.schema.numPartCols'='2','spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"_hoodie_commit_time","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_commit_seqno","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_record_key","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_partition_path","type":"string","nullable":true,"metadata":{}},{"name":"_hoodie_file_name","type":"string","nullable":true,"metadata":{}},{"name":"order_id","type":"string","nullable":false,"metadata":{}},{"name":"cate_id","type":"string","nullable":true,"metadata":{}},{"name":"parent_cate_id","type":"string","nullable":true,"metadata":{}},{"name":"trans_amount","type":"double","nullable":true,"metadata":{}},{"name":"gmt_create","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"hr","type":"string","nullable":true,"metadata":{}}]}','spark.sql.sources.schema.partCol.0'='dt','spark.sql.sources.schema.partCol.1'='hr','spark.sql.sources.schema.numParts'='1','spark.sql.sources.provider'='hudi')
	at org.apache.hudi.hive.ddl.JDBCExecutor.runSQL(JDBCExecutor.java:59) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.QueryBasedDDLExecutor.createTable(QueryBasedDDLExecutor.java:82) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.createTable(HoodieHiveClient.java:191) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncSchema(HiveSyncTool.java:237) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:182) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.doSync(HiveSyncTool.java:135) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:117) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 5 more
Caused by: org.apache.hudi.org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: SemanticException Cannot find class 'org.apache.hudi.hadoop.HoodieParquetInputFormat'
	at org.apache.hudi.org.apache.hive.jdbc.Utils.verifySuccess(Utils.java:267) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.Utils.verifySuccessWithInfo(Utils.java:253) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveStatement.runAsyncOnServer(HiveStatement.java:313) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.org.apache.hive.jdbc.HiveStatement.execute(HiveStatement.java:253) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.JDBCExecutor.runSQL(JDBCExecutor.java:57) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.ddl.QueryBasedDDLExecutor.createTable(QueryBasedDDLExecutor.java:82) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HoodieHiveClient.createTable(HoodieHiveClient.java:191) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncSchema(HiveSyncTool.java:237) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:182) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.doSync(HiveSyncTool.java:135) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	at org.apache.hudi.hive.HiveSyncTool.syncHoodieTable(HiveSyncTool.java:117) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	... 5 more
Caused by: java.lang.RuntimeException: org.apache.hive.service.cli.HiveSQLException:Error while compiling statement: FAILED: SemanticException Cannot find class 'org.apache.hudi.hadoop.HoodieParquetInputFormat'
	at org.apache.hive.service.cli.operation.Operation.toSQLException(Operation.java:380) ~[?:?]
	at org.apache.hive.service.cli.operation.SQLOperation.prepare(SQLOperation.java:206) ~[?:?]
	at org.apache.hive.service.cli.operation.SQLOperation.runInternal(SQLOperation.java:290) ~[?:?]
	at org.apache.hive.service.cli.operation.Operation.run(Operation.java:320) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementInternal(HiveSessionImpl.java:530) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementAsync(HiveSessionImpl.java:517) ~[?:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[?:?]
	at java.security.AccessController.doPrivileged(Native Method) ~[?:1.8.0_261]
	at javax.security.auth.Subject.doAs(Subject.java:422) ~[?:1.8.0_261]
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59) ~[?:?]
	at com.sun.proxy.$Proxy35.executeStatementAsync(Unknown Source) ~[?:?]
	at org.apache.hive.service.cli.CLIService.executeStatementAsync(CLIService.java:310) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.ExecuteStatement(ThriftCLIService.java:530) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1437) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1422) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
Caused by: org.apache.hadoop.hive.ql.parse.SemanticException: Cannot find class 'org.apache.hudi.hadoop.HoodieParquetInputFormat'
	at org.apache.hadoop.hive.ql.parse.ParseUtils.ensureClassExists(ParseUtils.java:263) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.StorageFormat.fillStorageFormat(StorageFormat.java:57) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.analyzeCreateTable(SemanticAnalyzer.java:11887) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.genResolvedParseTree(SemanticAnalyzer.java:11020) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.analyzeInternal(SemanticAnalyzer.java:11133) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.CalcitePlanner.analyzeInternal(CalcitePlanner.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.analyze(BaseSemanticAnalyzer.java:258) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.Driver.compile(Driver.java:512) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.Driver.compileInternal(Driver.java:1317) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.Driver.compileAndRespond(Driver.java:1295) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.cli.operation.SQLOperation.prepare(SQLOperation.java:204) ~[?:?]
	at org.apache.hive.service.cli.operation.SQLOperation.runInternal(SQLOperation.java:290) ~[?:?]
	at org.apache.hive.service.cli.operation.Operation.run(Operation.java:320) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementInternal(HiveSessionImpl.java:530) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementAsync(HiveSessionImpl.java:517) ~[?:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[?:?]
	at java.security.AccessController.doPrivileged(Native Method) ~[?:1.8.0_261]
	at javax.security.auth.Subject.doAs(Subject.java:422) ~[?:1.8.0_261]
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59) ~[?:?]
	at com.sun.proxy.$Proxy35.executeStatementAsync(Unknown Source) ~[?:?]
	at org.apache.hive.service.cli.CLIService.executeStatementAsync(CLIService.java:310) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.ExecuteStatement(ThriftCLIService.java:530) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1437) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1422) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
Caused by: java.lang.ClassNotFoundException: org.apache.hudi.hadoop.HoodieParquetInputFormat
	at java.net.URLClassLoader.findClass(URLClassLoader.java:382) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:418) ~[?:1.8.0_261]
	at java.lang.ClassLoader.loadClass(ClassLoader.java:351) ~[?:1.8.0_261]
	at java.lang.Class.forName0(Native Method) ~[?:1.8.0_261]
	at java.lang.Class.forName(Class.java:348) ~[?:1.8.0_261]
	at org.apache.hadoop.hive.ql.parse.ParseUtils.ensureClassExists(ParseUtils.java:261) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.StorageFormat.fillStorageFormat(StorageFormat.java:57) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.analyzeCreateTable(SemanticAnalyzer.java:11887) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.genResolvedParseTree(SemanticAnalyzer.java:11020) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.analyzeInternal(SemanticAnalyzer.java:11133) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.CalcitePlanner.analyzeInternal(CalcitePlanner.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.analyze(BaseSemanticAnalyzer.java:258) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.Driver.compile(Driver.java:512) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.Driver.compileInternal(Driver.java:1317) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hadoop.hive.ql.Driver.compileAndRespond(Driver.java:1295) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.cli.operation.SQLOperation.prepare(SQLOperation.java:204) ~[?:?]
	at org.apache.hive.service.cli.operation.SQLOperation.runInternal(SQLOperation.java:290) ~[?:?]
	at org.apache.hive.service.cli.operation.Operation.run(Operation.java:320) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementInternal(HiveSessionImpl.java:530) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementAsync(HiveSessionImpl.java:517) ~[?:?]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[?:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[?:?]
	at java.security.AccessController.doPrivileged(Native Method) ~[?:1.8.0_261]
	at javax.security.auth.Subject.doAs(Subject.java:422) ~[?:1.8.0_261]
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1657) ~[hadoop-common-2.7.2.jar:?]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59) ~[?:?]
	at com.sun.proxy.$Proxy35.executeStatementAsync(Unknown Source) ~[?:?]
	at org.apache.hive.service.cli.CLIService.executeStatementAsync(CLIService.java:310) ~[?:?]
	at org.apache.hive.service.cli.thrift.ThriftCLIService.ExecuteStatement(ThriftCLIService.java:530) ~[?:?]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1437) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.rpc.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1422) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56) ~[?:?]
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286) ~[flink-sql-connector-hive-2.3.6_2.11-1.12.2.jar:1.12.2]
	... 3 more
2022-01-21 10:46:54,563 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Triggering checkpoint 4 (type=CHECKPOINT) @ 1642733214554 for job 6e4b07d7cb802dfd8614a2a0b6563a8d.
2022-01-21 10:46:54,564 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [taking checkpoint 4] success!
2022-01-21 10:46:54,783 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220121104557] success!
2022-01-21 10:46:55,667 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220121104557] success!
2022-01-21 10:46:55,813 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220121104557] success!
2022-01-21 10:46:55,980 INFO  org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [handle write metadata event for instant 20220121104557] success!
2022-01-21 10:46:56,016 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Completed checkpoint 4 for job 6e4b07d7cb802dfd8614a2a0b6563a8d (1289352 bytes in 1456 ms).
2022-01-21 10:46:56,019 INFO  org.apache.hudi.client.AbstractHoodieWriteClient             [] - Committing 20220121104557 action deltacommit
2022-01-21 10:46:56,019 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading HoodieTableMetaClient from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-21 10:46:56,089 INFO  org.apache.hudi.common.table.HoodieTableConfig               [] - Loading table properties from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/hoodie.properties
2022-01-21 10:46:56,111 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Finished Loading Table of type MERGE_ON_READ(version=1, baseFileFormat=PARQUET) from hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-21 10:46:56,111 INFO  org.apache.hudi.common.table.HoodieTableMetaClient           [] - Loading Active commit timeline for hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi
2022-01-21 10:46:56,122 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Loaded instants upto : Option{val=[20220121104654__clean__COMPLETED]}
2022-01-21 10:46:56,122 INFO  org.apache.hudi.common.table.view.FileSystemViewManager      [] - Creating View Manager with storage type :REMOTE_FIRST
2022-01-21 10:46:56,123 INFO  org.apache.hudi.common.table.view.FileSystemViewManager      [] - Creating remote first table view
2022-01-21 10:46:56,123 INFO  org.apache.hudi.common.util.CommitUtils                      [] - Creating  metadata for null numWriteStats:11numReplaceFileIds:0
2022-01-21 10:46:56,123 INFO  org.apache.hudi.client.AbstractHoodieWriteClient             [] - Committing 20220121104557 action deltacommit
2022-01-21 10:46:56,196 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Marking instant complete [==>20220121104557__deltacommit__INFLIGHT]
2022-01-21 10:46:56,196 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Checking for file exists ?hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/20220121104557.deltacommit.inflight
2022-01-21 10:46:56,285 INFO  org.apache.hudi.common.table.timeline.HoodieActiveTimeline   [] - Create new file for toInstant ?hdfs://bdnode102:9000/hudi/lakehouse2_dwd_order_hudi/.hoodie/20220121104557.deltacommit
2022-01-21 10:46:56,






2022-01-21 13:31:59,569 ERROR org.apache.hudi.sink.StreamWriteOperatorCoordinator          [] - Executor executes action [sync hive metadata for instant 20220121133158] error
java.lang.NoSuchMethodError: org.apache.hadoop.hive.serde2.thrift.Type.getType(Lorg/apache/hudi/org/apache/hive/service/rpc/thrift/TTypeId;)Lorg/apache/hadoop/hive/serde2/thrift/Type;
	at org.apache.hudi.org.apache.hive.service.cli.TypeDescriptor.<init>(TypeDescriptor.java:47) ~[hudi-flink-bundle_2.11-0.9.0.jar:0.9.0]
	
	
	