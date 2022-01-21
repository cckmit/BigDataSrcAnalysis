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














