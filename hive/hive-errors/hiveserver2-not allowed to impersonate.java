
2022-01-21T00:52:33,134  INFO [HiveServer2-Handler-Pool: Thread-31] thrift.ThriftCLIService: Client protocol version: HIVE_CLI_SERVICE_PROTOCOL_V10
2022-01-21T00:52:33,156  INFO [HiveServer2-Handler-Pool: Thread-31] conf.HiveConf: Using the default value passed in for log id: 492daa41-143d-4594-ab95-31eba3d2b655


// HiveServer2 新建1个Session的 源码; 

org.apache.thrift.ProcessFunction.process(){
	args.read(iprot);
	result = this.getResult(iface, args);{//TCLIService$Processor$OpenSession.getResult
		
		OpenSession.getResult(){
			org.apache.thrift.protocol.TProtocol prot = client.getProtocolFactory().getProtocol(memoryTransport);
			return (new Client(prot)).recv_OpenSession();{
				ThriftCLIService.getSessionHandle(){
					// 是否启用 doAs ,hive.server2.enable.doAs控制,访问方式不一样;as the user making the calls to it
					if (cliService.getHiveConf().getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS) && (userName != null)) {
						String delegationTokenStr = getDelegationToken(userName);
						sessionHandle = cliService.openSessionWithImpersonation(protocol, userName);{//CLIService.openSessionWithImpersonation
							SessionHandle sessionHandle = sessionManager.openSession(protocol, ipAddress, configuration);{//SessionManager.
								HiveSession hiveSession = createSession(null, protocol, username, password, ipAddress,delegationToken);{
									
									HiveSession session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());{
										UserGroupInformation.doAs(){
											AccessController.doPrivileged();
										}
									}
									
									session.setOperationManager(operationManager);
									session.open(sessionConf);{// HiveSessionImpl.open()
										sessionState = new SessionState(sessionConf, username);
										sessionState.setUserIpAddress(ipAddress);
										SessionState.start(sessionState);{
											start(startSs, false, null);{//SessionState.start() //源码详解 下面 SessionState.start()
												
												startSs.createSessionDirs(sessionUGI.getShortUserName());
												String engine = HiveConf.getVar(startSs.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
												
											}
											return startSs;
										}
										
										sessionState.loadAuxJars();
										sessionState.loadReloadableAuxJars();
									}
								}
								return hiveSession.getSessionHandle();
							}
							return sessionHandle;
						}
					}else{
						sessionHandle = cliService.openSession(protocol, userName, req.getPassword(),ipAddress, req.getConfiguration());
					}
				}
			}
		}
		
		GetTables_call.getResult(){
			
		}
	}
}


SessionState.start(){
	
	startSs.createSessionDirs(sessionUGI.getShortUserName());{// 
		HiveConf conf = getConf();
		Path rootHDFSDirPath = createRootHDFSDir(conf);{// rootHivePath= /tmp/hive 目录;
			FsPermission writableHDFSDirPermission = new FsPermission((short)00733);
			FileSystem fs = rootHDFSDirPath.getFileSystem(conf);
			// 这里报错了;bigdata is not allowed to impersonate bigdata
			// 调用 hdfs api: 
			if (!fs.exists(rootHDFSDirPath)) {// hadoop.fs.FileSystem.exists()
				Utilities.createDirsWithPermission(conf, rootHDFSDirPath, writableHDFSDirPermission, true);
			}
		}
		
		// 1. HDFS scratch dir
		path = new Path(rootHDFSDirPath, userName);
		hdfsScratchDirURIString = path.toUri().toString();
		// 3. Download resources dir
		path = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR));
		createPath(conf, path, scratchDirPermission, true, false);
		// 4. HDFS session path
		hdfsSessionPath = new Path(hdfsScratchDirURIString, sessionId);
		createPath(conf, hdfsSessionPath, scratchDirPermission, false, true);
		// 7. HDFS temp table space
		hdfsTmpTableSpace = new Path(hdfsSessionPath, TMP_PREFIX);
		conf.set(TMP_TABLE_SPACE_KEY, hdfsTmpTableSpace.toUri().toString());
		
	}
	
	String engine = HiveConf.getVar(startSs.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
	
}





2022-01-21T00:52:33,157  INFO [492daa41-143d-4594-ab95-31eba3d2b655 HiveServer2-Handler-Pool: Thread-31] conf.HiveConf: Using the default value passed in for log id: 492daa41-143d-4594-ab95-31eba3d2b655
2022-01-21T00:52:33,156  WARN [HiveServer2-Handler-Pool: Thread-31] service.CompositeService: Failed to open session
java.lang.RuntimeException: java.lang.RuntimeException: org.apache.hadoop.ipc.RemoteException(org.apache.hadoop.security.authorize.AuthorizationException): User: bigdata is not allowed to impersonate bigdata
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:89) ~[hive-service-2.3.1.jar:2.3.1]
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36) ~[hive-service-2.3.1.jar:2.3.1]
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63) ~[hive-service-2.3.1.jar:2.3.1]


Caused by: org.apache.hadoop.ipc.RemoteException: User: bigdata is not allowed to impersonate bigdata
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
	at org.apache.hadoop.hive.ql.session.SessionState.createRootHDFSDir(SessionState.java:704) ~[hive-exec-2.3.1.jar:2.3.1]
	at org.apache.hadoop.hive.ql.session.SessionState.createSessionDirs(SessionState.java:650) ~[hive-exec-2.3.1.jar:2.3.1]
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:582) ~[hive-exec-2.3.1.jar:2.3.1]
	at org.apache.hadoop.hive.ql.session.SessionState.start(SessionState.java:544) ~[hive-exec-2.3.1.jar:2.3.1]
	at org.apache.hive.service.cli.session.HiveSessionImpl.open(HiveSessionImpl.java:164) ~[hive-service-2.3.1.jar:2.3.1]
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_261]
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62) ~[?:1.8.0_261]
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[?:1.8.0_261]
	at java.lang.reflect.Method.invoke(Method.java:498) ~[?:1.8.0_261]
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78) ~[hive-service-2.3.1.jar:2.3.1]
	... 21 more
2022-01-21T00:52:33,156  INFO [HiveServer2-Handler-Pool: Thread-31] session.SessionState: Updating thread name to 492daa41-143d-4594-ab95-31eba3d2b655 HiveServer2-Handler-Pool: Thread-31
2022-01-21T00:52:33,157  INFO [HiveServer2-Handler-Pool: Thread-31] session.SessionState: Resetting thread name to  HiveServer2-Handler-Pool: Thread-31


