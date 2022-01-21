

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
			
			Path rootHDFSDirPath = createRootHDFSDir(conf);{
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


