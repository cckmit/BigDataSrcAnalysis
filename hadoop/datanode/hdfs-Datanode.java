

DataNode.main(){
	secureMain(args, null);{
		StringUtils.startupShutdownMessage(DataNode.class, args, LOG);
		DataNode datanode = createDataNode(args, null, resources);{
			/*新建DN节点对象,其中比较重要的对象包括
			* 	* blockPoolManager: BlockPoolManager : 管理文件快
			*	* storage: DataStorage : 
			*	* conf: HdfsConfiguration 
			*	* ipcServer: ProtobufRpcEngine.Server 
			*/
			DataNode dn = instantiateDataNode(args, conf, resources);
			if (dn != null) {
				// 启动 blockPoolManager, dataXceiverServer, ipcServer 服务; 
				dn.runDatanodeDaemon();{
					blockPoolManager.startAll();
					dataXceiverServer.start();
					if (localDataXceiverServer != null) 
						localDataXceiverServer.start();
					ipcServer.start();
					startPlugins(conf);
				}
			}
			return dn;
		}
		if (datanode != null) {
			datanode.join();
		} else {
			errorCode = 1;
		}
	  terminate(errorCode);
	}
}



-- 心跳线程 "DataNode: [[[DISK]file:/tmp/hadoop/datanode/]]  heartbeating to bdnode101/192.168.51.101:9000"
BPServiceActor.run(){
	while (true) {
		connectToNNAndHandshake();{
			bpNamenode = dn.connectToNN(nnAddr);
			bpos.verifyAndSetNamespaceInfo(nsInfo);{//BPOfferService.verifyAndSetNamespaceInfo()
				dn.initBlockPool(this);{//DataNode.initBlockPool()
					NamespaceInfo nsInfo = bpos.getNamespaceInfo();
					blockPoolManager.addBlockPool(bpos);
					initStorage(nsInfo);{
						final StartupOption startOpt = getStartupOption(conf);
						storage.recoverTransitionRead(this, bpid, nsInfo, dataDirs, startOpt);{//DataStorage.recoverTransitionRead()
							recoverTransitionRead(datanode, nsInfo, dataDirs, startOpt);{
								addStorageLocations(datanode, nsInfo, dataDirs, startOpt, true, false);{
									
									ArrayList<StorageState> dataDirStates =new ArrayList<StorageState>(dataDirs.size());
									for(Iterator<StorageLocation> it = dataDirs.iterator(); it.hasNext();) {
										File dataDir = it.next().getFile();
										StorageDirectory sd = new StorageDirectory(dataDir);
										curState = sd.analyzeStorage(startOpt, this);{//Storage$StorageDirectory
											
											if (hasCurrent){
												return StorageState.COMPLETE_ROLLBACK;
											}
											return StorageState.RECOVER_ROLLBACK;
										}
										
										switch (curState) {
											case NORMAL:
											  break;
											case NON_EXISTENT:
											  it.remove();
											  continue;
											case NOT_FORMATTED: // format
											  format(sd, nsInfo, datanode.getDatanodeUuid());
											  break;
											default:  // recovery part is common
											  sd.doRecover(curState);
										}
									}
									
								}
							}
							for(StorageLocation dir : dataDirs) {
								
							}
						}
						if (data == null) {
							data = factory.newInstance(this, storage, conf);
						}
					}
					initPeriodicScanners(conf);
					data.addBlockPool(nsInfo.getBlockPoolID(), conf);
				}
			}
			register();
		}
	}
}

