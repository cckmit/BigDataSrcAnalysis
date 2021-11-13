

2020-06-29 07:19:38,377 ERROR [main] zookeeper.RecoverableZooKeeper: ZooKeeper create failed after 4 attempts
2020-06-29 07:19:38,378 ERROR [main] master.HMasterCommandLine: Master exiting
java.lang.RuntimeException: Failed construction of Master: class org.apache.hadoop.hbase.master.HMaster. 
	at org.apache.hadoop.hbase.master.HMaster.constructMaster(HMaster.java:2554)
	at org.apache.hadoop.hbase.master.HMasterCommandLine.startMaster(HMasterCommandLine.java:231)
	at org.apache.hadoop.hbase.master.HMasterCommandLine.run(HMasterCommandLine.java:137)
	at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:70)
	at org.apache.hadoop.hbase.util.ServerCommandLine.doMain(ServerCommandLine.java:127)
	at org.apache.hadoop.hbase.master.HMaster.main(HMaster.java:2564)
	
Caused by: org.apache.hadoop.hbase.ZooKeeperConnectionException: master:600000x0, quorum=ldsver53:2181,1dsver54:2181,ldsver55:2181, baseZNode=/hbase Unexpected KeeperException creating base node
	at org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.createBaseZNodes(ZooKeeperWatcher.java:211)
	at org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher.<init>(ZooKeeperWatcher.java:191)

// HMaster 主线程启动	
hbase.master.HMaster.main(){
	ServerCommandLine.doMain(){
		ToolRunner.run(){
			HMasterCommandLine.run(){
				HMasterCommandLine.startMaster(){
					HMaster.constructMaster(){
						
					}
				}
			}
		}
	}
}


MasterService.callBlockingMethod(){
	MasterRpcServices.createTable(){
		HMaster.createTable(){
			HMaster.ensureNamespaceExists(){
				HMaster.checkNamespaceManagerReady(){
					HMaster.checkInitialized(){
						
					}
				}
			}
			
		}
	}
}

ERROR: org.apache.hadoop.hbase.PleaseHoldException: Master is initializing
	at org.apache.hadoop.hbase.master.HMaster.checkInitialized(HMaster.java:2411)
	at org.apache.hadoop.hbase.master.HMaster.checkNamespaceManagerReady(HMaster.java:2416)
	at org.apache.hadoop.hbase.master.HMaster.ensureNamespaceExists(HMaster.java:2634)
	at org.apache.hadoop.hbase.master.HMaster.createTable(HMaster.java:1605)
	at org.apache.hadoop.hbase.master.MasterRpcServices.createTable(MasterRpcServices.java:472)
	at org.apache.hadoop.hbase.protobuf.generated.MasterProtos$MasterService$2.callBlockingMethod(MasterProtos.java:58714)
	at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:2191)
	at org.apache.hadoop.hbase.ipc.CallRunner.run(CallRunner.java:112)
	at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:183)
	at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:163)
