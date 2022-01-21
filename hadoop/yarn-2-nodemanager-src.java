
// yarnNM.1: NodeManager 启动和初始化

	// 1.1 启动定时线程, 定时做 LocalDirsHandlerService.checkDirs()
	LocalDirsHandlerService.serviceStart(){
		if (isDiskHealthCheckerEnabled) {
			dirsHandlerScheduler = new Timer("DiskHealthMonitor-Timer", true);
			// 以固定频率,定时checkDirs(), 
			// diskHealthCheckInterval, yarn.nodemanager.disk-health-checker.interval-ms, 默认 2 * 60 * 1000 = 2分钟;
			dirsHandlerScheduler.scheduleAtFixedRate(monitoringTimerTask,diskHealthCheckInterval, diskHealthCheckInterval);{
				LocalDirsHandlerService.MonitoringTimerTask.run(){
					LocalDirsHandlerService.checkDirs();// 源码如下; 
				}
			}
		}
	}


// yarnNM.2: NodeManager 提供 Container 启停和管理服务




launchContainer:211, DefaultContainerExecutor (org.apache.hadoop.yarn.server.nodemanager)
call:302, ContainerLaunch (org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher)
call:82, ContainerLaunch (org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher)
run:266, FutureTask (java.util.concurrent)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)






// yarnNM.3: 辅助服务和定时检查线程 : checkDir(), 

	// 3.1 定时执行 checkDir; 

	LocalDirsHandlerService.checkDirs(){
		Set<String> failedLocalDirsPreCheck =new HashSet<String>(localDirs.getFailedDirs());
		Set<String> failedLogDirsPreCheck =new HashSet<String>(logDirs.getFailedDirs());
		boolean isLocalDirsChanged = localDirs.checkDirs();{// DirectoryCollection.checkDirs()
			boolean setChanged = false;
			List<String> allLocalDirs =DirectoryCollection.concat(localDirs, failedDirs);
			// 标记每个文件目录,看其是否还是 DISK_FULL 状态; 
			Map<String, DiskErrorInformation> dirsFailedCheck = testDirs(allLocalDirs);{//DirectoryCollection.testDirs()
				for (final String dir : dirs) {
					File testDir = new File(dir);
					DiskChecker.checkDir(testDir);
					// 先判断 Usage使用率是否超过 diskUtilizationPercentageCutoff (参数 max-disk-utilization-per-disk-percentage,默认 90)
					if (isDiskUsageOverPercentageLimit(testDir)) {
						ret.put(dir,new DiskErrorInformation(DiskErrorCause.DISK_FULL, msg));
						continue
					}else if(isDiskFreeSpaceUnderLimit(testDir)){
						ret.put(dir,new DiskErrorInformation(DiskErrorCause.DISK_FULL, msg));
						continue;
					}
					
					verifyDirUsingMkdir(testDir);
				}
				return ret;
			}
			fullDirs.clear();
			
			// 只要有任何 dist不是 DISK_FULL 状态的, 就标记 setChanged=true; 返回异常情况; 
			for (Map.Entry<String, DiskErrorInformation> entry : dirsFailedCheck.entrySet()) {
				DiskErrorInformation errorInformation = entry.getValue();	
				switch (entry.getValue().cause) {
					case DISK_FULL:fullDirs.add(entry.getKey());break;
					case OTHER:errorDirs.add(entry.getKey());break;
				}
				if (preCheckGoodDirs.contains(dir)) {
					LOG.warn("Directory " + dir + " error, " + errorInformation.message + ", removing from list of valid directories");
					setChanged = true;
					numFailures++;
				}
				
			}
			
			return setChanged;
		}
		if (localDirs.checkDirs()) {
			disksStatusChange = true;
		}
		if (logDirs.checkDirs()) {
			disksStatusChange = true;
		}
		
		
		if (!disksFailed) {
			disksFailed =disksTurnedBad(failedLogDirsPreCheck, failedLogDirsPostCheck);
		}
		if (!disksTurnedGood) {
		  disksTurnedGood =disksTurnedGood(failedLogDirsPreCheck, failedLogDirsPostCheck);
		}
		
		logDiskStatus(disksFailed, disksTurnedGood);
	}




