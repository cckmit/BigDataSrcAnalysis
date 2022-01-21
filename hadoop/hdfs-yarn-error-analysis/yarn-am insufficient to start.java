2022-01-16 08:54:15,035 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue: 
Application removed - appId: application_1642039450749_0012 user: bigdata queue: 
default #user-pending-applications: 0 #user-active-applications: 0 #queue-pending-applications: 0 #queue-active-applications: 0

2022-01-16 08:54:15,035 WARN org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue: 
maximum-am-resource-percent is insufficient to start a single application in queue, it is likely set too low. 
skipping enforcement to allow at least one application to start
2022-01-16 08:54:15,035 WARN org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue: 
maximum-am-resource-percent is insufficient to start a single application in queue for user, it is likely set too low. 
skipping enforcement to allow at least one application to start


activateApplications:610, LeafQueue (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity)
addApplicationAttempt:659, LeafQueue (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity)
submitApplicationAttempt:475, LeafQueue (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity)
addApplicationAttempt:842, CapacityScheduler (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity)
handle:1256, CapacityScheduler (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity)
handle:121, CapacityScheduler (org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity)
run:671, ResourceManager$SchedulerEventDispatcher$EventProcessor (org.apache.hadoop.yarn.server.resourcemanager)
run:748, Thread (java.lang)



CapacityScheduler.handle(SchedulerEvent event){
	switch(event.getType()) {
		case NODE_ADDED:
		case NODE_REMOVED:
		case NODE_UPDATE:
		case APP_ADDED: {
			String queueName =resolveReservationQueueName(appAddedEvent.getQueue());
			if (queueName != null) {
				addApplication(appAddedEvent.getApplicationId(), queueName,appAddedEvent.getUser());
			}
		} 
			break;
		case APP_ATTEMPT_ADDED: 
			addApplicationAttempt(); {// CapacityScheduler.addApplicationAttempt()
				SchedulerApplication<FiCaSchedulerApp> application =applications.get(applicationAttemptId.getApplicationId());
				FiCaSchedulerApp attempt =new FiCaSchedulerApp(applicationAttemptId, application.getUser(),queue, queue.getActiveUsersManager(), rmContext);
				queue.submitApplicationAttempt(attempt, application.getUser());{//LeafQueue.submitApplicationAttempt()
					addApplicationAttempt(application, user);{
						pendingApplications.add(application);
						applicationAttemptMap.put(application.getApplicationAttemptId(), application);
						activateApplications();{//LeafQueue.activateApplications()
							// 这个代表什么? 怎么会是0了; 
							Resource amLimit = getAMResourceLimit();{//LeafQueue.getAMResourceLimit()
								// 队列剩余 <8192,1> 
								//queueResourceLimitsInfo: QueueResourceLimitsInfo{ queueCurrentLimit:Resource, clusterResource:Resource };
								Resource queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
								// 仅比较 内存,取其中最大; <8192,1> 
								Resource queueCap = Resources.max(resourceCalculator, lastClusterResource,absoluteCapacityResource, queueCurrentLimit);{
									return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
								}
								// maxAMResourcePerQueuePercent: AM资源占比, 由 PREFIX(yarn.scheduler.capacity.).maximum-am-resource-percent 指定,默认 0.1f; 
								// 取 queueCap.memory (8192) * maxAMResourcePerQueuePercent(0.1) + 0.5 作为memory; 
								return Resources.multiplyAndNormalizeUp( resourceCalculator, queueCap,maxAMResourcePerQueuePercent,minimumAllocation);{
									return calculator.multiplyAndNormalizeUp(lhs, by, factor);{//DefaultResourceCalculator.multiplyAndNormalizeUp(r,by,stepFactor)
										int a = (int)(r.getMemory() * by + 0.5);// 8192 * 0.1 + 0.5 = 819.5
										int memory = roundUp(a, stepFactor.getMemory());
										return Resources.createResource();
									}
								}
							}
							Resource userAMLimit = getUserAMResourceLimit();
							// 遍历所有的 pendingApplications: Set<FiCaSchedulerApp> 
							for (Iterator<FiCaSchedulerApp> i=pendingApplications.iterator(); i.hasNext(); ) {
								FiCaSchedulerApp application = i.next();
								
								// Check am resource limit
								Resource amIfStarted = Resources.add(application.getAMResource(), queueUsage.getAMUsed());
								boolean lessThan = Resources.lessThanOrEqual( resourceCalculator, lastClusterResource, amIfStarted, amLimit);{ // lessThanOrEqual(
									Resources.lessThanOrEqual(resourceCalculator,clusterResource, lhs, rhs){
										// 仅考虑内存资源, lhs.memory - rhs.memory ; 
										int compareNum = resourceCalculator.compare(clusterResource, lhs, rhs);{//DefaultResourceCalculator.compare()
											return lhs.getMemory() - rhs.getMemory(); 2048 - 0 = 2048;
										}
										return (compareNum <= 0); 2048 <= 0 = false;
									}
								}
								if (! lessThan) {
									if (getNumActiveApplications() < 1) {
										LOG.warn("maximum-am-resource-percent is insufficient to start a single application in queue, it is likely set too low"); 
									}else { // 
										LOG.info("not starting application as amIfStarted exceeds amLimit");
										continue;
									}
								}
								
								// Check user am resource limit
								User user = getUser(application.getUser());
								Resource userAmIfStarted = Resources.add(application.getAMResource(),user.getConsumedAMResources());
								
								if (!Resources.lessThanOrEqual(resourceCalculator, lastClusterResource, userAmIfStarted, userAMLimit)) {
									if (getNumActiveApplications() < 1) {
										LOG.warn("maximum-am-resource-percent is insufficient to start a single application in queue, it is likely set too low"); 
									}else { // 
										LOG.info("not starting application as amIfStarted exceeds userAmLimit");
										continue;
									}
								}
								
								user.activateApplication();
								activeApplications.add(application);
								queueUsage.incAMUsed(application.getAMResource());
								i.remove();
								LOG.info("Application " + application.getApplicationId() + " from user: " + application.getUser() + " activated in queue: " + getQueueName());
								
							}
						}
						
					}
				}
			}
			break;
		
		
	}
}




// yarn-rm  说 NM 不健康, 移除: reported UNHEALTHY with details: 1/1 local-dirs are bad

2022-01-19 11:46:26,240 INFO org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl: Node bdnode102.hjq.com:35650 reported UNHEALTHY with details: 1/1 local-dirs are bad: /opt/hadoop/tmpDir/nm-local-dir; 1/1 log-dirs are bad: /opt/hadoop/hadoop-2.7.2/logs/userlogs
2022-01-19 11:46:26,240 INFO org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl: bdnode102.hjq.com:35650 Node Transitioned from RUNNING to UNHEALTHY
2022-01-19 11:46:26,240 INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler: Removed node bdnode102.hjq.com:35650 cluster capacity: <memory:0, vCores:0>


// yarn-nodemgr日志


2022-01-18 22:44:17,440 WARN org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection: 
Directory /opt/hadoop/tmpDir/nm-local-dir error, used space above threshold of 90.0%, removing from list of valid directories

2022-01-18 22:44:17,440 WARN org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection: 
Directory /opt/hadoop/hadoop-2.7.2/logs/userlogs error, used space above threshold of 90.0%, removing from list of valid directories

2022-01-18 22:44:17,440 INFO org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService: Disk(s) failed: 1/1 local-dirs are bad: /opt/hadoop/tmpDir/nm-local-dir; 1/1 log-dirs are bad: /opt/hadoop/hadoop-2.7.2/logs/userlogs
2022-01-18 22:44:17,440 
ERROR org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService: 
Most of the disks failed. 1/1 local-dirs are bad: /opt/hadoop/tmpDir/nm-local-dir; 1/1 log-dirs are bad: /opt/hadoop/hadoop-2.7.2/logs/userlogs

2022-01-18 22:44:17,442 INFO org.apache.hadoop.yarn.event.AsyncDispatcher: Registering class org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType for class org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.NonAggregatingLogHandler

checkDirs:229, DirectoryCollection (org.apache.hadoop.yarn.server.nodemanager)
checkDirs:381, LocalDirsHandlerService (org.apache.hadoop.yarn.server.nodemanager)
access$400:49, LocalDirsHandlerService (org.apache.hadoop.yarn.server.nodemanager)
run:119, LocalDirsHandlerService$MonitoringTimerTask (org.apache.hadoop.yarn.server.nodemanager)
mainLoop:555, TimerThread (java.util)
run:505, TimerThread (java.util)




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



LocalDirsHandlerService.checkDirs(){
	Set<String> failedLocalDirsPreCheck =new HashSet<String>(localDirs.getFailedDirs());
	Set<String> failedLogDirsPreCheck =new HashSet<String>(logDirs.getFailedDirs());
	
	boolean isLocalDirsChanged = localDirs.checkDirs();{// DirectoryCollection.checkDirs()
		boolean setChanged = false;
		
		List<String> allLocalDirs =DirectoryCollection.concat(localDirs, failedDirs);
		// 标记每个文件目录,看其是否还是 DISK_FULL 状态; 
		Map<String, DiskErrorInformation> dirsFailedCheck = testDirs(allLocalDirs);{//DirectoryCollection.
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



