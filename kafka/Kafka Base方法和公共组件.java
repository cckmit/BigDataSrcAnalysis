
add:138, TimingWheel (kafka.utils.timer) [4]
add:153, TimingWheel (kafka.utils.timer) [3]
add:153, TimingWheel (kafka.utils.timer) [2]
add:153, TimingWheel (kafka.utils.timer) [1]
kafka$utils$timer$SystemTimer$$addTimerTaskEntry:91, SystemTimer (kafka.utils.timer)
add:84, SystemTimer (kafka.utils.timer)
tryCompleteElseWatch:223, DelayedOperationPurgatory (kafka.server)
kafka$coordinator$group$GroupCoordinator$$completeAndScheduleNextHeartbeatExpiration:625, GroupCoordinator (kafka.coordinator.group)
handleHeartbeat:384, GroupCoordinator (kafka.coordinator.group)
handleHeartbeatRequest:1243, KafkaApis (kafka.server)
handle:109, KafkaApis (kafka.server)
run:66, KafkaRequestHandler (kafka.server)
run:745, Thread (java.lang)


/**Base方法: SystemTimer.addTimerTaskEntry(): 根据expiration时刻判断是:立马submit() or delayQueue.offser() 本轮执行 or 放入overflowWheel下一轮执行;
*  
	addTimerTaskEntry(new TimerTaskEntry(timerTask));{//SystemTimer.addTimerTaskEntry()
		val addedForWaiting = timingWheel.add(timerTaskEntry);{//TimingWheel.add()若逾期执行时间expiration,在下个刻度(tickMs)内返回false立马执行; 在一个轮长(200ms)内 则添加到queue并返回true; 若在本轮以后,则添加到overflowWheel等待并返回true;
			val expiration = timerTaskEntry.expirationMs
			if (timerTaskEntry.cancelled) {
				false
			} else if (expiration < currentTime + tickMs) {//tickMs两个Entry间执行间隔(默认1 ms?(有时20,40,400,8000)?),若小于 表示该taskEntry已经到了(或过了)执行时间;
				false // expiration < 下一个执行刻度, 添加到timingWheel失败,返回false需要后面立马taskExecutor.submit()提交 或取消;
			} else if (expiration < currentTime + interval) {
				bucket.add(buckets((expiration / tickMs % wheelSize.toLong).toInt))
				queue.offer(bucket) //添加到SystemTimer.delayQueue队列,在"ExpirationReaper-0-{}"线程中被循环poll()出被执行或继续addTimerTaskEntry()
				true //若预期执行时间(expiration) 就在本轮interval(200毫秒)以内,则将该taskEntry加到delayQueue队里等待执行;
			} else {
			  if (overflowWheel == null) addOverflowWheel()
			  overflowWheel.add(timerTaskEntry)
			}
		}
		if(!addedForWaiting){//进入这里代表expiration <currentTime+tickMs 下个刻度前需要立马执行了;(或者cancelled)
			taskExecutor.submit(timerTaskEntry.timerTask);//启动一个名为"executor-{executorName}"的线程,执行其 onComplete(), onExpiration()方法;
		}
	}
*/

SystemTimer.addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs)){
	val addedForWaiting = timingWheel.add(timerTaskEntry);{//TimingWheel.add()若逾期执行时间expiration,在下个刻度(tickMs)内返回false立马执行; 在一个轮长(200ms)内 则添加到queue并返回true; 若在本轮以后,则添加到overflowWheel等待并返回true;
		val expiration = timerTaskEntry.expirationMs
		if (timerTaskEntry.cancelled) {
			false
		} else if (expiration < currentTime + tickMs) {//tickMs两个Entry间执行间隔(默认1 ms?(有时20,40,400,8000)?),若小于 表示该taskEntry已经到了(或过了)执行时间;
			false // expiration < 下一个执行刻度, 添加到timingWheel失败,返回false需要后面立马taskExecutor.submit()提交 或取消;
		} else if (expiration < currentTime + interval) {
			bucket.add(buckets((expiration / tickMs % wheelSize.toLong).toInt))
			queue.offer(bucket) //添加到SystemTimer.delayQueue队列,在"ExpirationReaper-0-{}"线程中被循环poll()出被执行或继续addTimerTaskEntry()
			true //若预期执行时间(expiration) 就在本轮interval(200毫秒)以内,则将该taskEntry加到delayQueue队里等待执行;
		} else {
		  if (overflowWheel == null) addOverflowWheel()
		  overflowWheel.add(timerTaskEntry)
		}
	}
	
	if (!addedForWaiting){//进入这里代表expiration <currentTime+tickMs 下个刻度前需要立马执行了;(或者cancelled)
		if (!timerTaskEntry.cancelled){
			
			/* 启动一个名为"executor-{executorName}"的线程,执行其 onComplete(), onExpiration()方法;
			*	- 关于线程名: 在 taskExecutor = Executors.newFixedThreadPool()时: Utils.newThread("executor-"+executorName, runnable, false)
			*/
			taskExecutor.submit(timerTaskEntry.timerTask);{//ThreadPoolExecutor -> execute(ftask):
				DelayedOperation.run(){//各种不同的Delayed子类,这里都是共用 Delayed的run()方法;
					val isCompleted= forceComplete(){//DelayedOperation.:
						if (completed.compareAndSet(false, true)) {
							cancel()
							onComplete(){// 抽象类,由继承类实现
								//1. 当 timeTask==DelayedFetch时, 
								DelayedFetch.onComplete(){
									val logReadResults = replicaManager.readFromLocalLog()
									//..
								}
								
								// 2. timeTask==DelayedCreateTopics时
								DelayedCreateTopics.onComplete(){
									val results = createMetadata.map { metadata =>
									  if (metadata.error.isSuccess && missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet) > 0)
										(metadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
									  else
										(metadata.topic, metadata.error)
									}.toMap
									
									responseCallback(results)
								}
								
								// 心跳检测
								
							}
							true
						}else { false}
					}
					
					if(isCompleted){
						onExpiration();{//抽象类,由子类方法实现;
							
							DelayedHeartbeat.onExpiration();{//DelayedHeartbeat.: 心跳检测;
								coordinator.onExpireHeartbeat(group, member, heartbeatDeadline);{//GroupCoordinator.onExpireHeartbeat()
									group synchronized {
										if (!shouldKeepMemberAlive(member, heartbeatDeadline))
											onMemberFailure(group, member);{//GroupCoordinator.onMemberFailure()
												debug(s"Member ${member.memberId} in group ${group.groupId} has failed")
												group.remove(member.memberId)//从内存中移除该成员(的clientId);
												group.currentState match {
													case Dead | Empty =>
													case Stable | AwaitingSync => maybePrepareRebalance(group)
													case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
												}
											}
									}
								}
							}
							
						}
					}
				}
			}
		}
	}
}



// Base组件之: 定时任务之DelayedOperationPurgatory 重要方法: 
	- tryCompleteElseWatch()
	- checkAndComplete()



DelayedOperation.DelayedOperationPurgatory {
	/* 方法1:  先safeTryComplete()两次,若还未completed就addTimerTaskEntry()添加到timingWheel或立马submit();
	* 
		tryCompleteElseWatch(delayedCreate, delayedCreateKeys){
			var isCompletedByMe = operation.safeTryComplete()//默认调父类DelayedOperation.safeTryComplete(),有些如Heartbeat有自己实现的方法;
			if (isCompletedByMe){return true;}//会进行2次的 safeTryComplete(),如果完成了直接结束返回;
			if (!operation.isCompleted) {//该DelayedOperation.completed 还是false;
				timeoutTimer.add(operation){//SystemTime.add()
					addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs));//根据expiration时刻判断是:立马submit() or delayQueue.offser() 本轮执行 or 放入overflowWheel下一轮执行;
				}
			}
		}
	*/
	DelayedOperationPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys){//DelayedOperation.tryCompleteElseWatch(operation: T, watchKeys: Seq[Any])
		var isCompletedByMe = operation.safeTryComplete(){//默认调父类DelayedOperation.safeTryComplete(),有些如Heartbeat有自己实现的方法;
			synchronized{
				tryComplete(){//抽象方法, 由各种实现类执行;
					DelayedCreateTopics.tryComplete();
					DelayedHeartbeat.tryComplete();
				}
			}
		}
		if (isCompletedByMe){return true;}
		var watchCreated = false
		for(key <- watchKeys) {
			if (operation.isCompleted) return false
			watchForOperation(key, operation);{//DelayedOperationPurgatory.watchForOperation()
				inReadLock(removeWatchersLock) {
					val watcher = watchersForKey.getAndMaybePut(key)
					watcher.watch(operation)
				}
			}
			if (!watchCreated) {
				watchCreated = true
				estimatedTotalOperations.incrementAndGet()
			}
		}
		//再对该 DelayOperation进行safeTryComplete()操作;如果其DelayedOperation.completed==true,则直接结束方法返回true; 否则
		isCompletedByMe = operation.safeTryComplete()
		if (isCompletedByMe) return true
		
		// 进入这里,说明该 DelayedOperation上面2次safeTryComplete()都不能完成计算; 需要将其添加到SystemTimer.delayQueue队列中等待定时执行;
		if (!operation.isCompleted) {//该DelayedOperation.completed 还是false;
			if (timerEnabled){
				timeoutTimer.add(operation){//SystemTime.add()
					readLock.lock()
					try {
						addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs));
					} finally {
						readLock.unlock()
					}
				}
			}
			if (operation.isCompleted) {
				operation.cancel()
			}
		}
		return false
	}
	
	
	/* 方法2 : 
		checkAndComplete(memberKey){
			if(watchers != null) watchers.tryCompleteWatched();{//Watchers.tryCompleteWatched(operation,watchKeys)
				while (operations.iterator().hasNext) {
					if(curr.isCompleted) {//若该DelayOperation.completed ==true,表示操作已经执行外;这里直接从该Watchers.operations中移除该实例;
						iter.remove()//从watchers.operations中删除该DelayOperation实例;
					} else if (curr.safeTryComplete(){//DelayedOperation.safeTryComplete()
						synchronized {tryComplete();{//抽象类,由子类实现:
							DelayedHeartbeat.tryComplete();
						}}
					}){
						iter.remove();//若调用其safeTryComplete()方法也正常执行,也将该实例从operations中移除;
					}
				}
			}
		}
	*/
	DelayedOperationPurgatory.checkAndComplete(memberKey);{//以memberId为key
		val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
		if(watchers == null)
			0
		else
			watchers.tryCompleteWatched();{//Watchers.tryCompleteWatched()
				var completed = 0
				val iter = operations.iterator() //Watchers.operations:ConcurrentLinkedQueue[T], 在 DelayedOperationPurgatory.tryCompleteElseWatch()中添加;
				while (iter.hasNext) {
					val curr = iter.next()
					if (curr.isCompleted) {//若该DelayOperation.completed ==true,表示操作已经执行外;这里直接从该Watchers.operations中移除该实例;
						iter.remove() //从watchers.operations中删除该DelayOperation实例;
					} else if (curr.safeTryComplete();{//DelayedOperation.safeTryComplete()
						synchronized {
							tryComplete();{//抽象类,由子类实现:
								DelayedHeartbeat.tryComplete();//心跳检测相关的实现类;
								
							}
						}
					}) {
						iter.remove();//若调用其safeTryComplete()方法也正常执行,也将该实例从operations中移除;
						completed += 1
					}
				}
				if (operations.isEmpty)//正常进入这里;
					removeKeyIfEmpty(key, this)
				completed
			}
	}
}


// Base组件之 : 
RequestFuture.raise(DisconnectException.INSTANCE);{//RequestFuture.raise()
	RequestFuture.fireFailure();{
		while(true){
			RequestFutureListener<T> listener = listeners.poll();
			listener.onFailure(exception);{
				AbstractCoordinator.CoordinatorResponseHandler.onFailure();
				GroupCoordinatorResponseHandler.onFailure();
			}
		}
	}
}


// Base组件之 DelayedOperation的方法
	- DelayedOperation.run()

DelayedOperation.run(){
	val isForceCompleted = forceComplete(){//DelayedOperation.forceComplete()
		//该DelayedOperation完成的标志,就是将其DelayedOperation.completed从false成功置为true;
		if (completed.compareAndSet(false, true)) {//重要步骤:标记完成状态为true;
			cancel(){//DelayedOperation.cancel()
				synchronized {
				  if (timerTaskEntry != null) timerTaskEntry.remove()
				  timerTaskEntry = null
				}
			}
			onComplete();{//抽象方法,由子类实现;
				
			}
			true
		} else {
			false
		}
	}
	if (isForceCompleted)
		onExpiration();{//DelayedHeartbeat的抽象方法,有子类实现; 
			
		}
}