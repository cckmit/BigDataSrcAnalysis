
// 线程和模块功能
TM: 系统定时触发?: "Source: Custom Source -> Map -> Sink":  频率很高 是每个element还是定时触发?

StreamingFileSink.initializeState(){
	Buckets<IN, ?> buckets = bucketsBuilder.createBuckets(getRuntimeContext().getIndexOfThisSubtask());
	ProcessingTimeService procTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
	this.helper =new StreamingFileSinkHelper<>(buckets,context.isRestored(),
				context.getOperatorStateStore(), procTimeService, bucketCheckInterval);{
		this.procTimeService = procTimeService;
        if (isRestored) {
            buckets.initializeState(bucketStates, maxPartCountersState);
        }
        long currentProcessingTime = procTimeService.getCurrentProcessingTime();
		// 这里就将 this: StreamingFileSinkHelper 作为target变量 设定为 Time定时任务; 执行; 
        procTimeService.registerTimer(currentProcessingTime + bucketCheckInterval, this);{
			ProcessingTimeCallback targetCallback = addQuiesceProcessingToCallback(processingTimeCallbackWrapper.apply(target));
			return timerService.registerTimer(timestamp,targetCallback);{// SystemProcessingTimeService.registerTimer
				long delay =ProcessingTimeServiceUtil.getProcessingTimeDelay( timestamp, getCurrentProcessingTime());
				// delay = 1ms, 
				ScheduledTask task = wrapOnTimerCallback(callback, timestamp);
				return timerService.schedule(task, delay, TimeUnit.MILLISECONDS);{
					SystemProcessingTimeService.ScheduledTask.run(){
						callback.onProcessingTime(nextTimestamp);{
							ProcessingTimeServiceImpl.addQuiesceProcessingToCallback(ProcessingTimeCallback callback);{
								if (!isQuiesced()) {
									// callback: ProcessingTimeCallback = StreamTask.deferCallbackToMailbox() 中定义的
									// callback= timestamp -> { mailboxExecutor.execute(() -> invokeProcessingTimeCallback(callback, timestamp),"Timer callback for %s @ %d",callback,timestamp);};
									callback.onProcessingTime(timestamp);{
										// callback: StreamingFileSinkHelper, 
										ThrowingRunnable<? extends Exception> command = () -> invokeProcessingTimeCallback(callback, timestamp);{
											callback.onProcessingTime(timestamp);
										}
										// 这里execute就是把 command:ThrowingRunnable 添加到TaskMailboxImpl.queue: Deque<Mail>任务队列中
										MailboxExecutorImpl.execute(command,"Timer callback for %s @ %d",callback,timestamp);{
											mailbox.put(new Mail(command, priority, actionExecutor, descriptionFormat, descriptionArgs));{
												queue.addLast(mail);// queue: Deque<Mail> 就是任务队列; 
											}
										}
										
									}
								}
							}
						}
						nextTimestamp += period;
					}
				}
			}
		}
	}
}



TM: 系统定时触发?: "Source: Custom Source -> Map -> Sink":  高频触发: 检查达到滚动时间间隔(60s)后把inProgressPart置为null,以生成新文件;

StreamTask.invokeProcessingTimeCallback(ProcessingTimeCallback callback, timestamp){
	// 这个callback就是 StreamingFileSinkHelper
	callback.onProcessingTime(timestamp);{// StreamingFileSinkHelper.onProcessingTime
		buckets.onProcessingTime(currentTime);{//Buckets.
			// activeBuckets: Map<BucketID, Bucket<IN, BucketID>> , key=2022-04-07--09, 所以一个分区目录一个Bucket; 
			for (Bucket<IN, BucketID> bucket : activeBuckets.values()) {
				bucket.onProcessingTime(timestamp);{//Bucket.onProcessingTime() 对单个目录触发 onProcessTime()
					boolean shouldRoll = rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp);{
						boolean canRollByTime = currentTime - partFileState.getCreationTime() >= rolloverInterval;	//默认60s 1分钟滚动;
						boolean canRollByInactivityInterval = currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;	//默认60s,1分钟滚动;
						return canRollByTime || canRollByInactivityInterval;
					}
					if (inProgressPart != null && shouldRoll ) {
						closePartFile();{//Bucket.closePartFile() 把 Bucket.inProgressPart:InProgressFileWriter置为null 以滚动生产新的;
							if (inProgressPart != null) {
								pendingFileRecoverable = inProgressPart.closeForCommit();
								pendingFileRecoverablesForCurrentCheckpoint.add(pendingFileRecoverable);
								inProgressPart = null;
							}
						}
					}
				}
			}
		}
		procTimeService.registerTimer(currentTime + bucketCheckInterval, this);
	}
}





TM: 数据处理线程: "Legacy Source Thread: Custom Source -> Map -> Sink" : FileSink算子处理每一条要写出的 element;
	1. 根据每一个element先生成bucketId来查找Bucket分桶, 向该bucket.inProgressPart(分区文件)中追加数据; 
	2. 另一 ProcessingTime线程会按时间间隔滚动设置 inProgressPart=null,或 inProgressPart.size > partSize(128M)时, 则累加partCounter 并生成新part文件;

StreamingFileSink.invoke(IN value, SinkFunction.Context context){
	this.helper.onElement(value,context.timestamp(),context.currentWatermark());{//StreamingFileSinkHelper.
		buckets.onElement(value, currentProcessingTime, elementTimestamp, currentWatermark);{//Buckets.onElement()
			// 先根据element生成 bucketId: BucketID; 从activeBuckets:Map<BucketID, Bucket<IN, BucketID>> 中查找该Bucket; 
			final BucketID bucketId = bucketAssigner.getBucketId(value, bucketerContext);
			final Bucket<IN, BucketID> bucket = getOrCreateBucketForBucketId(bucketId);{
				Bucket<IN, BucketID> bucket = activeBuckets.get(bucketId);
				if (bucket == null) {// 若该bucket桶不存在,则新建
					bucket =bucketFactory.getNewBucket();
					activeBuckets.put(bucketId, bucket);
				}
				return bucket;
			}
			//用相应bucketId的 Bucket桶写出数据; 
			bucket.write(value, currentProcessingTime);{
				// 1. 在processTime线程中隔固定时间间隔后,会在closePartFile()中把 inProgressPart=null置空;
				// 2. 或者当 inProgressPart.size > rollingPolicy.partSize (默认128M)时,就 roll滚动生成新part文件; 
				boolean shouldRollFile = rollingPolicy.shouldRollOnEvent(inProgressPart, element);{
					// 比较当前子文件大小(partFile.size) 大于 rollingPolicy中设置的 文件大小时, 
					return partFileState.getSize() > partSize;
				}
				if (inProgressPart == null || shouldRollFile) {
					inProgressPart = rollPartFile(currentTime);{// Bucket.rollPartFile()
						closePartFile();
						final Path partFilePath = assembleNewPartPath();{ // 根据 Bucket.partCounter 变量来计数累加 子文件序号; 
							long currentPartCounter =  partCounter++;
							String newFilePath = outputFileConfig.getPartPrefix() + '-'+ subtaskIndex+ '-'+ currentPartCounter+ outputFileConfig.getPartSuffix();
							return new Path(bucketPath,newFilePath);
						}
						return bucketWriter.openNewInProgressFile(bucketId, partFilePath, currentTime);
					}
				}
				inProgressPart.write(element, currentTime);{//RowWisePartWriter.write(IN element, long currentTime)
					encoder.encode(element, currentPartStream);{//SimpleStringEncoder.encode()
						stream.write(element.toString().getBytes(charset));{//HadoopRecoverableFsDataOutputStream.write
							out.write(b, off, len);{// FSDataOutputStream.write()
								out.write(b, off, len);
								position += len; 
								if (statistics != null) {
									statistics.incrementBytesWritten(len);
								}
							}
						}
						stream.write('\n');
					}
					markWrite(currentTime);
				}
			}
		}
	}
}





JM/TM: checkpoint成功后commit: "Source: Custom Source -> Map -> Sink": 

StreamTask.notifyCheckpointComplete(checkpointId){
	subtaskCheckpointCoordinator.notifyCheckpointComplete(checkpointId, operatorChain, this::isRunning);{
		super.notifyCheckpointComplete(checkpointId);
		if (userFunction instanceof CheckpointListener) {
			((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);{
				
				StreamingFileSink.notifyCheckpointComplete(checkpointId){
					this.helper.commitUpToCheckpoint(checkpointId);{
						buckets.commitUpToCheckpoint(checkpointId);{//Buckets.commitUpToCheckpoint()
							Iterator<Map.Entry<BucketID, Bucket<IN, BucketID>>> activeBucketIt = activeBuckets.entrySet().iterator();
							while (activeBucketIt.hasNext()) {
								Bucket<IN, BucketID> bucket = activeBucketIt.next().getValue();
								// 核心: 就是这里触发提交: 
								bucket.onSuccessfulCompletionOfCheckpoint(checkpointId);{//Bucket.
									// it: Map<checkpointId,List<PendingFileRecoverable>>  遍历Bucket分桶下的每一个文件, 
									Iterator<Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>>> it  =pendingFileRecoverablesPerCheckpoint
										.headMap(checkpointId, true).entrySet().iterator();
									while (it.hasNext()) {//
										Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>> entry = it.next();
										for (InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable :entry.getValue()) {
											bucketWriter
												.recoverPendingFile(pendingFileRecoverable)
												.commit();{//OutputStreamBasedPartFileWriter$OutputStreamBasedPendingFile.commit()
													committer.commit();{//HadoopRecoverableFsDataOutputStream$HadoopFsCommitter.commit()
														
														// 这里进行 hdfs的 rename: 
														HadoopFsCommitter.commit(){
															Path src = recoverable.tempFile(); // .prefix-0-14.txt.inprogress.dfe9798d-78c4-4bd3-9843-c893997ca336
															Path dest = recoverable.targetFile();// prefix-0-14.txt
															mlong expectedLength = recoverable.offset();
															FileStatus srcStatus = fs.getFileStatus(src);
															fs.rename(src, dest);
														}
													}
												}
										}
										it.remove();
									}
								}
								if (!bucket.isActive()) {
									activeBucketIt.remove();
									notifyBucketInactive(bucket);
								}
							}
						}
					}
				}
				
			}
		}
	}
}


