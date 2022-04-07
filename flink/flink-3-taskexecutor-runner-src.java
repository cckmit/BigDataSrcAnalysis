


//TM.1 TaskManger init 启动和初始化



// Window(TumblingEventTimeWindows(3000), EventTimeTrigger, CoGroupWindowFunction) -> Map -> Filter -> Sink: Print to Std. Err (2/4) 线程: 

Task.run(){
    doRun();{
        TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());
        Environment env = new RuntimeEnvironment();
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        
        // 这里invokable的实例对象是: OneInputStreamTask;是StreamTask的继承类:
        // StreamTask的还有其他3个继承类: AbstractTwoInputStreamTask, SourceReaderStreamTask, SourceStreamTask; ?
        invokable.invoke();{// OneInputStreamTask.invoke() -> 调用父类StreamTask.invoke()方法;
            StreamTask.invoke(){
                beforeInvoke(); // 真正消费数据前,先进行初始化
                // 在这里里面循环接受消息,并运行;
                runMailboxLoop();{
                    mailboxProcessor.runMailboxLoop();// 源码细节如下 MailboxProcessor.runMailboxLoop()
                }
                
                // 结束消费和处理后,资源释放;
                afterInvoke();
                
            }
        }
    }
}
 


// 1.2 Source 线程的 启动? 以 KafkaSource + FromCollect Source 为例 
SourceStreamTask.LegacySourceFunctionThread.run(){
    headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);{//StreamSource
        run(lockingObject, streamStatusMaintainer, output, operatorChain);{
            this.ctx = StreamSourceContexts.getSourceContext();
            userFunction.run(ctx);{//FlinkKafkaConsumerBase.run()
                
                FlinkKafkaConsumerBase.run(){
                    this.kafkaFetcher = createFetcher();
                    kafkaFetcher.runFetchLoop();{
                        final Handover handover = this.handover;
                        // 启动Kafka消费线程, 持续从Kafka消费数据;
                        consumerThread.start();
                        
                        while (running) {
                            //consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
                            final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
                            for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
                                List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
                                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                    // 这里是处理业务逻辑的核心方法: 
                                    emitRecord(value, partition, record.offset(), record);
                                }
                            }
                            
                        }
                    }
                    
                }
                
                FromElementsFunction.run(){ // 
                    ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
                    // 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
                    while (isRunning && numElementsEmitted < numElements) {
                        T next = serializer.deserialize(input);
                        // 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
                        ctx.collect(next);
                        numElementsEmitted++;
                    }
                }
                
            }
        }
    }
    completionFuture.complete(null);
}



// TM.2 Operator处理1条数据的 全流程

// TM2.1 Source, TM算子 轮询 获取 消息/ 待处理事件?

MailboxProcessor.runMailboxLoop(){//MailboxProcessor.runMailboxLoop()
	boolean hasAction = processMail(localMailbox);{//源码中while(processMail(localMailbox)); 没有Mail时会一直阻塞在此,有消息才
		if (!mailbox.createBatch(){// TaskMailboxImpl.createBatch()
			if (!hasNewMail) {
				return !batch.isEmpty();
			}
			
		}) {
			return true;
		}
		
		while (isDefaultActionUnavailable() && isMailboxLoopRunning()) { //循环阻塞在此,等到Mail消息; 有新消息才会进入while()循环中的 runDefaultAction();
			// 阻塞方法, 一直等到直到 queue不为空,取出了一个 headMail:Mail
			mailbox.take(MIN_PRIORITY).run();{//TaskMailboxImpl.take(int priority)
				Mail head = takeOrNull(batch, priority);
				while ((headMail = takeOrNull(queue, priority)) == null) {
					// 接受线程信号; 阻塞在此, 一旦("OutputFlusher for Source")线程发出 notEmpty.signal()信号,就结束等待,处理消息;
					notEmpty.await();
				}
				hasNewMail = !queue.isEmpty(); // 用于 createBatch()中判断;
				return headMail;
			}
		}
		return isMailboxLoopRunning();// return mailboxLoopRunning;
	}
	
	while (hasAction = processMail(localMailbox)) {//阻塞在条件判断的方法中, 判断还处于Running状态时,会进入下面的 runDefaultAction()
		mailboxDefaultAction.runDefaultAction(defaultActionContext); {
			this.processInput();{
				StreamTask.processInput();{
					// 这里不同的 inputProcessor:StreamInputProcessor 实现类,进行不同处理;
					InputStatus status = inputProcessor.processInput();{
						StreamoneInputProcessor.processInput();{}
					}
				}
			
				SourceStreamTask.processInput(controller);{
					
				}
				
				
				
			}
		}
	}
}

	// TM2.1.1 : StreamSource 算子 循环获取/生成 Record的 示例: Kafka Fetcher 为例
	FlinkKafkaConsumerBase.run(){
		this.kafkaFetcher = createFetcher();
		kafkaFetcher.runFetchLoop();{
			final Handover handover = this.handover;
			// 启动Kafka消费线程, 持续从Kafka消费数据;
			consumerThread.start();
			
			while (running) {
				//consumerThread线程拉取的kafka数据存放在handover这个中间容器/缓存中;
				final ConsumerRecords<byte[], byte[]> records = handover.pollNext();
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {
					List<ConsumerRecord<byte[], byte[]>> partitionRecords =records.records(partition.getKafkaPartitionHandle());
					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
						
						// 这里是处理业务逻辑的核心方法: 
						emitRecord(value, partition, record.offset(), record);{//KafkaFetcher.emitRecord()//源码细节 如下 KafkaFetcher.emitRecord()
							emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());
						}
					}
				}
				
			}
		}
		
	}




	// TM2.2 Source 算子处理1个 StreamRecord 


		// 2.2.1 Kafka Source 开始正式处理1条 Record 
		KafkaFetcher.emitRecord(){//KafkaFetcher.emitRecord()
			emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());{//AbstractFetcher.
				sourceContext.collectWithTimestamp(record, timestamp);{//StreamSourceContexts.
					collect(element);{//StreamSourceContexts.NonTimestampContext
						output.collect(reuse.replace(element));{//AbstractStreamOperator.CountingOutput
							numRecordsOut.inc();
							output.collect(record);{// Operator.CopyingChainingOutput.
								pushToOperator(record);{//
									StreamRecord<T> castRecord = (StreamRecord<T>) record;
									StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
									operator.processElement(copy);{//StreamMap.
										// 这个element:StreamRecord(value:T,hasTimestamp:Boolean,timestamp:Long);
										// 这个 userFunction 即用户定义的函数体: 相当于Java中的 @Interface 接口的函数实现; 
										// 这个userFunction 也可以是MapFunction的实现类: MyMapFuncImpl, 则直接调用 MyMapFuncImpl.map()进行处理;
										X element = userFunction.map(element.getValue());{// 
											
										}
										
										output.collect(element.replace(element){//StreamRecord.replace() 将原来.value字段替换成新的值,避免新new StreamRecord 的转换成本;
											this.value = (T) element; 
											return (StreamRecord<X>) this; //这里就是类型装换了, 因为原来可能是: value:String 类型 => value:(String,String) 
										});{//AbstractStreamOperator.CountingOutput.collect()
											numRecordsOut.inc(); //所谓的CountingOutput,就是这里简单对StreamRecord技术+1;
											output.collect(record);{//OperatorChain.WatermarkGaugeExposingOutput 的几个接口
												// outputs: 即addSink()所添加的输出; 
												OperatorChain.BroadcastingOutputCollector.collect(record){
													for (Output<StreamRecord<T>> output : outputs) {
														output.collect(record);
													}    
												}
												
												// 当该算子后面还接另一个filter/map()/等算子时,
												OperatorChain.CopyingChainingOutput.collect(){
													if (this.outputTag != null) return;
													//将该Record发给 Operator操作器;
													pushToOperator(record);{//CopyingChainingOutput.
														StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
														operator.processElement(copy);{ //对于FilterStream的苏州尼
															// 这里的userFunction: FilterFunction<T>的实现类 MyFilterFuncImpl的实例对象;
															if (userFunction.filter(element.getValue())) {
																output.collect(element);
															}
														}
													}
												}
												
												// Shuffle的写出的,就用这个Output;
												RecordWriterOutput[implements WatermarkGaugeExposingOutput].collect(){
													
												}
												
												// DirectedOutput[implements WatermarkGaugeExposingOutput]: 
												
												// ChiningOutput [implements WatermarkGaugeExposingOutput]: 
												
											}
										}
									}
								}
							}
						}
					}
				}
				partitionState.setOffset(offset);
			}
		}


		// 2.2.2  对于 FromElements的计算逻辑: 
		FromElementsFunction.run(){ // 
			ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
			// 循环遍历规定数量(numElements)的元素, 这个数是由Driver分配决定?
			while (isRunning && numElementsEmitted < numElements) {
				T next = serializer.deserialize(input);
				// 所谓的collect()就是一层层的往下面传;当需要shuffle或者输出了,就停止;
				ctx.collect(next);{//StreamSourceContexts.NonTimestampContext.collect()
					//中间标准的装换, 最后还是调: element.replace(userFunction.map()); 或者 userFunction.xxx(): map(),filter(),sum(),reduce()..
					output.collect(reuse.replace(element));-> output.collect(record);{//RecordWriterOutput.collect()
						if (this.outputTag != null) return;
						pushToRecordWriter(record);{//RecordWriterOutput.
							serializationDelegate.setInstance(record);
							recordWriter.emit(serializationDelegate);{
								int nextChannelToSendTo = channelSelector.selectChannel(record);{// RebalancePartitioner.selectChannel() 决定分区?
									nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
									return nextChannelToSendTo;
								}
								emit(record, nextChannelToSendTo);{//ChannelSelectorRecordWriter.emit()
									serializer.serializeRecord(record);
									
									boolean pruneTriggered = copyFromSerializerToTargetChannel(targetChannel);{// RecordWriter.copyFromSerializerToTargetChannel()
										SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
										while (result.isFullBuffer()) {
											finishBufferBuilder(bufferBuilder);
										}
										if (flushAlways) flushTargetPartition(targetChannel);
									}
									
									if (pruneTriggered) { //
										serializer.prune();
									}
								}
							}
						}
					}
				}
				numElementsEmitted++;
			}
		}



	// TM2.3 StreamTaskNetworkInput 网络传送的?
	
	StreamTaskNetworkInput.emitNext();{//StreamTaskNetworkInput.emitNext()
		while (true) {
			DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
			if (result.isFullRecord()) {//isFullRecord字段为true;
				processElement(deserializationDelegate.getInstance(), output);{//StreamTaskNetworkInput.
					if (recordOrMark.isRecord()){ //return getClass() == StreamRecord.class;
						output.emitRecord(recordOrMark.asRecord());{
							OneInputStreamTask.StreamTaskNetworkOutput.emitRecord();
						}
					} else if (recordOrMark.isWatermark()) { // return getClass() == Watermark.class;
						statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
					} else if (recordOrMark.isLatencyMarker()) { // return getClass() == LatencyMarker.class;
						output.emitLatencyMarker(recordOrMark.asLatencyMarker());
					} else if (recordOrMark.isStreamStatus()) {
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
					} else {
						throw new UnsupportedOperationException("Unknown type of StreamElement");
					} 
				}
			}
		}
	}


	PushingAsyncDataInput.DataOutput 接口的实现类:
	StreamTaskSourceOutput
	StreamTaskNetworkOutput
	SortingPhaseDataOutput
	CollectingDataOutput
	
	PushingAsyncDataInput.DataOutput.emitRecord(){
		// 但输出时 
		OneInputStreamTask.StreamTaskNetworkOutput.emitRecord(){
			numRecordsIn.inc();
			operator.setKeyContextElement1(record);
			operator.processElement(record);{// OneInputStreamOperator.processElement, 其实现类很多
				
				output.collect(element.replace(userFunction.map(element.getValue())));{
					KeyedStream$$anon$2.map(){
						applyWithState(in, cleanFun);{  // applyWithState(in: I, fun: (I, Option[S]) => (O, Option[S])): O
							val (o, s: Option[S]) = fun(in, Option(state.value())){
								// 这里就是用户定义的 计算逻辑: 累加求和;
								
							}
							
							s match {
								case Some(v) => state.update(v)
								case None => state.update(null.asInstanceOf[S])
							}
							
						}
						
					}
				}
			}
		}
	}
	
	OneInputStreamOperator 相关实现类: KeyedProcessOperator, AggregateWindowOperator , 
	
	
	OneInputStreamOperator.processElement(StreamRecord<IN> element){
		
		KeyedProcessOperator.processElement(element){
			collector.setTimestamp(element);
			context.element = element;
			// 在udf中 解析key, 并查找相应的 key的 state信息; 
			userFunction.processElement(element.getValue(), context, collector);{
				
				hudi.sink.StreamWriteFunction.processElement(){
					bufferRecord((HoodieRecord<?>) value);{
						final String bucketID = getBucketID(value);
						final DataItem item = DataItem.fromHoodieRecord(value);
						// 通过dirct内存直接放到hudi的 堆外中? 
						boolean flushBucket = bucket.detector.detect(item);{//StreamWriteFunction.BufferSizeDetector
							// sampling() random.nextInt(DENOMINATOR) == 1; 0.01 概率,进行 record.size 计算? 
							if (lastRecordSize == -1 || sampling()) {// 什么情况进入这里 ?
								// 第一次或 1%机会 要计算 record.size时, 加载到内存失败了; 
								lastRecordSize = ObjectSizeCalculator.getObjectSize(record);{
									ObjectSizeCalculator sizeCalc = new ObjectSizeCalculator(CurrentLayout.SPEC);{
										
										{// 其成功变量会创建大对象
											private final Set<Object> alreadyVisited = Collections.newSetFromMap(new IdentityHashMap<>());
											// 16 kb的对象,放不下? 
											private final Deque<Object> pending = new ArrayDeque<>(16 * 1024);
										}
										arrayHeaderSize = memoryLayoutSpecification.getArrayHeaderSize();
										objectHeaderSize = memoryLayoutSpecification.getObjectHeaderSize();
										objectPadding = memoryLayoutSpecification.getObjectPadding();
										referenceSize = memoryLayoutSpecification.getReferenceSize();
										superclassFieldPadding = memoryLayoutSpecification.getSuperclassFieldPadding();
									}
									return obj == null ? 0 : sizeCalc.calculateObjectSize(obj);
								}
							}
							totalSize += lastRecordSize;
							return totalSize > this.batchSizeBytes;
						}
						
						bucket.records.add(item);
					}
				}
				
			}
			// 通过 
			context.element = null;
		}
		
		AggregateWindowOperator
		
		CheckingTimelyStatefulOperator
		
	}
	
		
	
	


	
	// TM2.4 水位算子的处理; 
	StatusWatermarkValue.inputWatermark();{//StatusWatermarkValue.inputWatermark()
		// 重要逻辑: 基于水位的过滤?
		if (watermark.getTimestamp() > channelStatuses[channelIndex].watermark) {
			channelStatuses[channelIndex].watermark = watermarkMillis;
			findAndOutputNewMinWatermarkAcrossAlignedChannels();{
				if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
					lastOutputWatermark = newMinWatermark;
					output.emitWatermark(new Watermark(lastOutputWatermark));{//OneInputStreamTask.StreamTaskNetworkOutput 
						operator.processWatermark(watermark){//AbstractStreamOperator
							if (timeServiceManager != null) {
								timeServiceManager.advanceWatermark(mark);{//InternalTimeServiceManager
									for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
										service.advanceWatermark(watermark.getTimestamp());{//InternalTimeServiceManager
											while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
												eventTimeTimersQueue.poll();
												triggerTarget.onEventTime(timer);{// WindowOperator.onEventTime
													TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
													if (triggerResult.isFire()) {
														emitWindowContents(triggerContext.window, contents);{//WindowOperator.
															timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
															userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);{//InternalIterableWindowFunction.
																wrappedFunction.apply(key, window, input, out);
															}
														}
													}
												}
											}
										}
									}
								}
							}
							output.emitWatermark(mark);
							
						}
					}
				}
			}
		}
	}


	// TM2.5 Sink 算子
	



// TM.3 TaskExecutor 的辅助进程: 心跳, Rpc通信,ckp等













