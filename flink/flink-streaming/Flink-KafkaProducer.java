

// Flink Producer Sink 写入线程
-> 线程名: "Window(TumblingEventTimeWindows(10000), EventTimeTrigger, SiteWindowPipe) -> Flat Map -> Sink: Unnamed (2/2)#0"  in group "Flink Task Threads"

-- finkSrc.1
AbstractStreamOperator.processWatermark(){
	if (timeServiceManager != null) {
		timeServiceManager.advanceWatermark(mark);{//InternalTimeServiceManagerImpl.
			for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
				service.advanceWatermark(watermark.getTimestamp());{//InternalTimerServiceImpl.
					currentWatermark = time;
					while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
						eventTimeTimersQueue.poll();
						keyContext.setCurrentKey(timer.getKey());
						triggerTarget.onEventTime(timer);{//WindowOperator.onEventTime()
							
							TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
							
							if (triggerResult.isFire()) {
								ACC contents = windowState.get();
								 emitWindowContents(triggerContext.window, contents);{//WindowOperator.
									 // 用户逻辑1: 进入我写的 SiteWindowPipe.apply()中的逻辑; 
									 // 具体的 WindowFunction.process()执行逻辑和执行完 windowFunc后的 写入sink逻辑, 见下方 finkSrc.2 源码
									 userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
									 
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

-- finkSrc.2

 emitWindowContents(triggerContext.window, contents);{//WindowOperator.
	 // 用户逻辑1: 进入我写的 SiteWindowPipe.apply()中的逻辑; 
	 userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);{
		 // todo... user logic
		 TreeMap<Long, Map<String,Object>> timePointValues = calculateByUserFunction();
		 for(Map.Entry<Long, Map<String, Object>> entry: timePointValues.entrySet() ){
			 out.collect(deviceTimeData);{// 床给下个FlatMap算子 TimestampedCollector.collect()
				 output.collect(reuse.replace(record));
					-> output.collect(record);//CopyingChainingOutput.
					-> CopyingChainingOutput.pushToOperator(record);
					-> input.processElement(copy);//StreamSink
					-> userFunction.flatMap(element.getValue(), collector);{//StreamSink
						// User defined logic: 把  deviceTimeData 转成 String[] json字符串数组; 
						String[] lines = transformer.transformDataAsJsonStr(value, modelId, modelIdPath);
						for (String line : lines) {
							out.collect(Tuple2.of(assetId, line));{// TimestampedCollector.collect()
								 output.collect(reuse.replace(record));
									-> output.collect(record);//CopyingChainingOutput.
									-> CopyingChainingOutput.pushToOperator(record);
									-> input.processElement(copy);//StreamSink
									-> userFunction.invoke(element.getValue(), sinkContext);{// 
										// 这里接的是Sink,它由 两阶段提交 SinkFunc 的实现类 KafkaProducer实现; 
										TwoPhaseCommitSinkFunction.invoke(currentTransactionHolder.handle, value, context);{
											FlinkKafkaProducer.invoke(){
												byte[] serializedKey = keyedSchema.serializeKey(next);
												byte[] serializedValue = keyedSchema.serializeValue(next);
												String targetTopic = keyedSchema.getTargetTopic(next);
												if (keyedSchema != null) {
													if (flinkKafkaPartitioner != null) {// 用户由设置了 flinkKafkaPartitioner分区器, 就用用户分区; 
														int partiton= flinkKafkaPartitioner.partition(next, serializedKey, serializedValue, targetTopic, partitions);
														record = new ProducerRecord<>(targetTopic, partiton, timestamp, serializedKey, serializedValue);
													}else{
														record = new ProducerRecord<>(targetTopic, null, timestamp, serializedKey, serializedValue);
													}
												}else if (kafkaSchema != null){
													record = kafkaSchema.serialize(next, context.timestamp());
												}else{
													throw new RuntimeException("have neither KafkaSerializationSchema nor KeyedSerializationSchema")
												}
												// 下面就到了调用 KafkaClient的api了; 
												transaction.producer.send(record, callback);{// FlinkKafkaInternalProducer.send
													kafkaProducer.send(record, callback);{
														ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
														return doSend(interceptedRecord, callback);// 到Kafka的KafkaProducer.send() 代码了参考 -- kfkaSrc.3
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


-- kfkaSrc.3

/*
	KafkaProducer.send() 应该不是直接socket写入,而是先放到缓存区,
	靠 满后益写+ 定时刷新 ?
*/
kafka.clients.KafkaProducer.send(){
	ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
	return doSend(interceptedRecord, callback);{
		ClusterAndWaitTime clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
		byte[] serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
		byte[] serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
        // 这里就是Kafka分区策略, 若数据 ProducerRecord中partition不为空,则用单条数据的,否则用默认的 DefaultPartitioner
		int partition = partition(record, serializedKey, serializedValue, cluster);{
			Integer partition = record.partition();
			return partition != null ? partition :
                partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);{
					DefaultPartitioner.partition(){
						if (keyBytes == null) {
							return stickyPartitionCache.partition(topic, cluster);
						}
						List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
						int numPartitions = partitions.size();
						// 调用 MurmurHash2 的哈希算法, 适用于hash查找的: 很好的随机分布性，计算速度非常快，使用简单
						return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
					}
				}
    
		}
		
		tp = new TopicPartition(record.topic(), partition);
		setReadOnly(record.headers());
		int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(), compressionType, serializedKey, serializedValue, headers);
		long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
		RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,serializedValue, interceptCallback, true);
		return result.future;
	}
}



/** 最后一个WindowFunction 源码总结
*  位于线程: "Window(TumblingEventTimeWindows(xx)) -> Flat Map -> Sink" 线程; 
*	逻辑包括 WindoFun.apply() + KafkaProducer.send() 
*	英文是多线程并行, 所以应该由JVM中应该由多个 flinkKafkaProducer 示例
*/
