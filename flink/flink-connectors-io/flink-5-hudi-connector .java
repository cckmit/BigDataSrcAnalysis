

//当 insert into hudi_table 时


HoodieTableSink.getSinkRuntimeProvider(Context context): SinkRuntimeProvider {
	 // setup configuration
      long ckpTimeout = dataStream.getExecutionEnvironment()
          .getCheckpointConfig().getCheckpointTimeout();
      conf.setLong(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT, ckpTimeout);
	
	// bulk_insert mode
    final String writeOperation = this.conf.get(FlinkOptions.OPERATION);
    if (WriteOperationType.fromValue(writeOperation) == WriteOperationType.BULK_INSERT) {
	
	}
	
	// 1, 将 RowData 转换成 HoodieRecord 格式; 
	// stream write 
	int parallelism = dataStream.getExecutionConfig().getParallelism();
	StreamWriteOperatorFactory<HoodieRecord> operatorFactory = new StreamWriteOperatorFactory<>(conf);
	RowDataToHoodieFunction<RowData, HoodieRecord> rowDataFunction = RowDataToHoodieFunctions.create(rowType, conf);{
		if (conf.getLong(FlinkOptions.WRITE_RATE_LIMIT) > 0) {
			return new RowDataToHoodieFunctionWithRateLimit<>(rowType, conf);
		}else {
			return new RowDataToHoodieFunction<>(rowType, conf); {
				
				void open(Configuration parameters){
					super.open(parameters);
					this.avroSchema = StreamerUtil.getSourceSchema(this.config);
					this.converter = RowDataToAvroConverters.createConverter(this.rowType);
					this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(flinkConf2TypedProperties(FlinkOptions.flatOptions(this.config)));
					this.payloadCreation = PayloadCreation.instance(config);
				}
				// map 转化方法
				O map(I i) {
					return (O) toHoodieRecord(i);{// RowDataToHoodieFunction.toHoodieRecord()
						GenericRecord gr = (GenericRecord) this.converter.convert(this.avroSchema, record);
						final HoodieKey hoodieKey = keyGenerator.getKey(gr);
						HoodieRecordPayload payload = payloadCreation.createPayload(gr);
						HoodieOperation operation = HoodieOperation.fromValue(record.getRowKind().toByteValue());
						return new HoodieRecord<>(hoodieKey, payload, operation);
					}
				}
			}
		}
	}
	DataStream<HoodieRecord> dataStream1 = dataStream.map(rowDataFunction, TypeInformation.of(HoodieRecord.class));
	
	// bootstrap index
	if (conf.getBoolean(FlinkOptions.INDEX_BOOTSTRAP_ENABLED)) {
		ProcessOperator indexProcess = new ProcessOperator<>(new BootstrapFunction<>(conf));
		dataStream1 = dataStream1.rebalance()
            .transform("index_bootstrap",TypeInformation.of(HoodieRecord.class),indexProcess )
            .setParallelism(conf.getOptional(FlinkOptions.INDEX_BOOTSTRAP_TASKS).orElse(parallelism))
            .uid("uid_index_bootstrap_" + conf.getString(FlinkOptions.TABLE_NAME));
	}
	
	// bucket_assigner: Key-by record key, to avoid multiple subtasks write to a bucket at the same time
	DataStream<Object> pipeline = dataStream1
          .keyBy(HoodieRecord::getRecordKey)
          .transform("bucket_assigner", TypeInformation.of(HoodieRecord.class),
              new BucketAssignOperator<>(new BucketAssignFunction<>(conf)))
          .uid("uid_bucket_assigner_" + conf.getString(FlinkOptions.TABLE_NAME))
          .setParallelism(conf.getOptional(FlinkOptions.BUCKET_ASSIGN_TASKS).orElse(parallelism))
          // shuffle by fileId(bucket id)
          .keyBy(record -> record.getCurrentLocation().getFileId())
          .transform("hoodie_stream_write", TypeInformation.of(Object.class), operatorFactory)
          .uid("uid_hoodie_stream_write" + conf.getString(FlinkOptions.TABLE_NAME))
          .setParallelism(conf.getInteger(FlinkOptions.WRITE_TASKS));
	
	// compaction
	// equals(FlinkOptions.TABLE_TYPE_MERGE_ON_READ) && conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED)
	if (StreamerUtil.needsAsyncCompaction(conf)) { // MERGE_ON_READ 模式且 compaction.async.enabled=true 时;
		
		compactFunction = new ProcessOperator<>(new CompactFunction(conf));{
			
			void open(Configuration parameters){
				this.taskID = getRuntimeContext().getIndexOfThisSubtask();
				this.writeClient = StreamerUtil.createWriteClient(conf, getRuntimeContext());
				if (this.asyncCompaction) {
				  this.executor = new NonThrownExecutor(LOG);
				}
			}
			
			processElement(CompactionPlanEvent event, context, Collector<CompactionCommitEvent> collector){
				final String instantTime = event.getCompactionInstantTime();
				final CompactionOperation compactionOperation = event.getOperation();
				
				if (asyncCompaction) { // 异步执行压缩; 
					executor.execute( () -> doCompaction(instantTime, compactionOperation, collector) );
				}else{ // 串行 压缩
					LOG.info("Execute compaction for instant {} from task {}", instantTime, taskID);
					doCompaction(instantTime, compactionOperation, collector);{
						List<WriteStatus> writeStatuses = FlinkCompactHelpers.compact(writeClient, instantTime);{
							HoodieFlinkMergeOnReadTableCompactor compactor = new HoodieFlinkMergeOnReadTableCompactor();
							return compactor.compact(new HoodieFlinkCopyOnWriteTable<>());
						}
						collector.collect(new CompactionCommitEvent(instantTime, writeStatuses, taskID));
					}
				}
			}
		}
		
		return pipeline.transform("compact_plan_generate",
            TypeInformation.of(CompactionPlanEvent.class),
            new CompactionPlanOperator(conf))
            .setParallelism(1) // plan generate must be singleton
            .rebalance()
			// 压缩相关逻辑; 
            .transform("compact_task", TypeInformation.of(CompactionCommitEvent.class),compactFunction)
            .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS))
			// 压缩并提交 主逻辑; 
            .addSink(new CompactionCommitSink(conf)) {
				CompactionCommitSink.invoke(CompactionCommitEvent event, Context context){
					final String instant = event.getInstant();
					// 生产消费者模式,异步提交 CommitEvent 
					commitBuffer.computeIfAbsent(instant, k -> new ArrayList<>()).add(event);
					commitIfNecessary(instant, commitBuffer.get(instant));{//CompactionCommitSink.commitIfNecessary
						HoodieCompactionPlan compactionPlan = CompactionUtils.getCompactionPlan();
						boolean isReady = compactionPlan.getOperations().size() == events.size();
						if (!isReady) {
							return;
						}
						
						// 准备提交元数据
						if (this.writeClient.getConfig().shouldAutoCommit()) {
							// Prepare the commit metadata.
							HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
							for (HoodieWriteStat stat : updateStatusMap) {
								metadata.addWriteStat(stat.getPartitionPath(), stat);
							}
							metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, writeClient.getConfig().getSchema());
							this.writeClient.completeCompaction(metadata, statuses, this.writeClient.getHoodieTable(), instant);
						}
						
						// commit the compaction 核心,正式提交 
						this.writeClient.commitCompaction(instant, statuses, Option.empty());{
							HoodieFlinkTable<T> table = getHoodieTable();
							HoodieCommitMetadata metadata = FlinkCompactHelpers.newInstance().createCompactionMetadata();
							extraMetadata.ifPresent(m -> m.forEach(metadata::addMetadata));
							completeCompaction(metadata, writeStatuses, table, compactionInstantTime);{
								finalizeWrite(table, compactionCommitTime, writeStats);{
									table.finalizeWrite(context, instantTime, stats);{//HudiTable.finalizeWrite
										reconcileAgainstMarkers(context, instantTs, stats, config.getConsistencyGuardConfig().isConsistencyCheckEnabled());{
											WriteMarkers markers = WriteMarkersFactory.get(config.getMarkersType(), this, instantTs);
											
											Set<String> invalidDataPaths = getInvalidDataPaths(markers);
											// Contains list of partially created files. These needs to be cleaned up.
											invalidDataPaths.removeAll(validDataPaths);
											if (!invalidDataPaths.isEmpty()) {
												
												// Now delete partially written files
												deleteInvalidFilesByPartitions(context, invalidPathsByPartition);
											}
											
										}
									}
								}
								LOG.info("Committing Compaction {} finished with result {}.", compactionCommitTime, metadata);
								FlinkCompactHelpers.newInstance().completeInflightCompaction(table, compactionCommitTime, metadata);
							}
						}
						
						// Whether to cleanup the old log file when compaction
						if (!conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED)) {
							this.writeClient.clean();
						}
					}
				}
			}
            .name("compact_commit")
            .setParallelism(1);
			
	}else {
		// 直接提交和清理
		return pipeline.addSink(new CleanFunction<>(conf))
            .setParallelism(1)
            .name("clean_commits");
	}
	
	

}





processElement:78, LookupJoinRunner (org.apache.flink.table.runtime.operators.join.lookup)
processElement:34, LookupJoinRunner (org.apache.flink.table.runtime.operators.join.lookup)
processElement:66, ProcessOperator (org.apache.flink.streaming.api.operators)
pushToOperator:71, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:46, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:26, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:50, CountingOutput (org.apache.flink.streaming.api.operators)
collect:28, CountingOutput (org.apache.flink.streaming.api.operators)
processElement:39, StreamFilter (org.apache.flink.streaming.api.operators)
pushToOperator:71, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:46, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:26, CopyingChainingOutput (org.apache.flink.streaming.runtime.tasks)
collect:50, CountingOutput (org.apache.flink.streaming.api.operators)
collect:28, CountingOutput (org.apache.flink.streaming.api.operators)
processAndCollect:317, StreamSourceContexts$ManualWatermarkContext (org.apache.flink.streaming.api.operators)
collect:411, StreamSourceContexts$WatermarkContext (org.apache.flink.streaming.api.operators)
emitRecords:235, DebeziumChangeConsumer (com.alibaba.ververica.cdc.debezium.internal)
emitRecordsUnderCheckpointLock:223, DebeziumChangeConsumer (com.alibaba.ververica.cdc.debezium.internal)
handleBatch:168, DebeziumChangeConsumer (com.alibaba.ververica.cdc.debezium.internal)
lambda$notifying$2:82, ConvertingEngineBuilder (io.debezium.embedded)
handleBatch:-1, 1701205675 (io.debezium.embedded.ConvertingEngineBuilder$$Lambda$513)
run:812, EmbeddedEngine (io.debezium.embedded)
run:171, ConvertingEngineBuilder$2 (io.debezium.embedded)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)








