
/** fstx: Flink SQL TaskExecutor进程 version-1.12.2 和version-1.14.3 */

// fstx-1.0 Task启动和 run启停; 

// flink-1.14.3 版本的源码 



StreamTask.restoreInternal(){
	
	CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);{
		StreamTask.restoreGates(){
			reader.readOutputData(getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());
			operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());{//RegularOperatorChain.initializeStateAndOpenOperators
				StreamOperatorWrapper.ReadIterator<StreamOperatorWrapper> operators = getAllOperators(true);{ // reverse反转,从尾开始读; 
					return reverse
						? new StreamOperatorWrapper.ReadIterator(tailOperatorWrapper, true)
						: new StreamOperatorWrapper.ReadIterator(mainOperatorWrapper, false);
				}
				// 传入的 reverse=true,从tail不断向前(current.previous ) 遍历;
				// 源码: for (StreamOperatorWrapper<?, ?> operatorWrapper : operators) {
				while(operators.hasNext()){ // this.current != null;
					StreamOperatorWrapper<?, ?> operatorWrapper = operators.next(){//StreamOperatorWrapper.ReadIterator.next()
						if (hasNext()) {
							StreamOperatorWrapper<?, ?> next = current;
							current = reverse ? current.previous : current.next;
							return next;
						}
					}
					
					// 对于flinksql, 依次遍历 StreamSource, StreamFilter, StreamExecCalc,KeyedProcessOperator ; 
					StreamOperator<?> operator = operatorWrapper.getStreamOperator();
					operator.initializeState(streamTaskStateInitializer);
					operator.open();{
						
						StreamSource.open(){
							
						}
						
						StreamFilter.open(){
							
						}
						
						StreamExecCalc$21.open(){
							
						}
						
						KeyedProcessOperator.open(){
							
						}
						
					}
				}
			}
			
			channelIOExecutor.execute(() -> {reader.readInputData(inputGates);});
			
		}
	}
	
	mailboxProcessor.runMailboxLoop();
}
	
	// 对于"Source: TableSourceScana" 线程来说, 只有1个Operator: StreamSource 算子;
	RegularOperatorChain.initializeStateAndOpenOperators(){
		StreamOperatorWrapper.ReadIterator<StreamOperatorWrapper> operators = getAllOperators(true);
		// 对于flinksql, 依次遍历 StreamSource, StreamFilter, StreamExecCalc,KeyedProcessOperator ; 
		while(operators.hasNext()){ // this.current != null;
			StreamOperatorWrapper<?, ?> operatorWrapper = operators.next();
			StreamOperator<?> operator = operatorWrapper.getStreamOperator();
			operator.initializeState(streamTaskStateInitializer);
			operator.open();{
				StreamSource.open(){
					
				}
			}
		}
	}
	
	// 线程"GroupAggregate(groupBy=xx)" 中的初始阶段: StreamFilter, StreamExecCalc,KeyedProcessOperator ; 
	RegularOperatorChain.initializeStateAndOpenOperators(){
		StreamOperatorWrapper.ReadIterator<StreamOperatorWrapper> operators = getAllOperators(true);
		// 对于flinksql, 依次遍历 StreamSource, StreamFilter, StreamExecCalc,KeyedProcessOperator ; 
		while(operators.hasNext()){ // this.current != null;
			StreamOperatorWrapper<?, ?> operatorWrapper = operators.next();
			StreamOperator<?> operator = operatorWrapper.getStreamOperator();
			operator.initializeState(streamTaskStateInitializer);
			operator.open();{
				
				StreamFilter.open(){
					
				}
				
				StreamExecCalc$21.open(){
					
				}
				
				KeyedProcessOperator.open(){
					
				}
				
			}
		}
	}
	


	//对于 SinkMeterializer -> Sinkxx 线程来说, 主要执行 SinkUpertMeterializer, SinkOperator算子;
	RegularOperatorChain.initializeStateAndOpenOperators(){
		StreamOperatorWrapper.ReadIterator<StreamOperatorWrapper> operators = getAllOperators(true);
		// 对于flinksql, 依次遍历 SinkUpertMeterializer, SinkOperator算子;
		while(operators.hasNext()){ // this.current != null;
			StreamOperatorWrapper<?, ?> operatorWrapper = operators.next();
			StreamOperator<?> operator = operatorWrapper.getStreamOperator();
			operator.initializeState(streamTaskStateInitializer);
			operator.open();{
				
				SinkUpertMeterializer.open(){
					
				}
				
				SinkOperator.open(){
					super.open();{//AbstractUdfStreamOperator.open()
						super.open();
						FunctionUtils.openFunction(userFunction, new Configuration());{
							if (function instanceof RichFunction) {
								richFunction.open(parameters);{
									// 对于Jdbc算子, 通用的Open 实现; 源码详解 3.2.1 
									GenericJdbcSinkFunction.open(){
										super.open(parameters);
										RuntimeContext ctx = getRuntimeContext();
										outputFormat.setRuntimeContext(ctx);
										
										outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());{//JdbcOutputFormat.open()
											connectionProvider.getOrEstablishConnection();
											jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);
											this.scheduledFuture =this.scheduler.scheduleWithFixedDelay(()->flush(),);
										}
									}
									
								}
							}
						}
					}
					this.sinkContext = new SimpleContext(getProcessingTimeService());
				}
				
			}
		}
	}
	

		// "SinkMeterializer -> Sinkxx 线程" 中Sink Operator相关 Open 
		GenericJdbcSinkFunction.open(){
			super.open(parameters);
			RuntimeContext ctx = getRuntimeContext();
			outputFormat.setRuntimeContext(ctx);
			outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());{//JdbcOutputFormat.open
				connectionProvider.getOrEstablishConnection();
				
				jdbcStatementExecutor = createAndOpenStatementExecutor(statementExecutorFactory);{//JdbcOutputFormat.
					JdbcExec exec = statementExecutorFactory.apply(getRuntimeContext());{ //StatementExecutorFactory 
						
						//case 1: append sink: select * from tb_test;
						// 在 JdbcOutputFormatBuilder.build() 中定义的 StatementExecutorFactory 匿名内部类
						ctx -> createSimpleBufferedExecutor();{//JdbcOutputFormatBuilder.createSimpleBufferedExecutor()
							TypeSerializer<RowData> typeSerializer =rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
							 JdbcBatchStatementExecutor<RowData> statementExecutor =createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql);{//JdbcOutputFormatBuilder.createSimpleRowExecutor
								final JdbcRowConverter rowConverter = dialect.getRowConverter(RowType.of(fieldTypes));{
									MySQLDialect.getRowConverter(){
										return new MySQLRowConverter(rowType);{
											super(rowType);{//new AbstractJdbcRowConverter(), 主要是根据每个field的type创建相应的 converter; 
												this.fieldTypes =rowType.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
												this.toInternalConverters = new JdbcDeserializationConverter[rowType.getFieldCount()];
												this.toExternalConverters = new JdbcSerializationConverter[rowType.getFieldCount()];
												for (int i = 0; i < rowType.getFieldCount(); i++) {
													toInternalConverters[i] = createNullableInternalConverter(rowType.getTypeAt(i));
													// 对每个字段创建 converter 转换器 
													toExternalConverters[i] = createNullableExternalConverter(fieldTypes[i]);{// AbstractJdbcRowConverter.createNullableExternalConverter
														JdbcSerializationConverter jdbcConverter= createExternalConverter(type);{//AbstractJdbcRowConverter.createExternalConverter()
															switch (type.getTypeRoot()) {
																case BOOLEAN:
																	return (val, index, statement) ->
																			statement.setBoolean(index, val.getBoolean(index));
																case TINYINT:
																	return (val, index, statement) -> statement.setByte(index, val.getByte(index));
																case SMALLINT:
																	return (val, index, statement) -> statement.setShort(index, val.getShort(index));
																case DOUBLE:
																	return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
																case CHAR:
																case VARCHAR:
																	// value is BinaryString
																	return (val, index, statement) -> statement.setString(index, val.getString(index).toString());
																case BINARY:
																case VARBINARY:
																	return (val, index, statement) -> statement.setBytes(index, val.getBinary(index));
																case DATE:
																	return (val, index, statement) ->statement.setDate(index, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
																case DECIMAL:
																	final int decimalPrecision = ((DecimalType) type).getPrecision();
																	final int decimalScale = ((DecimalType) type).getScale();
																	return (val, index, statement) ->statement.setBigDecimal(index,val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
																case ARRAY:case MAP:case MULTISET:case ROW:case RAW:
																default: throw new UnsupportedOperationException("Unsupported type:" + type);
															}
														}
														return wrapIntoNullableExternalConverter(jdbcConverter, type);
													}
												}
											}
										}
									}
									
									IotDBDialect.getRowConverter(RowType rowType){
										
									}
								}
								
								StatementFactory stmtFactory = (connection) -> FieldNamedPreparedStatement.prepareStatement(connection, sql, fieldNames);
								return new TableSimpleStatementExecutor(stmtFactory,rowConverter)
							 }
							 
							return new TableBufferedStatementExecutor(statementExecutor,valueTransform);
						}
						
						//flink-1.14.3 中upsert jdbc sink; 
						// case 2: upsert sink: select id,count(*) from tb_test group by id; 
						createBufferReduceExecutor();{
							int[] pkFields = Arrays.stream(pkNames).mapToInt(Arrays.asList(opt.getFieldNames())::indexOf).toArray();
							TypeSerializer<RowData> typeSerializer =rowDataTypeInfo.createSerializer(ctx.getExecutionConfig());
							valueTransform =ctx.getExecutionConfig().isObjectReuseEnabled()? typeSerializer::copy: Function.identity();
							
							JdbcBatchStatementExecutor upsertExecutor = createUpsertRowExecutor();{//JdbcOutputFormatBuilder.createUpsertRowExecutor
								return dialect.getUpsertStatement(tableName, fieldNames, pkNames){
									MySQLDialect.getUpsertStatement(){
										//updateClause == `timestamp`=VALUES(`timestamp`), `status`=VALUES(`status`), `speed`=VALUES(`speed`), `temperature`=VALUES(`temperature`)
										String updateClause = Arrays.stream(fieldNames)
											.map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
											.collect(Collectors.joining(", "));
										String statement = getInsertIntoStatement(tableName, fieldNames) + " ON DUPLICATE KEY UPDATE " + updateClause;
										// Insert语句 + ON DUPLICATE KEY UPDATE + updateClause 
										//INSERT INTO `tb_device_upsert`(`timestamp`, `status`, `speed`, `temperature`) VALUES (:timestamp, :status, :speed, :temperature) ON DUPLICATE KEY UPDATE `timestamp`=VALUES(`timestamp`), `status`=VALUES(`status`), `speed`=VALUES(`speed`), `temperature`=VALUES(`temperature`)
										return Optional.of(statement);
									}
									
								}
									.map(sql -> createSimpleRowExecutor(dialect, fieldNames, fieldTypes, sql))
									.orElseGet(() -> createInsertOrUpdateExecutor());
									
							}
							JdbcBatchStatementExecutor deleteExecutor= createDeleteExecutor(dialect, tableName, pkNames, pkTypes);
							keyExtractor = createRowKeyExtractor(fieldTypes, pkFields);
							new TableBufferReducedStatementExecutor(stExecutor,deleteExecutor,keyExtractor);
						}
						
					}
					exec.prepareStatements(connectionProvider.getConnection());
					return exec;
				}
				
				if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
					this.scheduler =Executors.newScheduledThreadPool();
					this.scheduledFuture =this.scheduler.scheduleWithFixedDelay(()->flush(),
								executionOptions.getBatchIntervalMs(),
								executionOptions.getBatchIntervalMs(),
								TimeUnit.MILLISECONDS);
				}
			}
			
		}
		



	// TaskExecutor中 select, insert, upsert 相关源码
	// 1.14.3 中利用 TableBufferedStatementExecutor.buffer:List 做Record的缓存 addBatch(),并触发 executeBatch();


	// jdbc_TM_1.1 open 初始化阶段: 基于 dialect示例对象 对各每个field的type创建相应的 converter; 
	// 核心方法 AbstractJdbcRowConverter.createExternalConverter(type)



	SourceStreamTask.LegacySourceFunctionThread.run(){
		if (!operatorChain.isTaskDeployedAsFinished()) {
			mainOperator.run(lock, operatorChain);{//StreamSource.run()
				run(lockingObject, output, operatorChain);{//StreamSource.run()
					 configuration =this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
					long latencyTrackingInterval =getExecutionConfig().isLatencyTrackingConfigured()
							? getExecutionConfig().getLatencyTrackingInterval()
							: configuration.getLong(MetricOptions.LATENCY_INTERVAL);
					
					long watermarkInterval =getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
					
					this.ctx = StreamSourceContexts.getSourceContext(timeCharacteristic, watermarkInterval);
					userFunction.run(ctx);{// SourceFunction.run()
					   // flink kafka source 
					   FlinkKafkaConsumerBase.run()
					   
						// 2. jdbc 相关的 输入
						InputFormatSourceFunction.run(){
							if (isRunning && format instanceof RichInputFormat) {
								((RichInputFormat) format).openInputFormat();{// 
									JdbcRowDataInputFormat.openInputFormat(){
										Connection dbConn = connectionProvider.getOrEstablishConnection();
										if (autoCommit != null) {
											dbConn.setAutoCommit(autoCommit);
										}
										statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
										
									}
									
								}
							}
							OUT nextElement = serializer.createInstance();
							while (isRunning) {
								
								format.open(splitIterator.next());{
									//jdbc 相关input 实现
									JdbcRowDataInputFormat.open(){
										if (inputSplit != null && parameterValues != null) {
											for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
												Object param = parameterValues[inputSplit.getSplitNumber()][i];
												
											}
										}
										
										resultSet = statement.executeQuery();
										hasNext = resultSet.next();
									}
								}
								
								while (isRunning && !format.reachedEnd()) {
									
									nextElement = format.nextRecord(nextElement);{
										//jdbc 相关input 实现
										JdbcRowDataInputFormat.nextRecord(reuse){
											if (!hasNext) {
												return null;
											}
											RowData row = rowConverter.toInternal(resultSet);
											hasNext = resultSet.next();
											return row;
										}
										// hdfs /hive实现?
										
										
									}
									if (nextElement != null) {
										ctx.collect(nextElement);
									} else {
										break;
									}
								}
								format.close();
								completedSplitsCounter.inc();
								//分片函数 定义是否还有next; 
								if (isRunning) {
									isRunning = splitIterator.hasNext();
								}
							}
							
						}
					
					}
				}
			}
		}
		completeProcessing();
		completionFuture.complete(null);
		headOperator.run(getCheckpointLock(), getStreamStatusMaintainer(), operatorChain);{//StreamSource
			run(lockingObject, streamStatusMaintainer, output, operatorChain);{

			}
		}
		completionFuture.complete(null);
	}


	// jdbc_TM_1.1.2  select from table 的jdbc source 源码流程 
	SourceStreamTask.LegacySourceFunctionThread
	




// fstx-2.0		Operator的open() 初始化阶段: Source的初始化, Operator的初始化, Sink的初始化; 


// fstx-3.0		Operator的运行 invoke: source,operator,sink的 初始化; 

	// 3.1 source类算子的 invoke; 
	

	// 3.2  userFunction.invoke(record) : 处理每一条消息；
	// 核心方法  
	SinkOperator.processElement(StreamRecord<RowData> element){
		userFunction.invoke(element.getValue(), sinkContext);{
			//jdbc 的实现类: GenericJdbcSinkFunction.invoke(value,context)
			GenericJdbcSinkFunction.invoke(value,context){
				outputFormat.writeRecord(value);{//JdbcOutputFormat.writeRecord()
					checkFlushException();
					In recordCopy = copyIfNecessary(record);
					//1. 先把一个个record放到缓存; addBatch
					JdbcIn jdbcRecord = jdbcRecordExtractor.apply(recordCopy);
					addToBatch(record, jdbcRecord);{//JdbcOutputFormat.addToBatch(original,extracted)
						jdbcStatementExecutor.addToBatch(extracted);{
							TableBufferedStatementExecutor.addToBatch(record){
								RowData value = valueTransform.apply(record); // copy or not
								buffer.add(value);// buffer: List<RowData>, 在下面 flush() -> executeBatch()中遍历 buffer
							}
						}
					}
					
					batchCount++;
					// 2. 批量刷出； 当 batchCount > batchSize(默认 500/20000?)时,刷新落db: executeBatch()
					if (executionOptions.getBatchSize() > 0&& batchCount >= executionOptions.getBatchSize()) {
						flush();{//JdbcOutputFormat.flush()
							for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
								attemptFlush();{//JdbcOutputFormat.attemptFlush()
									jdbcStatementExecutor.executeBatch();{
										
										// table/sql api中默认的 buff执行器 ; 
										// PreparedStatement#executeBatch()may fail and clear buffered records, so we replay the records when retrying 
										TableBufferedStatementExecutor.executeBatch();{
											//把 buffer中所有元素都 ps.addBatch()
											for (RowData value : buffer) {
												statementExecutor.addToBatch(value);{// TableSimpleStatementExecutor.addToBatch(record)
													converter.toExternal(record, st);{// AbstractJdbcRowConverter.toExternal(rowData,statement)
														for (int index = 0; index < rowData.getArity(); index++) {
															// 每个字段的转换器执行转换： JdbcSerializationConverter.serialize()
															// 在open阶段的 AbstractJdbcRowConverter.createExternalConverter(type)方法中定义了每个字段的 converter; 
															toExternalConverters[index].serialize(rowData, index, statement);{
																switch (type.getTypeRoot()) {
																	case TINYINT:
																		return (val, index, statement) -> statement.setByte(index, val.getByte(index));
																	case BIGINT: case INTERVAL_DAY_TIME:
																		return (val, index, statement) -> statement.setLong(index, val.getLong(index));
																	case CHAR:
																	case VARCHAR: // value is BinaryString
																		return (val, index, statement) -> statement.setString(index, val.getString(index).toString());
																}
															}
														}
														return statement;
													}
													st.addBatch();{//FieldNamedPreparedStatementImpl.addBatch()
														// java.sql.PreparedStatement 接口执行 addBatch()
														statement.addBatch();{
															//mysql的 PS实现类
															JDBC42PreparedStatement.addBatch()
															
															IoTDBPreparedStatement.addBatch();
															
														}
													}
												}
											}
											
											// 批量刷新落db;
											statementExecutor.executeBatch();{//TableSimpleStatementExecutor.executeBatch()
												st.executeBatch();{//FieldNamedPreparedStatementImpl.executeBatch()
													return statement.executeBatch();{//PreparedStatement.executeBatch() 接口; 
														//mysql jdbc: 
														com.mysql.jdbc.StatementImpl.executeBatch()
														
													}
												}
											}
											buffer.clear();
										}
										
										// Insert or Upsert 相关的 
										TableInsertOrUpdateStatementExecutor.executeBatch();
										
										TableBufferReducedStatementExecutor.executeBatch();
										
									}
								}
								batchCount = 0;
								break;
							}
						}
					}
				}
			}
		}
	}





// fstx-4.0		辅助线程和其他逻辑; 


// jdbc_TM_3.1  定时线程刷出,定时遍历 buffer，加到 PreparedStatement中，并触发 ps.executeBatch() 落db;
	scheduler.scheduleWithFixedDelay(()->flush(),executionOptions.getBatchIntervalMs(),executionOptions.getBatchIntervalMs(),TimeUnit.MILLISECONDS);{
		JdbcOutputFormat.flush(){
			JdbcOutputFormat.attemptFlush();
				TableBufferedStatementExecutor.executeBatch();{
					for (RowData value : buffer) {
						statementExecutor.addToBatch(value);
					}
					statementExecutor.executeBatch();
					buffer.clear();
				}
		}
	}






















