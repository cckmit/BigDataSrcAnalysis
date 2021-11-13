Kafka Source相关源码的问题
* 构造Kafka的创建和运行
* KafkaSource.produce()方法消费过程: 包括StreamSet + KafkaLowAPI
* Kafka的offset管理机制;
* 集群Kafka的 运行区别;

待排除问题:
1. ProductionPipeline 执行 originSource.produce(maxBatchSize)参数maxBatchSize是如何确定的? 有上限吗?
2. 


// 创建StandaloneKafka的创建

// 初始化DSource时:

KafkaDSource 两种具体运行类:
-> StandaloneKafkaSource extends BaseKafkaSource[extends BaseSource[extends BaseStage<Source.Context>[implements Stage<C>] implements Source] implements OffsetCommitter ]
-> ClusterKafkaSource extends BaseKafkaSource implements OffsetCommitter, ClusterSource, ErrorListener


KafkaDSource extends DClusterSourceOffsetCommitter[extends DSourceOffsetCommitter[extends DSource[extends DStage<Source.Context> [implements Stage<C>] implements Source] implements OffsetCommitter] implements ClusterSource] implements ErrorListener

originStage.init(Info,Context){ // DStage.init(Info info, C context)
	if(stage == null) {
		stage:Stage<C> = createStage(){ // 由DClusterSourceOffsetCommitter.createStage()实现
			Stage<Source.Context> result = super[DSourceOffsetCommitter].createStage(){ // DSourceOffsetCommitter.createStage()
				source:Source = (Source) super[DSource].createStage(){ // DSource.create()
					return createSource();{ // 抽象类, 由 KafkaDSource.createSource()实现
						KafkaDSource.createSource(){
							// 同时创建 StandaloneKafkaSourceFactory 和 ClusterKafkaSourceFactory两种工程类,装进DelegationgKafka
							this.delegatingKafkaSource = new DelegatingKafkaSource(new StandaloneKafkaSourceFactory(kafkaConfigBean),
								new ClusterKafkaSourceFactory(kafkaConfigBean));
							return delegatingKafkaSource;	
						}
					}
				}
				
				offsetCommitter = (OffsetCommitter) source;
				return source;
			}
			
			LOG.info("Created source of type: {}", source);
				* Created source of type: com.bigdata.sdc.origin.kafka.DelegatingKafkaSource@2ce6c6ec
			clusterSource = (ClusterSource) source;
			return result;
		}
	}
	
	List<ConfigIssue> issues = stage.init(info, context){ // BaseStage.init(Info info, C context)
		issues.addAll(init(){ // DelegatingKafkaSource.init()
			boolean isClusterMode = (getContext().getExecutionMode()==ExecutionMode.CLUSTER_BATCH 
				|| getContext().getExecutionMode()==ExecutionMode.CLUSTER_YARN_STREAMING 
				|| getContext().getExecutionMode()==ExecutionMode.CLUSTER_MESOS_STREAMING);
			
			// 当Standalone或Preview时, 创建 Standalone
			if (getContext().isPreview()|| !isClusterMode ){
				delegate = standaloneKafkaSourceFactory.create(){
					BaseKafkaSource baseKafkaSource = new StandaloneKafkaSource(conf){
						super(conf){ // new BaseKafkaSource(){}
							this.conf = conf;
							
							SdcKafkaValidationUtilFactory kafkaUtilFactory= SdcKafkaValidationUtilFactory.getInstance(){
								return FactoriesBean.getKafkaValidationUtilFactory(){
									static FactoriesBean factoriesBean;
									// 此方法采用 动态加载机制(SPI)来从 resources/META-INF/services 目录下FactoriesBean接口名文件中加上其实现类; 若没指定任何实现类,则下面static代码报异常;
									static ServiceLoader<FactoriesBean> factoriesBeanLoader = ServiceLoader.load(FactoriesBean.class);
									static{
										for (FactoriesBean bean : factoriesBeanLoader) {
											LOG.info("Found FactoriesBean loader {}", bean.getClass().getName());
											factoriesBean = bean;
											serviceCount++;
										}
										if (serviceCount != 1) {
											throw new RuntimeException(Utils.format("Unexpected number of FactoriesBean: {} instead of 1", serviceCount));
										}
									}
									
									return factoriesBean.createSdcKafkaValidationUtilFactory(){ // Kafka11FactoriesBean.createSdcKafkaValidationUtilFactory()
										reutrn new Kafka09ValidationUtilFactory(); // 应该是kafka_09和kafka_11的消费api相同,所以代码还是依赖Kafka09Factory的.
									}
								}
							}
							
							kafkaValidationUtil = kafkaUtilFactory.create(){ // Kafka09ValidationUtilFactory.create()
								return new KafkaValidationUtil09(); //class KafkaValidationUtil09 extends BaseKafkaValidationUtil implements SdcKafkaValidationUtil
							}
						}
					}
					retrun baseKafkaSource
				}
			} 
			// 当集群模式且不是Preview时, 才创建Cluster Source
			else{ 
				delegate = clusterKafkaSourceFactory.create();
			}
			
			List<ConfigIssue> issues=delegate.init(getInfo(), getContext()){ // BaseStage.init(Info info, C context)
				issues.addAll(init(){ // 抽象方法,由实现类 StandaloneKafkaSource.init()
					
					/** 重要1 的Kafka初始化方法:
					* 	- 验证 broker : 	kafkaValidationUtil.validateKafkaBrokerConnectionString();
					* 	- 和Zookeeper的配置	kafkaValidationUtil.validateZkConnectionString();
					* 	- 正式创建 KafkaConsumer : SdcKafkaConsumerFactory.create(settings).create();
					*/
					List<ConfigIssue> issues = super[BaseKafkaSource].init(){ // BaseKafkaSource.init()
						errorRecordHandler = new DefaultErrorRecordHandler(getContext());
						if (conf.topic == null || conf.topic.isEmpty()) {
							issues.add(getContext().createConfigIssue());
						}
						if (conf.maxWaitTime < 1) {
							issues.add(getContext().createConfigIssue());
						}
						
						conf.init(getContext(), issues);
						
						conf.dataFormatConfig.init(getContext(),conf.dataFormat);
						
						// 创建Kafka消费数据的解析器
						parserFactory = conf.dataFormatConfig.getParserFactory();
						
						// Validate broker config
						List<HostAndPort> kafkaBrokers = kafkaValidationUtil.validateKafkaBrokerConnectionString(){// BaseKafkaValidationUtil.validateKafkaBrokerConnectionString()
							String[] brokers = connectionString.split(",");
							for (String broker : brokers) {
								kafkaBrokers.add(HostAndPort.fromString(broker));
							}
							return kafkaBrokers;
						}
						
						int partitionCount = kafkaValidationUtil.getPartitionCount();
						if (partitionCount < 1) {
							issues.add(getContext().createConfigIssue());
						}else {
							originParallelism = partitionCount; // 元数据并行度=topic的 partition数量;
						}
						
						// Validate zookeeper config
						kafkaValidationUtil.validateZkConnectionString(){ // BaseKafkaValidationUtil.validateZkConnectionString()
							int off = connectString.indexOf('/');
							if (off >= 0) {
								connectString = connectString.substring(0, off);
							}
							String brokers[] = connectString.split(",");
							for(String broker : brokers) {
								kafkaBrokers.add(HostAndPort.fromString(broker));
							}
							return kafkaBrokers;
						}
						
						//consumerGroup
						if (conf.consumerGroup == null || conf.consumerGroup.isEmpty()) {
							issues.add(getContext().createConfigIssue());
						}
						
						//validate connecting to kafka, 重要步骤1: 正式创建KafkaConsumer, 
						if (issues.isEmpty()) {
							Map<String, Object> kafkaConsumerConfigs = new HashMap<>();
							kafkaConsumerConfigs.putAll(conf.kafkaConsumerConfigs);
							kafkaConsumerConfigs.put(KafkaConstants.KEY_DESERIALIZER_CLASS_CONFIG, conf.keyDeserializer.getKeyClass());
							kafkaConsumerConfigs.put(KafkaConstants.VALUE_DESERIALIZER_CLASS_CONFIG, conf.valueDeserializer.getValueClass());
							kafkaConsumerConfigs.put(KafkaConstants.CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG,conf.dataFormatConfig.schemaRegistryUrls);
							ConsumerFactorySettings settings = new ConsumerFactorySettings(zk,bootstrapServers,topic,maxWaitTime,kafkaConsumerConfigs,consumerGroup,batchSize);
							
							// 创建 SDC的KafakaConsumer基础包装类: BasedKafkaConsumer09
							SdcKafkaConsumerFactory kafkaConsumerFactory= SdcKafkaConsumerFactory.create(settings){
								SdcKafkaConsumerFactory kafkaConsumerFactory = FactoriesBean.getKafkaConsumerFactory(){
									// factoriesBean变量生成的代码如上:
									factoriesBean.createSdcKafkaConsumerFactory(){ // Kafka11FactoriesBean.createSdcKafkaConsumerFactory()
										return new Kafka11ConsumerFactory();
									}
								}
								kafkaConsumerFactory.init(settings){ // Kafka11ConsumerFactory.init()
									this.settings = settings;
								}
								return kafkaConsumerFactory;
							}
							
							kafkaConsumer = kafkaConsumerFactory.create(){ // Kafka11ConsumerFactory.create()
								return new KafkaConsumer11(bootStrapServers,topic,consumerGroup,kafkaConsumerConfigs){
									super(bootStrapServers, topic, consumerGroup, kafkaConsumerConfigs, context, batchSize){// new KafkaConsumer09()
										super(topic, context, batchSize){ // new BaseKafkaConsumer09()
											this.recordQueue = new ArrayBlockingQueue<>(batchSize); // batchSize= 20为KafkaConfigBean.maxBatchSize的值;
											this.executorService = new ScheduledThreadPoolExecutor(1);
											this.rebalanceInProgress = new AtomicBoolean(false);
											this.needToCallPoll = new AtomicBoolean(false);
										}
										this.bootStrapServers = bootStrapServers;
										this.consumerGroup = consumerGroup;
									}
								}
							}
							
							/** 重要步骤: 正式创建KafkaConsumer, 
							*	-创建 官方的KafkaConsumer
							* 	- 订阅当个Topic
							* 	- 获取该Topic的全部分区;
							*/
							kafkaConsumer.validate(issues, getContext()){ // BaseKafkaConsumer09.validate()
								createConsumer(){ // BaseKafkaConsumer09.createConsumer()
									configureKafkaProperties(kafkaConsumerProperties){
										// 默认fasel,关闭 enable.auto.commit,禁用自动提交; 即offset叫由SDC自己管理;
										props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_ENABLED_DEFAULT);
										if (this.context.isPreview()) {
											// preview模式, auto.offset.reset=earliest,
											props.setProperty(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
										}
										addUserConfiguredProperties(props)
									}
									kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
								}
								
								// 订阅主题: 消费者自动再均衡(reblance)的功能
								subscribeConsumer(){
									kafkaConsumer.subscribe(Collections.singletonList(topic), this);
								}
								// 获取该Topic的全部分区
								kafkaConsumer.partitionsFor(topic);
							}
							
						}
						return issues;
					}
					
					/** 重要步骤2: 用100ms定时线程执行KafkaConsumerRunner.run(), 以作为生成者不断从KafakServer 拉去数据到 consumer.recordQueue队列中
					*
					*/
					if (issues.isEmpty()){
						// 对于预览模式, batch间最大waitTime固定为1秒;
						if (getContext().isPreview()) conf.maxWaitTime = 1000;
						
						kafkaConsumer.init(){ // BaseKafkaConsumer09.init()
							kafkaConsumerRunner = new KafkaConsumerRunner(this:BaseKafkaConsumer09){ // KafkaConsumerRunner implements Runnable()
								@Override public void run(){
									ConsumerRecords<String, byte[]> poll;
									synchronized (consumer.pollCommitMutex){
										poll = consumer.kafkaConsumer.poll(CONSUMER_POLLING_WINDOW_MS); //固定100 (毫秒)
										consumer.needToCallPoll.set(false);
										
										LOG.trace("Read {} messages from Kafka",poll.count(),consumer.recordQueue.size());
										for(ConsumerRecord<String, byte[]> r : poll) {
											if(consumer.needToCallPoll.get()) {
												consumer.recordQueue.clear();
												return;
											}
											
											boolean isQueueOK = putConsumerRecord(r){
												try{
													// 注意, ArrayBlockingQueue<ConsumerRecord<String, byte[]>> recordQueue=new ArrayBlockingQueue<>(batchSize);
													consumer.recordQueue.put(record); 
													return true;
												}catch (InterruptedException e) {
													// 中断当前线程 20毫秒 执行kafkaConsumerRunner.run()从Kafka拉下数据的poll线程; 只需中断标志,仍然继续执行;
													Thread.currentThread().interrupt();
													return false;
												}
											}
											
											if (! isQueueOK) { return;}
										}
									}
								}
							}
							
							executorService.scheduleWithFixedDelay(kafkaConsumerRunner, 0, 20, TimeUnit.MILLISECONDS);
							isInited = true;
						}
						LOG.info("Successfully initialized Kafka Consumer");
							* Successfully initialized Kafka Consumer
						
						// 这个是干嘛的?
						getContext().publishLineageEvent( getContext().createLineageEvent(LineageEventType.ENTITY_READ));
						
					}
				});
			}
			
			return issues;
		});
		
		if (requiresSuperInit && !superInitCalled) {
			 issues.add(context.createConfigIssue(null, null, Errors.API_20));
		}
		return issues;
	}
	
	return issues;
	
}


SourceRunner.runProduce(offset,maxBatchSize){
	BatchMakerImpl batchMaker = new BatchMakerImpl(((Source.Context) getContext()).getOutputLanes()){
		if(outputLanes.size() == 1) {
			singleLaneOutput = outputLanes.iterator().next();
		}else{
			singleLaneOutput = null;
		}
		
		for(String lane : outputLanes) {
			laneToRecordsMap.put(lane, new ArrayList<Record>());
		}
	}
	
	String newOffset = getStage().produce(lastOffset, maxBatchSize, batchMaker){ // DSource.produce()
		return getSource().produce(lastSourceOffset, maxBatchSize, batchMaker){ // DelegatingKafkaSource.produce()
			return delegate.produce(lastSourceOffset, maxBatchSize, batchMaker){ // StandaloneKafkaSource.produce()
				int recordCounter = 0;
				// 取配置或 管理器传入?的maxBatchSize 作为当前batchSize;
				int batchSize = conf.maxBatchSize > maxBatchSize ? maxBatchSize : conf.maxBatchSize;
				
				while (recordCounter < batchSize && (startTime + conf.maxWaitTime) > System.currentTimeMillis()) {
					
					MessageAndOffset message = kafkaConsumer.read(){ // BaseKafkaConsumer11.read() ->调用继承的BaseKafkaConsumer09.read() 
						gaugeMap.put(REBALANCE_IN_PROGRESS, rebalanceInProgress.get());
						gaugeMap.put(WAITING_ON_POLL, needToCallPoll.get());
						// On rebalancing or if we need to call poll first, there is no point to read any messages from the buffer
						// 当AbstractCoordinator.ensureActiveGroup()中, 当 needsJoinPrepare=true(初始化或initiateJoinGroup()后)时,会将rebalanceInProgress置位true
						// 在commit()中当KafkaConsuemr.commitSync()(向zk?)同步元数据时失败跑CommitFailedException时,就设needToCallPoll=true
						if(rebalanceInProgress.get() || needToCallPoll.get()) {
						  // Small back off to give Kafka time to rebalance or us to get a chance to call poll()
						  try {
							Thread.sleep(500);
						  } catch (InterruptedException e) {
							// Not really important to us
							Thread.currentThread().interrupt();
						  }

						  // Return no message
						  LOG.debug(
							  "Generating empty batch since the consumer is not ready to consume (rebalance={}, needToPoll={})",
							  rebalanceInProgress.get(),
							  needToCallPoll.get()
						  );
						  return null;
						}

						ConsumerRecord<String, byte[]> next;
						try {
						   // If no record is available within the given time return null
						   next = recordQueue.poll(500, TimeUnit.MILLISECONDS);
						} catch (InterruptedException e) {
						  Thread.currentThread().interrupt();
						  throw createReadException(e);
						}
						MessageAndOffset messageAndOffset = null;
						if(next != null) {
						  updateEntry(next);
						  messageAndOffset = new MessageAndOffset(next.value(), next.offset(), next.partition());
						}
						return messageAndOffset;
						
						
					}
					
					
					if (message != null) {
						// 调用父类 BaseKafkaSource的processKafkaMessageDefault()将byte[]消息转换成Record;
						records = processKafkaMessageDefault(message.getPartition(),message.getOffset(),getMessageID(message),(byte[]) message.getPayload()){
							BaseKafkaSource.(partition,offset,messageId,payload){
								if (payload == null) { //当消息的value为Null时,Kafka发送个错误Record to Pipeline
									Record record = getContext().createRecord(messageId);
									errorRecordHandler.onError(record,messageId)
								}
								return ;
							}
							
							// 实际是创建JsonCharDataParser(), 其实际封装 jackjson.ReaderBasedJsonParser()对象;
							DataParser parser = parserFactory.getParser(messageId, payload){//WrapperDataParserFactory.getParser()
								DataParser parser = this.factory.getParser(id, data){
									return this.getParser(id, data, 0, data.length){//DataParserFactory.getParser()
										return this.getParser(id, (InputStream)(new ByteArrayInputStream(data, offset, len)), (String)"0"){//DataParserFactory.getParser()
											return this.createParser(id, this.createReader(is), Long.parseLong(offset)){// //JsonDataParserFactory.getParser(String id, InputStream is, String offset)
												return new JsonCharDataParser(this.getSettings().getContext(), id, reader, offset, ((JsonMode)this.getSettings().getMode(JsonMode.class)).getFormat(), this.getSettings().getMaxRecordLen()){
													this.context = context;
													this.readerId = readerId;
													this.maxObjectLen = maxObjectLen;
													this.parser = ((ContextExtensions)context).createJsonObjectReader(reader, readerOffset, maxObjectLen, mode, Object.class){//ProtoContext.createJsonObjectReader()
														return JsonWriterReaderFactory.createObjectReader(reader, initialPosition, mode, objectClass, maxObjectLen){//JsonWriterReaderFactory.createObjectReader()
															return new OverrunJsonObjectReaderImpl(new OverrunReader(reader, OverrunReader.getDefaultReadLimit(), false, false), initialPosition, maxObjectLen, mode, objectClass){
																super(reader, initialPosition, mode, objectClass, objectMapper){
																	this.reader = reader;
																	if (mode == Mode.MULTIPLE_OBJECTS) {
																		if (initialPosition > 0L) {
																			IOUtils.skipFully(reader, initialPosition);
																			this.posCorrection += initialPosition;
																		}

																		if (reader.markSupported()) {
																			reader.mark(64);
																			int count = 0;
																			byte firstByte = -1;
																			while(count++ < 64 && (firstByte = (byte)reader.read()) != -1 && firstByte <= 32) {
																			}
																			if (firstByte > 32) {
																				this.firstNonSpaceChar = firstByte;
																			}
																			reader.reset();
																		}
																	}

																	this.jsonParser = this.getObjectMapper().getFactory().createParser(reader){
																		IOContext ctxt = this._createContext(r, false);
																		return this._createParser(this._decorate(r, ctxt), ctxt){
																			return new ReaderBasedJsonParser(ctxt, this._parserFeatures, r, this._objectCodec, this._rootCharSymbols.makeChild(this._factoryFeatures));
																		}
																	}
																	if (mode == Mode.ARRAY_OBJECTS && initialPosition > 0L) {
																		this.fastForwardJsonParser(initialPosition);
																	}
																													
																}
																
																this.countingReader = (OverrunReader)this.getReader();
															}
														}
													}
												}
											}
										}
									}
								}
								
								return new WrapperDataParserFactory.WrapperDataParser(parser);
							}
							
							// 从字符串转换成 -> Map(json) ;  Map -> Record
							Record record = parser.parse(){ // WrapperDataParser.parse()
								return this.dataParser.parse(){// JsonCharDataParser.parse()
									long offset = this.parser.getReaderPosition(){
										long maxAsOffset= Math.max(this.jsonParser.getTokenLocation().getCharOffset(), 0L);
										return this.mode == Mode.ARRAY_OBJECTS ? maxAsOffset : maxAsOffset + this.posCorrection;
									}
									/**
									* Object json里面是一个EnforceMap 对象: class EnforceMap extends LinkedHashMap;
									* List -> EnforcerList extends ArrayList
									*/
									
									Object json = this.parser.read(){ //JsonObjectReaderImpl.read()
										switch(this.mode) {
											case ARRAY_OBJECTS:
												value = this.readObjectFromArray();
												break;
											case MULTIPLE_OBJECTS:
												value = this.readObjectFromStream(){ //OverrunJsonObjectReaderImpl.readObjectFromStream()
													this.countingReader.resetCount();
													this.limitOffset = this.getJsonParser().getCurrentLocation().getCharOffset() + (long)this.maxObjectLen;
													TL.set(this);
													var1 = super.readObjectFromStream(){// JsonObjectReaderImpl.readObjectFromStream()
														if (this.nextToken != null) {
															value = this.jsonParser.readValueAs(this.getExpectedClass()){//JsonParser.readValueAs(Class<T> valueType==Object.class)
																return this._codec(){
																	ObjectCodec c = this.getCodec();
																	if (c == null) {
																		throw new IllegalStateException("No ObjectCodec defined for parser, needed for deserialization");
																	}else{
																		return c;
																	}
																}
																	.readValue(this, valueType){//ObjectMapper.readValue(JsonParser p, Class<T> valueType)
																		return this._readValue(this.getDeserializationConfig(), p, this._typeFactory.constructType(valueType));{// ObjectMapper._readValue(DeserializationConfig cfg, JsonParser p, JavaType valueType)
																			
																			JsonToken t = this._initForReading(p);
																			DefaultDeserializationContext ctxt;
																			Object result;
																			if (t == JsonToken.VALUE_NULL) {
																				ctxt = this.createDeserializationContext(p, cfg);
																				result = this._findRootDeserializer(ctxt, valueType).getNullValue(ctxt);
																			} else if (t != JsonToken.END_ARRAY && t != JsonToken.END_OBJECT) {
																				ctxt = this.createDeserializationContext(p, cfg);
																				JsonDeserializer<Object> deser = this._findRootDeserializer(ctxt, valueType);
																				if (cfg.useRootWrapping()) {
																					result = this._unwrapAndDeserialize(p, ctxt, cfg, valueType, deser);
																				} else {
																					result = deser.deserialize(p, ctxt);{//UntypedObjectDeserializer.deserialize()
																						switch(p.getCurrentTokenId()) {
																							case 1,2,5:
																								if (this._mapDeserializer != null) {
																									return this._mapDeserializer.deserialize(p, ctxt);{//OverrunJsonObjectReaderImpl.deserialise()
																										return (Map)jp.readValueAs(OverrunJsonObjectReaderImpl.EnforcerMap.class);{
																											JsonParse.readValueAs(); 递归又进一步解析子节点.
																											// 如果对Json的解析是一个node一个node的递归解析, 那么指定类型,会不会快很多. 
																										}
																									}
																								}
																							case 3: 
																						}
																					}
																				}
																			} else {
																				result = null;
																			}

																			p.clearCurrentToken();
																			return result;
																		}
																}
															}
															this.nextToken = this.jsonParser.nextToken();
															if (this.nextToken == null) {
																++this.posCorrection;
															}
														}
													}
												}	
												break;
										}
										return value;
									}
									
									/**
									* 递归调用 jsonToField() 不断把Map中元素解析成 Integer/Double/String.. 等基本类型; 并用Field封装下.
									*/
									if (json != null) {
										record = this.createRecord(offset, json);{//JsonCharDataParser.createRecord()
											Record record = this.context.createRecord(this.readerId + "::" + offset);
											record.set(this.jsonToField(json, offset){// JsonCharDataParser.jsonToField(Object json, long offset)
												
												Field field;
												if (json == null) {
												  field = Field.create(Field.Type.STRING, null);
												} else if (json instanceof List) {
												  List jsonList = (List) json;
												  List<Field> list = new ArrayList<>(jsonList.size());
												  for (Object element : jsonList) {
													  // 对其中每个元素 递归调用 jsonToField()
													list.add(jsonToField(element, offset));
												  }
												  field = Field.create(list);
												} else if (json instanceof Map) {
												  Map<String, Object> jsonMap = (Map<String, Object>) json;
												  Map<String, Field> map = new LinkedHashMap<>();
												  for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
													// 对每个元素 递归调用 jsonToField()
													map.put(entry.getKey(), jsonToField(entry.getValue(), offset));
												  }
												  field = Field.create(map);
												} else if (json instanceof String) {
												  field = Field.create((String) json);
												} else if (json instanceof Boolean) {
												  field = Field.create((Boolean) json);
												} 
												// Integer,Long,String,Double, Float,byte[]等进行cast
												else if (json instanceof BigDecimal) {
												  field = Field.create((BigDecimal) json);
												} else if (json instanceof BigInteger) {
												  field = Field.create(new BigDecimal((BigInteger) json));
												} else {
												  throw new DataParserException(Errors.JSON_PARSER_01, readerId, offset, json.getClass().getSimpleName());
												}
												return field;
												
											});
											return record;
										}
									}
									return record;
								}
							}
							
							while (record != null) {
								record.getHeader().setAttribute(HeaderAttributeConstants.TOPIC, conf.topic);
								record.getHeader().setAttribute(HeaderAttributeConstants.PARTITION, partition);
								record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, String.valueOf(offset));
								records.add(record);
								record = parser.parse();
							}
							
							if (conf.produceSingleRecordPerMessage) {
								records.forEach(record -> list.add(record.get()));
								Record record= records.get(0).set(Field.create(Field.Type.LIST, list));
							}
							return records;
						}
						
						// 若为Preview模式,截取 剩余小部分数据;
						if (getContext().isPreview() && recordCounter + records.size() > batchSize) {
							records = records.subList(0, batchSize - recordCounter);
						}
						for (Record record : records) {
							batchMaker.addRecord(record);
						}
						recordCounter += records.size();
					}
				}
				return lastSourceOffset;
			}
		}
	}
	
	// 整个KafkaSource.produce()方法执行完后, 提交 newOffset;
	if (getStage() instanceof OffsetCommitter) {
		((OffsetCommitter)getStage()).commit(newOffset){//DSourceOffsetCommitter.commit(offset)
			offsetCommitter.commit(offset){//DelegatingKafkaSource.commit()
				delegate.commit(offset){
					// Standalone模式:
					StandaloneKafkaSource.commit(offset){// 其内部的KafkaConsumer09提交offset实现, 是从缓存读取,而不是外部转入;
						kafkaConsumer.commit(){//KafkaConsumer11.commit() -> KafkaConsumer09.commit()
							
							synchronized (pollCommitMutex) {
							  // While rebalancing there is no point for us to commit offset since it's not allowed operation
							  if(rebalanceInProgress.get()) {
								LOG.debug("Kafka is rebalancing, not commiting offsets");
								return;
							  }
							  if(needToCallPoll.get()) {
								LOG.debug("Waiting on poll to be properly called before continuing.");
								return;
							  }

							  try {
								if(topicPartitionToOffsetMetadataMap.isEmpty()) {
								  LOG.debug("Skipping committing offsets since we haven't consume anything.");
								  return;
								}
								// topicPartitionToOffsetMetadataMap: Map<TopicPartition, OffsetAndMetadata> , 管理每个分区的最新offset
								kafkaConsumer.commitSync(topicPartitionToOffsetMetadataMap){
									acquire();
									try{
										coordinator.commitOffsetsSync(new HashMap<>(offsets), Long.MAX_VALUE){//ConsumerCoordinator.commitOffsetsSync()
											
											invokeCompletedOffsetCommitCallbacks();
											if (offsets.isEmpty())
												return true;

											long remainingMs = timeoutMs;
											do {
												if (coordinatorUnknown()) {
													if (!ensureCoordinatorReady(now, remainingMs))
														return false;

													remainingMs = timeoutMs - (time.milliseconds() - startMs);
												}

												RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
												client.poll(future, remainingMs);

												if (future.succeeded()) {
													if (interceptors != null)
														interceptors.onCommit(offsets);
													return true;
												}
												
												// 进入此处,说明提交offset失败, 将进入重试或抛异常终止提交;
												// 若因 rebalanced导致CommitFailedEx,则会讲BasedKafakConsumer09.needToCallPoll置为ture,并recordQueue.clear();
												// 当BasedKafkaConsumer.read() 再从recordQueue拉(一个)数据时,就会先等500ms(为什么? 等KafkaConsumerRunner线程重新poll,或等待rebalance?),并返回空数据;
												if (!future.isRetriable())
													throw future.exception();
												time.sleep(retryBackoffMs);
												now = time.milliseconds();
												remainingMs = timeoutMs - (now - startMs);
											} while (remainingMs > 0);

											return false;
											
											
										}
									}finally{
										release();
									}
								}
								
							  } catch(CommitFailedException ex) {
								LOG.warn("Can't commit offset to Kafka: {}", ex.toString(), ex);
								needToCallPoll.set(true);
								recordQueue.clear();
							  } finally {
								topicPartitionToOffsetMetadataMap.clear();
							  }
							}
							
						}
					}
				}
			}
		}
	}
}



StreamSets.KafkaDSource 算子相关参数:

KafkaConsumer 相关
	
	
	# 心跳检测决定:会话结束和Rebalacne
	session.timeout.ms = 100000 # 心跳坚持超过这个时间,就结束回话, 触发Rebalance将它分区分配给其他ConsumerId;
		- 该属性与heartbeat.interval.ms紧密相关, 一般是heartbeat.interval.ms的3倍;
		- 太小不好, 可能因网络延迟而导致心跳超时, 引发不必要的Rebalance.
		- 太大也不好, 
		- 而且需要在broker的group.min.session.timeout.ms 与 group.max.session.timeout.ms之间
	heartbeat.interval.ms = 3000 # consumerId与GroupCoordinator之间的心跳间隔,确保该consumer还是存活的. 默认3000毫秒;
		- 这是在0.10.1版本之后与poll主线程解耦后才实现的;
		- 适度调小heartbeat.interval.ms,可以确保尽早发现挂了的consumerId,保证数据及时消费;
	
	
	# 每次poll回话的参数
	max.poll.interval.ms=300000 # 一次poll的超时间隔, Server端判断若超过这个时间会引发Rebalance? 待验证.
	max.poll.records = 500		# 一次poll返回的Record数量;
	max.partition.fetch.bytes = 1048576	#每个分区里返回给消费者的最大字节数。默认lMB ; 
	
	# Broker返回数据的条件: 达到大小(fetch.min.bytes)或超时(fetch.max.wait.ms)
	fetch.min.bytes = 1			#从服务器会等待满足该大小的数据才返回; 可适度调大以降低consumer的工作负载;
	fetch.max.wait.ms = 500		# 等待服务器返回数据的最大时间, 与fetch.min.bytes属性只要满足了任何一条即返回;
	fetch.max.bytes = 52428800	# 消费者能读取的最大消息,以前叫fetch.message.max.bytes。这个值应该大于或等于message.max.bytes。
		- 需要比 max.message.size 大, 以免无法获取数据;
		- 不宜太大而导致poll一次超时引发Rebalance. 
	
	# 网络请求通用参数
	receive.buffer.bytes=65536 	# 位于Consumer端TCP-Socket接收数据包的缓冲区buffer大小;
		- 默认64 kB, 对于高吞吐量的环境而言都太小了, 
		- 设置为-1，表示使用操作系统的默认值;
		- 对于JVM中运行, JVM)中运行,建议使用固定大小的堆外内存-缓冲区, 避免GC频繁的回收;
		- 延迟为1毫秒或更多的高带宽的网络(如10 Gbps或更高)，请考虑将套接字缓冲区设置为8或16 MB。
	send.buffer.bytes = 131072	# 位于Consumer端TCP-Socket发送数据包的缓冲区buffer大小;
	
	reconnect.backoff.max.ms=1000 # 客户端尝试连接到Kafka服务的最大等待时间;
	reconnect.backoff.ms = 50	# 尝试重新连接到给定主机之前等待的基本时间, 避免循环重复连接Server;
	request.timeout.ms =305000	# 发起网络请求超时时间, 约5分钟;
	retry.backoff.ms = 100		# 重试失败请求之前的等待时间
	
	
	# 其他Consumer配置
	connections.max.idle.ms = 540000# 连接空闲超时时间, 默认9分钟; 当不主动close()连接时, 超过这个时间就关闭TCP长连接.
	metadata.max.age.ms = 300000 # Metadata数据(brokers和topicPartionts信息)刷新间隔;
	
	

KafkaDSource.KafkaConfigBean 页面配置
	kafkaConfigBean.maxBatchSize
		- BaseKafkaConsumer09.构造函数中: new ArrayBlockingQueue<>(batchSize);
		- StandaloneKafkaSource.produce()中:  while (recordCounter < batchSize && (startTime + conf.maxWaitTime) > System.currentTimeMillis()) {}
		
	kafkaConfigBean.maxWaitTime			# 定义每个batch最长等待时间, 与 maxBatchSize 一起满足任意一个条件, 就结束一个batch;
		- 在StandaloneKafkaSource.produce()中:  while (recordCounter < batchSize && (startTime + conf.maxWaitTime) > System.currentTimeMillis()) {}
		
	kafkaConfigBean.kafkaConsumerConfigs:	添加额外的KafakConsumer参数;
		- 在KafkaConsumer09.addUserConfiguredProperties()中: for (Map.Entry<String, Object> producerConfig : kafkaConsumerConfigs.entrySet()) {}
	


KafkaDSource.其他参数
	KafkaDSource.originParallelism: Server元数据中该Topic可用Partitions数量; 正常=分区数;
	recordQueue.poll(500,MILLISECONDS):		# 队列为空时等待时长.
	BaseKafkaConsumer09.read().sleep=500 	# 当处于Rebalacne或提交offset失败时, ConsumerRunner线程阻塞等待500毫秒;
	recordQueue=new ArrayBlockingQueue<>(batchSize):	# batchSize= kafkaConfigBean.maxBatchSize;
	CONSUMER_POLLING_WINDOW_MS=100,			# KafkaConsumerRunner线程中, 每次调consumer.poll(CONSUMER_POLLING_WINDOW_MS)时
	kafkaConfigBean.keyDeserializer			# 当Json DataPormat时不显示, 默认ByteArray
	kafkaConfigBean.valueDeserializer		# 当Json DataPormat时不显示, 默认String
	
	
	
# 关于频繁Rebalance的原因:
	- 原因1: 可能是 session.timeout.ms 过小而导致容易心跳超时;
	- 原因2: max.poll.interval.ms 过小,且有频繁的[poll失败的重试], 导致频繁的重新加入Group.


	提供稳定性和 提升性能的几点思考:
	- 目前的生产环境中,频繁出现 Rebalace的现象, 可以考虑调大 max.poll.interval.ms 和 session.timeout.ms 参数,已解决网络抖动问题;
	- 提升性能: Kafak高性能的保证之一就是批处理模式, 通过:
		* KafkaConsumer层面: 适度调大 max.poll.records, fetch.min.bytes, fetch.min.bytes, receive.buffer.bytes 参数, 提提高批处理能力; 待验证;
		* DSource层面: 
			- 适度调大kafkaConfigBean.maxBatchSize的参数; 如果maxBatchSize设置的比较大, 注意maxWaitTime不要太大,否则内存易被耗尽;
		
	


DSource.run(){
	
	// 1. 初始化
	BaseStage.init(Info,Context){
		/** 重要1 的Kafka初始化方法:
		* 	- 验证 broker : 	kafkaValidationUtil.validateKafkaBrokerConnectionString();
		* 	- 和Zookeeper的配置	kafkaValidationUtil.validateZkConnectionString();
		* 	- 正式创建 KafkaConsumer : SdcKafkaConsumerFactory.create(settings).create();
		*/
					
		StandaloneKafkaSource.init(){
			int partitionCount = kafkaValidationUtil.getPartitionCount();{
				List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topic);{//Fetcher.getTopicMetadata()
					// 集群中该Topic下 可用的Partitions
					for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.availablePartitionsForTopic(topic));
                    return topicsPartitionInfos;
				}
				if(partitionInfoList != null){
					return partitionInfoList.size();
				}
			}
			if(partitionCount>=1)}{
				// 设置并行度, Cluster模式时,使用; 这个数目是 本ConsumerGroup在当前Topic里被分配到分区. 如果ConsumerGroup里只有这一个client,则
				originParallelism = partitionCount; // 元数据并行度=topic的 partition数量?;
			}
			
			if (issues.isEmpty()){
				kafkaConsumer.validate(issues, getContext()){// BaseKafkaConsumer09.validate()
					createConsumer(){ // BaseKafkaConsumer09.createConsumer()
						configureKafkaProperties(kafkaConsumerProperties){
							// 默认fasel,关闭 enable.auto.commit,禁用自动提交; 即offset叫由SDC自己管理;
							props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_ENABLED_DEFAULT);
							if (this.context.isPreview()) {
								// preview模式, auto.offset.reset=earliest,
								props.setProperty(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
							}
							addUserConfiguredProperties(props)
						}
						kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
					}
					
					// 订阅主题: 消费者自动再均衡(reblance)的功能
					subscribeConsumer(){
						kafkaConsumer.subscribe(Collections.singletonList(topic), this);
					}
					// 获取该Topic的全部分区
					kafkaConsumer.partitionsFor(topic);
				}
			}
		
			
			
			
		
		
		if (issues.isEmpty()){
			{// 构造函数中
				this.recordQueue = new ArrayBlockingQueue<>(batchSize); // 来自于KafkaConfigBean.maxBatchSize;
			}
			
			if (getContext().isPreview()) conf.maxWaitTime = 1000;
						
			kafkaConsumer.init(){ // BaseKafkaConsumer09.init()
				kafkaConsumerRunner = new KafkaConsumerRunner(this:BaseKafkaConsumer09){ // KafkaConsumerRunner implements Runnable()
					@Override public void run(){
						ConsumerRecords<String, byte[]> poll;
						synchronized (consumer.pollCommitMutex){
							poll = consumer.kafkaConsumer.poll(CONSUMER_POLLING_WINDOW_MS); //固定100 (毫秒)
							consumer.needToCallPoll.set(false);
							
							LOG.trace("Read {} messages from Kafka",poll.count(),consumer.recordQueue.size());
							for(ConsumerRecord<String, byte[]> r : poll) {
								if(consumer.needToCallPoll.get()) {
									consumer.recordQueue.clear();
									return;
								}
								
								boolean isQueueOK = putConsumerRecord(r){
									try{
										// 注意, ArrayBlockingQueue<ConsumerRecord<String, byte[]>> recordQueue=new ArrayBlockingQueue<>(batchSize);
										consumer.recordQueue.put(record); 
										return true;
									}catch (InterruptedException e) {
										// 中断当前线程 20毫秒 执行kafkaConsumerRunner.run()从Kafka拉下数据的poll线程; 只需中断标志,仍然继续执行;
										Thread.currentThread().interrupt();
										return false;
									}
								}
								
								if (! isQueueOK) { return;}
							}
						}
					}
				}
				
				executorService.scheduleWithFixedDelay(kafkaConsumerRunner, 0, 20, TimeUnit.MILLISECONDS);
				isInited = true;
			}
			
				kafkaConsumer.validate(issues, getContext()){// BaseKafkaConsumer09.validate()
					createConsumer(){ // BaseKafkaConsumer09.createConsumer()
						configureKafkaProperties(kafkaConsumerProperties){
							// 默认fasel,关闭 enable.auto.commit,禁用自动提交; 即offset叫由SDC自己管理;
							props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_ENABLED_DEFAULT);
							if (this.context.isPreview()) {
								// preview模式, auto.offset.reset=earliest,
								props.setProperty(KafkaConstants.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_PREVIEW_VALUE);
							}
							addUserConfiguredProperties(props)
						}
						kafkaConsumer = new KafkaConsumer<>(kafkaConsumerProperties);
					}
					
					// 订阅主题: 消费者自动再均衡(reblance)的功能
					subscribeConsumer(){
						kafkaConsumer.subscribe(Collections.singletonList(topic), this);
					}
					// 获取该Topic的全部分区
					kafkaConsumer.partitionsFor(topic);
				}
			
		}
		
		
		
	}
	// 2. 运行各batch

	StandaloneKafkaSource.produce(lastSourceOffset, maxBatchSize, batchMaker){
		int batchSize = conf.maxBatchSize > maxBatchSize ? maxBatchSize : conf.maxBatchSize;
			* maxBatchSize = SourceRunner.producer()方法传进来的.
			* conf.maxBatchSize = kafkaConfigBean.maxBatchSize;
		
		while (recordCounter < batchSize && (startTime + conf.maxWaitTime) > System.currentTimeMillis()) {
				* conf.maxWaitTime = kafkaConfigBean.maxWaitTime;
			
			MessageAndOffset message = kafkaConsumer.read(){//BaseKafkaConsumer09.read()
				/** 
				* rebalanceInProgress	: 只有 Kafak在 Bebalance时=true
				*
				*  needToCallPoll	: 当commitSync()同步offsets失败时, catch CommitFailedException并设needToCallPoll=true
				*/
				if(rebalanceInProgress.get() || needToCallPoll.get()) {
					Thread.sleep(500);
				}
				recordQueue.poll(500, TimeUnit.MILLISECONDS);
				processKafkaMessageDefault(partition,offset,messageId,payload){//BaseKafkaSource.processKafkaMessageDefault
					// 实际是创建JsonCharDataParser(), 其实际封装 jackjson.ReaderBasedJsonParser()对象;
					DataParser parser = parserFactory.getParser(messageId, payload){//WrapperDataParserFactory.getParser()
						DataParser parser = this.factory.getParser(id, data);
						return new WrapperDataParserFactory.WrapperDataParser(parser);
					}
					
					// 从字符串转换成 -> Map(json) ;  Map -> Record
					Record record = parser.parse(){ // WrapperDataParser.parse()
						return this.dataParser.parse(){// JsonCharDataParser.parse()
							long offset = this.parser.getReaderPosition();
							/**
							* Object json里面是一个EnforceMap 对象: class EnforceMap extends LinkedHashMap;
							* List -> EnforcerList extends ArrayList
							*/
							
							Object json = this.parser.read(){ //JsonObjectReaderImpl.read()
								switch(this.mode) ;
								return value;
							}
							
							/**
							* 递归调用 jsonToField() 不断把Map中元素解析成 Integer/Double/String.. 等基本类型; 并用Field封装下.
							*/
							if (json != null) {
								record = this.createRecord(offset, json);{//JsonCharDataParser.createRecord()
									Record record = this.context.createRecord(this.readerId + "::" + offset);
									record.set(this.jsonToField(json, offset){// JsonCharDataParser.jsonToField(Object json, long offset)
									});
									return record;
								}
							}
							return record;
						}
					}
					
				}
			}
		}
	}


}


	