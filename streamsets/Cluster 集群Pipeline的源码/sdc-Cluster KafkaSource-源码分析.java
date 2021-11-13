

几个问题:
* 在哪里执行 new KafkaConsumer?
	-> DStreamGraph.start()-> DirectKafkaInputDStream.start() -> ConsumerStrategy.Subscribe.onStart()中

* 在哪里执行 kafkaConsumer.poll()
	* 线程: JobGenerator@6663 in group man: JobGenerator.generateJobs() -> DStreamGraph.generateJobs()
	* DStreamGraph.ForEachDStream.generateJob() -> DStream.getOrCompute() -> DStream.createRDDWithLocalProperties()
		-> TransformedDStream.compute() -> ...
			-> DirectKafkaInputDStream.compute() -> latestOffsets() -> paranoidPoll()-> KafakConsumer.poll()

* 什么时候 new KafkaRDD ? 里面封装了什么? 同上
	* 在 JobGenerator@6663 in group man线程中: JobGenerator.generateJobs() 触发;
	* 最终也是在 DirectKafkaInputDStream.compute()方法中 val rdd = new KafkaRDD[K, V](); 将latestOffsets()中获取的数据,封装进RDD;

* 在哪里执行 kafkaConsumer.commit()
* 如何把 KafakRDD里的数据转给下一个算子? 
	- 在一个JobSubmit中, 从FinalStage向头个RDD方向遍历, 并将其切分成若干个Stage: ResultStage or ShuffleTask, 
	- 一个Stage里封装了多个RDD, 执行时以其最后一个(结果)RDD作为Stage的计算触发和输出结果;
	-  将一个Stage按 Partition数量切分成n个Task, 计算该Task即 计算给Stage的结果RDD的结果;
	-  执行该RDD的compulte()方法,
	
* 什么时候分配offset? 
	- 在 Driver执行createDStream() or createKafakRDD时?
	- 答案: 在SparkContext.runJob()中 runJob(rdd, func, 0 until rdd.partitions.length)中的rdd.partitions.length 触发
	
	
	
* 哪些东西被封装在ResultTask中? 



Cluster KafakDSource 原理

在Driver端: 
	"JobGenerator"线程: 循环创建Job及其RDD, 
		- MapPartitionRDD[3]: 其mapPartition()方法中, 包含ClusterProvideFunction.startBatch()是向SDC输入Kafak拉取来的数据的.
			- prev: MapPartitionRDD[2]
				- KafkaRDD: // 由DirectKafakInputDStream.compute()方法中创建; 
				/**
				*
				*/
				DirectKafkaInputDStream.compute(time){
					/* 在SDC-cluster模型中, 涉及kafaka的两个sparkConf参数如下: 
					*	"spark.streaming.kafka.maxRatePerPartition" -> "400" 执行分区速率的, 由kafakConfigBean.maxRatePerPartition指定;
					* 	"spark.streaming.kafka.consumer.poll.max.retries" -> "5"	; 默认5, 
					*/
					// 计算各Partition/RDD实例 要消费消息的截止位置:utilOffset = 取 额定速率下最大offser 与 最新消息offer 的最小值为utilOffset;
					val untilOffsets:Map[TopicPartition,Long] = clamp(latestOffsets()){
						// 这个方法两个作用: poll(0)将新增的分区添加到currentOffsets; 重置所有currentOffsets里所有分区的消费位置到 End位置(是offset最新, 还消息末尾?) 
						val partsWithLatest:Map[TopicPartition, Long]  = latestOffsets(){//DirectKafkaInputDStream.latestOffsets()		
							val c = consumer
							// 更新元数据;  paranoid多余的,会一直阻塞直到它成功获取了所需的元数据信息,但他一般不返回数据;
							paranoidPoll(c)
							// 调用KafkaConsumer.assignment(): Set<TopicPartition> 方法获取本Group绑定的TP及Offset
							val parts = c.assignment().asScala
							
							// 比较出currentOffsets还没有的key, 那就是新分配/绑定的TopicPartiton; 若有新分区,将其消费位置也添加进 currentOffsets:Map[TopicPartition,Long]中;
							val newPartitions = parts.diff(currentOffsets.keySet)
							currentOffsets = currentOffsets ++ newPartitions.map(tp => tp -> c.position(tp)).toMap
							c.pause(newPartitions.asJava) //停止本consumer再从 newPartitions里的分区取拿数据;
							
							// 将所有(包括新订阅)的分区,都设置从最末尾位置(end offsets); 从consumer中更新返回;
							// 这里seek()操作,只在内存中将 TopicPartitionState.resetStrategy设置为 Latest,把TopicPartitionState.position 置为null; 
							// 在后面的position()操作中,当判断TPState.position==null时, 触发Fetcher.updateFetchPositions()从网络拉取end位置的offset设置TPState.position值;
							c.seekToEnd(currentOffsets.keySet.asJava)
							// 在consumer.position(tp)中,触发resetOffset(),从网络查该分区end位置(是所有日志的end/最新消息);
							parts.map(tp => tp -> {c.position(tp)}).toMap
							
						}
						
						val clampParts:Map[TopicPartition, Long]= clamp(partsWithLatest){//DirectKafkaInputDStream.clamp(offsets)
							
							// 计算每个partiton要处理的最大消息数: 1.先读取计算各Partiton单批次最大处理消息数量; 2. 再与秒级graph.batchDuration值相乘, 求出本批次最大处理数;
							val someParts = maxMessagesPerPartition(offsets)
							
							// 取 额定速率下最大offser 与 最新消息offer, 两者最小值为本批次实际untilOffset.
							someParts.map { mmp =>
								mmp.map { case (tp, messages) =>
									val uo = offsets(tp) //offsets即为传参进来的, 各分区的Latest Offset值(即最新的消息)
									// 取 速率上限offser和 最新日志offset的最小值,为本batch的untilOffset(消费截止offset); 这样保证了不消费还没到的消息; 当没有server没有新消息时, 这个值是相等的.
									// currentOffsets(tp) + messages 就是额定速率消费offser上限;
									// uo 就是之前latestOffsets()方法求出的Server端该分区最新消费的offset;
									tp -> Math.min(currentOffsets(tp) + messages, uo)
							  }
							}.getOrElse(offsets)
						}
						
						clampParts
					}
					
					//取上batch的currentOffsets为起始点fromOffset, 并与untilOffset一起封装到OffsetRange对象中;
					val offsetRanges = untilOffsets.map { case (tp, uo) =>
						val fo = currentOffsets(tp)
						OffsetRange(tp.topic, tp.partition, fo, uo)
					}
					val useConsumerCache = context.conf.getBoolean("spark.streaming.kafka.consumer.cache.enabled", true)
					val rdd = new KafkaRDD[K, V](context.sparkContext, executorKafkaParams, offsetRanges.toArray, getPreferredHosts, useConsumerCache)										
				}
			

		
	
Executor端: Executor.launchTask(taskDesc) -> TaskRunner.run() -> ResultTask.runTask():
	"Executor task launch worker for task $taskId"线程: 执行一个Stage中某分区的RDD的计算
	ResultTask.runTask() -> 
		* 1. 反序列化RDD和用户函数:	val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)]
		* 2. 递归调用各级父RDD.compute()直到算出上级RDD数据结果:  rdd.iterator(partition, context)
			Job2: MapPartitionRDD[3]
			 -prev: MapPartitionRDD[2] //前面MapPartitionRDD[2]已经计算过,并且cache()缓存了; 不必二次计算;
				-prev: KafkaRDD: KafakConsumer.poll() 
			
		* 3. 执行Action 定义的函数: func(context, it): Driver.process(rdd) -> 
				incomingRDD.mapPartitions(){
					val batch = iterator.map(someTansformFunc).toList.asJava
					
					// 在整个分区的级别,执行 SDC-Pipeline运行, 处理整个批次的数据: batch
					ClusterFunctionProvider.startBatch(batch){
						ClusterSource source = sdcPool.getNotStartedSDC().getSource()
						// 这里将batch数据存入 DataChannel.dataQueue: BlockingQueue<OffsetAndResult<Map.Entry>> ;  
						//   -> 由"ProductionPipeline-{pipelineId}"线程拉取到ClusterKafakDSource.produce()处理;
						offset = source.put(batch);
					}
					
					//"ProductionPipeline-{pipelineId}"线程的处理逻辑:  
					{
						SlaveStandaloneRunner.start(){
							StandaloneRunner.start(){
								ProductionPipelineRunnable.run() -> ProductionPipeline.run() -> Pipeline.run()
								-> ProductionPipelineRunner.run(){
									runPollSource(){
										while (!offsetTracker.isFinished() && !stop && !finished) {
											processPipe() -> StagePipe.execute() -> StageRuntime.execute(){
												case SOURCE: //运行KafkaDSource, DataGenerator等 Source类算子;
													ClusterKafakDSource.produce(){
														consumer.take();{
															// 从dataChannel 队列取出数据; 
															dataChannel.take(10, TimeUnit.MILLISECONDS);
														}
													}
												case PROCESSOR:
													// 运行PointSelect, HttpClient, PythonScript, WindowAggr等算子;
												
												case TARGET:
													// 运行Trash, KafkaProducer, DataViewer等算子;
											}
										}
									}
									
								}
							}
						}
					}
					
				}
				
			
		
		Job2: MapPartitionRDD[3]
		-prev: MapPartitionRDD[2] //前面MapPartitionRDD[2]已经计算过,并且ch
			-prev: KafkaRDD
		
		
1. 有几个Job? 2个
	
	Job1: MapPartitionRDD[2]
		-prev: KafkaRDD
		
	Job2: MapPartitionRDD[3]
		-prev: MapPartitionRDD[2] //前面MapPartitionRDD[2]已经计算过,并且ch
			-prev: KafkaRDD
	
	
	
	
2. ClusterProvideFunction.startBatch()的执行, 是在第几个Job的, 第几个RDD中?
	* 








ClusterPipeline的 KafakDSource 原理

1. 在Driver端
	- 创建DirectorKafakInputDStream对象
	- 哪里分配offset?

2. 在Executor端的代码
	- runJob() 触发最后一个RDD的rdd.foreach()
		- 1. 先执行KafkaRDD.compute(part: KafkaRDDPartition)从 CacheConsumer中读取数据;
			- 当part.fromOffset == untilOffset时,表示不用去kafka消费数据, 返回Iterator.empty
			- 当 part.untilOffset 大于 fromOffset时, 则new KafkaRDDIterator(part)这个迭代器返回, 他会保证只从 part.partition这个分区中
				读取 [fromOffset -> untilOffset]的数据; 也就是每个KafkaRDD 对应一个 KafkaRDDPartition, 只能从一个TopicPartiton分区读取数据;
				? 问题, 指定各KafakRDD的 fromOffset -> untilOffset是如何指定的? 与SDC中哪些参数指定?\
			- 在SparkContext.runJob()方法中 由rdd.partitions 触发其递归 firstParent[T].partitions()知道KafakRDD.getPartition()
				- 遍历offsetRanges.zipWithIndex.map()并将每个TP封装进new KafkaRDDPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset)
				- 换而言之, KafkaRDD的分区数= 其构造函数传入 OffsetRange[]的数组长度;
					? 在Streaming-Kafka中 JobGenerator 常见每个KafkaRDD是如何传的参数? 
	
				
				
			
		- 2. 在MapPartitionRDD.compute()中 执行Driver.foreach()中的逻辑
		- 3. 触发 ClusterProvideFunction.startBatch(List<Entry> batchData) 触发 EmbeddedSDC一个Batch的运行
			* 向 chanleController.queue队里中 存入从RDD拉去到的数据
			* 另一个ProductionPipelineRunner.run()线程 如何触发循环运行, 其依次执行 ClusterKafakDSource.producer() 和其他 DProcessor.process()完成一个SDC的 Batch;
					? 如何触发? 按什么频率触发?
				- ClusterKafakDSource.producer()中 循环从 ChanelController.queue队列中读取 bytes并序列化成Map对象封装进Record中;
				- 哪里提交offset?












// 在一个Executor上运行 执行一个批次的运行; 即数据源已准备好了(在batch:List<Map.Entry>)里, 并将其存入 Producer/Consumer.dataChannel 队列中
ClusterFunctionImpl.startBatch(List<Map.Entry> batch){
	boolean isPipelineCrashed = false;
	try {
		sdc = sdcPool.getNotStartedSDC();
		ClusterSource source = sdc.getSource();
		offset = source.put(batch);
		return getNextBatch(0, sdc);
    }catch (Exception | Error e) {
		throw new RuntimeException(errorStackTrace);
	}
}



在Executor上:

Cluster DataCollector相关类
* EmbeddedSDC
* ProductionPipelineRunner.runPollSource()
* StageRuntime.execute()
ClusterFunctionImpl


ClusterKafkaSource - 集群kafka算子相关

* Cluster KafkaDSource相关源码和类
	* ClusterKafkaSource.produce() & 
		- ClusterKafkaSource.commit() -> Consumer.commit()
		- ClusterKafkaSource.completeBatch() -> Producer.waitForCommit();
	* ControlChannel.getConsumerMessages() 将队列数据导入数组输出;
		- ControlChannel.producerComplete() 


* ClusterFunctionImpl.startBatch() //这个在Driver定义的算子中触发;
	- EmbeddedSDCPool.getNotStartedSDC();
	- ClusterKafkaSource.put(batch) -> Producer.put()
		


ProductionPipelineRunner.runPollSource() -> executeRunner()
	-> pipeRunner.executeBatch() -> originStage.runProduce() -> StageRuntime.execute()
		-> ((Source) getStage()).produce(previousOffset, batchSize, batchMaker): 触发KafakDSource.produce()的执行;





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














/**		1. SparkSreaming 中 一般的KafakSource 如何构建DStream对象并传输数据的;
	
*/



KafkaUtils.createDirectStream(){
	val ppc = new DefaultPerPartitionConfig(ssc.sparkContext.getConf)
	createDirectStream[K, V](ssc, locationStrategy, consumerStrategy, ppc){//KafkaUtils.createDirectStream()
		new DirectKafkaInputDStream[K, V](ssc, locationStrategy, consumerStrategy, perPartitionConfig){
			val executorKafkaParams = {
				val ekp = new ju.HashMap[String, Object](consumerStrategy.executorKafkaParams)
				KafkaUtils.fixKafkaParams(ekp)
				ekp
			}
			val checkpointData =new DirectKafkaInputDStreamCheckpointData();
			protected val commitQueue = new ConcurrentLinkedQueue[OffsetRange]
			protected val commitCallback = new AtomicReference[OffsetCommitCallback]
			
	
		}
	}



Executor.TaskRunner.run(){
	
	val value = try {
          val res = task.run(taskAttemptId = taskId,attemptNumber = taskDescription.attemptNumber, metricsSystem = env.metricsSystem){
			  
		  }
		  
          threwException = false
          res
        }
}

















	