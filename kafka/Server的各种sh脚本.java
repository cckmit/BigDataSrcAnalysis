
1. 启动相关脚本 kafka-server-start.sh


exec /home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/kafka-run-class.sh -daemon -name kafkaServer -loggc kafka.Kafka "$@"(/home/app/stream/kafka/kafka_2.11-0.11.0.0/config/server.properties)

```
if [ "x$KAFKA_LOG4J_OPTS" = "x" ]; then
    export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/../config/log4j.properties"
fi
if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
fi
EXTRA_ARGS=${EXTRA_ARGS-'-name kafkaServer -loggc'}

COMMAND=$1
case $COMMAND in -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
esac
exec $base_dir/kafka-run-class.sh $EXTRA_ARGS kafka.Kafka "$@"
	- exec kafka-run-class.sh -daemon -name kafkaServer -loggc kafka.Kafka "$@"
```

	1.1 功能: 添加远程Debug参数:
	export KAFKA_DEBUG=true
	export JAVA_DEBUG_PORT=45060
	export DEBUG_SUSPEND_FLAG=n





2. kafka-run-class.sh 启动Java具体class的脚本

```

# Set Debug options if enabled: 设置Kafka的Debug参数
if [ "x$KAFKA_DEBUG" != "x" ]; then
    # Use default ports
    DEFAULT_JAVA_DEBUG_PORT="5005"
    if [ -z "$JAVA_DEBUG_PORT" ]; then
        JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
    fi
    # Use the defaults if JAVA_DEBUG_OPTS was not set
    DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=$JAVA_DEBUG_PORT"
    if [ -z "$JAVA_DEBUG_OPTS" ]; then
        JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
    fi
    echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
    KAFKA_OPTS="$JAVA_DEBUG_OPTS $KAFKA_OPTS"
fi


exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@"

/home/app/java/jdk1.8.0_65/bin/java -Xmx1G -Xms1G -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true -Xloggc:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../logs/kafkaServer-gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false  -Dkafka.logs.dir=/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../logs -Dlog4j.configuration=file:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../config/log4j.properties -cp .:/home/app/java/jdk1.8.0_65/jre/lib/rt.jar:/home/app/java/jdk1.8.0_65/lib/dt.jar:/home/app/java/jdk1.8.0_65/lib/tools.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/aopalliance-repackaged-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/argparse4j-0.7.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/commons-lang3-3.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-api-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-file-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-json-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-runtime-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-transforms-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/guava-20.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/hk2-api-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/hk2-locator-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/hk2-utils-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-annotations-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-core-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-databind-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-jaxrs-base-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-jaxrs-json-provider-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-module-jaxb-annotations-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javassist-3.21.0-GA.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.annotation-api-1.2.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.inject-1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.inject-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.servlet-api-3.1.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.ws.rs-api-2.0.1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-client-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-common-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-container-servlet-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-container-servlet-core-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-guava-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-media-jaxb-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-server-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-continuation-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-http-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-io-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-security-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-server-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-servlet-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-servlets-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-util-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jopt-simple-5.0.3.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka_2.11-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka_2.11-0.11.0.0-sources.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka_2.11-0.11.0.0-test-sources.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-clients-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-log4j-appender-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-streams-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-streams-examples-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-tools-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/log4j-1.2.17.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/lz4-1.3.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/maven-artifact-3.5.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/metrics-core-2.2.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/osgi-resource-locator-1.0.1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/plexus-utils-3.0.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/reflections-0.9.11.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/rocksdbjni-5.0.1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/scala-library-2.11.11.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/scala-parser-combinators_2.11-1.0.4.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/slf4j-api-1.7.25.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/slf4j-log4j12-1.7.25.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/snappy-java-1.1.2.6.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/validation-api-1.1.0.Final.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/zkclient-0.10.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/zookeeper-3.4.10.jar  kafka.Kafka /home/app/stream/kafka/kafka_2.11-0.11.0.0/config/server.properties
详解: /home/app/java/jdk1.8.0_65/bin/java -Xmx1G -Xms1G 
	-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC 
	-Djava.awt.headless=true 
	-Xloggc:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../logs/kafkaServer-gc.log 
	-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M 
	-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false  
	-Dkafka.logs.dir=/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../logs -Dlog4j.configuration=file:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../config/log4j.properties 
	-cp .:/home/app/java/jdk1.8.0_65/jre/lib/rt.jar:/home/app/java/jdk1.8.0_65/lib/dt.jar:/home/app/java/jdk1.8.0_65/lib/tools.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/aopalliance-repackaged-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/argparse4j-0.7.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/commons-lang3-3.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-api-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-file-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-json-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-runtime-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/connect-transforms-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/guava-20.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/hk2-api-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/hk2-locator-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/hk2-utils-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-annotations-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-core-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-databind-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-jaxrs-base-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-jaxrs-json-provider-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jackson-module-jaxb-annotations-2.8.5.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javassist-3.21.0-GA.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.annotation-api-1.2.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.inject-1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.inject-2.5.0-b05.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.servlet-api-3.1.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/javax.ws.rs-api-2.0.1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-client-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-common-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-container-servlet-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-container-servlet-core-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-guava-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-media-jaxb-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jersey-server-2.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-continuation-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-http-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-io-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-security-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-server-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-servlet-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-servlets-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jetty-util-9.2.15.v20160210.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/jopt-simple-5.0.3.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka_2.11-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka_2.11-0.11.0.0-sources.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka_2.11-0.11.0.0-test-sources.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-clients-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-log4j-appender-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-streams-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-streams-examples-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/kafka-tools-0.11.0.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/log4j-1.2.17.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/lz4-1.3.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/maven-artifact-3.5.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/metrics-core-2.2.0.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/osgi-resource-locator-1.0.1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/plexus-utils-3.0.24.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/reflections-0.9.11.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/rocksdbjni-5.0.1.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/scala-library-2.11.11.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/scala-parser-combinators_2.11-1.0.4.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/slf4j-api-1.7.25.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/slf4j-log4j12-1.7.25.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/snappy-java-1.1.2.6.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/validation-api-1.1.0.Final.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/zkclient-0.10.jar:/home/app/stream/kafka/kafka_2.11-0.11.0.0/bin/../libs/zookeeper-3.4.10.jar  
	kafka.Kafka 
	/home/app/stream/kafka/kafka_2.11-0.11.0.0/config/server.properties
	 -> 执行core包下面的kafka.Kafka.main()方法
		-> KafkaServerStartable.startup()

```



3. 启动Kafak Broker服务: Kafka.main()

kafka.Kafka.main(){
	try {
		val serverProps = getPropsFromArgs(args)
		val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook") {
			override def run(): Unit = kafkaServerStartable.shutdown()
		})
		
		// 正式启动Kafak服务
		kafkaServerStartable.startup(){//KafkaServerStartable.startup()
			try {
				server.startup(){//KafkaServer.startup()
					try{
						info("starting")
						val canStartup = isStartingUp.compareAndSet(false, true)
						if (canStartup) {
							kafkaScheduler.startup()
							zkUtils = initZk()
							_clusterId = getOrGenerateClusterId(zkUtils)
							info(s"Cluster ID = $clusterId")
							
							// 启动数据服务,即logManager
							logManager = LogManager(config, zkUtils, brokerState, kafkaScheduler, time, brokerTopicStats)
							logManager.startup()
							
							socketServer = new SocketServer(config, metrics, time, credentialProvider)
							socketServer.startup()
			
							replicaManager = createReplicaManager(isShuttingDown)
							replicaManager.startup()
			
							kafkaController = new KafkaController(config, zkUtils, time, metrics, threadNamePrefix)
							kafkaController.startup()

							adminManager = new AdminManager(config, metrics, metadataCache, zkUtils)
							
							// 启动ConsumerGroup的协调者服务
							groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM)
							groupCoordinator.startup()
			
					
							// 启动相应数据 API的服务;
							apis = new KafkaApis(socketServer.requestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,
							kafkaController, zkUtils, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
							brokerTopicStats, clusterId, time)
			
							kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, listeners, zkUtils, config.rack, config.interBrokerProtocolVersion)
							kafkaHealthcheck.startup()
							
							checkpointBrokerId(config.brokerId)
							AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString)
							info("started")
			
						}
					
					}catch {
					  case e: Throwable =>fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
						isStartingUp.set(false)
						shutdown()
						throw e
					}
				}
			}catch {
			  case _: Throwable =>fatal("Exiting Kafka.")
				Exit.exit(1)
			}
		}
		kafkaServerStartable.awaitShutdown()
    }
    catch {
      case e: Throwable => fatal(e)
        Exit.exit(1)
    }
    Exit.exit(0) // 正常退出,返回0; 
}



KafkaRequestHandler.run()



KafkaRequestHandlerPool构造函数
new KafkaRequestHandlerPool(){
	private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)
	val runnables = new Array[KafkaRequestHandler](numThreads)
	for(i <- 0 until numThreads) {
		runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis, time)
		Utils.daemonThread("kafka-request-handler-" + i, runnables(i)).start(){//KafkaRequestHandler.start()
			def run(){// KafkaRequestHandler.run() 统一处理Api的请求步骤
				while (true) {
					try {
						var req : RequestChannel.Request = null
						while (req == null) {//循环阻塞300ms, 直到接受到一个请求,进入 apis.handle()处理
						  val startSelectTime = time.nanoseconds
						  req = requestChannel.receiveRequest(300)
						  val endTime = time.nanoseconds
						  if (req != null)
							req.requestDequeueTimeNanos = endTime
						  val idleTime = endTime - startSelectTime
						  aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
						}

						if (req eq RequestChannel.AllDone) {
						  latch.countDown()
						  return
						}
						trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
						
						apis.handle(req){//KafkaApis.handle(req:RequestChannel.Request)
							try {
								ApiKeys.forId(request.requestId) match {
									/** API.1	生产者存储数据的Api: ProducerRequest
									* KafkaApis.handleProduceRequest()-> ReplicaManager.appendRecords()->  Partition.appendRecordsToLeader()
									* 	-> Log.appendAsLeader() -> Log.append() -> LogSegment.append() -> FileRecords.append(MemoryRecords records)
									*/
									case ApiKeys.PRODUCE => handleProduceRequest(request){//KafkaApis.handleProduceRequest()	处理生产者发来的: 存储消息日志
										val produceRequest = request.body[ProduceRequest]
										val numBytesAppended = request.header.toStruct.sizeOf + request.bodyAndSize.size
										
										val (existingAndAuthorizedForDescribeTopics, nonExistingOrUnauthorizedForDescribeTopics) =
										  produceRequest.partitionRecordsOrFail.asScala.partition { case (tp, _) =>
											authorize(request.session, Describe, new Resource(Topic, tp.topic)) && metadataCache.contains(tp.topic)
										  }
										
										if (authorizedRequestInfo.isEmpty){
											sendResponseCallback(Map.empty)
										}else {// 进入这里
											val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId
											  // call the replica manager to append messages to the replicas	将消息追加到该partition的主节点(leader replicas)中,并让其同步其他replicas
											replicaManager.appendRecords(timeout = produceRequest.timeout.toLong, requiredAcks = produceRequest.acks,internalTopicsAllowed = internalTopicsAllowed,
													isFromClient = true,entriesPerPartition = authorizedRequestInfo, responseCallback = sendResponseCallback){//ReplicaManager.appendRecords()
												if (isValidRequiredAcks(requiredAcks)) {
												  val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,isFromClient = isFromClient, entriesPerPartition, requiredAcks){
														entriesPerPartition.map { case (topicPartition, records) =>
														  brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
														  brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

														  // reject appending to internal topics if it is not allowed
														  if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
															(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo,Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
														  } else {// 正常,进入这里
															try {
															  val partitionOpt = getPartition(topicPartition)
															  val info = partitionOpt match {
																case Some(partition) =>//正常,进入这里,调用 Partition来完成具体的写入
																	partition.appendRecordsToLeader(records, isFromClient, requiredAcks){//Partition.appendRecordsToLeader()
																		val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
																			leaderReplicaIfLocal match {
																				case Some(leaderReplica) =>
																					val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient){
																						Log.append(records, isFromClient, assignOffsets = true, leaderEpoch){//Log.append()
																							
																							val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)
																							try {
																							  // they are valid, insert them in the log
																							  lock synchronized {
																								if(assignOffsets) {
																								  val validateAndOffsetAssignResult = try {
																									LogValidator.validateMessagesAndAssignOffsets(validRecords,offset,now)
																								  } catch {
																									case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
																								  }
																								}else{
																									if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
																										throw new IllegalArgumentException("Out of order offsets found in " + records.records.asScala.map(_.offset))
																								}

																								// update the epoch cache with the epoch stamped onto the message by the leader
																								validRecords.batches.asScala.foreach { batch =>
																								  if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
																									leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
																								}
																								// maybe roll the log if this segment is full
																								val segment = maybeRoll(messagesSize = validRecords.sizeInBytes, maxTimestampInMessages = appendInfo.maxTimestamp, maxOffsetInMessages = appendInfo.lastOffset)
																								val logOffsetMetadata = LogOffsetMetadata()

																								// 核心代码?
																								segment.append(firstOffset = appendInfo.firstOffset,largestOffset = appendInfo.lastOffset, largestTimestamp = appendInfo.maxTimestamp,
																								  shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,records = validRecords){//LogSegment.append()
																									LogSegment.append(){
																										if (records.sizeInBytes > 0) {
																											val appendedBytes = log.append(records){//FileRecords.append(MemoryRecords records)
																												int written = records.writeFullyTo(channel){
																													buffer.mark();
																													int written = 0;
																													while (written < sizeInBytes())
																														written += channel.write(buffer){
																															
																														}
																													buffer.reset();
																													return written;
																												}
																												size.getAndAdd(written);
																												return written;
																											}
																											
																											if(bytesSinceLastIndexEntry > indexIntervalBytes) {
																												index.append(firstOffset, physicalPosition)
																												timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
																												bytesSinceLastIndexEntry = 0
																											}
																											bytesSinceLastIndexEntry += records.sizeInBytes
																										}
																									}
																								  }

																								// update the producer state
																								for ((producerId, producerAppendInfo) <- updatedProducers) {
																								  producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
																								  producerStateManager.update(producerAppendInfo)
																								}
																								for (completedTxn <- completedTxns) {
																								  val lastStableOffset = producerStateManager.completeTxn(completedTxn)
																								  segment.updateTxnIndex(completedTxn, lastStableOffset)
																								}
																								
																								producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)
																								updateLogEndOffset(appendInfo.lastOffset + 1)
																								updateFirstUnstableOffset()
																								appendInfo
																							  }
																							} catch {
																							  case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
																							}
																						}
																					}
																					
																					replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
																					(info, maybeIncrementLeaderHW(leaderReplica))
																				
																				case None => throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d" .format(topicPartition, localBrokerId))
																			}
																			
																			if (leaderHWIncremented) tryCompleteDelayedRequests(){//Partition.tryCompleteDelayedRequests()
																				val requestKey = new TopicPartitionOperationKey(topicPartition)
																				replicaManager.tryCompleteDelayedFetch(requestKey)
																				replicaManager.tryCompleteDelayedProduce(requestKey)
																				replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
																			}
																			return info
																		}
																	}

																case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
																  .format(topicPartition, localBrokerId))
															  }

															  val numAppendedMessages =
																if (info.firstOffset == -1L || info.lastOffset == -1L)
																  0
																else
																  info.lastOffset - info.firstOffset + 1

															  // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
															  brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
															  brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
															  brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
															  brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

															  trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
																.format(records.sizeInBytes, topicPartition.topic, topicPartition.partition, info.firstOffset, info.lastOffset))
															  (topicPartition, LogAppendResult(info))
															} catch {
															  // NOTE: Failed produce requests metric is not incremented for known exceptions
															  // it is supposed to indicate un-expected failures of a broker in handling a produce request
															  case e: KafkaStorageException =>
																fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
																Exit.halt(1)
																(topicPartition, null)
															  case e@ (_: UnknownTopicOrPartitionException |
																	   _: NotLeaderForPartitionException |
																	   _: RecordTooLargeException |
																	   _: RecordBatchTooLargeException |
																	   _: CorruptRecordException |
																	   _: InvalidTimestampException) =>
																(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
															  case t: Throwable =>
																brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
																brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
																error("Error processing append operation on partition %s".format(topicPartition), t)
																(topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(t)))
															}
														  }
    }
														
												  }

												  if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
													// create delayed produce operation
													val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)
													// ...
												  } else {
													// we can respond immediately
													val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
													responseCallback(produceResponseStatus)
												  }
												} else {
													val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
														topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,LogAppendInfo.UnknownLogAppendInfo.firstOffset, RecordBatch.NO_TIMESTAMP)
													}
													responseCallback(responseStatus){// KafkaApis.handleProduceRequest().sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse])
														val mergedResponseStatus = responseStatus ++ 
															unauthorizedForWriteRequestInfo.mapValues(_ => new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)) ++
															nonExistingOrUnauthorizedForDescribeTopics.mapValues(_ => new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
													  
														// When this callback is triggered, the remote API call has completed
														request.apiRemoteCompleteTimeNanos = time.nanoseconds

														quotas.produce.recordAndMaybeThrottle(request.session.sanitizedUser,request.header.clientId, numBytesAppended,produceResponseCallback){//clientQuotaManager.recordAndMaybeThrottle()
															val clientSensors = getOrCreateQuotaSensors(sanitizedUser, clientId);
															
															recordAndThrottleOnQuotaViolation(clientSensors, value, callback){
																clientSensors.quotaSensor.record(value)
																callback(0)
																return throttleTimeMs;
															}
														}
													}
												}
											}
											// hence we clear its data here inorder to let GC re-claim its memory since it is already appended to log
											produceRequest.clearPartitionRecords()
										}
										
									}
									
									/** API.2 消费者获取数据	FetchRequest
									*
									*/
									case ApiKeys.FETCH => handleFetchRequest(request){//KafkaApis.handleFetchRequest()
										val fetchRequest = request.body[FetchRequest]
										//..
										if (authorizedRequestInfo.isEmpty){
											processResponseCallback(Seq.empty)
										}else {
											// call the replica manager to fetch messages from the local replica
											replicaManager.fetchMessages(fetchRequest.maxWait.toLong, fetchRequest.replicaId, fetchRequest.minBytes,fetchRequest.maxBytes, versionId <= 2, authorizedRequestInfo,
													replicationQuota(fetchRequest), processResponseCallback, fetchRequest.isolationLevel){// ReplicaManager.fetchMessages()
												
												val fetchOnlyCommitted: Boolean = ! Request.isValidBrokerId(replicaId)
												val logReadResults = readFromLocalLog(){
													val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
													var minOneMessage = !hardMaxBytesLimit
													readPartitionInfo.foreach { case (tp, fetchInfo) =>
															val readResult = read(tp, fetchInfo, limitBytes, minOneMessage){//ReplicaManager.readFromLocalLog().read()
																brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
																brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
																
																val localReplica = if (fetchOnlyFromLeader){
																	getLeaderReplicaIfLocal(tp)
																}else{
																	getReplicaOrException(tp)
																}
																
																val logReadInfo = localReplica.log match {
																	case Some(log) =>
																		val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)
																		val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel){// Log.read()
																				
																			
																			
																			
																			
																			
																		}
																		
																		if (shouldLeaderThrottle(quota, tp, replicaId)){
																			return FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
																		}else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete){
																			return FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
																		}else{return fetch}
																	
																	case None => 
																		return FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
																}
																
															}
															val messageSetSize = readResult.info.records.sizeInBytes
															if (messageSetSize > 0) minOneMessage = false
															limitBytes = math.max(0, limitBytes - messageSetSize)
															result += (tp -> readResult)
													}
													result
												}
											}
											
										}
									}
									
									case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
									case ApiKeys.METADATA => handleTopicMetadataRequest(request)
									case ApiKeys.LEADER_AND_ISR => handleLeaderAndIsrRequest(request)
									case ApiKeys.STOP_REPLICA => handleStopReplicaRequest(request)
									case ApiKeys.UPDATE_METADATA_KEY => handleUpdateMetadataRequest(request)
									case ApiKeys.CONTROLLED_SHUTDOWN_KEY => handleControlledShutdownRequest(request)
									case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
									case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request)
									case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
									case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
									case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
									case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request)
									case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
									case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupRequest(request)
									case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request)
									case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
									case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
									case ApiKeys.CREATE_TOPICS => handleCreateTopicsRequest(request){//		创建Topic
										val createTopicsRequest = request.body[CreateTopicsRequest]
										// createTopicsRequest.topics== Map<Topic, TopicDetails> {case TopicDetail(int numPartitions,short replicationFactor,Map<Integer, List<Integer>> replicasAssignments,Map<String, String> configs}
										
										if (!controller.isActive) {
										  val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
											(topic, new ApiError(Errors.NOT_CONTROLLER, null))
										  }
										  sendResponseCallback(results)
										} else if (!authorize(request.session, Create, Resource.ClusterResource)) {
										  val results = createTopicsRequest.topics.asScala.map { case (topic, _) =>
											(topic, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, null))
										  }
										  sendResponseCallback(results)
										} else { // 正常的话,直接进入这里
										  val (validTopics, duplicateTopics) = createTopicsRequest.topics.asScala.partition { case (topic, _) =>
											!createTopicsRequest.duplicateTopics.contains(topic)
										  }
										  
											adminManager.createTopics(createTopicsRequest.timeout,createTopicsRequest.validateOnly, validTopics,sendResponseWithDuplicatesCallback ){//AdminManager.createTopics()
												// 1. map over topics creating assignment and calling zookeeper
												val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
													// 向/$KAFKA/config/topics/{topic} 和/$KAFKA/brokers/topics/{topic} 两个ZK路径写入partition信息;
												val metadata = createInfo.map { case (topic, arguments) =>
												  try {
													LogConfig.validate(configs)
													val assignments = {
													  if ((arguments.numPartitions != NO_NUM_PARTITIONS || arguments.replicationFactor != NO_REPLICATION_FACTOR)&& !arguments.replicasAssignments.isEmpty)
														throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. Both cannot be used at the same time.")
													  else if (!arguments.replicasAssignments.isEmpty) {
														arguments.replicasAssignments.asScala.map { case (partitionId, replicas) =>(partitionId.intValue, replicas.asScala.map(_.intValue))}
													  } else{//正常 进入这里
															AdminUtils.assignReplicasToBrokers(brokers, arguments.numPartitions, arguments.replicationFactor){
																if (brokerMetadatas.forall(_.rack.isEmpty))
																  assignReplicasToBrokersRackUnaware(nPartitions, replicationFactor, brokerMetadatas.map(_.id), fixedStartIndex, startPartitionId){
																	  
																	val ret = mutable.Map[Int, Seq[Int]]()
																	val brokerArray = brokerList.toArray
																	val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
																	var currentPartitionId = math.max(0, startPartitionId)
																	var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
																	for (_ <- 0 until nPartitions) {
																	  if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
																		nextReplicaShift += 1
																	  val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
																	  val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
																	  for (j <- 0 until replicationFactor - 1)
																		replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
																	  ret.put(currentPartitionId, replicaBuffer)
																	  currentPartitionId += 1
																	}
																	return ret;// ret=Map<Integer[Partition?],ArrayBuffer[Broker?]>
																	  
																  }
																else {
																  if (brokerMetadatas.exists(_.rack.isEmpty)){
																	  throw new AdminOperationException("Not all brokers have rack information for replica rack aware assignment")
																  }
																  assignReplicasToBrokersRackAware(nPartitions, replicationFactor, brokerMetadatas, fixedStartIndex,startPartitionId)
																}
															}
													  }
														
													}
													trace(s"Assignments for topic $topic are $assignments ")

													createTopicPolicy match {
													  case Some(policy) =>
														AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
													  case None =>
														if (validateOnly)
														  AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
														else
														  AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false){
															validateCreateOrUpdateTopic(zkUtils, topic, partitionReplicaAssignment, config, update)
															if (!update) {
															  writeEntityConfig(zkUtils, getEntityConfigPath(ConfigType.Topic, topic), config){
																//entityPath = /config/topics/testCreate01: /config/topics/{topic}
																zkUtils.updatePersistentPath(entityPath, Json.encode(map)){
																	val acl = if (acls eq UseDefaultAcls) ZkUtils.defaultAcls(isSecure, path) else acls
																	// 向/config/topics/{topic} 路径,写入 data={version:1,config:{}}
																	zkClient.writeData(path, data)
																}
															  }
															}
															
															writeTopicPartitionAssignment(zkUtils, topic, partitionReplicaAssignment, update){
																val zkPath = getTopicPath(topic) // 获取的是topic正式位置 /brokers/topics/{topic}路径
																
																// 获取该Topic的分区和分区所在节点信息: partitons== Map<String(分区),Seq[Int](主副节点?)]
																val jsonPartitionData = zkUtils.replicaAssignmentZkData(replicaAssignment.map(e => e._1.toString -> e._2))

																if (!update) {
																	info("Topic creation " + jsonPartitionData.toString)
																	zkUtils.createPersistentPath(zkPath, jsonPartitionData)
																} else {
																	info("Topic update " + jsonPartitionData.toString)
																		* [2020-03-14 00:52:11,159] INFO Topic creation {"version":1,"partitions":{"1":[0],"0":[0]}} (kafka.admin.AdminUtils$)
																	// 向 /$KAFKA/brokers/topics/{topic}路径文件,写入字符串: {"version":1,"partitions":{"1":[0],"0":[0]}}
																	zkUtils.updatePersistentPath(zkPath, jsonPartitionData)
																}
																
															}
															
														  }
													}
													
													return CreateTopicMetadata(topic, assignments, ApiError.NONE)
												  } catch {
													// Log client errors at a lower level than unexpected exceptions
													case e@ (_: PolicyViolationException | _: ApiException) =>
													  info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
													  CreateTopicMetadata(topic, Map(), ApiError.fromThrowable(e))
													case e: Throwable =>
													  error(s"Error processing create topic request for topic $topic with arguments $arguments", e)
													  CreateTopicMetadata(topic, Map(), ApiError.fromThrowable(e))
												  }
												}

												// 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
												if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
												  val results = metadata.map { createTopicMetadata =>
													if (createTopicMetadata.error.isSuccess() && !validateOnly) {
													  (createTopicMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
													} else {
													  (createTopicMetadata.topic, createTopicMetadata.error)
													}
												  }.toMap
												  responseCallback(results)
												} else {// 正常,进入这里
													// 3. else pass the assignments and errors to the delayed operation and set the keys
													val delayedCreate = new DelayedCreateTopics(timeout, metadata.toSeq, this, responseCallback)
													val delayedCreateKeys = createInfo.keys.map(new TopicKey(_)).toSeq
													// try to complete the request immediately, otherwise put it into the purgatory
													topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys){//DelayedOperation.tryCompleteElseWatch()
														var isCompletedByMe = operation.safeTryComplete(){
															synchronized{
																tryComplete(){//抽象方法, 由DelayedCreateTopics.tryComplete()实现
																	val leaderlessPartitionCount = createMetadata.filter(_.error.isSuccess)
																	  .foldLeft(0) { case (topicCounter, metadata) =>
																		topicCounter + missingLeaderCount(metadata.topic, metadata.replicaAssignments.keySet)
																	  }
																	  
																	if (leaderlessPartitionCount == 0) {
																	  trace("All partitions have a leader, completing the delayed operation")
																	  forceComplete()
																	} else {
																	  trace(s"$leaderlessPartitionCount partitions do not have a leader, not completing the delayed operation")
																	  false
																	}
																	
																	
																}
															}
														}
														if (isCompletedByMe){return true;}
														
														var watchCreated = false
														for(key <- watchKeys) {
														  // If the operation is already completed, stop adding it to the rest of the watcher list.
														  if (operation.isCompleted)
															return false
														  watchForOperation(key, operation)

														  if (!watchCreated) {
															watchCreated = true
															estimatedTotalOperations.incrementAndGet()
														  }
														}

														isCompletedByMe = operation.safeTryComplete()
														if (isCompletedByMe) return true

														// if it cannot be completed by now and hence is watched, add to the expire queue also
														if (!operation.isCompleted) {
														  if (timerEnabled)
															timeoutTimer.add(operation){//kafka.Time.SystemTime.add()
																readLock.lock()
																try {
																	addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs)){
																		if (!timingWheel.add(timerTaskEntry)) {
																			if (!timerTaskEntry.cancelled){taskExecutor.submit(timerTaskEntry.timerTask){
																				
																				DelayedCreateTopics.run(){
																					val isCompleted= forceComplete(){
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
																							}
																							true
																						}else { false}
																					}
																					if(isCompleted){onExpiration()}
																				}
																				
																			}}
																		}
																																			
																	}
																} finally {
																	readLock.unlock()
																}
															}
														  if (operation.isCompleted) {
															// cancel the timer task
															operation.cancel()
														  }
														}
														return false
													}
												}
												
											}
										}
									}
									case ApiKeys.DELETE_TOPICS => handleDeleteTopicsRequest(request)
									case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
									case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request)
									case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
									case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request)
									case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request)
									case ApiKeys.END_TXN => handleEndTxnRequest(request)
									case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request)
									case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request)
									case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
									case ApiKeys.CREATE_ACLS => handleCreateAcls(request)
									case ApiKeys.DELETE_ACLS => handleDeleteAcls(request)
									case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
									case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
							  }
							} catch {
								case e: FatalExitError => throw e
								case e: Throwable => handleError(request, e)
							} finally {
								request.apiLocalCompleteTimeNanos = time.nanoseconds
							}
							
						}
						
					} catch {
						case e: FatalExitError =>latch.countDown()
						  Exit.exit(e.statusCode)
						case e: Throwable => error("Exception when handling request", e)
					}
				}
				
				
			}
		}
	}
	
	
	
}

