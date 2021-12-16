

// 1.1 flink run WordCount.jar 

exec /usr/java/jdk-release/bin/java \
-agentlib:jdwp=transport=dt_socket,server=n,suspend=n,address=192.168.51.1:42020 \
-Dlog.file=/opt/flink/flink-1.12.2/log/flink-bigdata-client-bdnode102.log -Dlog4j.configuration=file:/opt/flink/conf/log4j-cli.properties -Dlog4j.configurationFile=file:/opt/flink/conf/log4j-cli.properties -Dlogback.configurationFile=file:/opt/flink/conf/logback.xml \
-classpath '/opt/flink/flink-1.12.2/lib/flink-connector-hive_2.11-1.12.1.jar:/opt/flink/flink-1.12.2/lib/flink-csv-1.12.2.jar:/opt/flink/flink-1.12.2/lib/flink-json-1.12.2.jar:/opt/flink/flink-1.12.2/lib/flink-shaded-zookeeper-3.4.14.jar:/opt/flink/flink-1.12.2/lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:/opt/flink/flink-1.12.2/lib/flink-table_2.11-1.12.2.jar:/opt/flink/flink-1.12.2/lib/flink-table-blink_2.11-1.12.2.jar:/opt/flink/flink-1.12.2/lib/hive-exec-1.2.1.jar:/opt/flink/flink-1.12.2/lib/log4j-1.2-api-2.12.1.jar:/opt/flink/flink-1.12.2/lib/log4j-api-2.12.1.jar:/opt/flink/flink-1.12.2/lib/log4j-core-2.12.1.jar:/opt/flink/flink-1.12.2/lib/log4j-slf4j-impl-2.12.1.jar:/opt/flink/flink-1.12.2/lib/flink-dist_2.11-1.12.2.jar:/opt/hadoop/etc/hadoop:/opt/hadoop/hadoop-2.7.2/share/hadoop/common/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/common/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/yarn/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/*:/opt/hadoop/hadoop-release/contrib/capacity-scheduler/*.jar:/opt/hadoop/etc/hadoop:' \
org.apache.flink.client.cli.CliFrontend \
run /opt/flink/flink-release/examples/batch/WordCount.jar -input hdfs:///flink/input/words.txt


cli.CliFrontend.main(){
	// 加载多个 命令行, 默认3各: GenericCLI, FlinkYarnSessionCli, DefaultCLI;  
	final List<CustomCommandLine> customCommandLines=loadCustomCommandLines(configuration, configurationDirectory);{
		customCommandLines.add(new GenericCLI(configuration, configurationDirectory));
		customCommandLines.add(loadCustomCommandLine(flinkYarnSessionCLI,configuration,configurationDirectory,"y","yarn"));// "org.apache.flink.yarn.cli.FlinkYarnSessionCli"
		customCommandLines.add(new DefaultCLI());
	}
	final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
	int retCode =SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(args));{//CliFrontend.parseAndRun()
		String action = args[0];// run/applicaton-run 
		switch (action) {
			case ACTION_RUN: run(params);{//CliFrontend.run()
					final CommandLine commandLine = getCommandLine(commandOptions, args, true);
					activeCommandLine =validateAndGetActiveCommandLine(checkNotNull(commandLine));{
						for (CustomCommandLine cli : customCommandLines) {
							cli.isActive(commandLine){
								GenericCLI.isActive(){return configuration.getOptional(DeploymentOptions.TARGET).isPresent()
									|| commandLine.hasOption(executorOption.getOpt())
									|| commandLine.hasOption(targetOption.getOpt());}
							
								FlinkYarnSessionCli.isActive(){
									if (!super.isActive(commandLine)) {
										boolean isYarnMode = isYarnPropertiesFileMode(commandLine);{
											// 奇怪,只要 args=>commandLine 中不含有 "m" 参数,就是 canApplyYarn就==ture ? 默认都采用 yarn?
											boolean canApplyYarnProperties = !commandLine.hasOption(addressOption.getOpt()); // commandLine.hasOption("m")
											if (canApplyYarnProperties) {
												for (Option option : commandLine.getOptions()) {
													if (!isDetachedOption(option)) {
														canApplyYarnProperties = false;
														break;
													}
												}
											}
											return canApplyYarnProperties;
										}
										// 尝试/tmp/.yarn-properties-bigdata. ($java.io.tmpdir/.yarn-properties-$user/ 目录下查看 存放 ApplicationID 对应的session; 
										File yarnPropertiesLocation = getYarnPropertiesLocation(yarnPropertiesFileLocation);{
											if (yarnPropertiesFileLocation != null) {
												propertiesFileLocation = yarnPropertiesFileLocation;
											}else {
												propertiesFileLocation = System.getProperty("java.io.tmpdir");
											}
											return new File(propertiesFileLocation, YARN_PROPERTIES_FILE + currentUser);
										}
										yarnPropertiesFile.load(new FileInputStream(yarnPropertiesLocation));
										
										final String yarnApplicationIdString =yarnPropertiesFile.getProperty(YARN_APPLICATION_ID_KEY);// 读取applicationID
										yarnApplicationIdFromYarnProperties =ConverterUtils.toApplicationId(yarnApplicationIdString);
										return ( isYarnMode && yarnApplicationIdFromYarnProperties != null);
									}
									return true;
								};
							}
							if (cli.isActive(commandLine)) {
								return cli;
							}
						}
					}
					
					final List<URL> jobJars = getJobJarAndDependencies(programOptions);
					// 定义有效的核心配置,包括 execution.target, 
					Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);{
						commandLineConfiguration =activeCustomCommandLine.toConfiguration(commandLine);{//CustomCommandLine.toConfiguration()
							FlinkYarnSessionCli.toConfiguration(){}
							
							DefaultCLI.toConfiguration()
							
							KubernetesSessionCli.toConfiguration(){}
							
						}
						return new Configuration(commandLineConfiguration);
					}
					
					executeProgram(effectiveConfiguration, program);{
						ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);{
							// 把环境变量和各执行上下文 封装进 StreamContextEnvironment
							ContextEnvironment.setAsContext();
							StreamContextEnvironment.setAsContext();
							
							program.invokeInteractiveModeForExecution();{
								mainMethod = entryClass.getMethod("main", String[].class);
								// 执行 App的main()方法,如 WordCount.main()
								mainMethod.invoke(null, (Object) args);{
									// app重点 env.execute()触发各种作业的执行; 源码见 flink-core: ExecutionEnvironment.execute()
									ExecutionEnvironment.execute();
									StreamExecutionEnvironment.execute();
								}
							}
						}
					}
				}
			case ACTION_RUN_APPLICATION: 
				runApplication(params); 
			case ACTION_STOP:	
				stop(params);
				
		}
	}
}
CliFrontend.main() -> run()
	- validateAndGetActiveCommandLine(commandLine); 	依据是否有 /tmp/.yarn-properties-$user 文件创建Commaon Cli对象; 
	- effectiveConfiguration =getEffectiveConfiguration(commandLine) 解析和创建 执行环境Configuration;
	- executeProgram() -> mainMethod.invoke() 执行 WordCount等应用App的 main()方法
	- env.execute() 开始提交Job执行; 

// 1.2 App中 env.execute() 提交作业; 

env.execute()
	- executorServiceLoader.getExecutorFactory() 通过 加载和比较所有的 PipelineExecutorFactory.name()是否==  execution.target
	- PipelineExecutorFactory.getExecutor() 创建相应 PipelineExecutor实现类: YarnSession,YarnPerJob, KubernetesExecutor,LocalExecutor 等; 
	- PipelineExecutor.execute() 提交执行相应的job作业; 
	- YarnClusterClientFactory.getClusterDescriptor() 创建相应的yarn作业上下文; 

ExecutionEnvironment.execute(){
	// Streaming 的执行
	StreamExecutionEnvironment.execute(){
		return execute(getStreamGraph(jobName));{
			final JobClient jobClient = executeAsync(streamGraph);{
				// 这里定义了 执行模式和执行引擎; 主要通过 加载和比较所有的 PipelineExecutorFactory.name()是否==  execution.target
				final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);{//core.DefaultExecutorServiceLoader.
					final ServiceLoader<PipelineExecutorFactory> loader = ServiceLoader.load(PipelineExecutorFactory.class);
					while (loader.iterator().hasNext()) {
						// 根据 execution.target 配置项和 PipelineExecutorFactory.NAME 进行比较,看是否相等; 
						boolean isCompatible = factories.next().isCompatibleWith(configuration);{
							//Yarn的三种部署模式:  yarn-per-job, yarn-session, yarn-application
							YarnSessionClusterExecutorFactory.isCompatibleWith(){ // 看execution.target== yarn-session  
								YarnSessionClusterExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
							}
						}
						if (factory != null && isCompatible) compatibleFactories.add(factories.next());
					}
					if (compatibleFactories.size() > 1) { 
						throw new IllegalStateException("Multiple compatible client factories found for:\n" + configStr + ".");
					}
					if (compatibleFactories.isEmpty()) {
						throw new IllegalStateException("No ExecutorFactory found to execute the application.");
					}
					return compatibleFactories.get(0); // 只能定义1个 PipelineExecutorFactory, 否则报错; 
				}
				CompletableFuture<JobClient> jobClientFuture = executorFactory
					.getExecutor(configuration){//PipelineExecutorFactory.getExecutor() 
						YarnSessionClusterExecutorFactory.getExecutor(){
							return new YarnSessionClusterExecutor();
						}
					}
					.execute(streamGraph, configuration, userClassloader);{//PipelineExecutor.execute(pipeline,configuration,userCodeClassloader)
						// YarnCluster, KubeClient 等都是 这个
						AbstractSessionClusterExecutor.execute(){
							final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
							// 判断和创建想要的 Cluster链接端
							ClusterDescriptor clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);{
								final String configurationDirectory = configuration.get(DeploymentOptionsInternal.CONF_DIR);
								return getClusterDescriptor(configuration);{
									
									YarnClusterClientFactory.getClusterDescriptor(){
										YarnClient yarnClient = YarnClient.createYarnClient();
										yarnClient.init(yarnConfiguration);
										yarnClient.start();
										return new YarnClusterDescriptor(yarnConfiguration,yarnClient,YarnClientYarnClusterInformationRetriever.create(yarnClient));
									}
									
									kubernetesClusterClientFactory.getClusterDescriptor(){
										
									}
									
								}
							}
							
							ClusterClientProvider<ClusterID> clusterClientProvider =clusterDescriptor.retrieve(clusterID);
							return clusterClient
								.submitJob(jobGraph)
								.thenApplyAsync()
								.thenApplyAsync()
								.whenComplete((ignored1, ignored2) -> clusterClient.close());
						}
						
					}
					
				JobClient jobClient = jobClientFuture.get();
				jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
				return jobClient;
			}
			jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
			return jobExecutionResult;
		}
	}
	// 分本地执行环境和 远程执行环境
	LocalStreamEnvironment.execute(){
		return super.execute(streamGraph);
	}
	
	RemoteStreamEnvironment.execute(){
		
	}
	StreamContextEnvironment.execute(){};
	StreamPlanEnvironment.execute();{}// ? strema sql ?
	
	
}





// JobManager 进程的日志

2021-12-10 18:46:58,846 INFO  org.apache.flink.runtime.dispatcher.StandaloneDispatcher     [] - Received JobGraph submission 060198a5f431f6141fabf404d32de631 (Streaming WordCount).
2021-12-10 18:46:58,846 INFO  org.apache.flink.runtime.dispatcher.StandaloneDispatcher     [] - Submitting job 060198a5f431f6141fabf404d32de631 (Streaming WordCount).
2021-12-10 18:46:58,847 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcService             [] - Starting RPC endpoint for org.apache.flink.runtime.jobmaster.JobMaster at akka://flink/user/rpc/jobmanager_23 .
2021-12-10 18:46:58,847 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Initializing job Streaming WordCount (060198a5f431f6141fabf404d32de631).
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Using restart back off time strategy NoRestartBackoffTimeStrategy for Streaming WordCount (060198a5f431f6141fabf404d32de631).
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Running initialization on master for job Streaming WordCount (060198a5f431f6141fabf404d32de631).
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Successfully ran initialization on master in 0 ms.
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology [] - Built 1 pipelined regions in 0 ms
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - No checkpoint found during restore.
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Using failover strategy org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy@6ea59405 for Streaming WordCount (060198a5f431f6141fabf404d32de631).
2021-12-10 18:46:58,848 INFO  org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl      [] - JobManager runner for job Streaming WordCount (060198a5f431f6141fabf404d32de631) was granted leadership with session id 00000000-0000-0000-0000-000000000000 at akka.tcp://flink@bdnode102.hjq.com:36356/user/rpc/jobmanager_23.
2021-12-10 18:46:58,849 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Starting execution of job Streaming WordCount (060198a5f431f6141fabf404d32de631) under job master id 00000000000000000000000000000000.
2021-12-10 18:46:58,850 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Starting scheduling with scheduling strategy [org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy]
2021-12-10 18:46:58,850 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job Streaming WordCount (060198a5f431f6141fabf404d32de631) switched from state CREATED to RUNNING.
2021-12-10 18:46:58,850 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Custom File Source (1/1) (46415bb66a47a28606fac377898dc35f) switched from CREATED to SCHEDULED.
2021-12-10 18:46:58,850 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Split Reader: Custom File Source -> Flat Map (1/1) (a55105a1e173720fad78124f20335095) switched from CREATED to SCHEDULED.
2021-12-10 18:46:58,850 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Keyed Aggregation -> Sink: Print to Std. Out (1/1) (02b5354e3a76ace09c18b66a72043fab) switched from CREATED to SCHEDULED.
2021-12-10 18:46:58,854 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl     [] - Cannot serve slot request, no ResourceManager connected. Adding as pending request [SlotRequestId{a16fbb8f4e7d1da902bb29f59b432298}]
2021-12-10 18:46:58,855 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Connecting to ResourceManager akka.tcp://flink@bdnode102.hjq.com:36356/user/rpc/resourcemanager_*(00000000000000000000000000000000)
2021-12-10 18:46:58,855 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Resolved ResourceManager address, beginning registration
2021-12-10 18:46:58,856 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Registering job manager 00000000000000000000000000000000@akka.tcp://flink@bdnode102.hjq.com:36356/user/rpc/jobmanager_23 for job 060198a5f431f6141fabf404d32de631.
2021-12-10 18:46:58,856 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Registered job manager 00000000000000000000000000000000@akka.tcp://flink@bdnode102.hjq.com:36356/user/rpc/jobmanager_23 for job 060198a5f431f6141fabf404d32de631.
2021-12-10 18:46:58,859 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - JobManager successfully registered at ResourceManager, leader id: 00000000000000000000000000000000.
2021-12-10 18:46:58,859 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl     [] - Requesting new slot [SlotRequestId{a16fbb8f4e7d1da902bb29f59b432298}] and profile ResourceProfile{UNKNOWN} with allocation id 733410d152b6593fb100c4dff5cc6260 from resource manager.
2021-12-10 18:46:58,859 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Request slot with profile ResourceProfile{UNKNOWN} for job 060198a5f431f6141fabf404d32de631 with allocation id 733410d152b6593fb100c4dff5cc6260.
2021-12-10 18:46:58,859 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Requesting new worker with resource spec WorkerResourceSpec {cpuCores=1.0, taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=230.400mb (241591914 bytes)}, current pending count: 1.
2021-12-10 18:46:58,859 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Requesting new TaskExecutor container with resource TaskExecutorProcessSpec {cpuCores=1.0, frameworkHeapSize=128.000mb (134217728 bytes), frameworkOffHeapSize=128.000mb (134217728 bytes), taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemorySize=230.400mb (241591914 bytes), jvmMetaspaceSize=256.000mb (268435456 bytes), jvmOverheadSize=192.000mb (201326592 bytes)}, priority 1.
2021-12-10 18:47:03,110 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Received 1 containers.
2021-12-10 18:47:03,110 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Received 1 containers with priority 1, 1 pending container requests.
2021-12-10 18:47:03,111 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Removing container request Capability[<memory:1024, vCores:1>]Priority[1].
2021-12-10 18:47:03,111 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Accepted 1 requested containers, returned 0 excess containers, 0 pending container requests of resource <memory:1024, vCores:1>.
2021-12-10 18:47:03,111 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - TaskExecutor container_1638950543890_0036_01_000022(bdnode102.hjq.com:45836) will be started on bdnode102.hjq.com with TaskExecutorProcessSpec {cpuCores=1.0, frameworkHeapSize=128.000mb (134217728 bytes), frameworkOffHeapSize=128.000mb (134217728 bytes), taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemorySize=230.400mb (241591914 bytes), jvmMetaspaceSize=256.000mb (268435456 bytes), jvmOverheadSize=192.000mb (201326592 bytes)}.
2021-12-10 18:47:03,113 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Creating container launch context for TaskManagers
2021-12-10 18:47:03,120 INFO  org.apache.flink.yarn.YarnResourceManagerDriver              [] - Starting TaskManagers
2021-12-10 18:47:03,121 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Requested worker container_1638950543890_0036_01_000022(bdnode102.hjq.com:45836) with resource spec WorkerResourceSpec {cpuCores=1.0, taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=230.400mb (241591914 bytes)}.
2021-12-10 18:47:03,122 INFO  org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl [] - Processing Event EventType: START_CONTAINER for Container container_1638950543890_0036_01_000022
2021-12-10 18:47:03,122 INFO  org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy [] - Opening proxy : bdnode102.hjq.com:45836
2021-12-10 18:47:05,108 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Registering TaskManager with ResourceID container_1638950543890_0036_01_000022(bdnode102.hjq.com:45836) (akka.tcp://flink@bdnode102.hjq.com:36510/user/rpc/taskmanager_0) at ResourceManager
2021-12-10 18:47:05,119 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker container_1638950543890_0036_01_000022(bdnode102.hjq.com:45836) is registered.
2021-12-10 18:47:05,119 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker container_1638950543890_0036_01_000022(bdnode102.hjq.com:45836) with resource spec WorkerResourceSpec {cpuCores=1.0, taskHeapSize=25.600mb (26843542 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=230.400mb (241591914 bytes)} was requested in current attempt. Current pending count after registering: 0.

2021-12-10 18:47:05,158 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Custom File Source (1/1) (46415bb66a47a28606fac377898dc35f) switched from SCHEDULED to DEPLOYING.
2021-12-10 18:47:05,158 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying Source: Custom File Source (1/1) (attempt #0) with attempt id 46415bb66a47a28606fac377898dc35f to container_1638950543890_0036_01_000022 @ bdnode102.hjq.com (dataPort=39695) with allocation id 733410d152b6593fb100c4dff5cc6260
2021-12-10 18:47:05,159 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Split Reader: Custom File Source -> Flat Map (1/1) (a55105a1e173720fad78124f20335095) switched from SCHEDULED to DEPLOYING.
2021-12-10 18:47:05,159 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying Split Reader: Custom File Source -> Flat Map (1/1) (attempt #0) with attempt id a55105a1e173720fad78124f20335095 to container_1638950543890_0036_01_000022 @ bdnode102.hjq.com (dataPort=39695) with allocation id 733410d152b6593fb100c4dff5cc6260
2021-12-10 18:47:05,159 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Keyed Aggregation -> Sink: Print to Std. Out (1/1) (02b5354e3a76ace09c18b66a72043fab) switched from SCHEDULED to DEPLOYING.
2021-12-10 18:47:05,159 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Deploying Keyed Aggregation -> Sink: Print to Std. Out (1/1) (attempt #0) with attempt id 02b5354e3a76ace09c18b66a72043fab to container_1638950543890_0036_01_000022 @ bdnode102.hjq.com (dataPort=39695) with allocation id 733410d152b6593fb100c4dff5cc6260

2021-12-10 18:47:05,256 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Split Reader: Custom File Source -> Flat Map (1/1) (a55105a1e173720fad78124f20335095) switched from DEPLOYING to RUNNING.
2021-12-10 18:47:05,257 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Custom File Source (1/1) (46415bb66a47a28606fac377898dc35f) switched from DEPLOYING to RUNNING.
2021-12-10 18:47:05,257 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Keyed Aggregation -> Sink: Print to Std. Out (1/1) (02b5354e3a76ace09c18b66a72043fab) switched from DEPLOYING to RUNNING.

2021-12-10 18:47:05,879 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: Custom File Source (1/1) (46415bb66a47a28606fac377898dc35f) switched from RUNNING to FINISHED.
2021-12-10 18:47:05,927 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Split Reader: Custom File Source -> Flat Map (1/1) (a55105a1e173720fad78124f20335095) switched from RUNNING to FINISHED.
2021-12-10 18:47:05,929 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Keyed Aggregation -> Sink: Print to Std. Out (1/1) (02b5354e3a76ace09c18b66a72043fab) switched from RUNNING to FINISHED.
2021-12-10 18:47:05,929 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job Streaming WordCount (060198a5f431f6141fabf404d32de631) switched from state RUNNING to FINISHED.
2021-12-10 18:47:05,929 INFO  org.apache.flink.runtime.checkpoint.CheckpointCoordinator    [] - Stopping checkpoint coordinator for job 060198a5f431f6141fabf404d32de631.
2021-12-10 18:47:05,929 INFO  org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore [] - Shutting down
2021-12-10 18:47:05,929 INFO  org.apache.flink.runtime.dispatcher.StandaloneDispatcher     [] - Job 060198a5f431f6141fabf404d32de631 reached globally terminal state FINISHED.
2021-12-10 18:47:05,930 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Stopping the JobMaster for job Streaming WordCount(060198a5f431f6141fabf404d32de631).
2021-12-10 18:47:05,930 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl     [] - Suspending SlotPool.
2021-12-10 18:47:05,930 INFO  org.apache.flink.runtime.jobmaster.JobMaster                 [] - Close ResourceManager connection 9b0423c3ad576ea33a187cca3c2a9676: Stopping JobMaster for job Streaming WordCount(060198a5f431f6141fabf404d32de631)..
2021-12-10 18:47:05,930 INFO  org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl     [] - Stopping SlotPool.
2021-12-10 18:47:05,930 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Disconnect job manager 00000000000000000000000000000000@akka.tcp://flink@bdnode102.hjq.com:36356/user/rpc/jobmanager_23 for job 060198a5f431f6141fabf404d32de631 from the resource manager.



// 2.1 FTM: YarnSessionClusterEntrypoint进程:  先申请Slot,没有TaskExecutor的话就申请创建 TM container; 

ResourceManager.requestSlot(){
	JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);
	slotManager.registerSlotRequest(slotRequest);{//SlotManagerImpl
		internalRequestSlot(pendingSlotRequest);{
			allocateSlot(taskManagerSlot, pendingSlotRequest);
			fulfillPendingSlotRequestWithPendingTaskManagerSlot();{//SlotManagerImpl.
				pendingTaskManagerSlotOptional = allocateResource(resourceProfile);{
					final int numPendingSlots = getNumberPendingTaskManagerSlots();
					if (!resourceActions.allocateResource(defaultWorkerResourceSpec)) { {//ResourceManager$ResourceActionsImpl.allocateResource()
						return startNewWorker(workerResourceSpec);{//ActiveResourceManager.
							requestNewWorker(workerResourceSpec);{//ActiveResourceManager.
								taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);
								requestResourceFuture =resourceManagerDriver.requestResource(taskExecutorProcessSpec);{//YarnResourceManagerDriver.
									final CompletableFuture<YarnWorkerNode> requestResourceFuture = new CompletableFuture<>();
									resourceManagerClient.addContainerRequest(getContainerRequest(resource, priority));
									return requestResourceFuture;
								}
							}
						}
					}
						return Optional.empty();
					}
				}
			}
			
		}
	}
	return CompletableFuture.completedFuture(Acknowledge.get());
}


// 2.2 FTM: YarnSessionClusterEntrypoint进程: 当TaskExecutor容器资源申请成功后, 就提交Java命令启动TM容器;  

YarnResourceManagerDriver.onContainersAllocated(){
	checkInitialized();
	for (Map.Entry<Priority, List<Container>> entry :groupContainerByPriority(containers).entrySet()) {
        onContainersOfPriorityAllocated(entry.getKey(), entry.getValue());{
			final Iterator<AMRMClient.ContainerRequest> pendingContainerRequestIterator =getPendingRequestsAndCheckConsistency(priority, resource, pendingRequestResourceFutures.size()).iterator();
			while (containerIterator.hasNext() && pendingContainerRequestIterator.hasNext()) {
				startTaskExecutorInContainerAsync(container, taskExecutorProcessSpec, resourceId, requestResourceFuture);{
					//异步: 创建 TaskExecutor的上下文; 
					createTaskExecutorLaunchContext();{//YarnResourceManagerDriver.createTaskExecutorLaunchContext
						final String taskManagerDynamicProperties =BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);
						taskExecutorLaunchContext =Utils.createTaskExecutorContext();{
							
							String launchCommand =BootstrapTools.getTaskManagerShellCommand();
							// $JAVA_HOME/bin/java -Xmx161061270 -Xms161061270 -XX:MaxDirectMemorySize=201326592 -XX:MaxMetaspaceSize=268435456 "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=42050" -Dlog.file=<LOG_DIR>/taskmanager.log -Dlog4j.configuration=file:./log4j.properties -Dlog4j.configurationFile=file:./log4j.properties org.apache.flink.yarn.YarnTaskExecutorRunner -D taskmanager.memory.framework.off-heap.size=134217728b -D taskmanager.memory.network.max=67108864b -D taskmanager.memory.network.min=67108864b -D taskmanager.memory.framework.heap.size=134217728b -D taskmanager.memory.managed.size=241591914b -D taskmanager.cpu.cores=1.0 -D taskmanager.memory.task.heap.size=26843542b -D taskmanager.memory.task.off-heap.size=0b -D taskmanager.memory.jvm-metaspace.size=268435456b -D taskmanager.memory.jvm-overhead.max=201326592b -D taskmanager.memory.jvm-overhead.min=201326592b --configDir . -Djobmanager.rpc.address='bdnode102.hjq.com' -Djobmanager.memory.jvm-overhead.min='201326592b' -Dtaskmanager.resource-id='container_1638950543890_0037_01_000004' -Dweb.port='0' -Djobmanager.memory.off-heap.size='134217728b' -Dweb.tmpdir='/tmp/flink-web-f1186450-d2b6-4280-b232-c60e0ba9cc06' -Dinternal.taskmanager.resource-id.metadata='bdnode102.hjq.com:45836' -Djobmanager.rpc.port='45108' -Drest.address='bdnode102.hjq.com' -Djobmanager.memory.jvm-metaspace.size='268435456b' -Djobmanager.memory.heap.size='469762048b' -Djobmanager.memory.jvm-overhead.max='201326592b' 1> <LOG_DIR>/taskmanager.out 2> <LOG_DIR>/taskmanager.err
							
							ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
							ctx.setCommands(Collections.singletonList(launchCommand));
							
							//设置 $FLINK_CLASSPATH & CLASSPATH: 
							String classPathString =checkNotNull(configuration.getFlinkClasspath(),"Environment variable %s not set",YarnConfigKeys.ENV_FLINK_CLASSPATH);{
								YarnResourceManagerDriverConfiguration.getFlinkClasspath(){
									// :lib/flink-connector-hive_2.11-1.12.1.jar:lib/flink-csv-1.12.2.jar:lib/flink-json-1.12.2.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-sql-connector-hive-1.2.2_2.11-1.12.2.jar:lib/flink-table-blink_2.11-1.12.2.jar:lib/flink-table_2.11-1.12.2.jar:lib/hive-exec-1.2.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.2.jar:flink-conf.yaml:
									return flinkClasspath;// 这里的 flinkClasspath 应该就是 JobMgr的AmConnerContext里的 flinkClassPath吧
								}
							}
							containerEnv.put(ENV_FLINK_CLASSPATH, classPathString);
							setupYarnClassPath(yarnConfig, containerEnv);{
								addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
								String[] applicationClassPathEntries =conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);{
									String valueString = get(name);// 获取yarn.application.classpath 变量
									if (valueString == null) {// 采用Yarn默认CP: YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH, 包括7个;
										return defaultValue;// 默认YarnCP包括4类: CONF_DIR和 share下的common,hdfs,yar3个模块的目录;
									} else {
										return StringUtils.getStrings(valueString);
									}
								}
								for (String c : applicationClassPathEntries) {
									addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim());
								}
							}
							
						}
					}
					nodeManagerClient.startContainerAsync(container, context);
				}
				removeContainerRequest(pendingRequest);
			}
		}
    }
	if (getNumRequestedNotAllocatedWorkers() <= 0) {
		resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
	}
}


%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%

$JAVA_HOME/bin/java \
-Xmx161061270 -Xms161061270 -XX:MaxDirectMemorySize=201326592 -XX:MaxMetaspaceSize=268435456 \
"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=42050" \
-Dlog.file=<LOG_DIR>/taskmanager.log -Dlog4j.configuration=file:./log4j.properties -Dlog4j.configurationFile=file:./log4j.properties \
org.apache.flink.yarn.YarnTaskExecutorRunner \
-D taskmanager.memory.framework.off-heap.size=134217728b -D taskmanager.memory.network.max=67108864b -D taskmanager.memory.network.min=67108864b -D taskmanager.memory.framework.heap.size=134217728b -D taskmanager.memory.managed.size=241591914b -D taskmanager.cpu.cores=1.0 -D taskmanager.memory.task.heap.size=26843542b -D taskmanager.memory.task.off-heap.size=0b -D taskmanager.memory.jvm-metaspace.size=268435456b -D taskmanager.memory.jvm-overhead.max=201326592b -D taskmanager.memory.jvm-overhead.min=201326592b --configDir . -Djobmanager.rpc.address='bdnode102.hjq.com' -Djobmanager.memory.jvm-overhead.min='201326592b' -Dtaskmanager.resource-id='container_1638950543890_0037_01_000004' -Dweb.port='0' -Djobmanager.memory.off-heap.size='134217728b' -Dweb.tmpdir='/tmp/flink-web-f1186450-d2b6-4280-b232-c60e0ba9cc06' -Dinternal.taskmanager.resource-id.metadata='bdnode102.hjq.com:45836' -Djobmanager.rpc.port='45108' -Drest.address='bdnode102.hjq.com' -Djobmanager.memory.jvm-metaspace.size='268435456b' -Djobmanager.memory.heap.size='469762048b' -Djobmanager.memory.jvm-overhead.max='201326592b' \
1> <LOG_DIR>/taskmanager.out 2> <LOG_DIR>/taskmanager.err



//2.3.1 FTM: YarnSessionClusterEntrypoint进程: : "akka通信" 线程: 接收到 client端的 Job提交,并运行; 

AkkaRpcActor.handleRpcInvocation(){
	Method rpcMethod = lookupRpcMethod(methodName, parameterTypes);
	result = rpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs());{// Dispatcher.submitJob()
		StandaloneDispatcher -> Dispatcher.submitJob(){
			return internalSubmitJob(jobGraph);{
				// 异步执行
				waitForTerminatingJob( ()-> persistAndRunJob() );{//Dispatcher.persistAndRunJob
					jobGraphWriter.putJobGraph(jobGraph);
					runJob(jobGraph, ExecutionType.SUBMISSION);{
						jobManagerRunnerFuture =createJobManagerRunner(jobGraph, initializationTimestamp);
						dispatcherJob =DispatcherJob.createFor();
						runningJobs.put(jobGraph.getJobID(), dispatcherJob);
						// 异步清理 Job容器;
						handleDispatcherJobResult(jobId, dispatcherJobResult, executionType);
						removeJob(jobId, cleanupJobState)
						
						registerDispatcherJobTerminationFuture(jobId, jobTerminationFuture);
					}
				}
				cleanUpJobData(jobGraph.getJobID(), true);
			}
		}
	}
}



// 2.3.2 FTM: YarnSessionClusterEntrypoint进程:  "flink-akka.actor.default-xx" 线程: 接收到 client端的 Job提交,并运行; 

ResourceManagerConnection.onRegistrationSuccess(){
	if (this == resourceManagerConnection) establishResourceManagerConnection(success);{//JobMaster.
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();
		if (resourceManagerConnection != null
                && Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {
			final ResourceManagerGateway resourceManagerGateway =resourceManagerConnection.getTargetGateway();
			establishedResourceManagerConnection =new EstablishedResourceManagerConnection();
			
		
		}
	}
}


//  TaskManager模块: YarnSessionClusterEntrypoint进程: "akka通信" 线程: 
NMClientAsyncImpl.ContainerEventProcessor.run(){
	ContainerId containerId = event.getContainerId();
	if (event.getType() == ContainerEventType.QUERY_CONTAINER) {
		ContainerStatus containerStatus = client.getContainerStatus(containerId, event.getNodeId());
		callbackHandler.onContainerStatusReceived(containerId, containerStatus);
	}else{
		containers.get(containerId).handle(event);
		if (isCompletelyDone(container)) {
		   containers.remove(containerId);
		}
	}
	
}




