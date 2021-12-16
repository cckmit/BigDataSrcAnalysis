

// 1. flink run 识别 execution.target 和相关运行参数, 并创建相应的 Execution对象; 
CliFrontend.main() -> run()
	- validateAndGetActiveCommandLine(commandLine); 	依据是否有 /tmp/.yarn-properties-$user 文件创建Commaon Cli对象; 
	- effectiveConfiguration =getEffectiveConfiguration(commandLine) 解析和创建 执行环境Configuration;
	- executeProgram() -> mainMethod.invoke() 执行 WordCount等应用App的 main()方法
	- env.execute() 开始提交Job执行; 

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


// 2. 触发作业执行:  env.execute() : 创建相应Factory和Executor,生产上下文, submittJob()提交执行; 
env.execute()
	- executorServiceLoader.getExecutorFactory() 通过 加载和比较所有的 PipelineExecutorFactory.name()是否==  execution.target
	- PipelineExecutorFactory.getExecutor() 创建相应 PipelineExecutor实现类: YarnSession,YarnPerJob, KubernetesExecutor,LocalExecutor 等; 
	- PipelineExecutor.execute() 提交执行相应的job作业; 


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
							RemoteExecutorFactory.isCompatibleWith(){
								return RemoteExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET)); //execution.target==remote
							}
							LocalExecutorFactory.isCompatibleWith(){ // 看execution.target== local
								return LocalExecutor.NAME.equalsIgnoreCase(configuration.get(DeploymentOptions.TARGET));
							}
							KubernetesSessionClusterExecutorFactory.isCompatibleWith(){//看execution.target是否== kubernetes-session
								return configuration.get(DeploymentOptions.TARGET).equalsIgnoreCase(KubernetesSessionClusterExecutor.NAME);
							}
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
						LocalExecutorFactory.getExecutor()
						RemoteExecutorFactory.getExecutor()
						EmbeddedExecutorFactory.getExecutor()
						WebSubmissionExecutorFactory.getExecutor()
						
						KubernetesSessionClusterExecutorFactory.getExecutor(){}
						YarnJobClusterExecutorFactory.getExecutor(){}
						YarnSessionClusterExecutorFactory.getExecutor(){
							return new YarnSessionClusterExecutor();
						}
						
					}
					.execute(streamGraph, configuration, userClassloader);{//PipelineExecutor.execute(pipeline,configuration,userCodeClassloader)
						LocalExecutor.execute()
						AbstractJobClusterExecutor.execute();
						
						// YarnCluster, KubeClient 等都是 这个
						AbstractSessionClusterExecutor.execute(){
							final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
							// 判断和创建想要的 Cluster链接端
							ClusterDescriptor clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);{//AbstractSessionClusterExecutor.
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
								.submitJob(jobGraph){// RestClusterClient.submitJob()
									
									Future<JobSubmitResponseBody> submissionFuture = requestFuture.thenCompose(sendRetriableRequest());
									// 创建request: JobSubmitRequestBody
									Tuple2<JobSubmitRequestBody, Collection<FileUpload>> requestFuture= jobGraphFileFuture.thenApply(){
										final JobSubmitRequestBody requestBody =new JobSubmitRequestBody(jobGraphFile.getFileName().toString(),jarFileNames,artifactFileNames);
										return Tuple2.of(requestBody, Collections.unmodifiableCollection(filesToUpload));
									}
									// 发送JobSumbit请求: sendRequest(request)
									submissionFuture= sendRetriableRequest(request);{
										getWebMonitorBaseUrl()
										return restClient.sendRequest(messageParameters,request, filesToUpload);{//RestClient.
											return sendRequest();{//RestClient.sendRequest()
												String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, messageParameters);// = /v1/jobs
												objectMapper.writeValue(new StringWriter(), request);
												Request httpRequest =createRequest(targetUrl,payload);
												return submitRequest(targetAddress, targetPort, httpRequest, responseType);{//RestClient.
													connectFuture = bootstrap.connect(targetAddress, targetPort);
													httpRequest.writeTo(channel);
													future = handler.getJsonFuture();
													parseResponse(rawResponse, responseType);
												}
											}
										}
									}
									
									return submissionFuture.thenApply()
										.exceptionally();
								}
								.thenApplyAsync()
								.thenApplyAsync()
								.whenComplete((ignored1, ignored2) -> clusterClient.close());
						}
						RemoteExecutor[extends AbstractSessionClusterExecutor].execute();
						
						EmbeddedExecutor.execute();
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



submitJob:392, RestClusterClient (org.apache.flink.client.program.rest)
execute:78, AbstractSessionClusterExecutor (org.apache.flink.client.deployment.executors)
executeAsync:1905, StreamExecutionEnvironment (org.apache.flink.streaming.api.environment)
executeAsync:135, StreamContextEnvironment (org.apache.flink.client.program)
execute:76, StreamContextEnvironment (org.apache.flink.client.program)
execute:1782, StreamExecutionEnvironment (org.apache.flink.streaming.api.environment)
main:97, WordCount (org.apache.flink.streaming.examples.wordcount)
invoke0:-1, NativeMethodAccessorImpl (sun.reflect)
invoke:62, NativeMethodAccessorImpl (sun.reflect)
invoke:43, DelegatingMethodAccessorImpl (sun.reflect)
invoke:498, Method (java.lang.reflect)
callMainMethod:349, PackagedProgram (org.apache.flink.client.program)
invokeInteractiveModeForExecution:219, PackagedProgram (org.apache.flink.client.program)
executeProgram:114, ClientUtils (org.apache.flink.client)
executeProgram:812, CliFrontend (org.apache.flink.client.cli)
run:246, CliFrontend (org.apache.flink.client.cli)
parseAndRun:1054, CliFrontend (org.apache.flink.client.cli)
lambda$main$10:1132, CliFrontend (org.apache.flink.client.cli)
call:-1, 1862347028 (org.apache.flink.client.cli.CliFrontend$$Lambda$76)
run:-1, 419280591 (org.apache.flink.runtime.security.contexts.HadoopSecurityContext$$Lambda$77)
doPrivileged:-1, AccessController (java.security)
doAs:422, Subject (javax.security.auth)
doAs:1657, UserGroupInformation (org.apache.hadoop.security)
runSecured:41, HadoopSecurityContext (org.apache.flink.runtime.security.contexts)
main:1132, CliFrontend (org.apache.flink.client.cli)
