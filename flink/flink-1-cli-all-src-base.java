
/** 1 flinkrun: CliFrontend 启动
*/

// 1.1  flink run 识别 execution.target 和相关运行参数, 并创建相应的 Execution对象; 
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



// 1.2 Yarn Cli 
// 1. FlinkYarnSessionCli 进程: "main"线程 构建并向yarnRM 发送 amContainer 

// Cli的main线程主要是创建YarnClient, 并最终构建1个 appContext: ApplicationSubmissionContext ,包含Java命令和相关jar/env变量等; 
// YarnClusterDescriptor.startAppMaster() 中创建 appContext,并调 yarnClient.submitApplication(appContext) 发给Yarn;
// %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%
FlinkYarnSessionCli.run()
	- yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration); 链接Yarn RM,并建立RMClient;
	- yarnClusterDescriptor.deploySessionCluster() 
	- yarnApplication = yarnClient.createApplication(); 创建Application
	- startAppMaster(); 
		* appContext = yarnApplication.getApplicationSubmissionContext(); 创建应用执行上下文 appCtx;
		* amContainer =setupApplicationMasterContainer(); 中创建 %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 格式的命令;
		* yarnClient.submitApplication(appContext); 将 appCtx发给RM/NM 进行远程Container/Java进程启动; 
	

FlinkYarnSessionCli.main(){
	final String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
	final FlinkYarnSessionCli cli = new FlinkYarnSessionCli(flinkConfiguration,configurationDirectory,"");
	retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));{//FlinkYarnSessionCli.run(){
		final CommandLine cmd = parseCommandLineOptions(args, true);
		// 主要耗时1:  链接Yarn ResurceManager
		final YarnClusterDescriptor yarnClusterDescriptor =yarnClusterClientFactory.createClusterDescriptor(effectiveConfiguration);{
			return getClusterDescriptor(configuration);{
				final YarnClient yarnClient = YarnClient.createYarnClient();
				yarnClient.init(yarnConfiguration);{
					super.serviceStart();
				}
				yarnClient.start();{//AbstractService.start()
					serviceStart();{//YarnClientImpl.serviceStart()
						rmClient = ClientRMProxy.createRMProxy(getConfig(),ApplicationClientProtocol.class);{
							return createRMProxy(configuration, protocol, INSTANCE);{//RMProxy.createRMProxy()
								RetryPolicy retryPolicy = createRetryPolicy(conf);
								if (HAUtil.isHAEnabled(conf)) {
									RMFailoverProxyProvider<T> provider =instance.createRMFailoverProxyProvider(conf, protocol);
									return (T) RetryProxy.create(protocol, provider, retryPolicy);
								}else{// 非HA, 这里; 
									InetSocketAddress rmAddress = instance.getRMAddress(conf, protocol);
									LOG.info("Connecting to ResourceManager at " + rmAddress);
									T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
									return (T) RetryProxy.create(protocol, proxy, retryPolicy);
								}
							}
						}
						if (historyServiceEnabled) {
							historyClient.start();
						}
					}
				}
				return new YarnClusterDescriptor(configuration,yarnConfiguration,yarnClient);
			}
		}
		if (cmd.hasOption(applicationId.getOpt())) {
			clusterClientProvider = yarnClusterDescriptor.retrieve(yarnApplicationId);
		}else{
			final ClusterSpecification clusterSpecification = yarnClusterClientFactory.getClusterSpecification(effectiveConfiguration);
			// 主要耗时2: 发布应用; 
			clusterClientProvider = yarnClusterDescriptor.deploySessionCluster(clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
				return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint());// 源码详解 yarn: resourcenanger-src 
			}
		}
	}
}




/**	1.2.1 flinkCli-yarn-SubmitAM: deploySessionCluster()-> startAppMaster()-> yarnClient.submitApplication(appContext) 

YarnClusterDescriptor.startAppMaster(): 构建并启动 am: ApplicationMaster
	//1. 构建AM命令: YarnClusterDescriptor.setupApplicationMasterContainer(): 按照%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 构建AM命令;
	//2. 拼接CLASSPATH: YarnClusterDescriptor.startAppMaster()拼接$CLASSPATH,依次采用: $FLINK_CLASSPATH() + yarn.application.classpath ,其构成如下
			userClassPath(jobGraph.getUserJars(), pipeline.jars, usrlib) 
			* 	systemClassPaths = yarn.ship-files配置 + $FLINK_LIB_DIR变量下jars + logConfigFile;
			*	localResourceDescFlinkJar.getResourceKey() + jobGraphFilename + "flink-conf.yaml"
			yarn.application.classpath 默认采用: $HADOOP_CONF_DIR和 share下的common,hdfs,yar3个模块的目录;
	//3. 调用yarn api: YarnClientImpl.submitApplication() 与Yarn RM通信并提交启动 ApplicationMaster: YarnSessionClusterEntrypoint;
*/

YarnClusterDescriptor.deploySessionCluster(ClusterSpecification clusterSpecification);{//YarnClusterDescriptor.deploySessionCluster()
	return deployInternal(clusterSpecification, getYarnSessionClusterEntrypoint());{
		isReadyForDeployment(clusterSpecification);
		checkYarnQueues(yarnClient);
		final YarnClientApplication yarnApplication = yarnClient.createApplication();
		final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();
		freeClusterMem = getCurrentFreeClusterResources(yarnClient);
		final int yarnMinAllocationMB = yarnConfiguration.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
		ApplicationReport report =startAppMaster();{//YarnClusterDescriptor.startAppMaster()
			final FileSystem fs = FileSystem.get(yarnConfiguration);
			ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
			final List<Path> providedLibDirs =Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);
			final YarnApplicationFileUploader fileUploader =YarnApplicationFileUploader.from();
			
			userJarFiles.addAll(jobGraph.getUserJars().stream().map(f -> f.toUri()).map(Path::new).collect(Collectors.toSet()));
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
			// 设置AM(ApplicationMaster)的资源: amContainer 主要包括 env,javaCammand, localResource本地jar包资源;
			processSpec =JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap();{
				CommonProcessMemorySpec processMemory = PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config);{
					if (options.getRequiredFineGrainedOptions().stream().allMatch(config::contains)) {
						
					}else if (config.contains(options.getTotalFlinkMemoryOption())) {//jobmanager.memory.flink.size
						return deriveProcessSpecWithTotalFlinkMemory(config);
					}else if (config.contains(options.getTotalProcessMemoryOption())) {// jobmanager.memory.process.size
						return deriveProcessSpecWithTotalProcessMemory(config);{
							MemorySize totalProcessMemorySize =getMemorySizeFromConfig(config, options.getTotalProcessMemoryOption());
							// Metaspace默认 256Mb, jobmanager.memory.jvm-metaspace.size
							JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =deriveJvmMetaspaceAndOverheadWithTotalProcessMemory(config, totalProcessMemorySize);
							// 约等于 total - metaspace - overhead = 1024 - 256 -196 = 576Mb
							MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());
							// 又把 576 进一步分层heap /offHeap, 堆内 448, 堆外 128Mb; 
							FM flinkInternalMemory =flinkMemoryUtils.deriveFromTotalFlinkMemory(config, totalFlinkMemorySize);
							return new CommonProcessMemorySpec<>(flinkInternalMemory, jvmMetaspaceAndOverhead);
						}
					}
					return failBecauseRequiredOptionsNotConfigured();
				}
				return new JobManagerProcessSpec(processMemory.getFlinkMemory(), processMemory.getJvmMetaspaceAndOverhead());
			}
			flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
			ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
			
			// 拼接 %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects% 命令;
			JobManagerProcessSpec processSpec =JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
			final ContainerLaunchContext amContainer =setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec);{//YarnClusterDescriptor.
				String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
				javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
				startCommandValues.put("java", "$JAVA_HOME/bin/java");
				startCommandValues.put("jvmmem", jvmHeapMem);{
					jvmArgStr.append("-Xmx").append(processSpec.getJvmHeapMemorySize().getBytes());
					jvmArgStr.append(" -Xms").append(processSpec.getJvmHeapMemorySize().getBytes());
					if (enableDirectMemoryLimit) {//jobmanager.memory.enable-jvm-direct-memory-limit
						jvmArgStr.append(" -XX:MaxDirectMemorySize=").append(processSpec.getJvmDirectMemorySize().getBytes());
					}
					jvmArgStr.append(" -XX:MaxMetaspaceSize=").append(processSpec.getJvmMetaspaceSize().getBytes());
				}
				startCommandValues.put("jvmopts", javaOpts);
				startCommandValues.put("class", yarnClusterEntrypoint);
				startCommandValues.put("args", dynamicParameterListStr);
				//使用yarn.container-start-command-template,或者采用默认 %java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%
				 String commandTemplate =flinkConfiguration.getString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
				String amCommand =BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
			}
			amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
			// 设置env: _FLINK_CLASSPATH 环境变量
			userJarFiles.addAll(jobGraph.getUserJars().stream().map(f -> f.toUri())); //添加 jobGraph.getUserJars() 中的jars
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet())); // 添加 pipeline.jars中的jars;
			final List<String> userClassPaths =fileUploader.registerMultipleLocalResources(
				userJarFiles, // =  jobGraph.getUserJars() + pipeline.jars 
				userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR, LocalResourceType.FILE); // 添加usrlib/目录下
			
			//FLINK_CLASSPATH 1: include-user-jar=first时,把 jobGraph.getUserJars() &pipeline.jars &usrlib 目录下jars 加到前面;
			if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST) classPathBuilder.append(userClassPath).append(File.pathSeparator);//yarn.per-job-cluster.include-user-jar
			// FLINK_CLASSPATH 2: systemClassPaths= shipFiles(yarn.ship-files配置) + logConfigFile +systemShipFiles(Sys.FLINK_LIB_DIR变量) , 包括 localResources中上传的13个flink的lib下jar包;
			addLibFoldersToShipFiles(systemShipFiles);{
				String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);//从系统变量读取FLINK_LIB_DIR 的值;
				effectiveShipFiles.add(new File(libDir));
			}
			for (String classPath : systemClassPaths) classPathBuilder.append(classPath).append(File.pathSeparator);
			// FLINK_CLASSPATH 3: 
			classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
			classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
			classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
			//FLINK_CLASSPATH 6: include-user-jar=last时, 把userClassPath 的jars加到CP后面; 
			if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) classPathBuilder.append(userClassPath).append(File.pathSeparator);
			
			appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
			appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES,fileUploader.getApplicationDir().toUri().toString());
			// 设置 CLASSPATH的参数
			Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
				// 1. 先把 _FLINK_CLASSPATH中 lib中13个flink相关jar包加到CP
				addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
				// 2. yarn.application.classpath + 
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
			amContainer.setEnvironment(appMasterEnv);
			appContext.setAMContainerSpec(amContainer);
			
			// 设置CPU/Memory资源大小; 
			capability.setMemory(clusterSpecification.getMasterMemoryMB());
			capability.setVirtualCores(flinkConfiguration.getInteger(YarnConfigOptions.APP_MASTER_VCORES));
			appContext.setResource(capability);
			
			setApplicationTags(appContext);
			yarnClient.submitApplication(appContext);{//YarnClientImpl.submitApplication()
				SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
				request.setApplicationSubmissionContext(appContext);
				rmClient.submitApplication(request);{
					// yarn 的resourceManager的 resourcemanager.ClientRMService 进行处理
					
				}
				while (true) {// 非waitingStates 就跳出返回 applicationId
					if (!waitingStates.contains(state)) {
						LOG.info("Submitted application " + applicationId);
						break;
					}
				}
				return applicationId;
			}
			
			LOG.info("Waiting for the cluster to be allocated");
			while (true) {
				report = yarnClient.getApplicationReport(appId);
				YarnApplicationState appState = report.getYarnApplicationState();
				switch (appState) {
					case FAILED: case KILLED:
						throw new YarnDeploymentException();
					case RUNNING:case FINISHED:
						break loop;
					default:
				}
				Thread.sleep(250);
			}
			
		}
		return () -> {return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());};
		
	}
}


// flink yarn CLASSPATH 定义
{
	
		
	/* 定义 
		jobGraph.userJars:		pipeline.jars:			来自 args[0] or -j or --jarfile 参数指定
		jobGraph.classpaths:	pipeline.classpaths:	来自 -C or --classpath 参数指定
	*/
	CliFrontend.run(){
		final List<URL> jobJars = getJobJarAndDependencies(programOptions);{
			// entryPointClass 就是 -c/--class参数指定值; 
			String entryPointClass = programOptions.getEntryPointClassName(); //未指定未空;
			// jarFilePath 即App Jar包, 默认 args[0] 或 -j 或 --jarfile 指定;
			String jarFilePath = programOptions.getJarFilePath();
			File jarFile = jarFilePath != null ? getJarFile(jarFilePath) : null;
			return PackagedProgram.getJobJarAndDependencies(jarFile, entryPointClass);{
				URL jarFileUrl = loadJarFile(jarFile);
			}
		}
		Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);{
			ExecutionConfigAccessor executionParameters = ExecutionConfigAccessor.fromProgramOptions(checkNotNull(programOptions), checkNotNull(jobJars));{
					final Configuration configuration = new Configuration();
					// 向config中写入 execution.attached,pipeline.classpaths等4个变量;
					options.applyToConfiguration(configuration);{
						// 来自 --classpath or -C 参数的指定;
						ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.CLASSPATHS, getClasspaths(), URL::toString);
					}
					ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, jobJars, Object::toString);
					return new ExecutionConfigAccessor(configuration);
				}
		}
	}
	
	AbstractJobClusterExecutor.execute(){
		final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);{
			ExecutionConfigAccessor executionConfigAccessor =ExecutionConfigAccessor.fromConfiguration(configuration);{
				return new ExecutionConfigAccessor(checkNotNull(configuration));
			}
			JobGraph jobGraph =FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());
			// 从config中读取 pipeline.jars 作为变量;
			List<URL> jarFilesToAttach = executionConfigAccessor.getJars();{
				return ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URL::new);//取pipeline.jars变量;
			}
			jobGraph.addJars(jarFilesToAttach);{
				for (URL jar : jarFilesToAttach) {
					addJar(new Path(jar.toURI()));{//往 jobGraph.userJars:List<Path> 中添加 pipeline.jars变量的有效URI
						if (!userJars.contains(jar)) {
							userJars.add(jar);
						}
					}
				}
			}
			//读取pipeline.classpaths 的变量值,并赋值 jobGraph.classpaths: List<URL> 
			jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
		}
		clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);
	}
	
	/* 1. 定义 dist,ship,archives资源路径: 
		flinkJarPath:	从 yarn.flink-dist-jar配置,或者将 this.codesource本包作为 dist包路径; 	未配置默认: /opt/flink/flink-1.12.2/lib/flink-dist_2.11-1.12.2.jar
		shipFiles:		从 yarn.ship-files读取			未配默认为空;
		shipArchives:	从 yarn.ship-archives 读取 		未配默认为空;
	*/
	AbstractJobClusterExecutor.execute().createClusterDescriptor().getClusterDescriptor(){
		final YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration); yarnClient.start();
		
		new YarnClusterDescriptor();{
			this.userJarInclusion = getUserJarInclusionMode(flinkConfiguration);
			//1 从 yarn.flink-dist-jar配置,或者将 this.codesource本包作为 dist包路径,并赋值 YarnClusterDescriptor.flinkJarPath 变量;
			getLocalFlinkDistPath(flinkConfiguration).ifPresent(this::setLocalJarPath);{
				String localJarPath = configuration.getString(YarnConfigOptions.FLINK_DIST_JAR); // yarn.flink-dist-jar
				if (localJarPath != null) {
					return Optional.of(new Path(localJarPath));
				}
				final String decodedPath = getDecodedJarPath();{//从 Class.pd.codesource.location.path //this的类就是 flink-dist.jar导进的;
					final String encodedJarPath =getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
					return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
				}
				return decodedPath.endsWith(".jar")? Optional.of(new Path(new File(decodedPath).toURI())): Optional.empty();
			}
			//2 从 yarn.ship-files读取 资源文件路径,并赋值 YarnClusterDescriptor.shipFiles 变量;
			decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_FILES){// YarnClusterDescriptor.decodeFilesToShipToCluster
				final List<File> files =ConfigUtils.decodeListFromConfig(configuration, configOption, File::new);// yarn.ship-files 定义 ship:jar包船?
				return files.isEmpty() ? Optional.empty() : Optional.of(files);
			}.ifPresent(this::addShipFiles);
			//3 从 yarn.ship-archives 读取 资源文件路径,并赋值 YarnClusterDescriptor.shipArchives 变量;
			decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_ARCHIVES).ifPresent(this::addShipArchives);
			this.yarnQueue = flinkConfiguration.getString(YarnConfigOptions.APPLICATION_QUEUE);
		}
	}
	
	
	

	/* 2. 定义$FLINK_CLASSPATH, CLASSPATH
		List<Path> providedLibDirs:		从 yarn.provided.lib.dirs 中读取配置	未配置则未空;
		
	$FLINK_CLASSPATH 
		systemClassPaths = yarn.ship-files配置 + $FLINK_LIB_DIR变量下jars + logConfigFile
			- fileUploader.providedSharedLibs 中的 非dist非plugin 部分;
				* List<Path> providedLibDirs: yarn.provided.lib.dirs属性中有效dir部分
			- uploadedDependencies: 仅添加systemShipFiles中 非PUBLIc&& 非dist 的部分;
				- systemShipFiles
					- logConfigFilePath
					- $FLINK_LIB_DIR, 当 providedLibDirs(yarn.provided.lib.dirs) 为空时, 才添加 $FLINK_LIB_DIR
			- userClassPaths:  仅当yarn.per-job-cluster.include-user-jar=order时, 添加 userJarFiles
				- userJarFiles:	
					* JobGraph.userJars 	pipeline.jars(args[0]/-j/--jarfile指定) ,如examples/batch/WordCount.jar
					* jarUrls:				pipeline.jars:		存在且当 YarnApplication模式时, 才被加到 userJarFiles 中, 一般是 PerJob/YarnSession 所以不加入 CP;


		userClassPath: 		取[非PUBLIc &&非dist]的userJarFiles;
			- userJarFiles:	 
				- JobGraph.userJars 	pipeline.jars(args[0]/-j/--jarfile指定) ,如examples/batch/WordCount.jar
				- pipeline.jars:		存在且当 YarnApplication模式时, 才被加到 userJarFiles 中, 一般是 PerJob/YarnSession 所以不加入 CP;
				
		
		flinkJarPath: (yarn.flink-dist-jar 或 this.codesource.localpath
			yarn.flink-dist-jar
				若不存在,则使用 this.codesource.localpath(即flink-dist本包)
		
		localResourceDescFlinkJar.getResourceKey()
		jobGraphFilename
		"flink-conf.yaml"
		
	yarn.application.classpath
		$HADOOP_CONF_DIR
		common: $HADOOP_COMMON_HOME/share/*/common/*
		hdfs: $HADOOP_HDFS_HOME/share/*/hdfs/*
		yarn: $HADOOP_YARN_HOME/share/*/yarn/*
		
	*/

	YarnClusterDescriptor.startAppMaster(){//YarnClusterDescriptor.startAppMaster()
		final FileSystem fs = FileSystem.get(yarnConfiguration);
		ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
		// 从 yarn.provided.lib.dirs 中读取配置;若存在则加到 systemClassPaths->CLASSPATH; 若不存在,则会启用加载$FLINK_LIB_DIR到systemClassPaths(->CP);
		final List<Path> providedLibDirs =Utils.getQualifiedRemoteSharedPaths(configuration, yarnConfiguration);{
			return getRemoteSharedPaths(){//Utils.
				// yarn.provided.lib.dirs
				final List<Path> providedLibDirs =ConfigUtils.decodeListFromConfig(configuration, YarnConfigOptions.PROVIDED_LIB_DIRS, strToPathMapper);
				return providedLibDirs;
			}
		}
		//重点是过滤 providedLibDirs(yarn.provided.lib.dirs) 中 为dir目录的,并赋值给 fileUploader.providedSharedLibs
		final YarnApplicationFileUploader fileUploader =YarnApplicationFileUploader.from(fs,providedLibDirs);{new YarnApplicationFileUploader(){
			this.applicationDir = getApplicationDir(applicationId);
			this.providedSharedLibs = getAllFilesInProvidedLibDirs(providedLibDirs);{
				Map<String, FileStatus> allFiles = new HashMap<>();
				providedLibDirs.forEach(path -> {
					if (!fileSystem.exists(path) || !fileSystem.isDirectory(path)) {
						LOG.warn("Provided lib dir {} does not exist or is not a directory. Ignoring.",path);
					}else{
						final RemoteIterator<LocatedFileStatus> iterable =fileSystem.listFiles(path, true).forEach(()-> allFiles.put(name, locatedFileStatus););
					}
				});
				return Collections.unmodifiableMap(allFiles);
			}
		}}
		
		// 若shipFiles有(yarn.ship-files 或空), 添加到 systemShipFiles中,并最终 -> uploadedDependencies -> systemClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		Set<File> systemShipFiles = new HashSet<>(shipFiles.size());
		for (File file : shipFiles) {
			 systemShipFiles.add(file.getAbsoluteFile());
		}
		// $internal.yarn.log-config-file ,如果存在则加到 systemShipFiles中; 一般这里是: /opt/flink/conf/log4j.properties; 最终 -> uploadedDependencies -> systemClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		final String logConfigFilePath =configuration.getString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
		if (null !=logConfigFilePath) {
			systemShipFiles.add(new File(logConfigFilePath));
		}
		// 若 yarn.provided.lib.dirs不存在, 则加载 FLINK_LIB_DIR 到 systemShipFiles -> systemClassPaths -> CLASSPATH;
		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			addLibFoldersToShipFiles(systemShipFiles);{//YarnClusterDescriptor.addLibFoldersToShipFiles()
				String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);// 即 $FLINK_LIB_DIR 环境变量;
				if (libDir != null) {
					File directoryFile = new File(libDir);
					if (directoryFile.isDirectory()) {
						effectiveShipFiles.add(directoryFile);//effectiveShipFiles 即传入的 systemShipFiles;
					}
				}else if(shipFiles.isEmpty()){// 如果 null == libDir($FLINK_LIB_DIR为空),而 shipFiles(yarn.ship-files)也为空,则warn告警;
					LOG.warn("Environment variable 'FLINK_LIB_DIR' not set and ship files have not been provided manually. Not shipping any library files");
				}
			}
		}
		
		final Set<Path> userJarFiles = new HashSet<>();
		// 将JobGraph.userJars添加到 userJarFiles中; 	应该就是 -jar指定的App包,如examples/batch/WordCount.jar;  并最终 userJarFiles-> userClassPaths -> $FLINK_CLASSPATH -> CLASSPATH
		if (jobGraph != null) {
			List<Path> jobUserJars = jobGraph.getUserJars().stream().map(f -> f.toUri()).map(Path::new).collect(Collectors.toSet());
			userJarFiles.addAll(jobUserJars);
		}
		//从 pipeline.jars中读取值并赋值给 jarUrls;  默认就是-jar 路径: examples/batch/WordCount.jar
		final List<URI> jarUrls =ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URI::create);// 从pipeline.jars读取值;
		//只有当 YarnApplication 模式时,才会加到 userClassPaths ->$FLINK_CLASSPATH中;  一般 yarnClusterEntrypoint是 YarnJob or YarnSession, 所以不加入 CP;
		if (jarUrls != null && YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)) {
			userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
		}
		
		// Register all files in provided lib dirs as local resources with public visibility and upload the remaining dependencies as local resources with APPLICATION visibility.
		// 把fileUploader.providedSharedLibs( yarn.provided.lib.dirs属性中有效dir部分) 中的 非dist非plugin的, 
		final List<String> systemClassPaths = fileUploader.registerProvidedLocalResources();{// YarnApplicationFileUploader.registerProvidedLocalResources()
			final ArrayList<String> classPaths = new ArrayList<>();
			providedSharedLibs.forEach((fileName, fileStatus)->{
				final Path filePath = fileStatus.getPath();
				if (!isFlinkDistJar(filePath.getName()) && !isPlugin(filePath)) {// 把非dist非plugin的依赖文件,添加到 classPaths中;
					classPaths.add(fileName);
				}else if (isFlinkDistJar(filePath.getName())) { // 如果是flink-dist文件,直接赋值给 flinkDist;
					flinkDist = descriptor;
				}
			});
		}
		// 将systemShipFiles中(logConfigFile + $FLINK_LIB_DIR(不存在yarn.provided.lib.dirs时) )内容赋给 shipFiles;
		Collection<Path> shipFiles = systemShipFiles.stream().map(e -> new Path(e.toURI())).collect(Collectors.toSet());
		// 将shipFiles中(1或2项)所有(递归遍历)内容,过滤出 [PUBLIC] && 非dist] 的所有 archives & resources, 一起返回赋给uploadedDependencies;
		final List<String> uploadedDependencies =fileUploader.registerMultipleLocalResources(shipFiles,Path.CUR_DIR,LocalResourceType.FILE);{
			// 解析shipFiles中,所有目录的所有的文件,都加载到 localPath中;
			final List<Path> localPaths = new ArrayList<>();
			for (Path shipFile : shipFiles) {
				if (Utils.isRemotePath(shipFile.toString())) {
					
				}else{
					final File file = new File(shipFile.toUri().getPath());
					if (file.isDirectory()) {// 
						Files.walkFileTree();//把目前下所有配置都加载?
					}
				}
				localPaths.add(shipFile);
				relativePaths.add(new Path(localResourcesDirectory, shipFile.getName()));
			}
			// 只有 非dist( !isFlinkDistJar ) 且 非Publich ( !alreadyRegisteredAsLocalResource() ) 的filePath才会被添加;
			for (int i = 0; i < localPaths.size(); i++) {
				if (!isFlinkDistJar(relativePath.getName())) {
					// 往YarnApplicationFileUploader 中 remotePath,envShipResourceList,localResources等变量 添加;
					YarnLocalResourceDescriptor resourceDescriptor =registerSingleLocalResource(key,loalpath,true,true);
					if (!resourceDescriptor.alreadyRegisteredAsLocalResource(){// 只有非PUBLIC公开级别的资源 才添加; log4j.properties因为是APP级别被过滤掉;
						return this.visibility.equals(LocalResourceVisibility.PUBLIC)
					}) {
						if (key.endsWith("jar")) { //是jar的算到 archives,
							archives.add(relativePath.toString());
						}else{ //所有非jar的file 都算到 resource中; 
							resources.add(relativePath.getParent().toString());
						}
					}
				}
			}
			final ArrayList<String> classPaths = new ArrayList<>();
			resources.stream().sorted().forEach(classPaths::add);
			archives.stream().sorted().forEach(classPaths::add);
			return classPaths;
		}
		systemClassPaths.addAll(uploadedDependencies);
		
		if (providedLibDirs == null || providedLibDirs.isEmpty()) {
			// 取$FLINK_PLUGINS_DIR环境值,或采用默认"plugins" 作为 为插件目录并添加到shipOnlyFiles中;
			Set<File> shipOnlyFiles = new HashSet<>();
			addPluginsFoldersToShipFiles(shipOnlyFiles);{//YarnApplicationFileUploader.addPluginsFoldersToShipFiles()
				Optional<File> pluginsDir = PluginConfig.getPluginsDir();{
					String pluginsDir =System.getenv().getOrDefault(ConfigConstants.ENV_FLINK_PLUGINS_DIR,//FLINK_PLUGINS_DIR
						ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS);// "plugins"
					File pluginsDirFile = new File(pluginsDir);
					if (!pluginsDirFile.isDirectory()) {
						return Optional.empty();
					}
					return Optional.of(pluginsDirFile);
				}
				pluginsDir.ifPresent(effectiveShipFiles::add);// 如果plugins配置存在,则 add (到shipOnlyFiles),并返回;
			}
			fileUploader.registerMultipleLocalResources(); // 
		}
		if (!shipArchives.isEmpty()) {//若yarn.ship-archives不为空,
			shipArchivesFile = shipArchives.stream().map(e -> new Path(e.toURI())).collect(Collectors.toSet());
			fileUploader.registerMultipleLocalResources(shipArchivesFile);
		}
		
		// localResourcesDir= "."
		String localResourcesDir= userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR : Path.CUR_DIR, LocalResourceType.FILE;
		final List<String> userClassPaths =fileUploader.registerMultipleLocalResources(userJarFiles, localResourcesDir);{// 过滤其中[非PUBLIC] && 非dist] 
			for (int i = 0; i < localPaths.size(); i++) {
				final Path relativePath = localPaths.get(i).get(i);
				if (!isFlinkDistJar(relativePath.getName())) {
					// 只要不是PUBLIC 级别的, 就添加; 这里的 userJar(如:examples/batch/WordCount.jar) 被成功添加;
					if (!resourceDescriptor.alreadyRegisteredAsLocalResource(){// 只要非PUBLIC公开级别的, 就添加; 
						return this.visibility.equals(LocalResourceVisibility.PUBLIC)
					}) {
						if (key.endsWith("jar")) { //是jar的算到 archives,
							archives.add(relativePath.toString());
						}else{ //所有非jar的file 都算到 resource中; 
							resources.add(relativePath.getParent().toString());
						}
					}
				}
			}
		}
		// 当yarn.per-job-cluster.include-user-jar=order时, 添加userClassPaths到 systemClassPath
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.ORDER) {//yarn.per-job-cluster.include-user-jar=order时 
			systemClassPaths.addAll(userClassPaths);
		}
		
		//FLINK_CLASSPATH 1: include-user-jar=first时,把 jobGraph.getUserJars() &pipeline.jars &usrlib 目录下jars 加到前面;
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST){////yarn.per-job-cluster.include-user-jar=first时, userClassPath放前面;
			classPathBuilder.append(userClassPath).append(File.pathSeparator);
		} 
		Collections.sort(systemClassPaths);
		Collections.sort(userClassPaths);
		StringBuilder classPathBuilder = new StringBuilder();
		
		for (String classPath : systemClassPaths) {// 添加system级别的CP
            classPathBuilder.append(classPath).append(File.pathSeparator);
        }
		// 封装 flinkJarPath(yarn.flink-dist-jar 或 this.codesource.localpath本包,即flink-dist包); 并添加到 classPath中;
		final YarnLocalResourceDescriptor localResourceDescFlinkJar =fileUploader.uploadFlinkDist(flinkJarPath);
		classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
		
		// 把jobGraph序列号成文件,并把 "job.graph" 添加到classpath;
		if (jobGraph != null) {
			File tmpJobGraphFile = File.createTempFile(appId.toString(), null);
			// 把jobGraph对象写出到临时文件: /tmp/application_1639998011452_00604014191052203287620.tmp
			try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
				 ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
                    obOutput.writeObject(jobGraph);
            }
			final String jobGraphFilename = "job.graph";
			configuration.setString(JOB_GRAPH_FILE_PATH, jobGraphFilename);
			fileUploader.registerSingleLocalResource();
			classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
		}
		// 把"flink-conf.yaml" 添加到classPath中;
		File tmpConfigurationFile = File.createTempFile(appId + "-flink-conf.yaml", null);
		BootstrapTools.writeConfiguration(configuration, tmpConfigurationFile);
		String flinkConfigKey = "flink-conf.yaml";
		fileUploader.registerSingleLocalResource(flinkConfigKey);
		classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
		
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) {
            for (String userClassPath : userClassPaths) {
                classPathBuilder.append(userClassPath).append(File.pathSeparator);
            }
        }
		
		
		
		
		
		// FLINK_CLASSPATH 2: systemClassPaths= shipFiles(yarn.ship-files配置) + logConfigFile +systemShipFiles(Sys.FLINK_LIB_DIR变量) , 包括 localResources中上传的13个flink的lib下jar包;
		addLibFoldersToShipFiles(systemShipFiles);{
			String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);//从系统变量读取FLINK_LIB_DIR 的值;
			effectiveShipFiles.add(new File(libDir));
		}
		for (String classPath : systemClassPaths) classPathBuilder.append(classPath).append(File.pathSeparator);
		// FLINK_CLASSPATH 3: 
		classPathBuilder.append(localResourceDescFlinkJar.getResourceKey()).append(File.pathSeparator);
		classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
		classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);
		//FLINK_CLASSPATH 6: include-user-jar=last时, 把userClassPath 的jars加到CP后面; 
		if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) classPathBuilder.append(userClassPath).append(File.pathSeparator);
		
		appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());
		appMasterEnv.put(YarnConfigKeys.FLINK_YARN_FILES,fileUploader.getApplicationDir().toUri().toString());
		// 设置 CLASSPATH的参数
		Utils.setupYarnClassPath(yarnConfiguration, appMasterEnv);{
			// 1. 先把 _FLINK_CLASSPATH中 lib中13个flink相关jar包加到CP
			addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), appMasterEnv.get(ENV_FLINK_CLASSPATH));
			// 2. yarn.application.classpath + 
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
		amContainer.setEnvironment(appMasterEnv);
		
	}



	
}





/** 1.2.2 yarnCli: submitApplication 与Yarn RM通信,提交启动AM; 
	//1. 发送Rpc请求: ProtobufRpcEngine.invoke()
	//2. 通信等待非 waitingStates就结束阻塞,返回 applicationId
*/

YarnClientImpl.submitApplication(ApplicationSubmissionContext appContext){//YarnClientImpl.submitApplication()
	SubmitApplicationRequest request =Records.newRecord(SubmitApplicationRequest.class);
	request.setApplicationSubmissionContext(appContext);
	
	rmClient.submitApplication(request);{// ApplicationClientProtocolPBClientImpl.submitApplication()
		// yarn 的resourceManager的 resourcemanager.ClientRMService 进行处理
		SubmitApplicationRequestProto requestProto= ((SubmitApplicationRequestPBImpl) request).getProto();
		SubmitApplicationResponseProto proto= proxy.submitApplication(null,requestProto){
			// 实际执行: 
			ProtobufRpcEngine.invoke(Object proxy, Method method, Object[] args){}{
				// method= ApplicationClientProtocol.BlokingInterface.submitApplication()
				RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
				RpcRequestWrapper rpcRequest= new RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId,fallbackToSimpleAuth);
				RpcResponseWrapper val=(RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,rpcRequest);
				Message returnMessage = prototype.newBuilderForType().mergeFrom(val.theResponseRead).build();
				return returnMessage;
			}
		}
		return new SubmitApplicationResponsePBImpl(proxy.submitApplication(null,requestProto));
	}
	while (true) {// 非waitingStates 就跳出返回 applicationId
		if (!waitingStates.contains(state)) {
			LOG.info("Submitted application " + applicationId);
			break;
		}
	}
	return applicationId;
}



// 1.3 K8s Cli
// KubernetesSessionCli.main() 的Java提交命令
org.apache.flink.kubernetes.cli.KubernetesSessionCli.main(){
	final Configuration configuration = getEffectiveConfiguration(args);{
		final CommandLine commandLine = cli.parseCommandLineOptions(args, true);
		final Configuration effectiveConfiguration = new Configuration(baseConfiguration);
        effectiveConfiguration.addAll(cli.toConfiguration(commandLine));
        effectiveConfiguration.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
        return effectiveConfiguration;
	}
	final ClusterClientFactory<String> kubernetesClusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(configuration);
	final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, configDir);
	int retCode;
	try {
		final KubernetesSessionCli cli = new KubernetesSessionCli(configuration, configDir);
		retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.run(args));{//KubernetesSessionCli.run()
			final Configuration configuration = getEffectiveConfiguration(args);
			final ClusterClientFactory<String> kubernetesClusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(configuration);
			String clusterId = kubernetesClusterClientFactory.getClusterId(configuration);
			final FlinkKubeClient kubeClient = FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");
			
			if (clusterId != null && kubeClient.getRestService(clusterId).isPresent()) {
				clusterClient = kubernetesClusterDescriptor.retrieve(clusterId).getClusterClient();
			}else{// 第一次,进入这里; 
				clusterClient =kubernetesClusterDescriptor
										.deploySessionCluster(kubernetesClusterClientFactory.getClusterSpecification(configuration)){//KubernetesClusterDescriptor.deploySessionCluster
											KubernetesClusterDescriptor.deploySessionCluster(){
												final ClusterClientProvider<String> clusterClientProvider = deployClusterInternal(KubernetesSessionClusterEntrypoint.class.getName(),clusterSpecification, false);
												try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
													LOG.info("Create flink session cluster {} successfully, JobManager Web Interface: {}", clusterId, clusterClient.getWebInterfaceURL());
												}
												return clusterClientProvider;
											}
										}
										.getClusterClient();
				clusterId = clusterClient.getClusterId();
			}
			
			clusterClient.close();
			kubeClient.close();
		}
	} catch (CliArgsException e) {
		retCode = AbstractCustomCommandLine.handleCliArgsException(e, LOG);
	} catch (Exception e) {
		retCode = AbstractCustomCommandLine.handleError(e, LOG);
	}
	System.exit(retCode);
	
	
}






/** 2	env.execute() 提交执行
*
*/





// 2.1 env.execute() 触发作业执行:  env.execute() : 创建相应Factory和Executor,生产上下文, submittJob()提交执行; 
env.execute()
	- executorServiceLoader.getExecutorFactory() 通过 加载和比较所有的 PipelineExecutorFactory.name()是否==  execution.target
	- PipelineExecutorFactory.getExecutor() 创建相应 PipelineExecutor实现类: YarnSession,YarnPerJob, KubernetesExecutor,LocalExecutor 等; 
	- PipelineExecutor.execute() 提交执行相应的job作业; 


ExecutionEnvironment.execute(){
	// Streaming 的执行
	StreamExecutionEnvironment.execute(){
		return execute(getStreamGraph(jobName));{
			final JobClient jobClient = executeAsync(streamGraph);
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




// 2.2 各大引擎核心调用 Stream模式 异步执行作业
// PipelineExecutor.execute() clusterClient.submitJob(): RestClient.sendRequest() 向远程JobManager进程发送 JobSubmit 请求
StreamExecutionEnvironment.executeAsync(StreamGraph streamGraph);{
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
			LocalExecutor.execute(){//LocalExecutor.execute()
				final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig);
				return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory).submitJob(jobGraph);{// PerJobMiniClusterFactory.submitJob()
					MiniCluster miniCluster = miniClusterFactory.apply(miniClusterConfig);
					miniCluster.start();
					
					return miniCluster
						.submitJob(jobGraph){//MiniCluster.submitJob()
							final CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = getDispatcherGatewayFuture();
							final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);
							final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture
							.thenCombine(dispatcherGatewayFuture,(Void ack, DispatcherGateway dispatcherGateway) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout)){
								dispatcherGateway.submitJob(): 发起远程Rpc请求: 实际执行 Dispatcher.submitJob()
								Dispatcher.submitJob(){ //远程Rpc调用,并返回结果;
									//代码详情如下:
								}
							}
							.thenCompose(Function.identity());
							return acknowledgeCompletableFuture.thenApply((Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
							
						}
						.thenApply(result -> new PerJobMiniClusterJobClient(result.getJobID(), miniCluster))
						.whenComplete((ignored, throwable) -> {
							if (throwable != null) {
								// We failed to create the JobClient and must shutdown to ensure cleanup.
								shutDownCluster(miniCluster);
							}
						});
						
				}
			}
			
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
						submissionFuture= sendRetriableRequest(request);{//RestClusterClient.
							getWebMonitorBaseUrl()
							return restClient.sendRequest(messageParameters,request, filesToUpload);{//RestClient.
								return sendRequest();{//RestClient.sendRequest()
									String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, messageParameters);// = /v1/jobs
									objectMapper.writeValue(new StringWriter(), request);
									Request httpRequest =createRequest(targetUrl,payload);
									// 这里向集群: bdnode102.hjq.com:36384(YarnSessionClusterEntrypoint) JobManager发送JobSubmitRequest
									return submitRequest(targetAddress, targetPort, httpRequest, responseType);{//RestClient.submitRequest()
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
	
	try {
		JobClient jobClient = jobClientFuture.get();
		jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
		return jobClient;
	} catch (ExecutionException executionException) {//执行失败,从这里抛出异常; 
		throw new FlinkException(String.format("Failed to execute job '%s'.", streamGraph.getJobName()),strippedException);
	}
}



// 2.2.1 JobManager模块, 响应JobSubmit请求的逻辑: submitJob(): RestClient.sendRequest() -> 






/** 3	FlinkSqlCli 
*
*/



// SqlClient进程中 TableEnvInit初始化和 CatalogManager构建;
// client.start().openSession().build(): ExecutionContext.initializeTableEnvironment()初始化Table环境资源, initializeCatalogs()根据配置生成Catalogs和curdb;
// A. client.start().open().parseCommand(line).sqlParser.parse(stmt): PlannerContext.createCatalogReader() 将CatalogManager中curCatalog/DB作为defaultSchemas 封装进FlinkCatalogReader;
// B. client.start().open().callCommand().callSelect(cmdCall):executor.executeQuery():tableEnv.sqlQuery(selectQuery) 提交Table查询命令: TableEnvironmentImpl.sqlQuery()

SqlClient.main(){
	final SqlClient client = new SqlClient(true, options);
	client.start();{
		final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
        executor.start();
		final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
        appendPythonConfig(sessionEnv, options.getPythonConfiguration());
		context = new SessionContext(options.getSessionId(), sessionEnv);
		// 创建 ModuleManager, CatalogManager, FunctionCatalog
		String sessionId = executor.openSession(context);{// LocalExecutor.
			String sessionId = sessionContext.getSessionId();// defaul;
			this.contextMap.put(sessionId, createExecutionContextBuilder(sessionContext).build());{//ExecutionContext$Builder.build()
				return new ExecutionContext<>(this.sessionContext,this.sessionState,this.dependencies,,,);{//ExecutionContext()构造函数, 生成一堆的执行环境;
					classLoader = ClientUtils.buildUserCodeClassLoader();
					// 重要的环境变量解析和 运行对象生成
					initializeTableEnvironment(sessionState);{//ExecutionContext.initializeTableEnvironment()
						EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
						final TableConfig config = createTableConfig();
						if (sessionState == null) {
							// Step.1 Create environments
							final ModuleManager moduleManager = new ModuleManager();
							final CatalogManager catalogManager =CatalogManager.newBuilder()
										.classLoader(classLoader).config(config.getConfiguration())
										.defaultCatalog(settings.getBuiltInCatalogName(),
												new GenericInMemoryCatalog(settings.getBuiltInCatalogName(),settings.getBuiltInDatabaseName()))
										.build();{//CatalogManager.Builder.build()
											// default_catalog, default_database
											return new CatalogManager(defaultCatalogName,defaultCatalog,new DataTypeFactoryImpl(classLoader, config, executionConfig));
							}
							CommandLine commandLine =createCommandLine(environment.getDeployment(), commandLineOptions);
							clusterClientFactory = serviceLoader.getClusterClientFactory(flinkConfig);
							// Step 1.2 Initialize the FunctionCatalog if required.
							FunctionCatalog functionCatalog =new FunctionCatalog(config, catalogManager, moduleManager);
							// Step 1.3 Set up session state.
							this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);
							// Must initialize the table environment before actually the
							createTableEnvironment(settings, config, catalogManager, moduleManager, functionCatalog);
							// Step.2 Create modules and load them into the TableEnvironment.
							environment.getModules().forEach((name, entry) -> modules.put(name, createModule(entry.asMap(), classLoader)));
							// Step.3 create user-defined functions and temporal tables then register them.
							registerFunctions();
							// Step.4 Create catalogs and register them. 基于config配置文件,创建多个Catalog和 curCatalog,curDatabase;
							initializeCatalogs();{// ExecutionContext.initializeCatalogs
								// Step.1 Create catalogs and register them.
								environment.getCatalogs().forEach((name, entry) -> {
												Catalog catalog=createCatalog(name, entry.asMap(), classLoader);
												tableEnv.registerCatalog(name, catalog);{//TableEnvironmentImpl.registerCatalog
													catalogManager.registerCatalog(catalogName, catalog);{//CatalogManager.registerCatalog
														catalog.open();{// 不同的catalog,不同的实现;
															HiveCatalog.open();
														}
														catalogs.put(catalogName, catalog);
													}
												}
											});
								// Step.2 create table sources & sinks, and register them.
								environment.getTables().forEach((name, entry) -> {
												if (entry instanceof SourceTableEntry|| entry instanceof SourceSinkTableEntry) {
													tableSources.put(name, createTableSource(name, entry.asMap()));
												}
												if (entry instanceof SinkTableEntry|| entry instanceof SourceSinkTableEntry) {
													tableSinks.put(name, createTableSink(name, entry.asMap()));
												}
											});
								tableSources.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSourceInternal);
								tableSinks.forEach(((TableEnvironmentInternal) tableEnv)::registerTableSinkInternal);
								// Step.4 Register temporal tables.
								environment.getTables().forEach((name, entry) -> {registerTemporalTable(temporalTableEntry);});
								// Step.5 Set current catalog and database. 从 
								Optional<String> catalog = environment.getExecution().getCurrentCatalog();// "current-catalog" 参数
								Optional<String> database = environment.getExecution().getCurrentDatabase();// current-database 参数
								database.ifPresent(tableEnv::useDatabase);
							}
						}
					}
				}
			}
		}
		
		openCli(sessionId, executor);{//SqlClient.openCli
			CliClient cli = new CliClient(sessionId, executor, historyFilePath)
			cli.open();{//CliClient.
				terminal.writer().append(CliStrings.MESSAGE_WELCOME);
				while (isRunning) {
					terminal.writer().append("\n");
					// 读取一行数据; 
					String line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
					// 1. 解析用户查询语句生成 Calcite对象,并基于默认 curCatalog,curDB生成 FlinkCatalogReader;
					final Optional<SqlCommandCall> cmdCall = parseCommand(line);{//CliClient.
						parsedLine = SqlCommandParser.parse(executor.getSqlParser(sessionId), line);{
							Optional<SqlCommandCall> callOpt = parseByRegexMatching(stmt);
							if (callOpt.isPresent()) {//先用正则解析; 
								return callOpt.get();
							}else{// 没有正则, 进入这里; 
								return parseBySqlParser(sqlParser, stmt);{//SqlCommandParser.parseBySqlParser
									operations = sqlParser.parse(stmt);{//LocalExecutor.Parser匿名类.parse()
										return context.wrapClassLoader(() -> parser.parse(statement));{// ParserImpl.parse()
											CalciteParser parser = calciteParserSupplier.get();
											FlinkPlannerImpl planner = validatorSupplier.get();
											SqlNode parsed = parser.parse(statement);
											Operation operation =SqlToOperationConverter.convert(planner, catalogManager, parsed)
											.orElseThrow(() -> new TableException("Unsupported query: " + statement));{// SqlToOperationConverter.convert()
												final SqlNode validated = flinkPlanner.validate(sqlNode);{// FlinkPlannerImpl.validate()
													val validator = getOrCreateSqlValidator();{
														val catalogReader = catalogReaderSupplier.apply(false);{
															PlannerContext.createCatalogReader(){
																SqlParser.Config sqlParserConfig = getSqlParserConfig();
																SqlParser.Config newSqlParserConfig =SqlParser.configBuilder(sqlParserConfig).setCaseSensitive(caseSensitive).build();
																SchemaPlus rootSchema = getRootSchema(this.rootSchema.plus());
																// 这里的 currentDatabase,currentDatabase 来源与 CatalogManager.参数; 应该是加载 sql-client-defaults.yaml 后生成的; 
																// 把 currentCatalog("myhive"), currentDatabase("default") 作为默认的 SchemaPaths;
																List<List<String>> defaultSchemas = asList(asList(currentCatalog, currentDatabase), singletonList(currentCatalog));
																return new FlinkCalciteCatalogReader(CalciteSchema.from(rootSchema),defaultSchemas,typeFactory);
															}
														}
														validator = createSqlValidator(catalogReader)
													}
													validate(sqlNode, validator)
												}
												SqlToOperationConverter converter =new SqlToOperationConverter(flinkPlanner, catalogManager);
											}
											
										
										}
									}
									return new SqlCommandCall(cmd, operands);
								}
							}
						}
					}
					// 2. 提交执行sql Calcite命令; 
					cmdCall.ifPresent(this::callCommand);{
						switch (cmdCall.command) {
							case SET:
								callSet(cmdCall);
								break;
							case SELECT:
								callSelect(cmdCall);{//CliClient.callSelect()
									resultDesc = executor.executeQuery(sessionId, cmdCall.operands[0]);{//LocalExecutor.executeQuery()
										final ExecutionContext<?> context = getExecutionContext(sessionId);
										return executeQueryInternal(sessionId, context, query);{//LocalExecutor.
											final Table table = createTable(context, context.getTableEnvironment(), query);{
												return context.wrapClassLoader(() -> tableEnv.sqlQuery(selectQuery));{
													//TableEnvironmentImpl.sqlQuery(selectQuery);
												}
											}
											final DynamicResult<C> result =resultStore.createResult();
											pipeline = context.createPipeline(jobName);
											final ProgramDeployer deployer =new ProgramDeployer(configuration, jobName, pipeline, context.getClassLoader());
											deployer.deploy().get();
											return new ResultDescriptor();
										}
									}
									if (resultDesc.isTableauMode()) {
										tableauResultView =new CliTableauResultView();
									}
								}
								break;
							case INSERT_INTO:
							case INSERT_OVERWRITE:
								callInsert(cmdCall);
								break;
							case CREATE_TABLE:
								callDdl(cmdCall.operands[0], CliStrings.MESSAGE_TABLE_CREATED);
								break;
						}
					}
				}
			}
		}
	}
}






