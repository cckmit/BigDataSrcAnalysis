
// 1.0 
bin/flink run 的启动会话命令


// 1.1 bin/flink  脚本源码
bin=`dirname "$target"`
# 加载环境变量
. "$bin"/config.sh

CC_CLASSPATH=`constructFlinkClassPath`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-client-$HOSTNAME.log
# 定义JVM参数
FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_CLI}"

exec $JAVA_RUN $JVM_ARGS $FLINK_ENV_JAVA_OPTS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" org.apache.flink.client.cli.CliFrontend "$@"

	// java -cp "xx.jar"  org.apache.flink.client.cli.CliFrontend "$@"
	// java -cp "xx.jar"  org.apache.flink.client.cli.CliFrontend run -d -e kubernetes-session xx



// 1.2 KubernetesSessionCli.main() 的Java提交命令

org.apache.flink.client.cli.CliFrontend.main(){
	EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);
	final String configurationDirectory = getConfigurationDirectoryFromEnv();
	final Configuration configuration =GlobalConfiguration.loadConfiguration(configurationDirectory);
	final List<CustomCommandLine> customCommandLines =loadCustomCommandLines(configuration, configurationDirectory);
	final CliFrontend cli = new CliFrontend(configuration, customCommandLines);
	int retCode =SecurityUtils.getInstalledContext().runSecured(() -> cli.parseAndRun(args));{// CliFrontend.parseAndRun
		String action = args[0];
		final String[] params = Arrays.copyOfRange(args, 1, args.length);
		try {
            // do action
            switch (action) {
                case ACTION_RUN:
                    run(params);{
						final Options commandOptions = CliFrontendParser.getRunCommandOptions();
						final CommandLine commandLine = getCommandLine(commandOptions, args, true);
						final List<URL> jobJars = getJobJarAndDependencies(programOptions);
						final Configuration effectiveConfiguration =getEffectiveConfiguration(activeCommandLine, commandLine, programOptions, jobJars);
						
						executeProgram(effectiveConfiguration, program);{
							ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);{
								final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
								ContextEnvironment.setAsContext(executorServiceLoader,suppressSysout);
								StreamContextEnvironment.setAsContext(executorServiceLoader,suppressSysout);
								
								program.invokeInteractiveModeForExecution();{
									callMainMethod(mainClass, args);{
										// 将 driver/app类 反射获取其 main()方法,用于执行
										mainMethod = entryClass.getMethod("main", String[].class);
										// 执行其 main()方法:examples.join.WindowJoin.main() 或用户自定义的 UdfDriver.main()
										mainMethod.invoke(null, (Object) args);
										
									}
								}
							
							}
						}
						
					}
                    return 0;
                case ACTION_RUN_APPLICATION:
                    runApplication(params);
                    return 0;
                case ACTION_LIST:
                    list(params);
                    return 0;
                case ACTION_INFO:
                    info(params);
                    return 0;
                case ACTION_CANCEL:
                    cancel(params);
                    return 0;
                case ACTION_STOP:
                    stop(params);
                    return 0;
                case ACTION_SAVEPOINT:
                    savepoint(params);
                    return 0;
                case "-h":
                case "--help":
                    CliFrontendParser.printHelp(customCommandLines);
                    return 0;
                case "-v":
                case "--version":
                    
                    return 0;
                default:
                    
                    return 1;
            }
        } catch (CliArgsException ce) {
            return handleArgException(ce);
        } catch (ProgramParametrizationException ppe) {
            return handleParametrizationException(ppe);
        } catch (ProgramMissingJobException pmje) {
            return handleMissingJobException();
        } catch (Exception e) {
            return handleError(e);
        }
		
	}
	System.exit(retCode);
	
}



org.apache.flink.streaming.examples.join.WindowJoin.main(){
	final ParameterTool params = ParameterTool.fromArgs(args);
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	env.getConfig().setGlobalJobParameters(params);
	// 处理逻辑
	DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(grades, salaries, windowSize);
	
	// 提交执行
	env.execute("Windowed Join Example");{
		StreamGraph streamGraph = getStreamGraph(jobName);{
			StreamGraph streamGraph = getStreamGraphGenerator().setJobName(jobName).generate();
			return streamGraph;
		}
		return execute(streamGraph);{
			// 获取 JobMaster的递增和client,就是这儿吧
			final JobClient jobClient = executeAsync(streamGraph);
			
		}
	}
	
}


StreamExecutionEnvironment.execute(){
	return execute(getStreamGraph(jobName));{
		// 分本地执行环境和 远程执行环境
		LocalStreamEnvironment.execute(){
			return super.execute(streamGraph);{//StreamExecutionEnvironment.execute()
				final JobClient jobClient = executeAsync(streamGraph);{
					// 从CL中加载解析所有的 PipelineExecutorFactory 实现类,只有flink-clients中LocalExecutorFactory, RemoteExecutorFactory 2个类
					final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);{//core.DefaultExecutorServiceLoader.
						final ServiceLoader<PipelineExecutorFactory> loader = ServiceLoader.load(PipelineExecutorFactory.class);
						while (loader.iterator().hasNext()) {
							final PipelineExecutorFactory factory = factories.next();
							if (factory != null && factory.isCompatibleWith(configuration)){
								// configuration中的executor.target == kubernetes-session
								KubernetesSessionClusterExecutorFactory.isCompatibleWith(){
									return configuration
									.get(DeploymentOptions.TARGET)
									.equalsIgnoreCase(KubernetesSessionClusterExecutor.NAME);
								}
								// configuration中executor.target == yarn-per-job
								YarnJobClusterExecutorFactory.isCompatibleWith(){
									return YarnJobClusterExecutor.NAME.equalsIgnoreCase(
												configuration.get(DeploymentOptions.TARGET));
								}
								
								// configuration中executor.target == yarn-session
								
								
							} {
								compatibleFactories.add(factories.next());
							}
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
						.getExecutor(configuration) {// PipelineExecutor.getExecutor()
							// K8s Session模式
							KubernetesSessionClusterExecutorFactory.getExecutor(){
								return new KubernetesSessionClusterExecutor();
							}
							
							// Yarn Per Job模式
							YarnJobClusterExecutorFactory.getExecutor(){
								return new YarnJobClusterExecutor();
							}
							
							
						}
                        .execute(streamGraph, configuration, userClassloader);{
							
							KubernetesSessionClusterExecutor.execute(){
								AbstractSessionClusterExecutor.execute();{
									final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);{
										
									}
									final ClusterDescriptor<ClusterID> clusterDescriptor =clusterClientFactory.createClusterDescriptor(configuration);{
										
										KubernetesSessionClusterExecutorFactory.createClusterDescriptor(){
											if (!configuration.contains(KubernetesConfigOptions.CLUSTER_ID)) {
												final String clusterId = generateClusterId();
												configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId); //kubernetes.cluster-id
											}
											FlinkKubeClient flinkKubeClient = FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");{//FlinkKubeClientFactory.fromConfiguration()
												
												return new Fabric8FlinkKubeClient(flinkConfig, client, createThreadPoolForAsyncIO(poolSize, useCase));
											}
											return new KubernetesClusterDescriptor( configuration,flinkKubeClient);
										}
										
										YarnJobClusterExecutorFactory.createClusterDescriptor()
										
									}
									
									final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
									
									final ClusterClientProvider<ClusterID> clusterClientProvider =clusterDescriptor.retrieve(clusterID);{//
										final ClusterClientProvider<String> clusterClientProvider =createClusterClientProvider(clusterId);
										ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient();{
											final Configuration configuration = new Configuration(flinkConfig);
											final Optional<Endpoint> restEndpoint = client.getRestEndpoint(clusterId);{//Fabric8FlinkKubeClient.getRestEndpoint()
												final Service service = restService.get().getInternalResource();
												// 获取 restService,并取其中的 spec.ports.0.port=8081 作为 restPort; 
												final int restPort = getRestPortFromExternalService(service);
												final KubernetesConfigOptions.ServiceExposedType serviceExposedType =KubernetesConfigOptions.ServiceExposedType.valueOf(service.getSpec().getType());
												return getRestEndPointFromService(service, restPort);
											}
											return new RestClusterClient<>(configuration,clusterId,new StandaloneClientHAServices(getWebMonitorAddress(configuration)));
										}
										return clusterClientProvider;
										
									}
									
									// 异步提交和等待结果
									return clusterClient
											.submitJob(jobGraph)
											.thenApplyAsync()
											.thenApplyAsync()
											.whenCompleteAsync((ignored1, ignored2) -> clusterClient.close());
									
									
								}
							}
							
							LocalExecutor.execute()
							
							AbstractJobClusterExecutor.execute();
							
							AbstractSessionClusterExecutor.execute();
							
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
		
		RemoteStreamEnvironment.execute(){
			
		}
		
		StreamContextEnvironment.execute();
		
		StreamPlanEnvironment.execute();// ? strema sql ?
		
	}
}


// K8s Sesion的

KubernetesSessionClusterExecutorFactory.createClusterDescriptor(){
	if (!configuration.contains(KubernetesConfigOptions.CLUSTER_ID)) {
		final String clusterId = generateClusterId();
		configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId); //kubernetes.cluster-id
	}
	FlinkKubeClient flinkKubeClient = FlinkKubeClientFactory.getInstance().fromConfiguration(configuration, "client");{//FlinkKubeClientFactory.fromConfiguration()
		final String kubeContext = flinkConfig.getString(KubernetesConfigOptions.CONTEXT);
		// 检查看是否存在 k8s配置文件 kubernetes.config.file
		final String kubeConfigFile =flinkConfig.getString(KubernetesConfigOptions.KUBE_CONFIG_FILE);
		if (kubeConfigFile != null) {// 启用 kubernetes.config的flink配置文件;
			config =Config.fromKubeconfig( kubeContext, FileUtils.readFileUtf8(new File(kubeConfigFile)),null);
		}else{ // 使用默认k8s配置
			config = Config.autoConfigure(kubeContext);{
				Config config = new Config();
				return autoConfigure(config, context);{
					// 就是这个方法;
					if (!tryKubeConfig(config, context)) {{//Config.tryKubeConfig()
						if (Utils.getSystemPropertyOrEnvVar(KUBERNETES_AUTH_TRYKUBECONFIG_SYSTEM_PROPERTY, true)){
							// 从/home/bigdata/.kube/config 目录读取配置
							String fileName = Utils.getSystemPropertyOrEnvVar(KUBERNETES_KUBECONFIG_FILE, new File(getHomeDir(), ".kube" + File.separator + "config").toString());
							FileReader reader = new FileReader(kubeConfigFile);
							kubeconfigContents = IOHelpers.readFully(reader); // 就是 .kube/config 的整个文本文件内容; 
							Config.loadFromKubeconfig(config, context, kubeconfigContents, kubeConfigFile.getPath());{
								io.fabric8.kubernetes.api.model.Config kubeConfig = KubeConfigUtils.parseConfigFromString(kubeconfigContents);
								Context currentContext = KubeConfigUtils.getCurrentContext(kubeConfig);
								if (currentCluster != null) {
									config.setMasterUrl(currentCluster.getServer());
									config.setNamespace(currentContext.getNamespace());
									config.setTrustCerts(currentCluster.getInsecureSkipTlsVerify() != null && currentCluster.getInsecureSkipTlsVerify());
								}
								
							}
							return true;
						}
					}
					  tryServiceAccount(config);
					  tryNamespaceFromPath(config);
					}
					// 从系统属性中, overwide重写覆盖 .kube/config的配置; 
					configFromSysPropsOrEnvVars(config);{
						config.setTrustCerts(Utils.getSystemPropertyOrEnvVar(KUBERNETES_TRUST_CERT_SYSTEM_PROPERTY, config.isTrustCerts()));
						config.setDisableHostnameVerification(Utils.getSystemPropertyOrEnvVar(KUBERNETES_DISABLE_HOSTNAME_VERIFICATION_SYSTEM_PROPERTY, config.isDisableHostnameVerification()));
						// 可通过 kubernetes.master 重写master
						config.setMasterUrl(Utils.getSystemPropertyOrEnvVar(KUBERNETES_MASTER_SYSTEM_PROPERTY, config.getMasterUrl()));
						config.setApiVersion(Utils.getSystemPropertyOrEnvVar(KUBERNETES_API_VERSION_SYSTEM_PROPERTY, config.getApiVersion()));
						config.setNamespace(Utils.getSystemPropertyOrEnvVar(KUBERNETES_NAMESPACE_SYSTEM_PROPERTY, config.getNamespace()));
						config.setCaCertFile(Utils.getSystemPropertyOrEnvVar(KUBERNETES_CA_CERTIFICATE_FILE_SYSTEM_PROPERTY, config.getCaCertFile()));
						
						config.setOauthToken(Utils.getSystemPropertyOrEnvVar(KUBERNETES_OAUTH_TOKEN_SYSTEM_PROPERTY, config.getOauthToken()));
						config.setUsername(Utils.getSystemPropertyOrEnvVar(KUBERNETES_AUTH_BASIC_USERNAME_SYSTEM_PROPERTY, config.getUsername()));
						config.setPassword(Utils.getSystemPropertyOrEnvVar(KUBERNETES_AUTH_BASIC_PASSWORD_SYSTEM_PROPERTY, config.getPassword()));

					}

					if (!config.masterUrl.toLowerCase(Locale.ROOT).startsWith(HTTP_PROTOCOL_PREFIX) && !config.masterUrl.toLowerCase(Locale.ROOT).startsWith(HTTPS_PROTOCOL_PREFIX)) {
					  config.masterUrl = (SSLUtils.isHttpsAvailable(config) ? HTTPS_PROTOCOL_PREFIX : HTTP_PROTOCOL_PREFIX) + config.masterUrl;
					}

					if (!config.masterUrl.endsWith("/")) {
					  config.masterUrl = config.masterUrl + "/";
					}
					return config;
				}
			}
		}
		
		final String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
		config.setNamespace(namespace);
		final NamespacedKubernetesClient client = new DefaultKubernetesClient(config);
		final int poolSize = flinkConfig.get(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE);
		
		return new Fabric8FlinkKubeClient(flinkConfig, client, createThreadPoolForAsyncIO(poolSize, useCase));
	}
	return new KubernetesClusterDescriptor( configuration,flinkKubeClient);
}







getRestEndpoint:167, Fabric8FlinkKubeClient (org.apache.flink.kubernetes.kubeclient)
lambda$createClusterClientProvider$0:96, KubernetesClusterDescriptor (org.apache.flink.kubernetes)
getClusterClient:-1, 1423491597 (org.apache.flink.kubernetes.KubernetesClusterDescriptor$$Lambda$201)
retrieve:145, KubernetesClusterDescriptor (org.apache.flink.kubernetes)
retrieve:66, KubernetesClusterDescriptor (org.apache.flink.kubernetes)
execute:75, AbstractSessionClusterExecutor (org.apache.flink.client.deployment.executors)
executeAsync:1905, StreamExecutionEnvironment (org.apache.flink.streaming.api.environment)
executeAsync:135, StreamContextEnvironment (org.apache.flink.client.program)
execute:76, StreamContextEnvironment (org.apache.flink.client.program)
execute:1782, StreamExecutionEnvironment (org.apache.flink.streaming.api.environment)
main:88, WindowJoin (org.apache.flink.streaming.examples.join)
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
call:-1, 69062746 (org.apache.flink.client.cli.CliFrontend$$Lambda$71)
runSecured:28, NoOpSecurityContext (org.apache.flink.runtime.security.contexts)
main:1132, CliFrontend (org.apache.flink.client.cli)





