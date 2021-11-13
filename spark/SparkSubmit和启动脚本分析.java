
1. spark-submit 脚本

```
if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

export PYTHONHASHSEED=0
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"

```


2. spark-class 脚本

```
. "${SPARK_HOME}"/bin/load-spark-env.sh
SPARK_JARS_DIR="${SPARK_HOME}/jars"

CMD=()
while IFS= read -d '' -r ARG; do
  CMD+=("$ARG")
done < <(build_command "$@")

COUNT=${#CMD[@]}
LAST=$((COUNT - 1))
LAUNCHER_EXIT_CODE=${CMD[$LAST]}

CMD=("${CMD[@]:0:$LAST}")
exec "${CMD[@]}"
```





3. SparkSubmit源码分析: 以SparkOn YarnCluster模式为例;

//java -cp /home/app/stream/spark/spark-2.2.0-hdp2.7/conf/:/home/app/stream/spark/spark-2.2.0-hdp2.7/jars/*:/home/app/hadoop/hadoop-2.7.1/etc/hadoop/ -Xdebug -Xrunjdwp:transport=dt_socket,server=n,suspend=n,address=192.168.51.1:35056 org.apache.spark.deploy.SparkSubmit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi --num-executors 2  /home/app/stream/spark/spark-2.2.0-hdp2.7/examples/jars/spark-examples_2.11-2.2.0.jar 10 
//java -cp /home/app/stream/spark/spark-2.2.0-hdp2.7/conf/:/home/app/stream/spark/spark-2.2.0-hdp2.7/jars/*:/home/app/hadoop/hadoop-2.7.1/etc/hadoop/ org.apache.spark.deploy.SparkSubmit --master yarn --deploy-mode cluster --class org.apache.spark.examples.SparkPi --num-executors 2  /home/app/stream/spark/spark-2.2.0-hdp2.7/examples/jars/spark-examples_2.11-2.2.0.jar 10 


```
SparkSubmit: main() -> submit() -> doRunMain() -runMain()
    * 反射创建Application(MyWordCount.scala)的实例对象并执行其main()
    * MyWordCount.main(): 创建SparkContext 并执行读写和处理逻辑
        - new SparkContext()
            * SparkConfig.validateSettings() 初始化
            * createSparkEnv():SparkEnv, 环境初始化
            * SparkUI.createLiveUI(): SparkUI, 启动Jetty服务
            * SparkHadoopUtil.get.newConfiguration(conf) 创建hadoopConfiguration
            * SparkContext.createTaskScheduler()创建(TaskSchedulerImpl, taskScheduler)
                - TaskSchedulerImpl.initialize() 创建rootPool 和设置FIFO默认的调度算法
            * 创建 DAGScheduler
            * 执行TaskSchedulerImpl.start(): -> SparkDeployStandaloneSchedulerBackend.start()
            * BlockManager.initialize()初始化 存储引擎
        - SparkContext.textFile()
            * sc.hadoopFile():RDD[(K,V)]
                - 用SerializableWriteable封装 hadoopConfiguration, 并传入广播变量;
                - 利用BroadcastManager.newBroadcast()创建广播;
                -  为key=LongWriteable,value=Text new一个HadooopRDD实例;
                * HadoopRDD[K,V](sc,hadoopConfBroadcast, initLoalJobConfFuncOpt, inputFormatClass, keyClass,valueClass, minPartitions) extands RDD[(K,V)] 
                
        - lines.flatMap(_split(" ")
            * 执行RDD.map()方法, 实际-> MapPartitionsRDD.compute()方法;
        - words.reduceByKey()
SparkSubmit.main(){
	
	val appArgs = new SparkSubmitArguments(args)
	if (appArgs.verbose) {
		printStream.println(appArgs)
	}
	
	appArgs.action match {
		case SparkSubmitAction.SUBMIT => submit(appArgs){ //SparkSubmit.submit()
			val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)
			SparkSubmit.doRunMain(){
				if (args.proxyUser != null) {
					
				}else{
					SparkSubmit.runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose){
						// 设置类加载器: 若driver.userClassPathFirst=true,就先用子ClassLoader
						val loader = 
							if (sysProps.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
								new ChildFirstURLClassLoader(new Array[URL](0),currentThread)
							}else{new MutableURLClassLoader(new Array[URL](0),currentThread)}
						Thread.currentThread.setContextClassLoader(loader)
						for (jar <- childClasspath) {
							addJarToClasspath(jar, loader)
						}
						
						//childMainClass 为 RestSubmissionClient/ spark.deploy.Client/ yarn.Client 中的一个;
						val mainMethod = Utils.classForName(childMainClass).getMethod("main", new Array[String](0).getClass)
						mainMethod.invoke(null, childArgs.toArray){
							// 当为Spark On Yarn运行模式时
							yarn.Client.main(argStrings){
								System.setProperty("SPARK_YARN_MODE", "true")
								val args = new ClientArguments(argStrings)
								new Client(args, sparkConf).run( // yarn.Client.run()
									this.appId = submitApplication(){ //Client.submitApplication()
										launcherBackend.connect()
										
										yarnClient.init(yarnConf)
										yarnClient.start()
										
										val newApp = yarnClient.createApplication(){// YarnClientImpl.createApplication()
											GetNewApplicationResponse newApp = getNewApplication(){
												GetNewApplicationRequest request=Records.newRecord(GetNewApplicationRequest.class);
												return rmClient.getNewApplication(request){ // ApplicationClientProtocolPBClientImpl.getNewApplication()
													GetNewApplicationRequestProto requestProto =((GetNewApplicationRequestPBImpl) request).getProto();
													
													GetNewApplicationResponseProto response = proxy.getNewApplication(null,requestProto){ //ApplicationClientProtocolPB.getNewApplication()
														// 解释proxy的源码
														new ApplicationClientProtocolPBClientImpl(){
															RPC.setProtocolEngine(conf, ApplicationClientProtocolPB.class,ProtobufRpcEngine.class){// RPC.setProtocolEngine(conf,protocol,engine)
																conf.setClass(ENGINE_PROP+"."+protocol.getName(), engine, RpcEngine.class);
															}
															// 此proxy类,最终实例应该是 ProtobufRpcEngine
															this.proxy[ApplicationClientProtocolPB] = RPC.getProxy(ApplicationClientProtocolPB.class, clientVersion, addr, conf){
																return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy(){
																	RpcEngine engine = PROTOCOL_ENGINES[Map<Class<?>,RpcEngine> PROTOCOL_ENGINES].get(protocol);
																	if (engine == null) {
																	  Class<?> impl = conf.getClass(ENGINE_PROP+"."+protocol.getName(),WritableRpcEngine.class);
																	  engine = (RpcEngine)ReflectionUtils.newInstance(impl, conf);
																	  PROTOCOL_ENGINES.put(protocol, engine);  // 保证每个class都是单例模式的 RpcEngine
																	}
																	return engine;
	
																}
															}
														}
														
														ApplicationClientProtocolPB.getNewApplication(){
															ProtobufRpcEngine.invoke(Object proxy, Method method, Object[] args){
																// method = ApplicationClientProtocol.BlokingInterface.getNewApplication(); 由远程端RM执行并返回;
																RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
																// 核心2: 执行远程的RPC方法;
																final RpcResponseWrapper val=(RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER, new RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId,fallbackToSimpleAuth);
																Message returnMessage = prototype.newBuilderForType()
																return returnMessage;
															}
														}
													}
													
													return new GetNewApplicationResponsePBImpl(response);
												}
											}
											return new YarnClientApplication(newApp, context);
										}
										
										verifyClusterResources(newAppResponse)
										
										/** 核心2之: 上传 sparkStaging资源 并准备AM的启动命令;
										*	1. prepare本地资源: 先将相关jars/文件等,copy到本地机器(/tmp/spark../下); 再将这些文件用FileUtil.copy()拷贝到hdfs的/.sparkStaging上;
										* 	2. 构建AppMaster的JAVA命令: commands= prefixEnv JAVA_HOME java -server javaOpts amArgs "1> LOG_DIR/stdout" "2> LOG_DIR/stderr" 
										*/
										val containerContext = createContainerLaunchContext(newAppResponse){//Client.createContainerLaunchContext(GetNewApplicationResponse)
											logInfo("Setting up container launch context for our AM")
												* INFO Client: Setting up container launch context for our AM
											val appStagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId))
											val launchEnv = setupLaunchEnv(appStagingDirPath, pySparkArchives)
											
											// 将Local的SparkStage资源上传到Hdfs上;
											val localResources = prepareLocalResources(appStagingDirPath, pySparkArchives){
												logInfo("Preparing resources for our AM container")
												val fs = destDir.getFileSystem(hadoopConf)
												
												FileSystem.mkdirs(fs, destDir, new FsPermission(STAGING_DIR_PERMISSION))
												if (sparkArchive.isDefined) {
													distribute(Utils.resolveURI(archive).toString,resType = LocalResourceType.ARCHIVE);
												}else{
													sparkConf.get(SPARK_JARS) match { //spark.yarn.jars 中指定的;
														case Some(jars) =>{
															jars.foreach { jar =>
																val path = getQualifiedLocalPath(Utils.resolveURI(jar), hadoopConf)
																val pathFs = FileSystem.get(path.toUri(), hadoopConf)
																pathFs.globStatus(path).filter(_.isFile()).foreach { entry =>
																	val uri = entry.getPath().toUri()
																	distribute(uri.toString(), targetDir = Some(LOCALIZED_LIB_DIR))
																}
															}
														}
															
														case None => {
															val jarsDir = new File(YarnCommandBuilderUtils.findJarsDir(sparkConf.getenv("SPARK_HOME")))
															val jarsArchive = File.createTempFile(LOCALIZED_LIB_DIR,".zip",new File(Utils.getLocalDir(sparkConf)))
															val jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))
															jarsDir.listFiles().foreach { 
																if (f.isFile && f.getName.toLowerCase(Locale.ROOT).endsWith(".jar") && f.canRead) {
																	Files.copy(f, jarsStream) // 逐条把 $JAVA_HOME/jars/下的 jar复制到 jarsArchive文件中;
																	jarsStream.closeEntry()
																}
															}
															
															distribute(jarsArchive.toURI.getPath,resType = LocalResourceType.ARCHIVE){ //Client.distribute(path: String,destName,targetDir)
																val localURI = Utils.resolveURI(trimmedPath)
																if (localURI.getScheme != LOCAL_SCHEME) {
																	val localPath = getQualifiedLocalPath(localURI, hadoopConf)
																	val destPath = copyFileToRemote(destDir, localPath, replication, symlinkCache){// Client.copyFileToRemote()
																		if (force || !compareFs(srcFs, destFs)) {
																			logInfo(s"Uploading resource $srcPath -> $destPath")
																			FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf){ //fs.FileUtil.copy()
																				return copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf){
																					dst = checkDest(src.getName(), dstFS, dst, overwrite);
																					if (srcStatus.isDirectory()) {
																						FileStatus contents[] = srcFS.listStatus(src);
																						for (int i = 0; i < contents.length; i++) {
																							copy(srcFS, contents[i], dstFS,new Path(dst, contents[i].getPath().getName()),deleteSource, overwrite, conf);
																						}
																					}else{
																						InputStream in = srcFS.open(src);
																						OutputStream out = dstFS.create(dst, overwrite);
																						IOUtils.copyBytes(in, out, conf, true);
																					}
																				}
																			}
																		}
																	}
																	
																	val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
																	distCacheMgr.addResource(destFs, hadoopConf, destPath, localResources, resType, linkname, statCache)
																	(false, linkname)
																}else{
																	(true, trimmedPath)
																}
															}
														}
														
														jarsArchive.delete() // 将jarArchive文件删除掉;
													}
												}
												
											}
											
											val tmpDir = new Path(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)
											
											// Include driver-specific java options if we are launching a driver
											if (isClusterMode){
												sparkConf.get(DRIVER_JAVA_OPTIONS).foreach { opts =>
													javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
												}
												val libraryPaths = Seq(sparkConf.get(DRIVER_LIBRARY_PATH),
												sys.props.get("spark.driver.libraryPath")).flatten
												if(libraryPaths.nonEmpty) {
													prefixEnv = Some(getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(libraryPaths)))
												}
												if(sparkConf.get(AM_JAVA_OPTIONS).isDefined) {
													logWarning(s"${AM_JAVA_OPTIONS.key} will not take effect in cluster mode")
												}
											}
											
											// 当为Cluster模式时, AM是ApplicatonMaster, 当为Client模式时, AM=ExecutorLauncher;
											val amClass =
												if (isClusterMode) {
													Utils.classForName("org.apache.spark.deploy.yarn.ApplicationMaster").getName
												}else{
													Utils.classForName("org.apache.spark.deploy.yarn.ExecutorLauncher").getName
												}
											
											// Command for the ApplicationMaster
											val commands = prefixEnv ++ Seq(Environment.JAVA_HOME.$$() + "/bin/java", "-server") ++ javaOpts ++ 
												amArgs ++ Seq("1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout", "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
											
											amContainer.setCommands(commands.map(s => if (s == null) "null" else s).toList.asJava)
											val securityManager = new SecurityManager(sparkConf)
											amContainer.setApplicationACLs(YarnSparkHadoopUtil.getApplicationAclsForYarn(securityManager).asJava)
											
											amContainer[ContainerLaunchContext] // 其封装了 commands来返回;
										}
										
										val appContext = createApplicationSubmissionContext(newApp, containerContext){
											val appContext = newApp.getApplicationSubmissionContext
											appContext.setAMContainerSpec(containerContext)
											sparkConf.get(APPLICATION_TAGS).foreach { tags =>
												appContext.setApplicationTags(new java.util.HashSet[String](tags.asJava))
											}
											sparkConf.get(MAX_APP_ATTEMPTS) match { //spark.yarn.maxAppAttempts
												case Some(v) => appContext.setMaxAppAttempts(v)
											}
											
											capability.setMemory(amMemory + amMemoryOverhead)
											appContext[ApplicationSubmissionContext] // 封装了AM的运行参数
										}
										logInfo(s"Submitting application $appId to ResourceManager")
										
										// 执行 AM的启动脚本, 并固定间隔(200ms)拉下Yarn任务的状态;
										yarnClient.submitApplication(appContext){ // YarnClientImpl.submitApplication(ApplicationSubmissionContext appContext)
											SubmitApplicationRequest request=Records.newRecord(SubmitApplicationRequest.class);
											request.setApplicationSubmissionContext(appContext);
											
											rmClient.submitApplication(request){ // ApplicationClientProtocolPBClientImpl.submitApplication()
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
											
											EnumSet<YarnApplicationState> waitingStates=EnumSet.of(YarnApplicationState.NEW,YarnApplicationState.NEW_SAVING,YarnApplicationState.SUBMITTED);
											while(true){
												YarnApplicationState state = getApplicationReport(applicationId).getYarnApplicationState();
												if (!waitingStates.contains(state)) { //waitingStates=[NEW/NEW_SAVING/SUBMITTED]
													LOG.info("Submitted application " + applicationId);
													break;
												}
												
												// asyncApiPollTimeoutMillis由client.application-client-protocol.poll-timeout-ms设定,默认-1;
												if ((asyncApiPollTimeoutMillis >= 0)&& elapsedMillis>=asyncApiPollTimeoutMillis){ 
													throw new YarnException("Timed out while waiting for application " + applicationId + " to be submitted successfully");
												}
												
												if (++pollCount % 10 == 0) { // 每10个poll(State) 打印一次状态;
													LOG.info("Application submission is not finished, submitted application "+applicationId +" is still in " + state);
												}
												
												Thread.sleep(submitPollIntervalMillis); //先由client.application-client-protocol.poll-interval-ms决定,再由client.app-submission.poll-interval配置值,默认200(毫秒),
											}
											
											return applicationId;
										}
										
										launcherBackend.setAppId(appId.toString)
										reportLauncherState(SparkAppHandle.State.SUBMITTED)
										appId
									}
									
									// 获取相应Yarn状态,若为异常停止,则跑异常;
									if (!launcherBackend.isConnected() && fireAndForget) {
										val state = report.getYarnApplicationState
										if (state == YarnApplicationState.FAILED || state == YarnApplicationState.KILLED) {
											throw new SparkException(s"Application $appId finished with status: $state")
										}
									}else {
										val (yarnApplicationState, finalApplicationStatus) = monitorApplication(appId)
										if (yarnApplicationState == YarnApplicationState.FAILED ||
											finalApplicationStatus == FinalApplicationStatus.FAILED) {
											throw new SparkException(s"Application $appId finished with failed status")
										}
										if (yarnApplicationState == YarnApplicationState.KILLED ||
											finalApplicationStatus == FinalApplicationStatus.KILLED) {
											throw new SparkException(s"Application $appId is killed")
										}
										if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
											throw new SparkException(s"The final status of application $appId is undefined")
										}
									}
									
								)
							}
							
							// 当为Standalone运行模式时
							RestSubmissionClient.main(){
								
							}
							
							// 当为spark.Client运行模式时:
							spark.deploy.Client.main(){
								
							}
						}
						
					}
				}
			}
		}
		
		case SparkSubmitAction.KILL => kill(appArgs)
		case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
	}
}		
		
```















