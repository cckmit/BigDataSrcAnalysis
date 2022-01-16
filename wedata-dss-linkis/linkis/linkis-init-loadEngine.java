2021-12-30 19:52:59.153 [INFO ] [Linkis-Default-Scheduler-Thread-1       ] o.a.l.e.s.s.DefaultEngineConnResourceService (41) [info] - The file has no change in flinkEngineConn-v1.12.2, path: conf.zip
2021-12-30 19:52:59.153 [INFO ] [Linkis-Default-Scheduler-Thread-1       ] o.a.l.e.s.s.DefaultEngineConnResourceService (41) [info] - Ready to upload a new bmlResource for flinkEngineConn-v1.12.2. path: lib.zip
2021-12-30 19:53:00.931 [ERROR] [Linkis-Default-Scheduler-Thread-1       ] 

o.a.l.e.s.s.DefaultEngineConnResourceService (100) [apply] - error code（错误码）: 10905, 
Error message（错误信息）: URL /api/rest_j/v1/bml/upload request failed! 
ResponseBody is {"timestamp":1640865180851,"status":500,"error":"Internal Server Error","message":"","path":"/api/rest_j/v1/bml/upload"}..
		 org.apache.linkis.httpclient.exception.HttpClientResultException: errCode: 10905 ,
		 desc: URL /api/rest_j/v1/bml/upload request failed! 
		 ResponseBody is {"timestamp":1640865180851,"status":500,"error":"Internal Server Error","message":"","path":"/api/rest_j/v1/bml/upload"}.
		 ,ip: bdnode111.hjq.com ,port: 9103 ,serviceKind: linkis-cg-engineplugin
		 
	at org.apache.linkis.httpclient.dws.response.DWSResult$class.set(DWSResult.scala:54) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at org.apache.linkis.bml.response.BmlResult.set(BmlResult.scala:26) ~[linkis-bml-client-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.dws.DWSHttpClient$$anonfun$httpResponseToResult$2.apply(DWSHttpClient.scala:67) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.dws.DWSHttpClient$$anonfun$httpResponseToResult$2.apply(DWSHttpClient.scala:63) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at scala.Option.map(Option.scala:146) ~[scala-library-2.11.12.jar:?]
	at org.apache.linkis.httpclient.dws.DWSHttpClient.httpResponseToResult(DWSHttpClient.scala:63) ~[linkis-gateway-httpclient-support-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.AbstractHttpClient.responseToResult(AbstractHttpClient.scala:331) ~[linkis-httpclient-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.AbstractHttpClient.execute(AbstractHttpClient.scala:107) ~[linkis-httpclient-1.0.3.jar:1.0.3]
	at org.apache.linkis.httpclient.AbstractHttpClient.execute(AbstractHttpClient.scala:87) ~[linkis-httpclient-1.0.3.jar:1.0.3]
	at org.apache.linkis.bml.client.impl.HttpBmlClient.uploadResource(HttpBmlClient.scala:299) ~[linkis-bml-client-1.0.3.jar:1.0.3]
	
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService.org$apache$linkis$engineplugin$server$service$DefaultEngineConnResourceService$$uploadToBml(DefaultEngineConnResourceService.scala:59) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anonfun$org$apache$linkis$engineplugin$server$service$DefaultEngineConnResourceService$$refresh$2.apply(DefaultEngineConnResourceService.scala:139) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anonfun$org$apache$linkis$engineplugin$server$service$DefaultEngineConnResourceService$$refresh$2.apply(DefaultEngineConnResourceService.scala:135) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33) ~[scala-library-2.11.12.jar:?]
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186) ~[scala-library-2.11.12.jar:?]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService.org$apache$linkis$engineplugin$server$service$DefaultEngineConnResourceService$$refresh(DefaultEngineConnResourceService.scala:135) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2$$anonfun$apply$1$$anonfun$apply$mcV$sp$2.apply(DefaultEngineConnResourceService.scala:87) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2$$anonfun$apply$1$$anonfun$apply$mcV$sp$2.apply(DefaultEngineConnResourceService.scala:85) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at scala.collection.immutable.Map$Map1.foreach(Map.scala:116) ~[scala-library-2.11.12.jar:?]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2$$anonfun$apply$1.apply$mcV$sp(DefaultEngineConnResourceService.scala:85) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2$$anonfun$apply$1.apply(DefaultEngineConnResourceService.scala:83) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2$$anonfun$apply$1.apply(DefaultEngineConnResourceService.scala:83) ~[linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.common.utils.Utils$.tryCatch(Utils.scala:40) [linkis-common-1.0.3.jar:1.0.3]
	at org.apache.linkis.common.utils.Utils$.tryAndError(Utils.scala:97) [linkis-common-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2.apply(DefaultEngineConnResourceService.scala:83) [linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1$$anonfun$run$2.apply(DefaultEngineConnResourceService.scala:82) [linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33) [scala-library-2.11.12.jar:?]
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186) [scala-library-2.11.12.jar:?]
	at org.apache.linkis.engineplugin.server.service.DefaultEngineConnResourceService$$anon$1.run(DefaultEngineConnResourceService.scala:82) [linkis-engineconn-plugin-server-1.0.3.jar:1.0.3]
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511) [?:1.8.0_261]
	at java.util.concurrent.FutureTask.run(FutureTask.java:266) [?:1.8.0_261]
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180) [?:1.8.0_261]
	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149) [?:1.8.0_261]
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624) [?:1.8.0_261]
	at java.lang.Thread.run(Thread.java:748) [?:1.8.0_261]
	

// ps-publicservice 发生了oom; 

./linkis-ps-publicservice.log:2021-12-30 19:53:00.849 [WARN ] 
[qtp1111849105-25                        ] o.e.j.s.HttpChannel (600) [handleException] - /api/rest_j/v1/bml/upload org.springframework.web.util.NestedServletException: Request processing failed; nested exception is org.springframework.web.multipart.MultipartException: Failed to parse multipart servlet request; nested exception is java.lang.OutOfMemoryError: Java heap space

-rw-rw-r-- 1 bigdata bigdata 197M 12月 30 20:08 lib.zip

publicservice 会因为 这197M 而突然oom了?


cg-engineplugin

// curl enginePlugin/engineConn/refreshAll 后触发

DefaultEngineConnResourceService.refreshAll(){
	val refreshTask = new Runnable {override def run()}
	Utils.defaultScheduler.submit(refreshTask);{// 即上面的 Runnable.run()
	  engineConnBmlResourceGenerator
		// 从磁盘读取相关engineConn的 conf.zip,lib.zip ?
		.getEngineConnTypeListFromDisk(){}
		.foreach { engineConnType =>{
			engineConnBmlResourceGenerator
				.generate(engineConnType)
				.foreach { case (version, localize) =>{
					info(s"Try to initialize ${engineConnType}EngineConn-$version.")
					refresh(localize, engineConnType, version);{// DefaultEngineConnResourceService.refresh()
						localize.foreach { localizeResource =>{
							val resource = engineConnBmlResources.find(_.getFileName == localizeResource.fileName)
							if(resource.isEmpty) { //第一次,新建?
								// 先upload,再save()新增一行到mysql
								val bmlResource = uploadToBml(localizeResource);{//DefaultEngineConnResourceService.uploadToBml()
									inputStream: InputStream = localizeResource.getFileInputStream ; // 在 enginePlugin 这里就打开IO,这不太好吧, 
									val response = bmlClient.uploadResource(Utils.getJvmUser, localizeResource.fileName, inputStream);{// HttpBmlClient.uploadResource()
										val _inputStreams = new util.HashMap[String, InputStream]()
										_inputStreams.put("file", inputStream)
										val uploadAction = BmlUploadAction(null, _inputStreams)
										val result = dwsClient.execute(uploadAction);{//AbstractHttpClient.execute
											val action = prepareAction(requestAction.asInstanceOf[HttpAction])
											val req = prepareReq(action);
											val response = addAttempt();{executeRequest(req, Some(waitTime).filter(_ > 0))};{
												// 向gateway -> publicserver请求: 192.168.51.111:9001/api/rest_j/v1/bml/updateVersion
												val response = httpClient.execute(req);{// InternalHttpClient.execute()
													return this.doExecute(determineTarget(request), request, context);;{// InternalHttpClient.doExecute
														HttpRequestWrapper wrapper = HttpRequestWrapper.wrap(request, target);
														return this.execChain.execute(route, wrapper, localcontext, execAware);
													}
												}
											}
											responseToResult(response, action)
										}
									}
									val bmlResource = new BmlResource
								}
								engineConnBmlResourceDao.save(engineConnBmlResource);
							} else if(resource.exists(r => r.getFileSize != localizeResource.fileSize || r.getLastModified != localizeResource.lastModified)) {
								// 只要当  文件大小fileSize 或 修改时间LastModified 不同, 就触发依次 http上传更新; 
								Option.exists(p){
									!isEmpty && p(this.get)
								}
								// 当? 情况下? 上传资源,并update数据库;
								val bmlResource = uploadToBml(localizeResource, engineConnBmlResource.getBmlResourceId)
								engineConnBmlResourceDao.update(engineConnBmlResource)
							}else{
								// 文件存在, 且 版本没有变, 
								info(s"The file has no change in ${engineConnType}EngineConn-$version, path: " + localizeResource.fileName)
							}
						}}
					}
				}}
		}}
	}
}



// publicservice 服务的 BmlRestfulApi.updateVersion() 接口; 


ServletHandler.doFilter() -> WebMvcMetricsFilter.doFilterInternal() -> HttpServlet.service()
	-> FrameworkServlet.processRequest() -> StandardServletMultipartResolver.resolveMultipart(){
		return new StandardMultipartHttpServletRequest(request, this.resolveLazily);{
			parseRequest(request);{
				Collection<Part> parts = request.getParts();{
					if (!_parsed){ parse();{// MultiPartInputStreamParser.parse()
						MultiPart part = new MultiPart(name, filename);
						part.open();
						
						partInput = new FilterInputStream(_in)
						if (b >= 0 && b < byteBoundary.length && c == byteBoundary[b]){
							
						}else{
							part.write(13);
						}
						
					
					}}
					Collection<List<Part>> values = _parts.values();
					for (List<Part> o : values){
						parts.addAll(LazyList.getList(o, false));
					}
				}
				for (Part part : parts) {
					ContentDisposition disposition = ContentDisposition.parse(headerValue);
				}
			}
		}
	}


BmlRestfulApi.updateVersion(){
	ResourceTask resourceTask = taskService.createUpdateTask(resourceId, user, file, properties);{//TaskServiceImpl.createUpdateTask() 注意有多个方法,是sourceId的那个;
		String system = resourceDao.getResource(resourceId).getSystem();
		String newVersion = generateNewVersion(lastVersion);
		ResourceTask resourceTask = ResourceTask.createUploadTask(resourceId, user, properties);
		taskDao.insert(resourceTask);
		taskDao.updateState(resourceTask.getId(), TaskState.RUNNING.getValue(), new Date());
		LOGGER.info("Successful update task (成功更新任务 ) taskId:{}-resourceId:{} status is  {} .", resourceTask.getId(), resourceTask.getResourceId(), TaskState.RUNNING.getValue());
        // 调用 localFS/hdfsFs 上传; 
		versionService.updateVersion(resourceTask.getResourceId(), user, file, properties);{//VersionServiceImpl.updateVersion
			ResourceHelper resourceHelper = ResourceHelperFactory.getResourceHelper();
			// hdfs:///apps-data/bigdata/bml/20211230/0d8c3f39-b12a-4ec4-8b2a-3420a1f44b10_v000005
			String path = versionDao.getResourcePath(resourceId) + "_" + newVersion;
			long size = resourceHelper.upload(path, user, inputStream, stringBuilder, OVER_WRITE);{
				LocalResourceHelper.upload();
				// hdfs 的上传; 
				HdfsResourceHelper.upload(){
					FsPath fsPath = new FsPath(path);
					fileSystem = FSFactory.getFsByProxyUser(fsPath, user);
					fileSystem.init(new HashMap<String, String>());
					
					if (!fileSystem.exists(fsPath)) {
						FileSystemUtils.createNewFile(fsPath, user, true);
					}
					is0 = fileSystem.read(fsPath);
					outputStream = fileSystem.write(fsPath, overwrite);
				}
			}
			
		}
		taskDao.updateState(resourceTask.getId(), TaskState.SUCCESS.getValue(), new Date());
		
		if (result.isSuccess()){
			// UPDATE linkis_ps_bml_resources_task SET  state = #{state} , end_time = #{updateTime}, last_update_time = #{updateTime}
			taskDao.updateState(resourceTask.getId(), TaskState.SUCCESS.getValue(), new Date());
		}
	}
	message.data("resourceId",resourceId).data("version", resourceTask.getVersion()).data("taskId", resourceTask.getId());
}



