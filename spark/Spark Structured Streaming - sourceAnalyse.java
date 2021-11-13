

SparkSession sparkSession = SparkSession.builder()
		.appName("SparkDemo"){//Builder.appName()
			config("spark.app.name", name)
		}
        .master(master){//Builder.master()
			config("spark.master", master){//SparkSession.Builder.config()
				// options = new HashMap[String,String]
				options += key -> value
				this
			}
		}
		.getOrCreate();{//SparkSession.Builder.getOrCreate()
			var session = activeThreadSession.get()
			// 判断该线程的ThreadLocal中是否已存在 SparkSession对象了;若存在则直接复用; 保证每个线程一个Spark?
			if ((session ne null) && !session.sparkContext.isStopped) {
				options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
				if (options.nonEmpty) {
				  logWarning("Using an existing SparkSession; some configuration may not take effect.")
				}
				return session
			}
			
			SparkSession.synchronized {
				session = defaultSession.get()
				val sparkContext = userSuppliedContext.getOrElse {// 若userSuppliedContext为空则进入以下函数代码
				  val sparkConf = new SparkConf()
				  options.foreach { case (k, v) => sparkConf.set(k, v) }
				  if (!sparkConf.contains("spark.app.name")) {// 若appName没给,就用UUID随机起个
					sparkConf.setAppName(java.util.UUID.randomUUID().toString)
				  }
				  SparkContext.getOrCreate(sparkConf){
					  
				  }
				  // Do not update `SparkConf` for existing `SparkContext`, as it's shared by all sessions.
				}
				
		
			}
		}
		
sparkSession.sparkContext().setLogLevel(logLevel.toString());



# 当执行 DataStreamWriter.start()后:

DataStreamWriter.start(){
	if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not ")
    }
    if (source == "memory") {
		  assertNotPartitioned("memory")
		  if (extraOptions.get("queryName").isEmpty) {
			throw new AnalysisException("queryName must be specified for memory sink")
		  }
		  val (sink, resultDf) = trigger match {
			case _: ContinuousTrigger =>
			  val s = new MemorySinkV2()
			  val r = Dataset.ofRows(df.sparkSession, new MemoryPlanV2(s, df.schema.toAttributes))
			  (s, r)
			case _ =>
			  val s = new MemorySink(df.schema, outputMode)
			  val r = Dataset.ofRows(df.sparkSession, new MemoryPlan(s))
			  (s, r)
		  }
		  val chkpointLoc = extraOptions.get("checkpointLocation")
		  val recoverFromChkpoint = outputMode == OutputMode.Complete()
		  val query = df.sparkSession.sessionState.streamingQueryManager.startQuery(extraOptions.get("queryName"),chkpointLoc,df,trigger = trigger)
		  resultDf.createOrReplaceTempView(query.name)
		  query
    }else if(source == "foreach"){
		  assertNotPartitioned("foreach")
		  val sink = new ForeachSink[T](foreachWriter)(ds.exprEnc)
		  df.sparkSession.sessionState.streamingQueryManager.startQuery(extraOptions.get("queryName"), extraOptions.get("checkpointLocation"),df,trigger = trigger)
    } else { //kafka/console 等都是进入这里
      val ds = DataSource.lookupDataSource(source, df.sparkSession.sessionState.conf)
      val disabledSources = df.sparkSession.sqlContext.conf.disabledV2StreamingWriters.split(",")
      val sink = ds.newInstance() match {
        case w: StreamWriteSupport if !disabledSources.contains(w.getClass.getCanonicalName) => w
        case _ =>
          val ds = DataSource(df.sparkSession,className = source, options = extraOptions.toMap, partitionColumns = normalizedParCols.getOrElse(Nil))
          ds.createSink(outputMode)
      }

      df.sparkSession.sessionState.streamingQueryManager.startQuery(extraOptions.get("queryName"),extraOptions.get("checkpointLocation"),
		df,extraOptions.toMap,sink,outputMode, useTempCheckpointLocation = source == "console",recoverFromCheckpointLocation = true, trigger = trigger){//StreamingQueryManger.startQuery()
			val query = createQuery(userSpecifiedName,userSpecifiedCheckpointLocation,df,extraOptions,sink,outputMode,useTempCheckpointLocation,triggerClock)
			activeQueriesLock.synchronized {
			  userSpecifiedName.foreach { name =>
				if (activeQueries.values.exists(_.name == name)) {
				  throw new IllegalArgumentException(s"Cannot start query with name $name as a query with that name is already active")
				}
			  }
			  if (activeQueries.values.exists(_.id == query.id)) {
				throw new IllegalStateException(s"Cannot start query with id ${query.id} as another query with same id")
			  }
			  activeQueries.put(query.id, query)
			}
			try {
			  // user and can run arbitrary codes, we must not hold any lock here
				query.streamingQuery.start(){//StreamExecution.start()
					logInfo(s"Starting $prettyIdString. Use $resolvedCheckpointRoot to store the query checkpoint.")
						*MicroBatchExecution - Starting [id = f0c2a729-2774-4b5b-9440-3972df09e7e5, runId = 6e965182-71dc-49fb-8aec-b0859daf4942]. Use file:/E:/studyAndTest/sparkTemp/07341fbd-3575-4057-9470-ed6861d63f4c to store the query checkpoint.
						
					queryExecutionThread.setDaemon(true)
					queryExecutionThread.start(){// 这次启动Driver端最重要的主任务处理线程: "stream execution thread for *"
						QueryExecutionThread.run(){
							sparkSession.sparkContext.setCallSite(callSite)
							runStream(){//StreamExecution.runStream()
								try {
								  sparkSession.sparkContext.setJobGroup(runId.toString, getBatchDescriptionString,interruptOnCancel = true)
								  sparkSession.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, id.toString)
								  if (sparkSession.sessionState.conf.streamingMetricsEnabled) {
									sparkSession.sparkContext.env.metricsSystem.registerSource(streamMetrics)
								  }
								  postEvent(new QueryStartedEvent(id, runId, name))
								  startLatch.countDown()
								  SparkSession.setActiveSession(sparkSession)
								  updateStatusMessage("Initializing sources")
								  // force initialization of the logical plan so that the sources can be created
								  logicalPlan
								  val sparkSessionForStream = sparkSession.cloneSession()
								  sparkSessionForStream.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
								  sparkSessionForStream.conf.set(SQLConf.CBO_ENABLED.key, "false")
								  offsetSeqMetadata = OffsetSeqMetadata(batchWatermarkMs = 0, batchTimestampMs = 0, sparkSessionForStream.conf)

								  if (state.compareAndSet(INITIALIZING, ACTIVE)) {
									// Unblock `awaitInitialization`
									initializationLatch.countDown()
									runActivatedStream(sparkSessionForStream)
									updateStatusMessage("Stopped")
								  } else {
									// `stop()` is already called. Let `finally` finish the cleanup.
								  }
								} catch {
									
								} finally`{
									queryExecutionThread.runUninterruptibly {
									  startLatch.countDown()
									  initializationLatch.countDown()
									  try {
										stopSources()
										state.set(TERMINATED)
										currentStatus = status.copy(isTriggerActive = false, isDataAvailable = false)
										sparkSession.sparkContext.env.metricsSystem.removeSource(streamMetrics)
										sparkSession.streams.notifyQueryTermination(StreamExecution.this)
										postEvent(new QueryTerminatedEvent(id, runId, exception.map(_.cause).map(Utils.exceptionString)))
										// Delete the temp checkpoint only when the query didn't fail
										if (deleteCheckpointOnStop && exception.isEmpty) {
										  val checkpointPath = new Path(resolvedCheckpointRoot)
										  try {
											val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
											fs.delete(checkpointPath, true)
										  } catch {
											case NonFatal(e) => logWarning(s"Cannot delete $checkpointPath", e)
										  }
										}
									  } finally {
										awaitProgressLock.lock()
										try {
										  awaitProgressLockCondition.signalAll()
										} finally {
										  awaitProgressLock.unlock()
										}
										terminationLatch.countDown()
									  }
									}
								}	
							}
						}
					}
					startLatch.await() 
				}
			  
			} catch {
			  case e: Throwable => activeQueriesLock.synchronized {activeQueries -= query.id}; throw e
			}
			query
	  }
    }
	
}.awaitTermination(){//StreamingQueryWrapper.awaitTermination()
	streamingQuery.awaitTermination(){
		// 对于微批处理模式;
		MicroBatchExecution.awaitTermination(){
			assertAwaitThread()
			terminationLatch.await()
			if (streamDeathCause != null) {
			  throw streamDeathCause
			}
		}
		
		// 对于实时处理模式?
	}
}



