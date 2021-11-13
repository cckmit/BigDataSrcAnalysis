




// "SparkUI-40"线程
WebUI.attachPage(page: WebUIPage){
    val pagePath = "/" + page.prefix
	
	// 由Page的实现类完成相关请求: 
	/**
	* 	history &master:	HistoryPage,ApplicationPage, MasterPage, 
	* 	worker:		LogPage, WorkerPage, EnvironmentPage, 
	*	exec:		ExecutorsPage, ExecutorThreadDumpPage
	* 	jobs: 		ExecutorPage, AllJobsPage, AllStagesPage, JobPage, PoolPage, StagePage
	* 	storage: 	RDDPage, StoragePage
	* 	sql: 		AllExecutionsPage, ExecutionPage
	*	streaming:	BatchPage, StreamingPage
	*/
	val servletParams = (request: HttpServletRequest) => page.render(request){
		
		EnvironmentPage.render(request){
			//相关引用解释:  定义Environment的url路径为" /environment,绑定到EnvironmentPage对象;
			{	
				class EnvironmentTab(parent: SparkUI) extends SparkUITab(parent, "environment"){
					attachPage(new EnvironmentPage(this))
				}
			}
	
			val runtimeInformationTable = UIUtils.listingTable(propertyHeader, jvmRow, listener.jvmInformation, fixedWidth = true)
			val sparkPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow,Utils.redact(parent.conf, listener.sparkProperties), fixedWidth = true)

			val systemPropertiesTable = UIUtils.listingTable(propertyHeader, propertyRow, listener.systemProperties, fixedWidth = true)
			val classpathEntriesTable = UIUtils.listingTable(classPathHeaders, classPathRow, listener.classpathEntries, fixedWidth = true)
			
			val content =
			  <span>
				<h4>Runtime Information</h4> {runtimeInformationTable}
				<h4>Spark Properties</h4> {sparkPropertiesTable}
				<h4>System Properties</h4> {systemPropertiesTable}
				<h4>Classpath Entries</h4> {classpathEntriesTable}
			  </span>

			UIUtils.headerSparkPage("Environment", content, parent)	
		}
		
		// 显示所有 Stages; 最核心的duration指标 = Tasks中最早提交时间 - DAG收到CompletionEvent的时间 
		AllStagesPage.render(request){
			//引用解释: 在/stages路径下创建3个字路径: /stage -> StagePage; /pool -> PoolPage
			{
				class StagesTab(parent: SparkUI) extends SparkUITab(parent, "stages"){
					attachPage(new AllStagesPage(this))
					attachPage(new StagePage(this))
					attachPage(new PoolPage(this))//有关可调度池的信息, 仅在FAIR调度模式可用;
				}
			}
			
			
			listener.synchronized {
			  val activeStages = listener.activeStages.values.toSeq
			  val pendingStages = listener.pendingStages.values.toSeq
			  val completedStages = listener.completedStages.reverse
			  val numCompletedStages = listener.numCompletedStages
			  val failedStages = listener.failedStages.reverse
			  val numFailedStages = listener.numFailedStages
			  val subPath = "stages"

			  val activeStagesTable =
				new StageTableBase(request, activeStages, "active", "activeStage", parent.basePath, subPath,
				  parent.progressListener, parent.isFairScheduler,
				  killEnabled = parent.killEnabled, isFailedStage = false)
			  val pendingStagesTable =
				new StageTableBase(request, pendingStages, "pending", "pendingStage", parent.basePath,
				  subPath, parent.progressListener, parent.isFairScheduler,
				  killEnabled = false, isFailedStage = false)
			  
			  // 主要显示这里
			  val completedStagesTable =
				new StageTableBase(request, completedStages, "completed", "completedStage", parent.basePath,
				  subPath, parent.progressListener, parent.isFairScheduler,
				  killEnabled = false, isFailedStage = false){//new StageTableBase()构造函数
					val toNodeSeq = try {
							val pagedTable= new StagePagedTable(
							  stages,
							  tableHeaderID,
							  stageTag,
							  basePath,
							  subPath,
							  progressListener,
							  isFairScheduler,
							  killEnabled,
							  currentTime,
							  stagePageSize,
							  stageSortColumn,
							  stageSortDesc,
							  isFailedStage,
							  parameterOtherTable
							){//new StagePagedTable()构造函数
								val parameterPath = UIUtils.prependBaseUri(basePath) + s"/$subPath/?" +parameterOtherTable.mkString("&")

								override val dataSource = new StageDataSource(
									stages,
									listener,
									currentTime,
									pageSize,
									sortColumn,
									desc
								){// StageDataSource()构造函数
									private val data = stages
										.map(stageInfo =>{
											stageRow(stageInfo){// StageDataSource.stageRow(s: StageInfo): StageTableRowData 
												val stageDataOption = listener.stageIdToData.get((s.stageId, s.attemptId))
												if (stageDataOption.isEmpty) {
													return new MissingStageTableRowData(s, s.stageId, s.attemptId)
												}
												val stageData = stageDataOption.get
												val description = stageData.description
												val formattedSubmissionTime = s.submissionTime match {
												  case Some(t) => UIUtils.formatDate(new Date(t))
												  case None => "Unknown"
												}
												
												/* 计算该Stage的计算用时: Duration = 其最早Task提交时间 - DAGScheduler收到CompletionEvent的时间;
												* 
												*/
												
												//在"dag-scheduler-loop"线程中,但收到CompletionEvent时,调用 DAGScheduler.markStageAsFinished()
												val finishTime = s.completionTime.getOrElse(currentTime){
													//解释completionTime生成的时间:
													DAGScheduler.doOnReceive(evnent){
														case CompletionEvent =>dagScheduler.handleTaskCompletion(completion){
															event.reason match {
																case Success => task match {case rt: ResultTask[_, _] => {
																	val resultStage = stage.asInstanceOf[ResultStage]
																	resultStage.activeJob match {
																		if (!job.finished(rt.outputId)) {
																			if (job.numFinished == job.numPartitions) {
																				markStageAsFinished(resultStage){
																					if (errorMessage.isEmpty) {
																						// 获取SystemClock系统时间的当前时间, 并赋给ResultStage的completionTime中;
																						stage[ResultStage].latestInfo[StageInfo].completionTime = Some(clock.getTimeMillis())
																					}
																				}
																			}
																		}
																	}
																}}
															}
														}
													}
												}
												val taskLaunchTimes = stageData.taskData.values.map(_.taskInfo.launchTime).filter(_ > 0)
												val duration: Option[Long] =
												  if (taskLaunchTimes.nonEmpty) {
													//取该Stage的所有Tasks中,最早提交任务的时间为整个Stage的 startTime
													val startTime = taskLaunchTimes.min
													if (finishTime > startTime) {
													  Some(finishTime - startTime)
													} else {
													  Some(currentTime - startTime)
													}
												  } else {
													None
												  }
												val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")

												val inputRead = stageData.inputBytes
												val inputReadWithUnit = if (inputRead > 0) Utils.bytesToString(inputRead) else ""
												val outputWrite = stageData.outputBytes
												val outputWriteWithUnit = if (outputWrite > 0) Utils.bytesToString(outputWrite) else ""
												val shuffleRead = stageData.shuffleReadTotalBytes
												val shuffleReadWithUnit = if (shuffleRead > 0) Utils.bytesToString(shuffleRead) else ""
												val shuffleWrite = stageData.shuffleWriteBytes
												val shuffleWriteWithUnit = if (shuffleWrite > 0) Utils.bytesToString(shuffleWrite) else ""
												
												//AllStage表中一行记录: 表示一个Stage的记录;
												new StageTableRowData(
												  s,
												  stageDataOption,
												  s.stageId,					// Stages-"Stage Id"
												  s.attemptId,
												  stageData.schedulingPool,
												  description,					// Stages-Description
												  s.submissionTime.getOrElse(0),
												  formattedSubmissionTime,		//Stages-"Submitted"
												  duration.getOrElse(-1),		// duration= Tasks中最早提交时间 - DAG收到CompletionEvent的时间 
												  formattedDuration,			//Stages-"Duration"	; = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
												  inputRead,
												  inputReadWithUnit,
												  outputWrite,
												  outputWriteWithUnit,
												  shuffleRead,
												  shuffleReadWithUnit,
												  shuffleWrite,
												  shuffleWriteWithUnit
												)
												
											};
										})
										.sorted(ordering(sortColumn, desc))
								}
							}
							
							pagedTable.table(page)
						} catch {}
				  }
			  
			  val failedStagesTable =
				new StageTableBase(request, failedStages, "failed", "failedStage", parent.basePath, subPath,
				  parent.progressListener, parent.isFairScheduler,
				  killEnabled = false, isFailedStage = true)

			  // For now, pool information is only accessible in live UIs
			  val pools = sc.map(_.getAllPools).getOrElse(Seq.empty[Schedulable])
			  val poolTable = new PoolTable(pools, parent)
			  
			  val shouldShowActiveStages = activeStages.nonEmpty
			  val shouldShowPendingStages = pendingStages.nonEmpty
			  val shouldShowCompletedStages = completedStages.nonEmpty
			  val shouldShowFailedStages = failedStages.nonEmpty

			  val completedStageNumStr = if (numCompletedStages == completedStages.size) {
				s"$numCompletedStages"
			  } else {
				s"$numCompletedStages, only showing ${completedStages.size}"
			  }
			  

			  var content = summary ++
				{
				  if (sc.isDefined && isFairScheduler) {
					<h4>{pools.size} Fair Scheduler Pools</h4> ++ poolTable.toNodeSeq
				  } else {
					Seq[Node]()
				  }
				}
			  if (shouldShowActiveStages) {
				content ++= <h4 id="active">Active Stages ({activeStages.size})</h4> ++
				activeStagesTable.toNodeSeq
			  }
			  if (shouldShowPendingStages) {
				content ++= <h4 id="pending">Pending Stages ({pendingStages.size})</h4> ++
				pendingStagesTable.toNodeSeq
			  }
			  if (shouldShowCompletedStages) {
				content ++= <h4 id="completed">Completed Stages ({completedStageNumStr})</h4> ++
				completedStagesTable.toNodeSeq
			  }
			  if (shouldShowFailedStages) {
				content ++= <h4 id ="failed">Failed Stages ({numFailedStages})</h4> ++
				failedStagesTable.toNodeSeq
			  }
			  UIUtils.headerSparkPage("Stages for All Jobs", content, parent)
			}
		}
		
		// 显示单个 Stage: 
		StagePage.render(request){
			//引用解释: 在/stages路径下创建3个字路径: /stage -> StagePage; /pool -> PoolPage
			{
				class StagesTab(parent: SparkUI) extends SparkUITab(parent, "stages"){
					attachPage(new AllStagesPage(this))
					attachPage(new StagePage(this))
					attachPage(new PoolPage(this))//有关可调度池的信息, 仅在FAIR调度模式可用;
				}
			}
			
			progressListener.synchronized {

			  val taskPage = Option(parameterTaskPage).map(_.toInt).getOrElse(1)
			  val taskSortColumn = Option(parameterTaskSortColumn).map { sortColumn =>
				UIUtils.decodeURLParameter(sortColumn)
			  }.getOrElse("Index")
			  val taskSortDesc = Option(parameterTaskSortDesc).map(_.toBoolean).getOrElse(false)
			  val taskPageSize = Option(parameterTaskPageSize).map(_.toInt).getOrElse(100)
			  val taskPrevPageSize = Option(parameterTaskPrevPageSize).map(_.toInt).getOrElse(taskPageSize)

			  val stageId = parameterId.toInt
			  val stageAttemptId = parameterAttempt.toInt
			  val stageDataOption = progressListener.stageIdToData.get((stageId, stageAttemptId))

			  val stageHeader = s"Details for Stage $stageId (Attempt $stageAttemptId)"
			  if (stageDataOption.isEmpty) {
				val content =
				  <div id="no-info">
					<p>No information to display for Stage {stageId} (Attempt {stageAttemptId})</p>
				  </div>
				return UIUtils.headerSparkPage(stageHeader, content, parent)

			  }
			  if (stageDataOption.get.taskData.isEmpty) {
				val content =
				  <div>
					<h4>Summary Metrics</h4> No tasks have started yet
					<h4>Tasks</h4> No tasks have started yet
				  </div>
				return UIUtils.headerSparkPage(stageHeader, content, parent)
			  }

			  val stageData = stageDataOption.get
			  val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)
			  val numCompleted = stageData.numCompleteTasks
			  val totalTasks = stageData.numActiveTasks +stageData.numCompleteTasks + stageData.numFailedTasks
			  val totalTasksNumStr = if (totalTasks == tasks.size) {s"$totalTasks"} else {s"$totalTasks, showing ${tasks.size}"}

			  val allAccumulables = progressListener.stageIdToData((stageId, stageAttemptId)).accumulables
			  val externalAccumulables = allAccumulables.values.filter { acc => !acc.internal }
			  val hasAccumulators = externalAccumulables.nonEmpty
			  
			  val dagViz = UIUtils.showDagVizForStage(
				stageId, operationGraphListener.getOperationGraphForStage(stageId))

			  val accumulableHeaders: Seq[String] = Seq("Accumulable", "Value")
			  def accumulableRow(acc: AccumulableInfo): Seq[Node] = {
				(acc.name, acc.value) match {
				  case (Some(name), Some(value)) => <tr><td>{name}</td><td>{value}</td></tr>
				  case _ => Seq.empty[Node]
				}
			  }
			  val accumulableTable = UIUtils.listingTable(
				accumulableHeaders,
				accumulableRow,
				externalAccumulables.toSeq)

			  val page: Int = {
				if (taskPageSize <= taskPrevPageSize) {
				  taskPage
				} else {
				  1
				}
			  }
			  val currentTime = System.currentTimeMillis()
			  
			  
			  val (taskTable, taskTableHTML) = try {
				val _taskTable = new TaskPagedTable(
				  parent.conf,
				  UIUtils.prependBaseUri(parent.basePath)+ s"/stages/stage?id=${stageId}&attempt=${stageAttemptId}",
				  tasks,
				  hasAccumulators,
				  stageData.hasInput,
				  stageData.hasOutput,
				  stageData.hasShuffleRead,
				  stageData.hasShuffleWrite,
				  stageData.hasBytesSpilled,
				  currentTime,
				  pageSize = taskPageSize,
				  sortColumn = taskSortColumn,
				  desc = taskSortDesc,
				  executorsListener = executorsListener
				){//TaskPagedTable()构造函数
					override val dataSource: TaskDataSource = new TaskDataSource(){
						private val data = tasks
							.map(task=>taskRow(task){// StagePage.TaskDataSource.taskRow(taskData: TaskUIData): TaskTableRowData
								val info = taskData.taskInfo
								val metrics = taskData.metrics
								
								// 算完的Task: duration=TaskMetrics.executorRunTime == RDD运行时间()== func(context,rdd.iterator())用时== (taskFinish - taskStart) -task.executorDeserializeTime
								val duration = taskData.taskDuration(){//StagePage.TaskData.taskDuration: Option[Long]
									if (taskInfo.status == "RUNNING") {
										Some(_taskInfo.timeRunning(System.currentTimeMillis)) //System.currentTimeMillis - launchTime
									} else {//一般这里;
										_metrics.map(_.executorRunTime){
											{
												//创建TaskMerics的线程:"dag-scheduler-loop": 只创建tm但并没有为TaskMerics._executorRunTime设置值(其值对象LongAccumulator初始化值为0L)
												DAGScheduler.doOnReceive(event){
													case completion: CompletionEvent => dagScheduler.handleTaskCompletion(completion){
														val taskMetrics: TaskMetrics =
														  if (event.accumUpdates.nonEmpty) {
															try {
																// 该方法, 完成各种计算;
																TaskMetrics.fromAccumulators(event.accumUpdates){//TaskMetrics.fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics
																	//accums这个ArrayBuffer中,一般3个元素: executorDeserializeTime, resultSize, updateBlockStatuses
																	val tm = new TaskMetrics
																	for (acc <- accums) {
																	  val name = acc.name
																	  // TaskMetrics.nameToAccums:HashMap[String,AccumulatorV2], 里面封装了24个计算key-value
																	  if (name.isDefined && tm.nameToAccums.contains(name.get)) {
																		val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
																		tmAcc.metadata = acc.metadata
																		
																		// merge归并,就是传参进来acc的sum和count, 加总到tmAcc自己的sum和count中; 
																		tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]]){//AccumulatorV2.merge(other:AccumulatorV2)
																			other match {
																				case o: LongAccumulator => 
																					_sum += o.sum
																					_count += o.count
																					
																				case _ => throw new UnsupportedOperationException()
																			}
																		}
																	  } else {
																		tm.externalAccums += acc
																	  }
																	}
																	tm
																}
															} catch {
															  case NonFatal(e) =>
																logError(s"Error when attempting to reconstruct metrics for task $taskId", e)
																null
															}
														  } else {
															null
														  }
														
														listenerBus.post(SparkListenerTaskEnd(stageId, task.stageAttemptId, taskType, event.reason, event.taskInfo, taskMetrics))
													
													}
												}
												
												//2. 在"Executor task for *"线程中, Executor.TaskRunner.run()方法中设置各Runtime值
												TaskRunner.run(){
													// do task run...
													try{
														taskStart = System.currentTimeMillis()
														taskStartCpu = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
														  threadMXBean.getCurrentThreadCpuTime
														} else 0L
														
														task.run(){ResultTask/ShuffleMapTask.runTask(
															// 在runTask()中进行反序列化,并记下开启/结束时间以求 execDeserializeTime
															val deserializeStartTime = System.currentTimeMillis()
															val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)])
															_executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
															
															func(context, rdd.iterator(partition, context))//进行RDD计算
														);}
													}
													
													val taskFinish = System.currentTimeMillis()
													task.metrics.setExecutorDeserializeTime((taskStart - deserializeStartTime) + task.executorDeserializeTime)
													task.metrics.setExecutorDeserializeCpuTime((taskStartCpu - deserializeStartCpuTime) + task.executorDeserializeCpuTime)
													
													// We need to subtract Task.run()'s deserialization time to avoid double-counting
													/* task-executorRunTime= task结束时间(task.run()结束时)- task开始时间() - task反序列时间
													*	taskFinish:  	task.run()结束后;
													* 	taskStart:		task.run()开始前打桩;
													* 	task.execDeseTime:	在上面task.run().runTask()中 进行反序列化求(rdd,func)所用耗时;
													*		总结: ExecutorRunTime,指Task中除[反序列化用时]外的 Task执行时间, 一般可理解为就是 RDD计算用时:func(context,rdd.iterator())
													*/
													task.metrics.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
													task.metrics.setExecutorCpuTime((taskFinishCpu - taskStartCpu) - task.executorDeserializeCpuTime)
													task.metrics.setJvmGCTime(computeTotalGcTime() - startGCTime)
													task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)
												}
												
												//"SparkListenerBus"线程, 监听SparkListenerTaskEnd事件, 将其中taskMetrics转换成 TaskMetricsUIData对象;
												SparkListenerBus.doPostEvent(){
													case taskEnd: SparkListenerTaskEnd => listener.onTaskEnd(taskEnd){//JobProgressListener.onTaskEnd(taskEnd: SparkListenerTaskEnd)
														
														val taskMetrics = Option(taskEnd.taskMetrics)// SparkListenerTaskEnd.TaskMetrics
														taskMetrics.foreach { m =>
															val oldMetrics = stageData.taskData.get(info.taskId).flatMap(_.metrics)
															updateAggregateMetrics(stageData, info.executorId, m, oldMetrics)
														}
														
														val taskData = stageData.taskData.getOrElseUpdate(info.taskId, TaskUIData(info))
														taskData.updateTaskInfo(info)
														taskData.updateTaskMetrics(taskMetrics){//UIData.TaskUIData.updateTaskMetrics(metrics: Option[TaskMetrics])
															_metrics = metrics.map(metrics => TaskMetricsUIData.fromTaskMetrics(metrics){//TaskMetricsUIData.fromTaskMetrics(m: TaskMetrics): TaskMetricsUIData
																
																TaskMetricsUIData(
																	executorDeserializeTime = m.executorDeserializeTime,
																	executorDeserializeCpuTime = m.executorDeserializeCpuTime,
																	executorRunTime = m.executorRunTime,
																	executorCpuTime = m.executorCpuTime,
																	resultSize = m.resultSize,
																	jvmGCTime = m.jvmGCTime,
																	resultSerializationTime = m.resultSerializationTime,
																	memoryBytesSpilled = m.memoryBytesSpilled,
																	diskBytesSpilled = m.diskBytesSpilled,
																	peakExecutionMemory = m.peakExecutionMemory,
																	inputMetrics = InputMetricsUIData(m.inputMetrics),
																	outputMetrics = OutputMetricsUIData(m.outputMetrics),
																	shuffleReadMetrics = ShuffleReadMetricsUIData(m.shuffleReadMetrics),
																	shuffleWriteMetrics = ShuffleWriteMetricsUIData(m.shuffleWriteMetrics))
															}
														}
													}
												}
												
											}
											return TaskMetricsUIData.executorRunTime
										}
									}
								}.getOrElse(1L)
								val formatDuration = taskData.taskDuration.map(d => UIUtils.formatDuration(d)).getOrElse("")
								
								//整个Task调度用时 = Driver端TaskSetManager提交和清理Task总用时 - Execut端(RDD计算 + 序列化和反序列化)用时 - Driver端GetResult用时
								val schedulerDelay = metrics.map(getSchedulerDelay(info, _, currentTime)).getOrElse(0L){
									StagePage.getSchedulerDelay(info: TaskInfo, metrics: TaskMetricsUIData, currentTime: Long): Long = {
										if (info.finished) {
											
											{// TaskInfo.finishTime & TaskInfo.launchTime 的赋值时间; 都在TaskSetManger类里,分别在 handleSuccessfulTask() 和resourceOffer()中赋值;
												//说明 TaskInfo.finishTime赋值线程: [task-result-getter-3], 在StatusUpdate事件处理中, 判断TaskState==FINISHED, 就将系统当前时间赋值 TaskInfo.finishTime
												schedulerBackend.receive(){
													case StatusUpdate(taskId, state, serializedData) => scheduler.statusUpdate(taskId, state, serializedData){
														TaskSchedulerImpl.statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer){
															taskIdToTaskSetManager.get(tid) match {
																if (TaskState.isFinished(state)) {
																	cleanupTaskState(tid)
																	taskSet.removeRunningTask(tid)
																	if (state == TaskState.FINISHED) {
																		taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData){//TaskResultGetter.enqueueSuccessfulTask()
																			// 开启运行 "task-result-getter-*"线程
																			getTaskResultExecutor.execute(new Runnable {
																				def run(): Unit = Utils.logUncaughtExceptions {
																					val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData)
																					result.accumUpdates = result.accumUpdates.map ()
																					scheduler.handleSuccessfulTask(taskSetManager, tid, result){//TaskSchedulerImpl.handleSuccessfulTask()
																						taskSetManager.handleSuccessfulTask(tid, taskResult){
																							val info = taskInfos(tid)
																							info.markFinished(TaskState.FINISHED, clock.getTimeMillis()){//TaskInfo.markFinished()
																								assert(time > 0)
																								finishTime = time //这个time就是上面clock[SystemClock].getTimeMillis()系统当前时间
																							}
																						}
																					}
																				}
																			})
																		}
																		
																	}else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
																		taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
																	}
																}
															}
														}
													}
												}
											
												// TaskInfo.launchTime生成: [dispatcher-event-loop-*]线程的TaskSetManager.resourceOffer()方法中,以系统时间作为TaskInfo.launchTime
												TaskSetManager.reviveOffers(){
													val offers = IndexedSeq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
													tasks:Seq[Seq[TaskDescription]]  = scheduler.resourceOffers(offers){
														for (taskSet <- sortedTaskSets) {
															for (currentMaxLocality <- taskSet.myLocalityLevels) {
																launchedTaskAtCurrentMaxLocality = resourceOfferSingleTaskSet(taskSet, currentMaxLocality, shuffledOffers, availableCpus, tasks){
																	for (i <- 0 until shuffledOffers.size) {
																		taskSet.resourceOffer(execId, host, maxLocality){//TaskSetManager.resourceOffer()
																			val curTime = clock.getTimeMillis()
																			
																			dequeueTask(execId, host, allowedLocality).map {
																				case ((index, taskLocality, speculative)) =>{
																					val task = tasks(index)
																					val taskId = sched.newTaskId()
																					copiesRunning(index) += 1
																					val attemptNum = taskAttempts(index).size
																					// 这里的curTime参数 即为TaskInfo.launchTime
																					val info = new TaskInfo(taskId, index, attemptNum, curTime, execId, host, taskLocality, speculative)
																					
																					taskInfos(taskId) = info
																				}
																			}
																		}
																		
																		for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
																			
																		}
																	}
																}
															}
														}
													}
													
													for (task <- tasks.flatten) {
														
													}
												}
											}
											// totalExecutionTime:整个执行时间: TaskResultGetter检测到taskState变为Finished的时间 - TaskSetManager提交Task任务的时间
											val totalExecutionTime = info.finishTime - info.launchTime //由TaskSetManager类在Driver进程中两个提交前后方法的时间差求出;
											
											// 将executor的序列化和反序列化时间成为 Executor的Overhead(Executor运行用耗时), 
											val executorOverhead = metrics.executorDeserializeTime + metrics.resultSerializationTime
											
											/* 整个Task的调度耗时:schedulerDelay = Driver端TaskSetManager提交和清理Task总用时 - 减去
											*		- RDD计算用时
											*		- Executor 用于序列化和反序列化的 Overhead用时
											*		- Driver端获取Result结果用时: 	一般为0;
											*/
											math.max(0,totalExecutionTime - metrics.executorRunTime - executorOverhead - getGettingResultTime(info, currentTime))
										} else {
											0L
										}
									}
								}
								
								val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
								val taskDeserializationTime = metrics.map(_.executorDeserializeTime).getOrElse(0L)
								val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)
								val gettingResultTime = getGettingResultTime(info, currentTime)
								val totalShuffleBytes = maybeShuffleRead.map(_.totalBytesRead)
								val shuffleReadSortable = totalShuffleBytes.getOrElse(0L)
								val shuffleReadReadable = totalShuffleBytes.map(Utils.bytesToString).getOrElse("")
								val shuffleReadRecords = maybeShuffleRead.map(_.recordsRead.toString).getOrElse("")

								val remoteShuffleBytes = maybeShuffleRead.map(_.remoteBytesRead)
								val shuffleReadRemoteSortable = remoteShuffleBytes.getOrElse(0L)
								val shuffleReadRemoteReadable = remoteShuffleBytes.map(Utils.bytesToString).getOrElse("")

								val input =
								  if (hasInput) {
									Some(TaskTableRowInputData(inputSortable, s"$inputReadable / $inputRecords"))
								  } else {
									None
								  }

								val output =
								  if (hasOutput) {
									Some(TaskTableRowOutputData(outputSortable, s"$outputReadable / $outputRecords"))
								  } else {
									None
								  }

								val shuffleRead =
								  if (hasShuffleRead) {
									Some(TaskTableRowShuffleReadData(
									  shuffleReadBlockedTimeSortable,
									  shuffleReadBlockedTimeReadable,
									  shuffleReadSortable,
									  s"$shuffleReadReadable / $shuffleReadRecords",
									  shuffleReadRemoteSortable,
									  shuffleReadRemoteReadable
									))
								  } else {
									None
								  }

								val shuffleWrite =
								  if (hasShuffleWrite) {
									Some(TaskTableRowShuffleWriteData(
									  writeTimeSortable,
									  writeTimeReadable,
									  shuffleWriteSortable,
									  s"$shuffleWriteReadable / $shuffleWriteRecords"
									))
								  } else {
									None
								  }
								  
								 
								new TaskTableRowData(
								  info.index,
								  info.taskId,
								  info.attemptNumber,
								  info.speculative,
								  info.status,
								  info.taskLocality.toString,
								  s"${info.executorId} / ${info.host}",
								  info.launchTime,
								  duration,	// 指标1: duration=TaskMetrics.executorRunTime == func(context,rdd.iterator())用时== (taskFinish-taskStart) -task.executorDeserializeTime
								  formatDuration,
								  schedulerDelay,// 指标2: ?
								  taskDeserializationTime, 
								  gcTime,//指标4: 那段时间?
								  serializationTime,
								  gettingResultTime,
								  peakExecutionMemoryUsed,
								  if (hasAccumulators) Some(externalAccumulableReadable.mkString("<br/>")) else None,
								  input,
								  output,
								  shuffleRead,
								  shuffleWrite,
								  bytesSpilled,
								  taskData.errorMessage.getOrElse(""),
								  logs)
	  
							})
							.sorted(ordering(sortColumn, desc))
					}
				}
				
				(_taskTable, _taskTable.table(page))
			  } catch {
				  
			  }
			  
			  val taskIdsInPage = if (taskTable == null) Set.empty[Long] else taskTable.dataSource.slicedTaskIds
			  // Excludes tasks which failed and have incomplete metrics
			  val validTasks = tasks.filter(t => t.taskInfo.status == "SUCCESS" && t.metrics.isDefined)
			  
			  val executorTable = new ExecutorTable(stageId, stageAttemptId, parent)
			  val maybeAccumulableTable: Seq[Node] =
				if (hasAccumulators) { <h4>Accumulators</h4> ++ accumulableTable } else Seq()
					
			  UIUtils.headerSparkPage(stageHeader, content, parent, showVisualization = true)
			  
			}
			
		}
		
		
		ApplicationPage.render(request){
			//当standalone模式, 在/MasterUI路径下创建 /app -> ApplicationPage, / -> MasterPage
			{
				class MasterWebUI extends WebUI("MasterUI");
				initialize(){
					val masterPage = new MasterPage(this)
					attachPage(new ApplicationPage(this))
				}
			}
			
			
		}
		
		
	}
	
    val renderHandler = createServletHandler(pagePath,servletParams, securityManager, conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",(request: HttpServletRequest) => page.renderJson(request), securityManager, conf, basePath)
    
	attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler	
	
}














