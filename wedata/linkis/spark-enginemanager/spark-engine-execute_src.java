

// EngineExecutor.execute()-> executeLine() -> SparkEngineExecutor.executeLine() -> SparkSqlExecutor.execute()-> sqlMethod.invoke().SQLSession.showDF()
EngineExecutor.execute(executeRequest: ExecuteRequest){
	executedNum += 1
	ensureOp(){
		val engineExecutorContext = createEngineExecutorContext(executeRequest)
		val codes = Utils.tryCatch(codeParser.map(_.parse(hookedCode, engineExecutorContext)).getOrElse(Array(hookedCode)))
		codes.indices.foreach { index =>
			val code = codes(index)
			response = if(incomplete.nonEmpty) 
				executeCompletely(engineExecutorContext, code, incomplete.toString);{
					SparkEngineExecutor.executeCompletely(){//SparkEngineExecutor 重写了该方法
						val newcode = completedLine + code
						executeLine(engineExecutorContext, newcode);
					}
				}
			else 
				executeLine(engineExecutorContext, code);{
					SparkEngineExecutor.executeLine(){// Spark执行引擎 实现方法;
						// executeLine() ->SparkSqlExecutor.execute()-> sqlMethod.invoke().SQLSession.showDF() -> Dataset.toLocalIterator() -> SparkPlan.getByteArrayRdd().execute();
					}
					HiveEngineExecutor.executeLine(){// Hive执行引擎 实现方法;
						
					}
				}
			
		}
	}
}


// linkis-ujes-spark-enginemanager 包

SparkEngineExecutor.executeLine(){
	{// 成员变量
	val sc: SparkContext; 
	val sparkExecutors: Seq[SparkExecutor];
	}
	sc.setJobGroup(jobGroup, _code, true)
	val response = Utils.tryFinally(sparkExecutors
		.find(_.kind == kind)
		.map(_.execute(this, _code, engineExecutorContext, jobGroup)) {
			SparkScalaExecutor.execute();
			SparkSqlExecutor.execute(sparkEngineExecutor,code,engineExecutorContext,jobGroup);{
				engineExecutorContext.appendStdout(s"${EngineUtils.getName} >> $code")
				val sqlMethod = sqlContext.getClass.getMethod("sql", classOf[String])
				rdd = sqlMethod.invoke(sqlContext, code)
				
				Utils.tryQuietly(SparkSqlExtension.getSparkSqlExtensions().foreach(_.afterExecutingSQL(sqlContext,code,rdd.asInstanceOf[DataFrame],SQL_EXTENSION_TIMEOUT.getValue,sqlStartTime)))
				
				SQLSession.showDF(sc, jobGroup, rdd, null, SparkConfiguration.SHOW_DF_MAX_RES.getValue,engineExecutorContext);{//SQLSession.showDF()
					val dataFrame = df.asInstanceOf[DataFrame]
					val iterator =  dataFrame.toLocalIterator();{// spark.sql.Dataset.toLocalIterator() 调用sc.submitJob()执行sql;源码如下; 
						// Dataset.toLocalIterator() -> SparkPlan.getByteArrayRdd().execute() -> CollectLimitExec.doExecute() -> ShuffleExchangeExec.prepareShuffleDependency() -> new ShuffledRowRDD();
						SparkPlan.getByteArrayRdd().execute();
					}
				}
				SuccessExecuteResponse()
			}
			SparkPythonExecutor.execute();
		}
		.getOrElse(throw new NoSupportEngineException(40008, s"Not supported $kind executor!")));
}


val iterator =  dataFrame.toLocalIterator();
// new ShuffleDependency() -> _rdd.partitions() -> checkpointRDD.map(_.partitions) -> HadoopRDD.getPartitions() -> FileInputFormat.getSplits()


