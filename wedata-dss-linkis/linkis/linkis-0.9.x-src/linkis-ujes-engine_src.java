
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


