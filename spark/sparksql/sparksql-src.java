
// EngineExecutor.execute()-> executeLine() -> SparkEngineExecutor.executeLine() -> SparkSqlExecutor.execute()-> sqlMethod.invoke().SQLSession.showDF()
// SparkEngineExecutor.executeLine() -> SparkSqlExecutor.execute()-> sqlMethod.invoke().SQLSession.showDF() -> Dataset.toLocalIterator() -> SparkPlan.getByteArrayRdd().execute();


// Dataset.toLocalIterator() -> SparkPlan.getByteArrayRdd().execute() -> CollectLimitExec.doExecute() -> ShuffleExchangeExec.prepareShuffleDependency() -> new ShuffledRowRDD();

spark.sql.Dataset.toLocalIterator(){
	val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
	plan
		.executeToIterator(){//SparkPlan.executeToIterator()
			getByteArrayRdd(){//SparkPlan.getByteArrayRdd()
				execute(){//SparkPlan.execute
					doExecute();{
						CollectLimitExec.doExecute();{//extends UnaryExecNode (extends SparkPlan)
							val locallyLimited = child.execute().mapPartitionsInternal(_.take(limit))
							val dependency: ShuffleDependency = ShuffleExchangeExec.prepareShuffleDependency();{
								val part: Partitioner = newPartitioning match {
									case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
									case HashPartitioning(_, n) => {}
									case RangePartitioning(sortingExpressions, numPartitions) => {}
									
								}
								val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {}
								val dependency = new ShuffleDependency(rddWithPartitionIds);{// spark.core.ShuffleDependency()
									RDD.partitions()//... 
									// _rdd.partitions() -> checkpointRDD.map(_.partitions) -> HadoopRDD.getPartitions() -> FileInputFormat.getSplits()
								}
							}
							val shuffled = new ShuffledRowRDD();
							shuffled.mapPartitionsInternal(_.take(limit))
						}
					}
				}
					.mapPartitionsInternal ();
			}
				.map(_._2)
				.toLocalIterator.flatMap(decodeUnsafeRows)
		}
		.map(row => {
			objProj(row).get(0, null).asInstanceOf[T]
		})
	
}



val dependency = new ShuffleDependency(rddWithPartitionIds);
// new ShuffleDependency() -> _rdd.partitions() -> checkpointRDD.map(_.partitions) -> HadoopRDD.getPartitions() -> FileInputFormat.getSplits()
















