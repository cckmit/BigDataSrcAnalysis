
// Dataset.toLocalIterator() -> SparkPlan.getByteArrayRdd().execute() -> CollectLimitExec.doExecute() -> ShuffleExchangeExec.prepareShuffleDependency() -> new ShuffledRowRDD();



// new ShuffleDependency() -> _rdd.partitions() -> checkpointRDD.map(_.partitions) -> HadoopRDD.getPartitions() -> FileInputFormat.getSplits()
new ShuffleDependency(){
	val shuffleId: Int = _rdd.context.newShuffleId()
	partitions: Array[Partition] = _rdd.partitions();{
		checkpointRDD.map(_.partitions).getOrElse ();{
			getPartitions();{ // 由具体的 rdd实现类 调用 getPartitions()
				HadoopRDD.getPartitions(){}// FileInputFormat.getSplits()
			}
		}
	}
	val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(shuffleId, partitions.length, this)
	_rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
}

















