
Dataset.toLocalIterator(){
	val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
	plan
		.executeToIterator(){//SparkPlan.executeToIterator()
			getByteArrayRdd(){//SparkPlan.getByteArrayRdd()
				execute(){//SparkPlan.gexecute
					doExecute();{
						CollectLimitExec.doExecute();{//extends UnaryExecNode (extends SparkPlan)
							val locallyLimited = child.execute().mapPartitionsInternal(_.take(limit))
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
