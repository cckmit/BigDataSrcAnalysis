




ApplicationMaster.main(){
	
	SparkHadoopUtil.get.runAsSparkUser { () =>
		master = new ApplicationMaster(amArgs, new YarnRMClient);{//ApplicationMaster的构造函数;
			
			// 计算AM中最多运行的Executor失败次数;
			private val maxNumExecutorFailures = {
				// 先算有效数量, 若开启动态扩容, 已dynAllocation.maxExecutors为上限, 否则已executor.instances为准;
				val effectiveNumExecutors = if (Utils.isDynamicAllocationEnabled(sparkConf)) {
					sparkConf.get(DYN_ALLOCATION_MAX_EXECUTORS)
				  } else {
					sparkConf.get(EXECUTOR_INSTANCES).getOrElse(0)
				  }
				
				val defaultMaxNumExecutorFailures = math.max(3,
				  // 一般defaultFailures数为有效数的2倍;  若有效数量> 10.5亿,则设默认失败数可等于 无穷大(Int最大值)
				  if (effectiveNumExecutors > Int.MaxValue / 2) Int.MaxValue else (2 * effectiveNumExecutors))
				// 取 executor.failures参数为 失败次数, 若不存在 一般以有效executorNum的2倍为最大失败数据; 即worker*2 or dynAllocation.maxExecutors*2
				sparkConf.get(MAX_EXECUTOR_FAILURES).getOrElse(defaultMaxNumExecutorFailures)
			}

		}
		
		System.exit(master.run())
	}
}








































