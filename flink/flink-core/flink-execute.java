

// main入口  从 streaming_java包 -> core 

StreamExecutionEnvironment.execute(){
	return execute(getStreamGraph(jobName));{
		// 分本地执行环境和 远程执行环境
		LocalStreamEnvironment.execute(){
			return super.execute(streamGraph);{//StreamExecutionEnvironment.execute()
				final JobClient jobClient = executeAsync(streamGraph);{
					// 从CL中加载解析所有的 PipelineExecutorFactory 实现类,只有flink-clients中LocalExecutorFactory, RemoteExecutorFactory 2个类
					final PipelineExecutorFactory executorFactory = executorServiceLoader.getExecutorFactory(configuration);{//core.DefaultExecutorServiceLoader.
						final ServiceLoader<PipelineExecutorFactory> loader = ServiceLoader.load(PipelineExecutorFactory.class);
						while (loader.iterator().hasNext()) {
							compatibleFactories.add(factories.next());
						}
						if (compatibleFactories.size() > 1) { 
							throw new IllegalStateException("Multiple compatible client factories found for:\n" + configStr + ".");
						}
						if (compatibleFactories.isEmpty()) {
							throw new IllegalStateException("No ExecutorFactory found to execute the application.");
						}
						return compatibleFactories.get(0); // 只能定义1个 PipelineExecutorFactory, 否则报错; 
					}
					CompletableFuture<JobClient> jobClientFuture = executorFactory
                        .getExecutor(configuration)
                        .execute(streamGraph, configuration, userClassloader);{
							LocalExecutor.execute()
							
							AbstractJobClusterExecutor.execute();
							
							AbstractSessionClusterExecutor.execute();
							RemoteExecutor[extends AbstractSessionClusterExecutor].execute();
							
							EmbeddedExecutor.execute();
						}
						
					JobClient jobClient = jobClientFuture.get();
					jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
					return jobClient;
				}
				jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));
				return jobExecutionResult;
			}
		}
		
		RemoteStreamEnvironment.execute(){
			
		}
		
		StreamContextEnvironment.execute();
		
		StreamPlanEnvironment.execute();// ? strema sql ?
		
	}
}

