



/api/streamis/streamJobManager/job/execute


UJESJobException: errCode: 20300 ,desc: 
Cannot get applicationId from EngineConn ServiceInstance(linkis-cg-engineconn, bdnode111.hjq.com:42299). ,ip: 
bdnode102.hjq.com ,port: 9400 ,serviceKind: streamis-server


FlinkJobManager.launch(){
	val onceJob = buildOnceJob(job)
    onceJob.submit();
	createJobInfo(onceJob.getId, job.getSubmitUser);{//SimpleFlinkJobManager.createJobInfo
		val jobInfo = new FlinkJobInfo
		jobInfo.setId(id)
		fetchApplicationInfo(jobInfo){
			getOnceJob(jobInfo.getId, jobInfo.getUser).getOperator(EngineConnApplicationInfoOperator.OPERATOR_NAME) match {
				retryHandler.setRetryNum(JobLauncherConfiguration.FETCH_FLINK_APPLICATION_INFO_MAX_TIMES.getValue)
				
				val applicationInfo = retryHandler.retry(applicationInfoOperator(), "Fetch-Yarn-Application-Info");{
					OnceJobOperator.apply(){
						val builder = EngineConnOperateAction.newBuilder()
						val engineConnOperateAction = builder.build()
						val result = linkisManagerClient.executeEngineConnOperation(engineConnOperateAction)
						 resultToObject(result)
					}
				}
			}
		}
	}
}

Caused by: org.apache.linkis.ujes.client.exception.UJESJobException: errCode: 20300 ,desc: Cannot get applicationId from EngineConn ServiceInstance(linkis-cg-engineconn, bdnode111.hjq.com:42299). ,ip: bdnode102.hjq.com ,port: 9400 ,serviceKind: streamis-server
	at org.apache.linkis.computation.client.operator.impl.EngineConnApplicationInfoOperator$$anonfun$resultToObject$1.apply(EngineConnApplicationInfoOperator.scala:32) ~[linkis-computation-client-1.0.3.jar:1.0.3]
	at org.apache.linkis.computation.client.operator.impl.EngineConnApplicationInfoOperator$$anonfun$resultToObject$1.apply(EngineConnApplicationInfoOperator.scala:32) ~[linkis-computation-client-1.0.3.jar:1.0.3]
	at scala.Option.getOrElse(Option.scala:121) ~[scala-library-2.11.8.jar:?]
	at org.apache.linkis.computation.client.operator.impl.EngineConnApplicationInfoOperator.resultToObject(EngineConnApplicationInfoOperator.scala:32) ~[linkis-computation-client-1.0.3.jar:1.0.3]
	at org.apache.linkis.computation.client.operator.impl.EngineConnApplicationInfoOperator.resultToObject(EngineConnApplicationInfoOperator.scala:25) ~[linkis-computation-client-1.0.3.jar:1.0.3]
	at org.apache.linkis.computation.client.operator.OnceJobOperator$class.apply(OnceJobOperator.scala:67) ~[linkis-computation-client-1.0.3.jar:1.0.3]
	at org.apache.linkis.computation.client.operator.impl.EngineConnApplicationInfoOperator.apply(EngineConnApplicationInfoOperator.scala:25) ~[linkis-computation-client-1.0.3.jar:1.0.3]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.SimpleFlinkJobManager$$anonfun$1.apply(SimpleFlinkJobManager.scala:70) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.SimpleFlinkJobManager$$anonfun$1.apply(SimpleFlinkJobManager.scala:70) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at org.apache.linkis.common.utils.Utils$.tryCatch(Utils.scala:40) ~[linkis-common-1.0.3.jar:1.0.3]
	at org.apache.linkis.common.utils.RetryHandler$class.retry(RetryHandler.scala:55) ~[linkis-common-1.0.3.jar:1.0.3]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.SimpleFlinkJobManager$$anon$1.retry(SimpleFlinkJobManager.scala:65) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.SimpleFlinkJobManager.fetchApplicationInfo(SimpleFlinkJobManager.scala:70) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.SimpleFlinkJobManager.createJobInfo(SimpleFlinkJobManager.scala:57) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.FlinkJobManager$$anonfun$1.apply(FlinkJobManager.scala:53) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.FlinkJobManager$$anonfun$1.apply(FlinkJobManager.scala:53) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at org.apache.linkis.common.utils.Utils$.tryCatch(Utils.scala:40) ~[linkis-common-1.0.3.jar:1.0.3]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.FlinkJobManager$class.launch(FlinkJobManager.scala:53) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.launcher.linkis.manager.SimpleFlinkJobManager.launch(SimpleFlinkJobManager.scala:33) ~[streamis-job-launcher-linkis-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.manager.service.TaskService.executeJob(TaskService.scala:78) ~[streamis-job-manager-service-0.1.0.jar:?]
	at com.webank.wedatasphere.streamis.jobmanager.manager.service.TaskService$$FastClassBySpringCGLIB$$d96f76ca.invoke(<generated>) ~[streamis-job-manager-service-0.1.0.jar:?]
	at org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:218) ~[spring-core-5.2.12.RELEASE.jar:5.2.12.RELEASE]
	at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:771) ~[spring-aop-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163) ~[spring-aop-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.proceed(CglibAopProxy.java:749) ~[spring-aop-5.2.8.RELEASE.jar:5.2.8.RELEASE]
	... 99 more
