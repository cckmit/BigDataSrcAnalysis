


/usr/java/jdk-release/bin/java 
-cp /opt/spark/conf/:/opt/hadoop/data/conf/:/opt/dss_linkis/linkis_dss_release/linkis/linkis-ujes-spark-enginemanager/conf/:/opt/dss_linkis/linkis_dss_release/linkis/linkis-ujes-spark-enginemanager/lib/*:/opt/spark/spark-release/jars/* -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=45051 -Xmx1G -Dwds.linkis.configuration=linkis-engine.properties -Duser.timezone=Asia/Shanghai 
org.apache.spark.deploy.SparkSubmit --master yarn --deploy-mode client 
--conf spark.driver.memory=1G --conf spark.driver.cores=1 --conf spark.driver.extraClassPath=/opt/spark/conf:/opt/hadoop/data/conf:/opt/dss_linkis/linkis_dss_release/linkis/linkis-ujes-spark-enginemanager/conf:/opt/dss_linkis/linkis_dss_release/linkis/linkis-ujes-spark-enginemanager/lib/* --conf spark.driver.extraJavaOptions=-Dwds.linkis.configuration=linkis-engine.properties -Duser.timezone=Asia/Shanghai  --class com.webank.wedatasphere.linkis.engine.DataWorkCloudEngineApplication --name linkis --driver-cores 1 --executor-memory 3G --executor-cores 1 --num-executors 1 --queue default /opt/dss_linkis/dss_linkis-0.9.1/linkis/linkis-ujes-spark-enginemanager/lib/linkis-ujes-spark-engine-0.11.0.jar --dwc-conf _req_entrance_instance=sparkEntrance,192.168.51.111:9106 --dwc-conf wds.linkis.yarnqueue.memory.max=300G --dwc-conf wds.linkis.preheating.time=9:00 --dwc-conf wds.linkis.instance=1 --dwc-conf wds.linkis.tmpfile.clean.time=10:00 --dwc-conf ticketId=bd7001ae-baa1-4bc8-ab88-f9c761985d7c --dwc-conf dwc.application.instance=192.168.51.111:9105 --dwc-conf spark.driver.memory=1 --dwc-conf spark.executor.instances=1 --dwc-conf creator=IDE --dwc-conf spark.driver.cores=1 --dwc-conf wds.linkis.yarnqueue=default --dwc-conf wds.linkis.yarnqueue.cores.max=150 --dwc-conf spark.executor.memory=3 --dwc-conf spark.executor.cores=1 --dwc-conf wds.linkis.client.memory.max=30G --dwc-conf dwc.application.name=sparkEngineManager --dwc-conf user=bigdata --spring-conf eureka.client.serviceUrl.defaultZone=http://bdnode111.hjq.com:20303/eureka/ --spring-conf logging.config=classpath:log4j2-engine.xml --spring-conf spring.profiles.active=engine --spring-conf server.port=34977 --spring-conf spring.application.name=sparkEngine




// main 方法

SparkSubmit.main(){
	doSubmit();{
		submit();{
			
		}
	}
}

wait:-1, Object (java.lang)
wait:502, Object (java.lang)
main:79, DataWorkCloudEngineApplication$ (com.webank.wedatasphere.linkis.engine)
main:-1, DataWorkCloudEngineApplication (com.webank.wedatasphere.linkis.engine)
invoke0:-1, NativeMethodAccessorImpl (sun.reflect)
invoke:62, NativeMethodAccessorImpl (sun.reflect)
invoke:43, DelegatingMethodAccessorImpl (sun.reflect)
invoke:498, Method (java.lang.reflect)
start:52, JavaMainApplication (org.apache.spark.deploy)
org$apache$spark$deploy$SparkSubmit$$runMain:845, SparkSubmit (org.apache.spark.deploy)
doRunMain$1:161, SparkSubmit (org.apache.spark.deploy)
submit:184, SparkSubmit (org.apache.spark.deploy)
doSubmit:86, SparkSubmit (org.apache.spark.deploy)
doSubmit:920, SparkSubmit$$anon$2 (org.apache.spark.deploy)
main:929, SparkSubmit$ (org.apache.spark.deploy)
main:-1, SparkSubmit (org.apache.spark.deploy)




// 线程 : sparkEngineEngine-Thread-2


broadcast:1486, SparkContext (org.apache.spark)
<init>:89, HadoopTableReader (org.apache.spark.sql.hive)
org$apache$spark$sql$hive$execution$HiveTableScanExec$$hadoopReader$lzycompute:105, HiveTableScanExec (org.apache.spark.sql.hive.execution)
org$apache$spark$sql$hive$execution$HiveTableScanExec$$hadoopReader:105, HiveTableScanExec (org.apache.spark.sql.hive.execution)
apply:188, HiveTableScanExec$$anonfun$10 (org.apache.spark.sql.hive.execution)
apply:188, HiveTableScanExec$$anonfun$10 (org.apache.spark.sql.hive.execution)
withDummyCallSite:2470, Utils$ (org.apache.spark.util)
doExecute:187, HiveTableScanExec (org.apache.spark.sql.hive.execution)
apply:131, SparkPlan$$anonfun$execute$1 (org.apache.spark.sql.execution)
apply:127, SparkPlan$$anonfun$execute$1 (org.apache.spark.sql.execution)
apply:155, SparkPlan$$anonfun$executeQuery$1 (org.apache.spark.sql.execution)
withScope:151, RDDOperationScope$ (org.apache.spark.rdd)
executeQuery:152, SparkPlan (org.apache.spark.sql.execution)
execute:127, SparkPlan (org.apache.spark.sql.execution)
doExecute:41, CollectLimitExec (org.apache.spark.sql.execution)
apply:131, SparkPlan$$anonfun$execute$1 (org.apache.spark.sql.execution)
apply:127, SparkPlan$$anonfun$execute$1 (org.apache.spark.sql.execution)
apply:155, SparkPlan$$anonfun$executeQuery$1 (org.apache.spark.sql.execution)
withScope:151, RDDOperationScope$ (org.apache.spark.rdd)
executeQuery:152, SparkPlan (org.apache.spark.sql.execution)
execute:127, SparkPlan (org.apache.spark.sql.execution)
getByteArrayRdd:247, SparkPlan (org.apache.spark.sql.execution)
executeToIterator:318, SparkPlan (org.apache.spark.sql.execution)
apply:2822, Dataset$$anonfun$toLocalIterator$1 (org.apache.spark.sql)
apply:2817, Dataset$$anonfun$toLocalIterator$1 (org.apache.spark.sql)
apply:3370, Dataset$$anonfun$52 (org.apache.spark.sql)
apply:80, SQLExecution$$anonfun$withNewExecutionId$1 (org.apache.spark.sql.execution)
withSQLConfPropagated:127, SQLExecution$ (org.apache.spark.sql.execution)
withNewExecutionId:75, SQLExecution$ (org.apache.spark.sql.execution)
withAction:3369, Dataset (org.apache.spark.sql)
toLocalIterator:2817, Dataset (org.apache.spark.sql)
apply:229, SQLSession$$anonfun$3 (com.webank.wedatasphere.linkis.engine.executors)
apply:229, SQLSession$$anonfun$3 (com.webank.wedatasphere.linkis.engine.executors)
tryCatch:48, Utils$ (com.webank.wedatasphere.linkis.common.utils)
tryThrow:58, Utils$ (com.webank.wedatasphere.linkis.common.utils)
showDF:229, SQLSession$ (com.webank.wedatasphere.linkis.engine.executors)
execute:61, SparkSqlExecutor (com.webank.wedatasphere.linkis.engine.executors)
apply:138, SparkEngineExecutor$$anonfun$executeLine$2$$anonfun$2$$anonfun$apply$10 (com.webank.wedatasphere.linkis.engine.executors)
apply:138, SparkEngineExecutor$$anonfun$executeLine$2$$anonfun$2$$anonfun$apply$10 (com.webank.wedatasphere.linkis.engine.executors)
map:146, Option (scala)
apply:138, SparkEngineExecutor$$anonfun$executeLine$2$$anonfun$2 (com.webank.wedatasphere.linkis.engine.executors)
apply:138, SparkEngineExecutor$$anonfun$executeLine$2$$anonfun$2 (com.webank.wedatasphere.linkis.engine.executors)
tryFinally:62, Utils$ (com.webank.wedatasphere.linkis.common.utils)
apply:138, SparkEngineExecutor$$anonfun$executeLine$2 (com.webank.wedatasphere.linkis.engine.executors)
apply:108, SparkEngineExecutor$$anonfun$executeLine$2 (com.webank.wedatasphere.linkis.engine.executors)
tryFinally:62, Utils$ (com.webank.wedatasphere.linkis.common.utils)
executeLine:146, SparkEngineExecutor (com.webank.wedatasphere.linkis.engine.executors)
apply:141, EngineExecutor$$anonfun$execute$1$$anonfun$apply$10$$anonfun$apply$11 (com.webank.wedatasphere.linkis.engine.execute)
apply:140, EngineExecutor$$anonfun$execute$1$$anonfun$apply$10$$anonfun$apply$11 (com.webank.wedatasphere.linkis.engine.execute)
tryCatch:48, Utils$ (com.webank.wedatasphere.linkis.common.utils)
apply:141, EngineExecutor$$anonfun$execute$1$$anonfun$apply$10 (com.webank.wedatasphere.linkis.engine.execute)
apply:136, EngineExecutor$$anonfun$execute$1$$anonfun$apply$10 (com.webank.wedatasphere.linkis.engine.execute)
foreach:160, Range (scala.collection.immutable)
apply:136, EngineExecutor$$anonfun$execute$1 (com.webank.wedatasphere.linkis.engine.execute)
apply:116, EngineExecutor$$anonfun$execute$1 (com.webank.wedatasphere.linkis.engine.execute)
tryFinally:62, Utils$ (com.webank.wedatasphere.linkis.common.utils)
ensureIdle:60, AbstractExecutor (com.webank.wedatasphere.linkis.scheduler.executer)
ensureIdle:54, AbstractExecutor (com.webank.wedatasphere.linkis.scheduler.executer)
ensureOp$1:115, EngineExecutor (com.webank.wedatasphere.linkis.engine.execute)
execute:116, EngineExecutor (com.webank.wedatasphere.linkis.engine.execute)
apply:254, Job$$anonfun$3 (com.webank.wedatasphere.linkis.scheduler.queue)
apply:254, Job$$anonfun$3 (com.webank.wedatasphere.linkis.scheduler.queue)
tryCatch:48, Utils$ (com.webank.wedatasphere.linkis.common.utils)
run:254, Job (com.webank.wedatasphere.linkis.scheduler.queue)
call:511, Executors$RunnableAdapter (java.util.concurrent)
run:266, FutureTask (java.util.concurrent)
runWorker:1149, ThreadPoolExecutor (java.util.concurrent)
run:624, ThreadPoolExecutor$Worker (java.util.concurrent)
run:748, Thread (java.lang)






