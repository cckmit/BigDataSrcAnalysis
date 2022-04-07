

// 1.2. 调用 interpreter.sh -d flink -g flink 执行 flink Interpreter


 INFO [2022-02-19 23:01:30,793] ({SchedulerFactory4} FlinkInterpreterLauncher.java[buildEnvFromProperties]:85) - Choose FLINK_APP_JAR for non k8s-application mode: /opt/zeppelin/zeppelin-release/interpreter/flink/zeppelin-flink-0.11.0-SNAPSHOT-2.11.jar
 INFO [2022-02-19 23:01:30,801] ({SchedulerFactory4} ProcessLauncher.java[transition]:109) - Process state is transitioned to LAUNCHED
 INFO [2022-02-19 23:01:30,803] ({SchedulerFactory4} ProcessLauncher.java[launch]:96) - Process is launched: 
 
 [/opt/zeppelin/zeppelin-release/bin/interpreter.sh, 
 -d, /opt/zeppelin/zeppelin-release/interpreter/flink, 
 -c, 192.168.51.102, -p, 39750, -r, :, -i, flink-shared_process, -l, 
 /opt/zeppelin/zeppelin-release/local-repo/flink, -g, flink]
 
 
 
 // 1.3. interpreter.sh 会拼接执行启动 java -cp interpreter.remote.RemoteInterpreterServer flink-shared_process

 INFO [2022-02-19 23:01:31,737] ({Exec Stream Pumper} ProcessLauncher.java[processLine]:189) - 
 
 [INFO] Interpreter launch command: 
	/usr/java/jdk-release/bin/java 
	-Dfile.encoding=UTF-8 -Dlog4j.configuration=file:///opt/zeppelin/zeppelin-release/conf/log4j.properties 
	-Dlog4j.configurationFile=file:///opt/zeppelin/zeppelin-release/conf/log4j2.properties 
	-Dzeppelin.log.file=/opt/zeppelin/zeppelin-release/logs/zeppelin-interpreter-flink-shared_process-bigdata-bdnode102.log 
	-Xmx512m 
	-cp :/opt/zeppelin/zeppelin-release/local-repo/flink/*:/opt/flink/flink-release/lib/mysql-connector-java-5.1.49.jar:/opt/flink/flink-release/lib/log4j-slf4j-impl-2.17.1.jar:/opt/flink/flink-release/lib/log4j-core-2.17.1.jar:/opt/flink/flink-release/lib/log4j-api-2.17.1.jar:/opt/flink/flink-release/lib/log4j-1.2-api-2.17.1.jar:/opt/flink/flink-release/lib/kafka-clients-2.4.1.jar:/opt/flink/flink-release/lib/hudi-hadoop-mr-bundle-0.11.0-SNAPSHOT.jar:/opt/flink/flink-release/lib/hudi-flink-bundle_2.11-0.11.0-SNAPSHOT.jar:/opt/flink/flink-release/lib/hive-exec-2.3.1.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-shuffle-2.7.2.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-jobclient-2.7.2-tests.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-jobclient-2.7.2.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-hs-plugins-2.7.2.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-hs-2.7.2.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-core-2.7.2.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-common-2.7.2.jar:/opt/flink/flink-release/lib/hadoop-mapreduce-client-app-2.7.2.jar:/opt/flink/flink-release/lib/flink-table_2.11-1.14.3.jar:/opt/flink/flink-release/lib/flink-sql-connector-kafka_2.11-1.14.3.jar:/opt/flink/flink-release/lib/flink-sql-connector-hive-2.3.6_2.11-1.14.3.jar:/opt/flink/flink-release/lib/flink-shaded-zookeeper-3.4.14.jar:/opt/flink/flink-release/lib/flink-json-1.14.3.jar:/opt/flink/flink-release/lib/flink-dist_2.11-1.14.3.jar:/opt/flink/flink-release/lib/flink-csv-1.14.3.jar:/opt/flink/flink-release/lib/flink-connector-kafka_2.11-1.14.3.jar:/opt/flink/flink-release/lib/flink-connector-iotdb-bundle_2.11-1.14.3-iotdb-SNAPSHOT.jar:/opt/flink/flink-release/lib/flink-connector-hive_2.11-1.14.3.jar:/opt/flink/flink-release/lib/antlr-runtime-3.5.2.jar:::/opt/zeppelin/zeppelin-release/interpreter/zeppelin-interpreter-shaded-0.11.0-SNAPSHOT.jar:/opt/flink/flink-release/opt/flink-python_2.11-1.14.3.jar:/opt/zeppelin/zeppelin-release/interpreter/flink/zeppelin-flink-0.11.0-SNAPSHOT-2.11.jar:/opt/hadoop/etc/hadoop:/opt/hadoop/etc/hadoop:/opt/hadoop/hadoop-2.7.2/share/hadoop/common/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/common/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/yarn/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/*:/opt/hadoop/etc/hadoop:/opt/hadoop/hadoop-2.7.2/share/hadoop/common/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/common/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/hdfs/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/yarn/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/yarn/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/lib/*:/opt/hadoop/hadoop-2.7.2/share/hadoop/mapreduce/*:/opt/hadoop/hadoop-release/contrib/capacity-scheduler/*.jar:/opt/hadoop/hadoop-release/contrib/capacity-scheduler/*.jar 
	org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer 
	192.168.51.102 39750 flink-shared_process :
 

 INFO [2022-02-19 23:01:33,664] ({SchedulerFactory4} RemoteInterpreter.java[lambda$open$0]:134) - Open RemoteInterpreter org.apache.zeppelin.flink.IPyFlinkInterpreter
 INFO [2022-02-19 23:01:33,664] ({SchedulerFactory4} RemoteInterpreter.java[pushAngularObjectRegistryToRemote]:393) - Push local angular object registry from ZeppelinServer to remote interpreter group flink-shared_process
 
 
 
 
 // 2.  RemoteInterpreterServer 进程流程 
	YarnClusterDescriptor 原理链接Yarn并 创建并提交 flink yarn 任务;
	
	
